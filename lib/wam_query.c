/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* wam query runtime library
 */

#include <sys/stat.h>  // O_DIRECT
#include <stdlib.h>
#define _WITH_GETLINE  // getline on FreeBSD
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <assert.h>
#include <errno.h>
#include <time.h> // strftime, time()
#include <signal.h> // reset inherited SIGFPE handler
#include <stdarg.h>

#include <math.h> // log10, isfinite

#include <sys/resource.h> // rusage

#include <inttypes.h>  // portable printf format for ints

#ifdef __FreeBSD__
#include <sys/types.h>  // rprio
#include <sys/rtprio.h>
#include <sys/resource.h> // setpriority
#include <sys/endian.h>  // htobe64, be64toh
#endif

#if defined(__APPLE__)
#include <machine/endian.h>
#define htobe64 htonll
#define be64toh ntohll
#endif

// Mac OS X and others without O_DIRECT
#ifndef O_DIRECT
#define O_DIRECT 0
#endif

/* TODO: optimize out (we need only LLONG_MAX, LLONG_MIN, DBL_MAX from
   these headers */
#include <limits.h>
#include <float.h>

#include "wam.h"
#include "wam_random.h"
#include "wam_groupby.h"
#include "wam_strtbl.h"
#include "wam_smptbl.h"
#include "wam_ucount.h"
#include "wam_weighted_ucount.h"
#include "wam_hist.h"
#include "wam_ntiles.h"
#include "wam_orderby.h"
#include "wam_bins.h"
#include "wam_pqueue.h"


#define MIN(a, b) (((a) < (b)) ? (a): (b))
#define MAX(a, b) (((a) > (b)) ? (a): (b))

/* round up (x / size), i.e. skip padding after fitting x mod size elements */
#define ALIGN(x, size) (((x) + (size) - 1) / (size))

#define ARRAY_SIZE(a) (sizeof(a)/sizeof(a[0]))

/* constants used in pre-aggregated results header (descriptor) */
#define TUPLE_ITEM_CODE(code) ((code) & 0xff000000)

#define TUPLE_ITEM_FIELD64  0x00000000
#define TUPLE_ITEM_COUNT    0x10000000
#define TUPLE_ITEM_UCOUNT   0x20000000
/* special counts such as event count and sample count */
#define TUPLE_ITEM_COUNT_SPECIAL   0x30000000
#define COUNT_SPECIAL_EVENT   0
#define COUNT_SPECIAL_SAMPLE  1
#define COUNT_SPECIAL_STORAGE_SAMPLE  2


static int is_stream_mode = 0;

/* the number result records (to be set by query_postprocess()) */
static int result_count = 0;
/* the number of output result records -- controlled by the query's LIMIT
 * parameter; when there's no LIMIT specified, result_count == output_count;
 * when present, output_count = MIN(result_count, limit) */
static int output_count = 0;
static int matching_count = 0;  // the number of input rows for which WHERE is true

static inline tuple_t *find_or_alloc_group_tuple(tuple_t *tuple);
static tuple_t *get_group_tuple(void);
static tuple_t *get_sample_tuple(void);
static void load_bytes(void *p, int size, FILE *f);
static void skip_bytes(int size, FILE *f);
static void store_bytes(void *p, int size, FILE *f);
static void store_tuple(tuple_t *t, FILE *f);
static void load_tuple(tuple_t *t, FILE *f);
static void skip_tuple(FILE *f);
static void foreach_tuple(void (*f)(tuple_t *), int visit_count);

static void store_subquery_header(FILE *f, int merge_generation);
static void store_subquery_tuple_descriptor(FILE *f);
static void store_tuple_header(FILE *f);

static void load_subquery_header(wam_input_t *input);
static void load_subquery_tuple_descriptor(wam_input_t *input);
static int load_tuple_header(wam_input_t *input);

static void load_subquery_tuple_items(tuple_t *t, wam_input_t *input);
static void load_subquery_tuple_fields(wam_input_t *input);

static void skip_subquery_tuple_item(int code, wam_input_t *input);
static void skip_subquery_tuple_items(wam_input_t *input);

void MurmurHash3_x64_128(const void * key, const int len, const uint32_t seed, void *out);

static string_value_t *find_string_value(uint64_t value);
static uint64_t make_string_value(uint64_t value, int is_null, int is_value_used);


// boost hash combine
static
uint64_t hc(int num, uint64_t seed, ...) {
    va_list args;
    va_start(args, seed);
    int i = 0;
    for (i = 0; i < num - 1; i++) {
        uint64_t n = va_arg (args, uint64_t);
        seed ^= n + 0x9e3779b9 + (seed<<6) + (seed>>2);
    }
    va_end(args);
    return seed;
}

// djb2 hash
uint64_t
hash_cstr(const char *str)
{
    uint64_t hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }

    return hash;
}

#define MAX_TAGSET_LENGTH 4096
#define MAX_TAGSET_TAGS 100
static char tagset[MAX_TAGSET_LENGTH + 1];
static uint64_t hashes[MAX_TAGSET_TAGS];
static int hash_count = 0;

// This function is a hack to support exploding of one tagset during processing.
// Basically in stratified sampling if one dimension is tagset (the only example
// right now is ab_exposure), instead of generating one hash value representing
// this particular value, we need to instead generate a list of values, each
// representing a tag in the tagset.
//
//   - base is the combination of hash computed so far of the key structure.
//   - h is the hash (string) value of tagset as whole, this hash is generated
//     by Erlang at query compilation time.
//
// We need to:
//
//   - fetch string value from wam_strtbl
//   - split with strpos
//   - for each parts of string
//     - val = hash_cstr(part)
//     - hashes[hash_count++] = hc(2, base, val)
static void
hash_explode_tagset(uint64_t base, uint64_t value, const char* seps)
{
    //LOG("EXPLODE TAGSET: %" PRIx64 "\n", value);
    string_value_t *t = find_string_value(value);
    if (!t) {
      LOG("string not found for hash %" PRIx64 "\n", value);
    }
    int len = MIN(t->len, MAX_TAGSET_LENGTH);
    memcpy(tagset, t->str, len);
    tagset[len] = 0;
    // LOG("ab_exposure: %s\n", tagset);
    char* pos = strtok(tagset, seps);
    while (pos != NULL && hash_count < MAX_TAGSET_TAGS) {
        hashes[hash_count++] = hc(2, base, hash_cstr(pos));
        // LOG("k:%ld %s %ld\n", hashes[hash_count-1], pos, hash_cstr(pos));
        pos = strtok(NULL, seps);
    }
    // LOG("\n");
}

static inline
int64_t hash_value(int64_t value, int64_t salt)
{
    uint64_t out[2];
    MurmurHash3_x64_128(&value, sizeof(value), (uint32_t)salt, &out);

    return (int64_t)out[1];
}


/* individual field item representing field id and value */
struct field_item {
    uint32_t id;
    uint64_t value;
};

struct field_item_buf {
    struct field_item *buf;
    struct field_item *cur;
};

#define FOREACH_FIELD_ITEM(x, field_item_buf) \
for (x = field_item_buf.buf; x < field_item_buf.cur; x++)

static struct field_item_buf long_field_buf;
static struct field_item_buf long_attr_buf;

static inline void reset_long_field_items(void);
static inline void reset_star_projected_field_items(void);

static void process_long_selected_field(uint32_t code, uint64_t value, int is_field);
static void process_star_projected_field(uint32_t code, uint64_t value, int is_field);

struct wam_star;
typedef struct wam_star wam_star_t;
static void wam_star_reset(wam_star_t **star);
static void wam_star_append_field(wam_star_t *star, uint32_t field_id, uint64_t field_value);
static void wam_star_project_local_fields(wam_star_t *star);
static void wam_star_project_global_fields(wam_star_t *star, int event_id);
static void wam_star_load(wam_star_t *star, FILE *f);
static void wam_star_store(wam_star_t *star, FILE *f);
static void print_star(wam_star_t *star);
static void print_star_field_value_by_id(wam_star_t *star, int field_id);
static void star_value_buf_reset(void);

/* needed for dump-all queries */
struct global_field_value {
    uint64_t buffer_count;
    uint64_t event_count;  /* this is needed so that local fields can override global fields */
    uint64_t value;
};

/* generated code */
static inline void project_query_tuple(tuple_t *t);

/* intermediate tuple array -- set by the query and used by foreach_tuple() if
 * not NULL */
static tuple_t **tuple_array = NULL;

/* query output mode */
static enum {
    OUTPUT_QUERY_FINAL,     /* final results formatted as JSON */
    OUTPUT_QUERY_PARTIAL,   /* partial query results serialized in a binary format */

    OUTPUT_SUBQUERY,        /* subquery / precomputed results (materialized view) */
    OUTPUT_SUBQUERY_MERGE,  /* merge/combine subquery / precomputed results (materialized view) */
} output_mode = OUTPUT_QUERY_FINAL;

static FILE *output_file = NULL;
static int args_nice = 0;  /* whether --nice parameter was specified */
static int args_direct_io = 0;  /* whether --direct_io parameter was specified */
static int args_debug = 0; /* whether --debug parameter was specified */
static int args_delete_merge_input = 0; /* whether --delete-merge-input parameter was specified */

static int args_use_mpack_output_format = 0;
static int args_use_json_output_format = 0;  /* default */

/* produce separate output for each hour (when in OUTPUT_SUBQUERY mode) */
static int split_by_hour = 0;
/* prefix and suffix for the output filename */
static char *output_prefix = NULL;
static char *output_suffix = "";


/* 12-byte input record */
typedef
struct record {
    uint32_t id;
    uint32_t u32;
    uint32_t u32_upper;
} record_t;


/* whether to store the next string value */
int store_next_string_value = 0;
/* the known hash of the next sting value we are about to store */
uint32_t next_string_value_hash;


/* only in stream mode: sequentially allocated string id used for
 * referencing used string field values in strtbl */
static int stream_string_id = 0;

/* only in stream mode: called at the beginning of each buffer */
static void reset_string_values(void)
{
    wam_strtbl_reset();
    stream_string_id = 0;
}


static
uint64_t make_string_value(uint64_t value, int is_null, int is_value_used)
{
    /* NOTE: if the value is not being used, the actual value still matters,
     * because it can be used in group-by and stratified sampling keys
     *
     * currently, we use lower 32-bit which is erlang:phash2() hash as a string
     * value representation, the upper 32 bits could be used for something else
     */
    uint64_t hash = value & 0xffffffff;
    /* whether we should store the actual value -- no need to store NULL
     * values, as they are not supposed to be used */
    int store_value = is_value_used && !is_null;

    if (!is_stream_mode) {
        /* store the value unless already stored */
        store_value = store_value && !wam_strtbl_find(hash);
        value = hash;
    } else {
        /* in streaming mode we are storing every value one by one without
         * dedup and assigning each value a sequential id -- this way it is
         * guaranteed to be unique within the buffer */
        if (store_value) {
            /* sometimes string field can be both wide-selected and then
             * star-projected, e.g. ab_exposure -- in this case we want to
             * detect the second invocation and make it idempotent */
            uint64_t next_stream_string_id;
            if (store_next_string_value) {
                next_stream_string_id = stream_string_id - 1;
                assert(next_stream_string_id == next_string_value_hash);
            } else {
                next_stream_string_id = stream_string_id++;
            }
            value = (next_stream_string_id << 32) | hash;
            hash = next_stream_string_id;
        } else {
            value = hash;
            hash = 0;
        }
    }

    if (store_value) {
        /* signal the engine to retrieve the value and store it with the given hash */
        store_next_string_value = 1;
        next_string_value_hash = hash;
    }

    return value;
}


static
string_value_t *find_string_value(uint64_t value)
{
    uint64_t hash;

    if (is_stream_mode) {
        /* in stream mode we store each string value sequentially and maintain
         * unique sequential id in the upper 32 bits of the value */
        hash = value >> 32;
    } else {
        hash = value & 0xffffffff;
    }

    return wam_strtbl_find(hash);
}


#define RECORD_SIZE sizeof(struct record)


/* input buffer holding RECORD_BUF_SIZE input records */
#define RECORD_BUF_SIZE 4096
static
record_t record_buf[RECORD_BUF_SIZE];


static uint64_t buffer_count = 1;  /* initializing as 1 so comparison with 0 evaluates to false */
static uint64_t event_count = 1;
static uint64_t is_inside_event = 0;

/* the "event" field holder */
static int event = 0;


/* projection result printers
 *
 * TODO: move all this stuff to library once we migrate to wam_cat */
#include "wam_output.c"

static
void print_null(void)
{
    wam_output_null();
}


static
void print_int(int64_t x)
{
    wam_output_int64(x);
}


static
void print_float(double x)
{
    wam_output_double(x);
}


static
void print_bool(int64_t x)
{
    wam_output_bool(x);
}


static
void print_string(uint64_t value)
{
    /* TODO: quote and escape string according to JSON spec */
    string_value_t *s = find_string_value(value);

    /* protect against corrupted data: we've seen at least one case when we had
     * a hash but the string was missing */
    if (!s) {
        wam_output_string(NULL, 0);
        return;
    }

    wam_output_string(s->str, s->len);
}


static
void print_cstring(const char *str)
{
    wam_output_cstring((char *)str);
}


static
void print_int_as_string(int64_t x)
{
    wam_output_int64_as_string(x);
}


static
void print_enum(const char **ids, int size, int64_t code)
{
    if (0 <= code && code < size) {  /* code is defined in the schema */
        const char *str = ids[code];
        if (str) {
            /* NOTE: there should't be any special characters we'd need to quote
             * in enum names, so it is safe to print it as is */
            print_cstring(str);
            return;
        }
    }

    /* returning an unknown integer
     *
     * NOTE: formatting it as a string to keep typing consistent with valid
     * enum values which are symbolic strings. If we format them as
     * integers, Scuba will get confused about column types and all past and
     * future strings values will turn into NULLs */
    print_int_as_string(code);
}


static
void print_bins(binned_result_t *res, double total_min, double total_max)
{
    if (!res) {  /* empty histogram, i.e. there was no data points */
        wam_output_null();
        return;
    }

    output_bin_t *bins = res->bins;

    wam_output_map_begin_counting();
        wam_output_map_key("bins");
            wam_output_array_begin_counting();
            int i;
            for (i = 0; i < res->num_bins; i++)
            {
                output_bin_t *b = &bins[i];
                wam_output_array_begin(3);
                    wam_output_double(b->start);
                    wam_output_double(b->stop);
                    wam_output_int64(b->count);
                wam_output_array_end();
            }
            wam_output_array_end();
        wam_output_map_key("total_min"); wam_output_double(total_min);
        wam_output_map_key("total_max"); wam_output_double(total_max);
        wam_output_map_key("left_count"); wam_output_int64(res->left_count);
        wam_output_map_key("right_count"); wam_output_int64(res->right_count);
        wam_output_map_key("range_count"); wam_output_int64(res->range_count);
        wam_output_map_key("total_count"); wam_output_int64(res->total_count);
    wam_output_map_end();
}


static
void print_time(uint64_t ts, int gmtoff, char resolution)
{
    static char str[64] = {0};

    /* NOTE: we used to do it this way, but not any more, because on daylight
     * savings shifts, it produces duplicate timestamps up to an hour resolution
     * and skew between two consecuitive days on which the shift occurred
     *
     * struct tm *tm = (is_utc)? gmtime(&t) : localtime(&t);
     *
     * instead, we are now calculating GMT offset at query compile time and
     * applying it to format all timestamps in a consistent and non-overlaping
     * manner -- see wam_query.erl for details
     */
    time_t t = ts + gmtoff;
    struct tm *tm = gmtime(&t);

    int len = 0;

#define FMT(fmt) \
do{ \
    len = strftime(str, sizeof(str), fmt, tm); \
} while(0)

    /* NOTE: ISO 8601 format: "%Y-%m-%dT%H:%M:%S%z" */
    switch (resolution) {
        case 'y': FMT("%Y"); break;
        case 'm': FMT("%Y-%m"); break;
        case 'd': FMT("%Y-%m-%d"); break;
        case 'H': FMT("%Y-%m-%d %H"); break;
        case 'M': FMT("%Y-%m-%d %H:%M"); break;
        case 'S':
        default:  FMT("%Y-%m-%d %H:%M:%S");
    }

    if (len) {
        print_cstring(str);
    } else {
        // if strftime failed for some reason
        print_int_as_string(ts);
    }
}


static const char *platform_names[];
static
void print_version(uint64_t v, int resolution)
{
    int platform_code = v >> 56;  /* upper 8 bit */
    int v1 = (v >> 48) & 0xff;    /* next 8 bit */
    int v2 = (v >> 32) & 0xffff;    /* 16 bit */
    int v3 = (v >> 16) & 0xffff;  /* 16 bit */
    int v4 = v & 0xffff;          /* 16 bit */

    const char *platform = platform_names[platform_code];
    /* XXX: we may print extra 4th digit; should we not to print it if it is 0?
     * (potentially, we can discover if other records has non-zero in the last
     * digit and decide based on that) */
    switch (resolution) {
        case 0:
            wam_output_quoted_printf("%s", platform);
            break;
        case 1:
            wam_output_quoted_printf("%s/%d", platform, v1);
            break;
        case 2:
            wam_output_quoted_printf("%s/%d.%d", platform, v1, v2);
            break;
        case 3:
            wam_output_quoted_printf("%s/%d.%d.%d", platform, v1, v2, v3);
            break;
        case 4:
        default:
            wam_output_quoted_printf("%s/%d.%d.%d.%d", platform, v1, v2, v3, v4);
    }
}

static
void print_decimal_version(uint64_t v)
{
    // int platform_code = v >> 56;  /* upper 8 bit */
    int64_t v1 = (v >> 48) & 0xff;    /* next 8 bit */
    int64_t v2 = (v >> 32) & 0xffff;    /* 16 bit */
    int64_t v3 = (v >> 16) & 0xffff;  /* 16 bit */
    int64_t v4 = v & 0xffff;          /* 16 bit */

    // const char *platform = platform_names[platform_code];
    // Note: let's just use uniform 5 decimals for each version.
    int64_t decimal_encoded =
        v1 * 100000 * 100000 * 100000+
        v2 * 100000 * 100000 +
        v3 * 100000 +
        v4;
    print_int(decimal_encoded);
}


static
void print_field_separator(void)  /* field separator */
{
   // TODO remove
}


static
void print_open_record(int field_count)
{
    wam_output_array_begin(field_count);
}


static
void print_close_record(void)
{
    wam_output_array_end();

    if (args_use_json_output_format) {
        write_char('\n');
    }
}


uint64_t dump_prev_offset = 0;


/* minimum size of the dump chunk, the actual size can be up to size + record
 * size; use 0 for unlimited size */
static int dump_chunk_size = 0;

/* numbef of written records */
static uint64_t dump_written_record_count = 0;


static
void dump_init(int chunk_size)
{
    dump_chunk_size = chunk_size;
}


static void query_output_metadata(void);
static
void dump_open(void)
{
    if (args_use_mpack_output_format) {
        wam_output_map_begin_counting();  /* top-level map */

        query_output_metadata();

        wam_output_map_key("rows");
        wam_output_array_begin_counting();
    }
}


static
void dump_close(void)
{
    if (!dump_written_record_count) {  /* no output records have been written */
        return;  /* dump is empty => nothing to close */
    }

    if (args_use_mpack_output_format) {
        wam_output_array_end();  /* "rows" array */
        wam_output_map_end();  /* top-level map */
    }

    /* wam_cat expects buffer length followed by buffer contents */
    if (args_use_mpack_output_format) {
        FILE *f = output_file;
        uint32_t buffer_size = wam_output_get_current_buffer_size();
        if (fwrite(&buffer_size, sizeof(buffer_size), 1, f) != 1)
            handle_io_error("storing mpack buffer size", f);

        /* write buffered data, if any */
        wam_output_flush();
    }

    dump_prev_offset = wam_output_get_current_offset();
}


static void dump_pre_write_record(void) {
    if (!dump_written_record_count) {  /* no output records have been written yet? */

        if (args_use_json_output_format) {
            /* print metadata/header record */
            wam_output_map_begin_counting();
            query_output_metadata();
            wam_output_map_end();
            write_char('\n');
        }

        /* calling dump_open() lazily upon writing the first record */
        dump_open();
    }
    dump_written_record_count++;

    uint64_t out_offset = wam_output_get_current_offset();
    if (dump_chunk_size && out_offset - dump_prev_offset > dump_chunk_size) {
        /* close previous dump chunk and open a new one */
        dump_close();
        dump_open();  /* FIXME: last chunk can end up empty */
    }
}


#define qNaN 0x7fffffffffffffffULL  //0x7fff_ffff_ffff_ffff


static inline
uint64_t make_tuple_value_uint64(uint64_t value, int is_not_null)
{
    /* branchess equivalent of:
     *
     * if (is_not_null) return value;
     * else return qNaN
     */
    uint64_t is_null_mask = ((uint64_t)is_not_null) - 1; // 0 when is_not_null else 0xffff_ffff_ffff_ffff
    return (value & ~is_null_mask) | (qNaN & is_null_mask);
}


static inline
double make_tuple_value_double(double value, int is_not_null)
{
    return uint64_to_double(
        make_tuple_value_uint64(double_to_uint64(value), is_not_null)
    );
}


static inline
int is_tuple_value_null_uint64(uint64_t value)
{
    return (value == qNaN);
}


static inline
int is_tuple_value_null_double(double value)
{
    return (double_to_uint64(value) == qNaN);
}

/* handling of long fields */
enum field_type {
    WAM_TYPE_UNKNOWN = 0,
    WAM_TYPE_BOOL = 1,
    WAM_TYPE_INT = 2,
    WAM_TYPE_FLOAT = 3,
    WAM_TYPE_STRING = 4,
    WAM_TYPE_TS = 5,
    WAM_TYPE_VERSION = 6,
    WAM_TYPE_ENUM = 7,
};

static struct enum_descriptor {const char **ids; const int size;} __enum[];

static
void print_dynamic(uint32_t field_type, uint64_t value)
{
    switch (field_type) {
        case WAM_TYPE_BOOL:
            print_bool(value);
            break;
        case WAM_TYPE_INT:
            print_int(value);
            break;
        case WAM_TYPE_FLOAT:
            print_float(uint64_to_double(value));
            break;
        case WAM_TYPE_STRING:
            print_string(value);
            break;
        case WAM_TYPE_TS:
            print_time(value, 0 /* gmtoff */, 'S' /* full resolution */);
            break;
        case WAM_TYPE_VERSION:
            print_version(value, 4 /* full resolution */);
            break;
        default:  /* this is enum */
            {
                /* NOTE: enum_ids for projected fields must be fully valid at
                 * this point; also, WAM_TYPE_ENUM itself is never used */
                unsigned enum_id = field_type - (WAM_TYPE_ENUM + 1);
                print_enum(__enum[enum_id].ids, __enum[enum_id].size, value);
            }
    }
}


/* include generated query */
#ifndef WAM_QUERY
#define WAM_QUERY "query.c"
#endif

#include WAM_QUERY


static inline
tuple_t *find_or_alloc_group_tuple(tuple_t *tuple_template)
{
        /* see if there is a tuple with such key; if not, allocate and
         * initialize a new one */
        int is_new_tuple = 0;
        tuple_t *t = wam_groupby_find_or_alloc(tuple_template, &is_new_tuple);
        if (is_new_tuple) query_init_aggregate_tuple(t);

        return t;
}


/* find or alloc group tuple */
static
tuple_t *get_group_tuple(void)
{
    static tuple_t *prev_tuple = NULL;

    /* TODO, XXX: come up with a more efficient way to check whether there was a
     * change in the key; moreover this function will be run twice
     * -- once here and another time in wam_groupby_find_or_alloc() below;
     *  however this turns out to be a real saver on many type of queries,
     *  because hashtbl lookups seem to be more expensive */
    tuple_t *t;
    if (prev_tuple != NULL && !memcmp(prev_tuple, &tuple, groupby_key_size)) {
        /* there was no change in the aggregation key */
        t = prev_tuple;
    }
    else {
        t = find_or_alloc_group_tuple(&tuple);
        prev_tuple = t;
    }

    return t;
}


/* process sample tuple: alloc a new one or displace one of the current ones as
 * a step of reservoir sampling */
#if MAX_SAMPLE_SIZE == 0  /* no sampling */
#else
/* NOTE: this code is used only in sampled mode, i.e. when MAX_SAMPLE_SIZE > 0 */
#ifndef IS_RANDOM_SAMPLE  /* first N sample: process upto first matching MAX_SAMPLE_SIZE rows */
static
tuple_t *get_sample_tuple(void)
{
    if (sample_size == MAX_SAMPLE_SIZE) return NULL; /* can't fit more values */

    tuple_t *t = &sample_tuples[sample_size];
    query_reset_sample_tuple(t);

    sample_size++;
    return t;
}
#else  /* IS_RANDOM_SAMPLE -- random (reservoir) sampling */
static
tuple_t *get_sample_tuple(void)
{
    static uint64_t i = 0;  /* 0-based current position in the input stream */
    tuple_t *t;

    if (i < MAX_SAMPLE_SIZE) {
        t = &sample_tuples[i];
        sample_size++;
        i++;  /* append a new sample */
    } else {
        /* can't append a new sample => replace one of the previous ones with
         * probability 1/i or skip this one
         *
         * TODO, NOTE: this will also work correctly during the merge stage of
         * the parallel query as soon as the input data set is divided evenly
         * across workers; eventually, we'll need to handle it properly using
         * the weighted reservoir sampling variation as described here:
         * http://gregable.com/2007/10/reservoir-sampling.html
         *
         * TODO: optimize: we can precompute how many stream elements to skip
         * instead of calling RNG on each input element
         */
        uint64_t j = wam_random_uint64(0, i);
        if (j < MAX_SAMPLE_SIZE) {
            t = &sample_tuples[j];
            query_reset_sample_tuple(t);
        } else {
            t = NULL;
        }
        i++;
    }

    return t;
}
#endif  /* IS_RANDOM_SAMPLE */
#endif  /* MAX_SAMPLE_SIZE == 0 */


/* NOTE: there could at most MAX_LONG_SELECT_FIELDS elements in each table
 * (assuming there are no immediate duplicates to attributes and fields) */
static struct field_item long_field_buf_[MAX_LONG_SELECT_FIELDS];
static struct field_item long_attr_buf_[MAX_LONG_SELECT_FIELDS];

static struct field_item_buf long_field_buf = {long_field_buf_, long_field_buf_};
static struct field_item_buf long_attr_buf = {long_attr_buf_, long_attr_buf_};

static struct field_item star_projected_field_buf_[MAX_STAR_PROJECTED_FIELDS];
static struct field_item_buf star_projected_field_buf = {star_projected_field_buf_, star_projected_field_buf_};

/* allocate a long new field item (either field or buffer attribute) for storing
 * field value */
static inline
struct field_item *get_long_field_item(int is_field)
{
    struct field_item_buf *buf = is_field? &long_field_buf : &long_attr_buf;

    /* check that we haven't exceeded buffer capacity */
    assert(buf->cur < buf->buf + MAX_LONG_SELECT_FIELDS);
    /*
    if (buf->cur == buf->buf + MAX_LONG_SELECT_FIELDS - 1) {
        buf->cur->id = 11111;
        buf->cur->value = 0;
        return NULL;
    }
    */

    return buf->cur++;
}


/* allocate a new star-projected field item (either field or buffer attribute)
 * for storing field value
 *
 * TODO: rename to local event field */
static inline
struct field_item *get_star_projected_field_item(void)
{
    struct field_item_buf *buf = &star_projected_field_buf;

    /* check that we haven't exceeded buffer capacity */
    assert(buf->cur < buf->buf + MAX_STAR_PROJECTED_FIELDS);
    return buf->cur++;
}


static inline
void field_item_buf_reset(struct field_item_buf *buf)
{
    buf->cur = buf->buf;
}


static inline
void reset_long_field_items(void)
{
    field_item_buf_reset(&long_attr_buf);
    field_item_buf_reset(&long_field_buf);
}


static inline
void reset_star_projected_field_items(void)
{
    field_item_buf_reset(&star_projected_field_buf);
}


static
uint64_t process_field_value(uint32_t code, enum field_type type, uint64_t value)
{
    int is_null;

    if (type == WAM_TYPE_UNKNOWN) {
        is_null = 1;
    } else {
        is_null = (code & 0x80000000) != 0;
        int is_int = (code & 0x01000000) == 0;
        uint64_t is_float_mask = ((uint64_t)is_int) - 1; // 0 when is_int else 0xff..ff

        if (type == WAM_TYPE_STRING) {
            value = make_string_value(value, is_null, 1 /* field projected => it is being used */);
        } else if (type == WAM_TYPE_INT) {
            value = (value & ~is_float_mask) | (int64_t)uint64_to_double(value & is_float_mask);
        } else if (type == WAM_TYPE_FLOAT) {
            value = double_to_uint64((double)(value & ~is_float_mask)) | (value & is_float_mask);
        } else if (type == WAM_TYPE_BOOL) {
            value = (value != 0);
        } else {
            /* all others are integer types that don't require coercion:

                WAM_TYPE_TS = 5,
                WAM_TYPE_VERSION = 6,
                WAM_TYPE_ENUM = 7,
            */
        }
    }

    return make_tuple_value_uint64(value, !is_null);
}


/* long-select: selecting field_id, field_value */
static
void process_long_selected_field(uint32_t code, uint64_t value, int is_field)
{
    uint32_t field_id = (code & 0x00ffffff);

    /* NOTE: type can be legitimately unknown if the query doesn't care about
     * field types, or the field is deprecated, or there is an internal problem
     * when we store fields that are not defined in the schema */
    enum field_type type = WAM_TYPE_UNKNOWN;

    if (field_id < FIELD_TYPES_SIZE) {
        type = field_types[field_id];
    }

    uint64_t tuple_value = process_field_value(code, type, value);

    struct field_item *field_item = get_long_field_item(is_field);
    field_item->id = field_id;
    field_item->value = tuple_value;
}


static
void process_star_projected_field(uint32_t code, uint64_t value, int is_field)
{
    uint32_t field_id = (code & 0x00ffffff);

    /* return immediately if the field is not star-projected */
    if (! (field_id < IS_STAR_PROJECTED_SIZE && is_star_projected[field_id])) return;

    assert(field_id < FIELD_TYPES_SIZE);
    enum field_type type = field_types[field_id];
    if (type == WAM_TYPE_UNKNOWN) {
        LOG("code: %d field: %d type: %d value: %"PRIu64 "\n", code, field_id, type, value);
        exit(1);
    }

    uint64_t tuple_value = process_field_value(code, type, value);

    if (is_field) {
        if (is_star_projected[field_id] & 1) { /* field is star-projected as local */
            struct field_item *field_item = get_star_projected_field_item();
            field_item->id = field_id;
            field_item->value = tuple_value;

            /* make sure global field won't be projected when present, i.e.
             * local field overrides global fields */
            if (is_star_projected[field_id] & 2) {
                assert(field_id < GLOBAL_FIELD_VALUES_SIZE);
                struct global_field_value *global_field_value = &global_field_values[field_id];

                global_field_value->event_count = event_count;
            }
        }
    } else {
        if (is_star_projected[field_id] & 2) { /* field is star-projected as global */
            assert(field_id < GLOBAL_FIELD_VALUES_SIZE);
            struct global_field_value *global_field_value = &global_field_values[field_id];

            global_field_value->buffer_count = buffer_count;
            global_field_value->value = tuple_value;
        }
    }
}


struct wam_star {
    uint32_t offset;
    uint32_t size;
};


struct star_value {
    uint32_t id;
    uint64_t value;
};


static uint32_t star_value_buf_len = 0;
static uint32_t star_value_buf_cap = 0;
static struct star_value *star_value_buf = NULL;


static
int star_value_buf_alloc_value(void)
{
    int item_size = sizeof(*star_value_buf);

    if (!star_value_buf) {  /* allocate for the first time */
        star_value_buf_cap = 4096 / item_size;
        star_value_buf_len = 0;
        star_value_buf = malloc(star_value_buf_cap * item_size);
    } else if (star_value_buf_len == star_value_buf_cap) {
        /* double the capacity once we fill up the existing buffer */
        star_value_buf_cap *= 2;
        star_value_buf = realloc(star_value_buf, star_value_buf_cap * item_size);
    }

    return star_value_buf_len++;
}


static
void star_value_buf_reset(void)
{
    star_value_buf_len = 0;
}


static
struct star_value *star_value_alloc(wam_star_t *star)
{
    int new_offset = star_value_buf_alloc_value();
    if (!star->size) {
        star->offset = new_offset;
    }
    assert(star->offset + star->size == new_offset);

    star->size++;
    return &star_value_buf[new_offset];
}


static
void wam_star_store(wam_star_t *star, FILE *f)
{
    if (fwrite(&star->size, sizeof(star->size), 1, f) != 1)
        handle_io_error("storing star", f);

    uint32_t i;
    for (i = 0; i < star->size; i++) {
        struct star_value *star_value = &star_value_buf[star->offset + i];

        if (fwrite(&star_value->id, sizeof(star_value->id), 1, f) != 1)
            handle_io_error("storing star value id", f);
        if (fwrite(&star_value->value, sizeof(star_value->value), 1, f) != 1)
            handle_io_error("storing star value value", f);
    }
}


static
void wam_star_load(wam_star_t *star, FILE *f)
{
    uint32_t i, size;

    if (fread(&size, sizeof(size), 1, f) != 1)
        handle_io_error("loading star", f);

    for (i = 0; i < size; i++) {
        struct star_value *star_value = star_value_alloc(star);

        if (fread(&star_value->id, sizeof(star_value->id), 1, f) != 1)
            handle_io_error("loading star value id", f);
        if (fread(&star_value->value, sizeof(star_value->value), 1, f) != 1)
            handle_io_error("loading star value value", f);
    }
}


static const char *field_names[];
static
void print_star_field_name(int id)
{
    const char *field_name = field_names[id];
    wam_output_map_key((char *)field_name);
}


static
void print_star_field_value(struct star_value *star_value)
{
    print_dynamic(field_types[star_value->id], star_value->value);
}


static
void print_star(wam_star_t *star)
{
    uint32_t i;
    for (i = 0; i < star->size; i++) {
        struct star_value *star_value = &star_value_buf[star->offset + i];

        print_star_field_name(star_value->id);
        print_star_field_value(star_value);
    }
}


static
void print_star_field_value_by_id(wam_star_t *star, int field_id)
{
    /* TODO: this can be optimized to be linear for a bunch of ids -- shouldn't
     * be a bottleneck now however */
    uint32_t i;
    for (i = 0; i < star->size; i++) {
        struct star_value *star_value = &star_value_buf[star->offset + i];

        if (star_value->id == field_id) {
            print_star_field_value(star_value);
            return;
        }
    }

    /* print null if not found */
    print_null();
}


/* init or reset */
static
void wam_star_reset(wam_star_t **star)
{
    /* FIXME: too many allocations on large samples */
    /* FIXME: doesn't work with random sampling, because not actually freeing
     * previous value here */
    if (!(*star)) {
        *star = malloc(sizeof(wam_star_t));
    }
    (*star)->offset = star_value_buf_len;
    (*star)->size = 0;
}


static
void wam_star_append_field(wam_star_t *star, uint32_t field_id, uint64_t field_value)
{
    struct star_value *star_value = star_value_alloc(star);
    star_value->id = field_id;
    star_value->value = field_value;
}


static
void wam_star_project_local_fields(wam_star_t *star)
{
    /* event fields */
    struct field_item *field_item;
    FOREACH_FIELD_ITEM(field_item, star_projected_field_buf) {
        wam_star_append_field(star, field_item->id, field_item->value);
    }
}


static
void wam_star_project_global_fields(wam_star_t *star, int event_id)
{
    assert(event_id >= 0);

    /* find the list of global field ids for this event */
    if (event_id >= GLOBAL_EVENT_FIELD_IDS_SIZE) return;  /* event doesn't have global fields */
    int *field_ids = global_event_field_ids[event_id];
    if (!field_ids) return;  /* event doesn't have global fields */

    /* iterate over the list of global field ids until field_id = 0 representing end of sequence aka 0-terminator */
    for (; *field_ids; field_ids++) {
        int field_id = *field_ids;

        if (field_id >= GLOBAL_FIELD_VALUES_SIZE) continue;
        struct global_field_value *global_field_value = &global_field_values[field_id];

        if (global_field_value->buffer_count == buffer_count &&
            global_field_value->event_count != event_count &&  /* NOTE: skipping if local field is projected */
            !is_tuple_value_null_uint64(global_field_value->value))
        {
            wam_star_append_field(star, field_id, global_field_value->value);
        }
        /* mark as projected or ready to be projected which is needed for special star-projected fields
         *
         * TODO: this is pretty bad, because we are reusing some flags that are
         * meant to be used at a completely different query stage, but it
         * appears to be the easiest way to hack it at the momemnt */
        global_field_value->event_count = event_count;
    }
}


/* NOTE: io errors are not recoverable */
void handle_io_error(const char *where, FILE *f)
{
    if (ferror(f))
        LOG("error %s: %s\n", where, strerror(errno));
    else if (feof(f))
        LOG("error %s: unexpected EOF\n", where);
    else
        LOG("error %s: unknown error\n", where);

    assert(0);
}


/* NOTE: io errors are not recoverable */
void handle_input_io_error(const char *where, wam_input_t *input)
{
    if (ferror(input->file))
        LOG("error %s from %s: %s\n", where, input->filename, strerror(errno));
    else if (feof(input->file))
        LOG("error %s from %s: unexpected EOF\n", where, input->filename);
    else
        LOG("error %s from %s: unknown error\n", where, input->filename);

    assert(0);
}


static
void load_bytes(void *p, int size, FILE *f)
{
    if (fread(p, size, 1, f) != 1)
        handle_io_error("loading bytes", f);
}


static
void skip_bytes(int size, FILE *f)
{
    if (fseek(f, size, SEEK_CUR) != 0) {
        LOG("error skipping %d bytes: %s\n", size, strerror(errno));
        assert(0);
    }
}


static
void store_bytes(void *p, int size, FILE *f)
{
    if (fwrite(p, size, 1, f) != 1)
        handle_io_error("storing bytes", f);
}


/* store, load and skip_tuple() functions are used only in sampled query mode */
static
void store_tuple(tuple_t *t, FILE *f)
{
    if (fwrite(t, sizeof(tuple_t), 1, f) != 1)
        handle_io_error("storing tuple", f);
}


static
void load_tuple(tuple_t *t, FILE *f)
{
    if (fread(t, sizeof(tuple_t), 1, f) != 1)
        handle_io_error("loading tuple", f);
}


static
void skip_tuple(FILE *f)
{
    if (fseek(f, sizeof(tuple_t), SEEK_CUR) != 0)
        handle_io_error("skipping tuple", f);
}


static
void skip_subquery_tuple_item(int code, wam_input_t *input)
{
    switch (TUPLE_ITEM_CODE(code)) {
        case TUPLE_ITEM_FIELD64:
        case TUPLE_ITEM_COUNT:
        case TUPLE_ITEM_COUNT_SPECIAL:
            break;
        case TUPLE_ITEM_UCOUNT:
            /* skip_bytes(*(input->tuple_header_ptr), input->file); */
            wam_weighted_ucount_skip(input->file);
            break;
        default:
            /* unknown item */
            assert(0);
    }
    input->tuple_header_ptr++;
}


#define TUPLE_DESCRIPTOR_VERSION 1
static uint32_t tuple_descriptor_version;


static
void store_subquery_tuple_descriptor(FILE *f)
{
    tuple_descriptor_version = TUPLE_DESCRIPTOR_VERSION;
    if (fwrite(&tuple_descriptor_version, sizeof(tuple_descriptor_version), 1, f) != 1)
        handle_io_error("storing tuple descriptor version", f);

    int tuple_descriptor_len = sizeof(subquery_tuple_descriptor) / sizeof(subquery_tuple_descriptor[0]);
    if (fwrite(&tuple_descriptor_len, sizeof(tuple_descriptor_len), 1, f) != 1)
        handle_io_error("storing tuple descriptor length", f);

    if (fwrite(subquery_tuple_descriptor, sizeof(subquery_tuple_descriptor[0]), tuple_descriptor_len, f) != tuple_descriptor_len)
        handle_io_error("storing tuple descriptor", f);
}


#define SUBQUERY_HEADER_VERSION 2
static uint32_t subquery_header_version;
static uint32_t last_merge_generation;

static
void store_subquery_header(FILE *f, int merge_generation)
{
    subquery_header_version = SUBQUERY_HEADER_VERSION;
    if (fwrite(&subquery_header_version, sizeof(subquery_header_version), 1, f) != 1)
        handle_io_error("storing subquery header version", f);

    /* XXX: consider also storing index checksum to check for its integrity */
    last_merge_generation = merge_generation;
    if (fwrite(&last_merge_generation, sizeof(last_merge_generation), 1, f) != 1)
        handle_io_error("storing merge generation", f);

    store_subquery_tuple_descriptor(f);
}


static void wam_input_init(wam_input_t *input)
{
    memset(input, 0, sizeof(wam_input_t));

    /* NOTE: allocating the whole tuple (group-by key and aggregates) and
     * copying constant initializers from the 'tuple' vairable */
    input->key = (tuple_t *)malloc(sizeof(tuple_t));
    memcpy(input->key, &tuple, sizeof(tuple_t));
}


static wam_input_t *wam_input_alloc(void)
{
    wam_input_t *input = (wam_input_t *)malloc(sizeof(wam_input_t));

    wam_input_init(input);

    return input;
}


static void wam_input_clear(wam_input_t *input)
{
    free(input->key);
    free(input->tuple_descriptor);
    free(input->tuple_header);
}


static void wam_input_free(wam_input_t *input)
{
    wam_input_clear(input);

    free(input);
}


/* whether subquery tuple descriptor is compatible with the (merge query)
 * descriptor, meaning that input keys are supposed to be in order */
static int is_subquery_tuple_descriptor_compatible = 1;

/* global variable indicating that this query performes summary resampling */
int wam_summary_resampling_mode = 0;

static
void check_subquery_tuple_descriptor_compatibility(wam_input_t *input)
{
    if (!is_subquery_tuple_descriptor_compatible) {
        /* there was at least one incompatible descriptor in the input => no
         * need to check others */
        return;
    }

    /* query's tuple descriptor and header len */
    int tuple_header_len = sizeof(subquery_tuple_descriptor) / sizeof(subquery_tuple_descriptor[0]);
    int *tuple_descriptor = subquery_tuple_descriptor;

    /* NOTE: subqueries must have at least one 'ts' field in their group-by keys
     */
    int i;
    for (i = 0; i < MIN(tuple_header_len, input->tuple_header_len); i++) {
        if (tuple_descriptor[i] != input->tuple_descriptor[i]) {
            is_subquery_tuple_descriptor_compatible = 0;
            return;
        }
    }

    /* if we reach this point there could be 3 cases:
     *
     * 1) query group-by key and input grouby-key are equal => they are
     *    compatible by definition
     * 2) query group-by key has additional fields at the end that are not
     *    present in input group-by key => they are compatible, because query's
     *    group-by key is a specialization of input group-by meaning input
     *    groupby-keys are ordered (just have trailing NULLs)
     *    order
     * 3) query group-by key has less fields than input group-by key, meaning
     *    one or more fields were removed from the query's group-by key. this
     *    also implies compatibility, because input keys are still ordered by
     *    query's groupby key, just there could be several input keys per one
     *    query key
     *
     * therefore, all thee cases mean compatibility and case 1) means exact
     * compatibility */
}


static
void load_subquery_tuple_descriptor_v1(wam_input_t *input)
{
    if (fread(&input->tuple_header_len, sizeof(input->tuple_header_len), 1, input->file) != 1)
        handle_input_io_error("loading tuple descriptor length", input);

    /* allocate a couple of buffers, now that we know their sizes */
    input->tuple_descriptor = (int *)malloc(sizeof(input->tuple_descriptor[0]) * input->tuple_header_len);
    input->tuple_header = (uint64_t *)malloc(sizeof(input->tuple_header[0]) * input->tuple_header_len);

    if (fread(input->tuple_descriptor, sizeof(input->tuple_descriptor[0]), input->tuple_header_len, input->file) != input->tuple_header_len)
        handle_input_io_error("loading tuple descriptor", input);

    /* check descriptor compatibility as we are loading it */
    if (output_mode == OUTPUT_SUBQUERY_MERGE) {
        check_subquery_tuple_descriptor_compatibility(input);
    }
}


static
void load_subquery_tuple_descriptor(wam_input_t *input)
{
    if (fread(&tuple_descriptor_version, sizeof(tuple_descriptor_version), 1, input->file) != 1)
        handle_input_io_error("loading tuple descriptor version", input);

    assert(tuple_descriptor_version == TUPLE_DESCRIPTOR_VERSION);

    load_subquery_tuple_descriptor_v1(input);
}


static
void load_subquery_header(wam_input_t *input)
{
    if (fread(&subquery_header_version, sizeof(subquery_header_version), 1, input->file) != 1)
        handle_input_io_error("loading subquery header version", input);

    if (subquery_header_version == 1) {  /* backward compatibility */
        last_merge_generation = 1;
        load_subquery_tuple_descriptor_v1(input);
        return;
    }

    /* newer versions */
    assert(subquery_header_version == SUBQUERY_HEADER_VERSION);

    if (fread(&last_merge_generation, sizeof(last_merge_generation), 1, input->file) != 1)
        handle_input_io_error("loading merge generation", input);

    load_subquery_tuple_descriptor(input);
}


static
int load_tuple_header(wam_input_t *input)
{
    size_t res = fread(input->tuple_header, sizeof(input->tuple_header[0]), input->tuple_header_len, input->file);
    if (res != input->tuple_header_len) {
        if (!res && feof(input->file)) {
            return 0;  /* legitimate EOF at record boundary */
        } else {
            handle_input_io_error("loading tuple header", input);
        }
    }

    /* reset tuple header pointer to 0 */
    input->tuple_header_pos = 0;
    input->tuple_header_ptr = input->tuple_header;

    return 1;
}


static
void store_tuple_header(FILE *f)
{
    if (fwrite(&tuple_header_buf, sizeof(tuple_header_buf), 1, f) != 1)
        handle_io_error("storing tuple header", f);
}


/* process all descriptor raw fields (i.e. materialized view group-by key), some
 * or all of which may form a new group-by key */
static
void load_subquery_tuple_fields(wam_input_t *input)
{
    while (input->tuple_header_pos < input->tuple_header_len &&
           TUPLE_ITEM_CODE(input->tuple_descriptor[input->tuple_header_pos]) == TUPLE_ITEM_FIELD64)
    {
        load_subquery_tuple_item(NULL, input->tuple_descriptor[input->tuple_header_pos], input);
        input->tuple_header_pos++;
    }
}


/* process all the aggregate items; they follow raw fields in the descriptor */
static
void load_subquery_tuple_items(tuple_t *t, wam_input_t *input)
{
    while (input->tuple_header_pos < input->tuple_header_len)
    {
        load_subquery_tuple_item(t, input->tuple_descriptor[input->tuple_header_pos], input);
        input->tuple_header_pos++;
    }
}


/* skip all aggregate items */
static
void skip_subquery_tuple_items(wam_input_t *input)
{
    while (input->tuple_header_pos < input->tuple_header_len)
    {
        skip_subquery_tuple_item(input->tuple_descriptor[input->tuple_header_pos], input);
        input->tuple_header_pos++;
    }
}


static
void foreach_tuple(void (*f)(tuple_t *), int visit_count)
{
    /* read from tuple_array when it's defined */
    if (tuple_array) {  /* ordered tuples? */
        int i;
        for (i = 0; i < visit_count; i++) {
            f(tuple_array[i]);
        }
    } else if (MAX_SAMPLE_SIZE > 0) {  /* sample mode? */
        int i;
        for (i = 0; i < visit_count; i++) {
            f(&sample_tuples[i]);
        }
    } else {  /* groupped rows */
        wam_groupby_foreach_tuple(f, visit_count);
    }
}


static uint64_t total_file_count = 0;

static uint64_t file_size = 0;
static uint64_t total_size = 0;
static uint64_t total_record_count = 0;
static uint64_t total_field_count = 0;

static uint64_t total_string_count = 0;
static uint64_t total_string_size = 0;
static uint64_t read_string_count = 0;
static uint64_t read_string_size = 0;

static uint64_t total_wms_file_count = 0;
static uint64_t total_wms_record_count = 0;


#define is_suffix(s, suffix) \
    (!strcmp(s + strlen(s) - strlen(suffix), suffix))


static
void set_iobuf(FILE *f, char **iobuf)
{
/* page-aligned 64k input buffer */
#define IOBUF_SIZE 65536

    if (!*iobuf) {
        assert(!posix_memalign((void **)iobuf, 4096, IOBUF_SIZE));
    }

    assert(!setvbuf(f, *iobuf, _IOFBF, IOBUF_SIZE));
}


static
void set_fread_buf(FILE *f)
{
    static char *fread_buf = NULL;
    set_iobuf(f, &fread_buf);
}


static
FILE *wam_fopen_common(char *filename, int direct, int open_flags, char *fopen_mode)
{
    if (direct) {
        /* turn off caching; on high query load, this make system more stable;
         * we have plenty of disk IO bandwidth with 24 SSDs and RAM is too small
         * to make caching efficient on large queries */
        open_flags |= O_DIRECT;
    }

    int fd = open(filename, open_flags, 0644);
    if (fd < 0) return NULL;

    FILE* finput = fdopen(fd, fopen_mode);
    assert(finput);

    return finput;
}


static
FILE *wam_fopen_r(char *filename, int direct)
{
    return wam_fopen_common(filename, direct, O_RDONLY, "r");
}


static
FILE *wam_fopen_w(char *filename, int direct)
{
    return wam_fopen_common(filename, direct, O_WRONLY | O_CREAT | O_TRUNC, "w");
}


static
FILE *open_wam_file(char *filename)
{
    int open_flags = O_RDONLY;

    /* NOTE: non-nice'd queries run on rotation: min_max_ts, summaries, scribe
     * -- each of the queries processes the same file and actually benefit from
     *  caching */
    FILE *finput = wam_fopen_r(filename, args_direct_io);

    if (!finput) {
        if (errno == ENOENT) {  /* file is missing */
            if(is_suffix(filename, ".lz4")) {
                /* perhaps the file has been deleted by the expiration process
                 * -- skipping such file */
                return NULL;
            } else {
                /* it could be that that the .log file was rotated between
                 * the moment it was listed for inclusion in the query input
                 * and our attempt to open it -- trying the rotated version
                 * of it (<filename>.rotated is a symlink pointing to the
                 * rotated version of the original log) */
                strcat(filename, ".rotated");
                finput = wam_fopen_r(filename, args_direct_io);

                // temporary workaround for the race between foreign data
                // deletion and queries
                if (!finput && errno == ENOENT) return NULL;

                if (!finput) LOG(
                    "failed to open both the original and the rotated log %s: %s\n",
                    filename, strerror(errno)
                );
            }
        } else {  /* all other open() errors */
            LOG("failed to open input file %s: %s\n", filename, strerror(errno));
        }
    }

    set_fread_buf(finput);

    return finput;
}


static
void process_input_data(char *filename)
{
    int fd;

    FILE *f = NULL;
    int is_lz4 = 0;
    int is_stdin = 0;

    //LOG("process file: %s\n", filename);
    if (!strcmp(filename, "-")) {
        is_stdin = 1;
        fd = fileno(stdin);
        assert(fd >= 0);
    } else {
        /* call a custom open file function that handles file rotation and
         * expiration */
        f = open_wam_file(filename);
        if (!f) return;  /* input file is missing */

        if (is_suffix(filename, ".log.lz4") || is_suffix(filename, ".rotated")) {
            fclose(f);
            is_lz4 = 1;
            /* TODO: using exteranal lz4c decompressor for now */
            char command[1024];
            snprintf(command, sizeof(command), "./wam-read \"%s\"", filename);
            f = popen(command, "r");
            assert(f);
        }

        fd = fileno(f);
        assert(fd >= 0);
    }

    total_file_count++;

    /* these two varibles are used for reading string values from the input
     * record stream */
    int str_len = 0;
    char *dst_str = NULL;

    ssize_t read_size;
    int is_eof = 0;

    while (!is_eof) {
        read_size = 0;
        do {
            ssize_t rs = read(fd, (char *)record_buf + read_size, sizeof(record_buf) - read_size);
            if (rs < 0) {  // IO error
                LOG("last file: %s\n", filename);
                assert(0);
            }
            read_size += rs;

            if (!rs) {  /* eof */
                is_eof = 1;
                if (read_size % RECORD_SIZE == 0) {
                    /* legitimate EOF */
                    //LOG("EOF %s @%" PRIu64 "\n", filename, file_size);
                    break;
#if 0
                // commented out, originally added for storage summaries, but
                // should have never added it in the first place without a
                // command-line switch! we've had truncated corrupted/truncated
                // input looking exactly like this!
                } else if (read_size >= 4 && ((read_size - 4) % RECORD_SIZE) == 0) {
                    /* another way of representing end of input: the last read with
                     * 4 extra zero bytes, because stock Erlang port mechanism is
                     * not capable of sending us a proper EOF
                     *
                     * NOTE that a valid record can't start with 4 zeros */
                    //LOG("EOI %s @%" PRIu64 "\n", filename, file_size);
                    break;
#endif
                } else {
                    /* unexpected EOF; must be data corruption */
                    LOG("last file: %s\n", filename);
                    assert(0);
                }
            }

            /* keep reading until we read the whole buffer or stop reading
             * between records */
        } while ((read_size % RECORD_SIZE) != 0);

        int record_count = read_size / RECORD_SIZE;

        file_size += read_size;
        total_record_count += record_count;

        /* skip the rest of the input after we've had enough -- the dispatcher
         * should stop feeding input to this worker once it notices the "eoi"
         * line we've sent him -- see below */
        if (is_eoi) continue;

        if (is_stdin) {
            if (wam_groupby_get_tuple_count() > 40 * 1000 * 1000) {
                LOG("error: too many results");
                exit(1);
            }
        }

        int i = 0;
        int remaining_buf_size;
        int src_str_offset;
        char *src_str;
        int copy_size;

        if (str_len) {  /* still reading string value? */
            src_str = (char *)&record_buf[0];
            remaining_buf_size = record_count * sizeof(record_buf[0]);
            src_str_offset = 0;
            goto process_string;
        }

        for (; i < record_count && !is_eoi; i++) {
            record_t *rec = &record_buf[i];

            uint64_t value = *(uint64_t *)&rec->u32;

            if ((rec->id >> 16) == 0xffff) {  /* is this a string value? */
                str_len = rec->id & 0xffff;

                total_string_count++;
                total_string_size += str_len;

                if (store_next_string_value) {  /* reading of this string value was requested? */
                    /* create a hashtable entry with the allocated placeholder
                     * for the actual string value we are about to read */
                    string_value_t *string_value = wam_strtbl_insert(next_string_value_hash, NULL, str_len);
                    dst_str = string_value->str;

                    read_string_count++;
                    read_string_size += str_len;
                }

                remaining_buf_size = 8 + (record_count - (i + 1)) * sizeof(record_buf[0]);
                /* offset of the string being copied in the 12-byte record */
                src_str_offset = 4;
                src_str = (char *)&rec->u32;

/* read or skip the string */
process_string:
                copy_size = MIN(str_len, remaining_buf_size);

                if (dst_str) {  /* reading of string value was requested / is in progress? */
                    memcpy(dst_str, src_str, copy_size);
                    dst_str += copy_size;
                }

                str_len -= copy_size;
                if (!str_len) {  /* finished reading the string value? */
                    dst_str = NULL;
                    store_next_string_value = 0;
                    /* estimate how many input records the copied portion has
                     * spanned (excluding the current one) and increment the
                     * current position (i) in the input buffer accordingly */
                    i += ALIGN(src_str_offset + copy_size, sizeof(record_buf[0]));
                    /* we need to subtract 1, because it will be re-added again
                     * by the for loop */
                    i -= 1;
                }
                else {
                    /* the string spans until the end of the buffer -- the
                     * remainder of the string will be copied after we read the
                     * next buffer -- see above */
                    break;
                }
            } else {
                total_field_count++;
                query_process_field(rec->id, value);
            }
        }

        /* read inough input: notify the dispatcher about it so that it stops
         * feeding input to this worker */
        if (is_eoi) LOG("eoi\n");
    }

    total_size += file_size;
    file_size = 0;

    if (is_lz4) pclose(f);
    else if (f) fclose(f);
    else close(fd);
}


// process_input_data2 based on stdlib's file IO bufferring
#if 0
static
int read_input(char *filename, char *what, FILE *f, void *dst, int size, int expect_eof)
{
    int read_size = fread(dst, 1, size, f);
    if (read_size < size) {
        if (feof(f)) {
            if (expect_eof) {
                if (read_size == 0) {
                    /* legitimate EOF */
                    LOG("EOF %s @%" PRIu64 "\n", filename, file_size);
                    return 0;
                } else if (read_size == 4 && *(uint32_t *)((char *)dst) == 0) {
                    /* another way of representing end of input: the last read with
                     * 4 extra zero bytes, because stock Erlang port mechanism is
                     * not capable of sending us a proper EOF
                     *
                     * NOTE that a valid record can't start with 4 zeros */
                    LOG("EOI %s @%" PRIu64 "\n", filename, file_size);
                    return 0;
                }
            }
            /* unexpected EOF; must be data corruption */
            LOG("error %s from %s: unexpected EOF\n", what, filename);
        } else if (ferror(f)) {
            LOG("error %s from %s: %s\n", what, filename, strerror(errno));
        } else {
            LOG("error %s from %s: unknown error\n", what, filename);
        }
        assert(0);  /* error or unexpected EOF */
    }

    file_size += read_size;
    return read_size;  /* not EOF yet */
}


static
void process_input_data2(char *filename)
{
    FILE *f;
    int is_lz4 = 0;

    //LOG("process file: %s\n", filename);
    if (!strcmp(filename, "-")) {
        f = stdin;
    } else {
        /* call a custom open file function that handles file rotation and
         * expiration */
        f = open_wam_file(filename);
        if (!f) return;  /* input file is missing */

        if (is_suffix(filename, ".log.lz4") || is_suffix(filename, ".rotated")) {
            fclose(f);
            is_lz4 = 1;
            /* TODO: using exteranal lz4c decompressor for now */
            char command[1024];
            snprintf(command, sizeof(command), "./wam-read \"%s\"", filename);
            f = popen(command, "r");
            assert(f);
        }
    }

    total_file_count++;

    record_t r;  /* input record */

    int read_size;
    while ((read_size = read_input(filename, "reading record", f, &r, sizeof(r), 1))) {

        uint64_t value = *(uint64_t *)&r.u32;

        if ((r.id >> 16) == 0xffff) {  /* is this a string record? */
            int str_len = r.id & 0xffff;

            /* size of the part of the string value stored in this record
             *
             * NOTE: string values a padded with zeros so that they take exactly
             * N records */
            int copy_size = MIN(str_len, sizeof(r) - sizeof(r.id));
            long skip_bytes = 0;  /* how many bytes to skip in the input stream */

            total_string_count++;
            total_string_size += str_len;

            if (store_next_string_value) {  /* reading of this string value was requested? */
                store_next_string_value = 0;

                /* create a hashtable entry with the allocated placeholder
                 * for the actual string value we are about to read */
                string_value_t *string_value = wam_strtbl_insert(next_string_value_hash, NULL, str_len);
                char *dst_str = string_value->str;

                read_string_count++;
                read_string_size += str_len;

                /* copy the part of the string value stored in this record */
                memcpy(dst_str, (char *)&r.u32, copy_size);
                dst_str += copy_size;
                str_len -= copy_size;

                /* read the rest of the string if it doesn't fit in this record */
                if (str_len) {
                    read_input(filename, "reading string", f, dst_str, str_len, 0);
                    /* skip the padding, if any */
                    skip_bytes = ALIGN(str_len, sizeof(r)) * sizeof(r) - str_len;
                }
            } else {  /* skipping this string value */
                str_len -= copy_size;
                if (str_len) {
                    /* skip the next N records holding the reminder of the
                     * string value */
                    skip_bytes = ALIGN(str_len, sizeof(r)) * sizeof(r);
                }
            }

            if (skip_bytes) {
                static char buf[16 * 1024];  /* string discard buffer */
                read_input(filename, "skipping string", f, buf, skip_bytes, 0);
            }
        } else {
            total_field_count++;
            query_process_field(r.id, value);
        }
    }

    total_size += file_size;
    file_size = 0;

    if (is_lz4) pclose(f);
    else fclose(f);
}
#endif  // process_input_data2


static
void process_partial_query_results(char *filename)
{
    int is_lz4 = 0;

    FILE *f = fopen(filename, "r");
    if (!f) {
        /* it is fine for partial results to be missing -- moving on */
        if (errno == ENOENT) return;
        LOG("failed to open partial results file %s: %s\n", filename, strerror(errno));
        exit(1);
    }

    set_fread_buf(f);

    if (is_suffix(filename, ".lz4")) {
        is_lz4 = 1;
        fclose(f);  /* closing the original handle that we used to check for file existence */

        /* TODO: using exteranal lz4c decompressor for now */
        char command[1024];
        snprintf(command, sizeof(command), "./wam-read \"%s\"", filename);
        f = popen(command, "r");
        assert(f);
    }

    /* allocate static input data structre -- it remains constant for each query
     * partial result file */
    static wam_input_t *input = NULL;
    if (!input) {
        input = wam_input_alloc();

        /* when loading partial query results, length of input tuple header == length of output tuple header; moreover,
         * we can reuse the same buffer for input and for output */
        input->tuple_header_len = TUPLE_HEADER_LEN;
        input->tuple_header = tuple_header_buf;
    }

    input->file = f;
    input->filename = filename;

    int input_count, i;
    load_bytes(&input_count, sizeof(int), f);

    if (input_count) wam_strtbl_load(f);

    for (i = 0; i < input_count && !is_eoi; i++)
        load_query_tuple(input);

    if (is_lz4) pclose(f);
    else fclose(f);
}


static
void process_precomputed_results(char *filename)
{
    int is_lz4 = 0;

    FILE *f = wam_fopen_r(filename, args_direct_io);
    if (!f) {
        /* it is fine for precomputed data (materialized view) results to be
         * missing (this could happen on data expiration or when moving data to
         * its primary location) */
        if (errno == ENOENT) return;
        LOG("failed to open materialized view file %s: %s\n", filename, strerror(errno));
        exit(1);
    }

    set_fread_buf(f);

    if (is_suffix(filename, ".lz4")) {
        is_lz4 = 1;
        fclose(f);  /* closing the original handle that we used to check for file existence */

        /* TODO: using exteranal lz4c decompressor for now */
        char command[1024];
        snprintf(command, sizeof(command), "./wam-read \"%s\"", filename);
        f = popen(command, "r");
        assert(f);
    }
    total_wms_file_count++;

    wam_input_t *input = wam_input_alloc();
    input->file = f;
    input->filename = filename;

    load_subquery_header(input);
    wam_strtbl_load(f);

    while (1) {
        if (!load_subquery_tuple(input)) break;  /* eof */
        total_wms_record_count++;
    }

    wam_input_free(input);

    if (is_lz4) pclose(f);
    else fclose(f);
}


static
void process_file(char *filename)
{
    if (is_suffix(filename, ".wmq") || is_suffix(filename, ".wmq.lz4"))
        process_partial_query_results(filename);
    else if (is_suffix(filename, ".wms") || is_suffix(filename, ".wms.lz4"))
        process_precomputed_results(filename);
    else
        /*
        process_input_data(filename);
        process_input_data2(filename);
        */
        process_input_data(filename);

    if (wam_groupby_get_tuple_count() > 40 * 1000 * 1000) {
        LOG("error: too many results\n");
        exit(1);
    }
}


typedef
struct wam_merge_part {
    char *filename;
    char *id;
} wam_merge_part_t;


/* sort merge input by key */
static
int compare_merge_parts(const void *va, const void *vb)
{
    wam_merge_part_t *a = *(wam_merge_part_t **) va;
    wam_merge_part_t *b = *(wam_merge_part_t **) vb;

    return strcmp(a->id, b->id);
}


static
void sort_merge_parts(wam_merge_part_t **parts, int n)
{
    qsort(parts, n, sizeof(void *), compare_merge_parts);
}


static
void make_merge_part(char *arg, wam_merge_part_t *part)
{
    char *filename = arg;

    /* split id and filename separated by ':' */
    char *id_start = arg;
    char *id_end = strchr(id_start, ':');
    if (id_end) {
        filename = id_end + 1;
    } else {
        /* look for various id delimiters inside basename */
        char *last_slash = strrchr(arg, '/');
        char *basename = (last_slash) ? (last_slash + 1) : arg;

        id_start = basename;
        if (!id_end) id_end = strchr(id_start, '_');
        if (!id_end) id_end = strchr(id_start, '.');
    }

    if (id_end) {  /* found delimiter */
        part->id = strndup(id_start, id_end - id_start);
        part->filename = filename;
    } else {  /* no delimiters found => using basename as merge id (kind of a fallback) */
        part->id = id_start;
        part->filename = arg;
    }
}


static
wam_merge_part_t **make_merge_parts(int argc, char **argv)
{
    wam_merge_part_t *parts = (wam_merge_part_t *) malloc(argc * sizeof(wam_merge_part_t));
    assert(parts);

    wam_merge_part_t **part_ptrs = (wam_merge_part_t **) malloc(argc * sizeof(void *));
    assert(part_ptrs);

    int i;
    for (i = 0; i < argc; i++) {
        make_merge_part(argv[i], &parts[i]);
        part_ptrs[i] = &parts[i];
    }

    return part_ptrs;
}


/* chop extension ext from filename (only if it matches) */
#include <libgen.h>  // dirname, basename
static
char *rootname(char *filename, char *ext)
{
    int filename_len = strlen(filename);
    int ext_len = strlen(ext);

    int rootname_len = filename_len;
    if (filename_len > ext_len && !strcmp(filename + filename_len - ext_len, ext)) {
        rootname_len -= ext_len;
    }

    return strndup(filename, rootname_len);
}


static
char *read_merge_index_record(FILE *f)
{
    static char *line = NULL;
    static size_t cap = 0;

    /* NOTE: letting getline to alloc/realloc line and update line and cap
     * accordingly */
    ssize_t len = getline(&line, &cap, f);
    if (len == -1) {
        if (ferror(f)) {
            LOG("error reading index record: %s\n", strerror(errno));
            assert(0);
        } else {  // assuming EOF
            return NULL;
        }
    }

    /* chop newline */
    assert(line[len-1] == '\n');
    line[len-1] = '\0';

    return line;
}


static
void write_merge_index_record(FILE *f, char *str)
{
    if (fprintf(f, "%s\n", str) < 0) {
        LOG("error writing index record: %s\n", strerror(errno));
        assert(0);
    }
}


static
void wam_fclose_w(FILE *f, char *filename)
{
    if (fflush(f)) {
        LOG("failed to flush file %s: %s\n", filename, strerror(errno));
        exit(1);
    }
    if (fsync(fileno(f))) {
        LOG("failed to fsync file %s: %s\n", filename, strerror(errno));
        exit(1);
    }
    fclose(f);
}


#include <stdarg.h>
static
char *wam_sprintf(char *fmt, ...)
{
    char *p;
    va_list ap;
    va_start(ap, fmt);
    int len = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);

    char *res = (char *) malloc(len + 1);
    va_start(ap, fmt);
    vsnprintf(res, len + 1, fmt, ap);
    va_end(ap);

    return res;
}


/* returns prev_index_filename */
static
char *process_merge_index(char *merged_filename, char *output_filename, int merge_generation, wam_merge_part_t **parts, int num_parts)
{
    FILE *curr_index_file = NULL;
    FILE *next_index_file = NULL;;

    char *curr_index_filename = NULL;

    char *base_output_filename = rootname(output_filename, ".wms");
    char *next_index_filename = wam_sprintf("%s.index.%d", base_output_filename, merge_generation + 1);

    next_index_file = fopen(next_index_filename, "w");
    if (!next_index_file) {
        LOG("failed to open next index file %s: %s\n", next_index_filename, strerror(errno));
        exit(1);
    }

    /* sort merge parts by ids */
    sort_merge_parts(parts, num_parts);

    /* check that there are no duplicate merge ids */
    int i;
    for (i = 1; i < num_parts; i++) {
        if (!strcmp(parts[i-1]->id, parts[i]->id)) {
            LOG("duplicate merge part ids: %s\n", parts[i]->id);
            exit(1);
        }
    }

    if (merge_generation > 0) {
        /* open index file from previous merges */
        char *base_merged_filename = rootname(merged_filename, ".wms");
        curr_index_filename = wam_sprintf("%s.index.%d", base_merged_filename, merge_generation);

        curr_index_file = fopen(curr_index_filename, "r");
        if (!curr_index_file) {
            LOG("failed to open current index file %s: %s\n", curr_index_filename, strerror(errno));
            exit(1);
        }

        /* zip sorted new merge ids and already merged ids from the sorted
         * index; remove duplicates while zipping */
        char *merged_id = read_merge_index_record(curr_index_file);
        int i = 0;

        while (merged_id || i < num_parts) {
            int cmp;

            if (merged_id && i < num_parts) {
                cmp = strcmp(merged_id, parts[i]->id);
            } else if (merged_id) {
                cmp = -1;
            } else if (i < num_parts) {
                cmp = 1;
            }

            if (cmp <= 0) {
                /* duplicate merge part => skip during merging */
                if (cmp == 0) {
                    /* TODO: print a warning about dupicate id */
                    parts[i]->id = NULL;  /* mark as duplicate */
                    i++;  /* move to next */
                }

                write_merge_index_record(next_index_file, merged_id);
                merged_id = read_merge_index_record(curr_index_file);  /* read next */
            } else {  /* cmp > 0 */
                write_merge_index_record(next_index_file, parts[i]->id);
                i++;  /* move to next */
            }
        }
    } else {
        /* NOTE: current index is missing if this is the first merge, i.e.
         * merge_generation = 0 => just write sorted newly merged ids */
        int i;
        for (i = 0; i < num_parts; i++) {
            write_merge_index_record(next_index_file, parts[i]->id);
        }
    }

    wam_fclose_w(next_index_file, next_index_filename);
    if (curr_index_file) fclose(curr_index_file);  /* read-only */

    if (merged_filename == output_filename) {
        return curr_index_filename;  /* old index to delete, if any */
    } else {
        /* not deleting old index, because new output location is completely different */
        return NULL;
    }
}


static
void wam_fsync_dir(char *dirname)
{
    int fd = open(dirname, O_RDONLY);
    assert(fd >= 0);

    if (fsync(fd)) {
        LOG("failed to fsync directory %s: %s\n", dirname, strerror(errno));
        exit(1);
    }

    close(fd);
}


/* read next record */
static
void preload_next_record(wam_input_t *input)
{
    if (!load_tuple_header(input)) {
        /* end of input */
        //LOG("EOF %s\n", input->filename);
        fclose(input->file);
        wam_input_free(input);
        return;
    }
    total_wms_record_count++;
    //LOG("preload record from %s\n", input->filename);

    /* NOTE: there is no filter on merges; all input tuple match -- no need to
     * check for it */
    preprocess_subquery_tuple_header(input);

    /* add the newly loaded item to the priority queue (which is a min heap
     * by group key)
     *
     * NOTE: we are pushing the whole input state here to facilitate looping
     * after wam_pqueue_pop() */
    wam_pqueue_push(input);
}


static
wam_input_t *init_merge_input(char *filename)
{
    FILE *f = wam_fopen_r(filename, args_direct_io);
    if (!f) {
        /* XXX: it is fine for summary part to be missing -- e.g. when we delete
         * corrupted input to get things going again */
        if (errno == ENOENT) return NULL;

        LOG("failed to open summary file %s: %s\n", filename, strerror(errno));
        exit(1);
    }

    total_wms_file_count++;

    wam_input_t *input = wam_input_alloc();
    input->file = f;
    input->filename = filename;

    load_subquery_header(input);
    wam_strtbl_load(f);

    return input;
}


/* incrementally merge sorted materialized views files */
static
void merge_precomputed_results(char *merged_filename, char* output_filename, int argc, char **argv)
{
    wam_pqueue_init();

    char *tmp_filename = wam_sprintf("%s.tmp", output_filename);
    output_file = wam_fopen_w(tmp_filename, args_direct_io);
    assert(output_file);

    int merge_generation = 0;
    wam_input_t *merged_input = init_merge_input(merged_filename);
    if (merged_input) {
        /* take merge generation number from the opened merged input */
        merge_generation = last_merge_generation;
    }

    wam_merge_part_t **parts = make_merge_parts(argc, argv);

    /* rewrite merge index and filter out duplicates */
    char *prev_index_filename = process_merge_index(merged_filename, output_filename, merge_generation, parts, argc);

    /* list of opened inputs
     *
     * adding merged results as first element (if present) */
    wam_input_t **inputs = (wam_input_t **) malloc((argc + 1) * sizeof(void *));
    int inputs_i = 0;
    if (merged_input) {
        inputs[inputs_i++] = merged_input;
    }

    /* init merge input */
    int i;
    for (i = 0; i < argc; i++) {
        /* NOTE: entries with duplicate ids are marked by id = NULL in
         * process_merge_index() -- skipping them during merge
         *
         * TODO: make the whole merge pass a no-op if all input parts are
         * duplicates */
        if (!parts[i]->id) continue;

        wam_input_t *input = init_merge_input(parts[i]->filename);
        if (input) {
            inputs[inputs_i++] = input;
        }
    }

    store_subquery_header(output_file, merge_generation + 1);
    wam_strtbl_store(output_file);

    /* if we have incompatible descriptor at least one input, we can't use N-way
     * merge and need to merge using a hashtable, plus sort afterwards */
    if (is_subquery_tuple_descriptor_compatible) {
        /* proceed with N-way merge */

        /* seed merge state with the first record */
        {
            int i;
            for (i = 0; i < inputs_i; i++) {
                preload_next_record(inputs[i]);
            }
        }

        /* initialize output row */
        tuple_t output;
        query_init_merge_tuple(&output);

        /* seed the main loop: dequeue the next item with the smallest group key */
        wam_input_t *input = wam_pqueue_pop();

        /* main loop */
        while (input) {
            /* copy the group key to the output row */
            memcpy(&output, input->key, groupby_key_size);

            /* populate output state with the aggregate state from the first file
             *
             * plus, refill the merge state; we need to do it here beore reading
             * other inputs, because there's a chance we'll load next item from the
             * same input -- this could happen on key downsampling as we are running
             * the query, i.e. lowering ts resolution
             *
             * TODO, XXX: postpone loading: if none of the other inputs have this
             * key, there's no need to load the state, we can just copy serialized
             * state straight to the output -- the solution would probably involve
             * copying tuple items to a temporary buffer, because we can take
             * another item from the same file */
            load_subquery_tuple_items(&output, input);
            preload_next_record(input);

            wam_input_t *next_input;
            while (1) {
                next_input = wam_pqueue_pop();
                if (!next_input) break;  /* end of input */

                 if (memcmp(next_input->key, &output, groupby_key_size)) {
                     /* keys are different => flushing output for the current key
                      * and moving on to the next key */
                     break;
                     //LOG("keys are different\n");
                 } else {
                     /* keys are equal => merging */
                     //LOG("keys are equal => merging\n");
                     load_subquery_tuple_items(&output, next_input);
                     preload_next_record(next_input);
                 }
            }

            /* keys are different or end of input => flushing current output record
             * & promoting next item/key to the current item/key */
            store_query_tuple(&output);
            query_reset_merge_tuple(&output);

            result_count++;

            input = next_input;
        }
    } else {  /* !is_subquery_tuple_descriptor_compatible */

        /* set global variable indicating that this query performes summary
         * resampling; wam_ucount uses this flag to prevent loss of accuracy */
        wam_summary_resampling_mode = 1;

        /* process all records from this input at once using a hashtable for
         * group-by
         *
         * NOTE: same code as in process_precomputed_results() doing the same
         * thing -- querying subquery as keys were in random order
         *
         * XXX: consider using radix instead of a hashtable as the data should
         * be partially sorted and key length is single or low double-digit --
         * this may make memory representation more compact and eliminate the
         * need for sorting afterwards */
        int i;
        for (i = 0; i < inputs_i; i++) {
            while (1) {
                if (!load_subquery_tuple(inputs[i])) break;  /* eof */
                total_wms_record_count++;
            }

            /* end of input */
            //log("EOF %s\n", input->filename);
            fclose(inputs[i]->file);
            wam_input_free(inputs[i]);
        }

        /* sort results by groupby key */
        int result_count = wam_groupby_get_tuple_count();

        wam_orderby_init(result_count);
        wam_groupby_foreach_tuple(wam_orderby_visit, result_count);
        tuple_t **ordered_tuples = wam_orderby_group_key();

        for (i = 0; i < result_count; i++) {
            store_query_tuple(ordered_tuples[i]);
        }
    }

    /* all done and written to output */

    wam_pqueue_free();

    wam_fclose_w(output_file, tmp_filename);

    if (rename(tmp_filename, output_filename)) {
        LOG("failed to rename %s to %s: %s\n", tmp_filename, output_filename, strerror(errno));
        exit(1);
    }

    if (prev_index_filename) {
        /* NOTE, TODO: ignoring errors here -- not a critical failrue, ideally
         * we should print a warning */
        unlink(prev_index_filename);
    }

    /* fsync parent directory to "commit" output file renaming, new merge index
     * file creation and unlinking of old index (if in the same directory) */
    wam_fsync_dir(dirname(output_filename));

    /* cleanup merge input after successful merge */
    if (args_delete_merge_input) {
        for (i = 0; i < argc; i++) {
            /* NOTE, TODO: ideally we should print a warning on error unless ENOENT
             */
            unlink(parts[i]->filename);
        }
    }
}


int main(int argc, char *argv[])
{
    /* somehow, at least on Mac OS X, this signal handler is inherited when
     * wam_query is started from Erlang */
    signal(SIGFPE, SIG_DFL);

    int read_notify = 0;
    uint64_t random_seed_value = 0;
    uint64_t *random_seed = NULL;

    char *output_filename = NULL;  /* -o */
    char *merged_filename = NULL;  /* -m */

    int i;
    for (i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-p")) {
            output_mode = OUTPUT_QUERY_PARTIAL;
        } else if (!strcmp(argv[i], "-s")) {
            output_mode = OUTPUT_SUBQUERY;
        } else if (!strcmp(argv[i], "-m")) {
            i++;
            merged_filename = argv[i];
            output_mode = OUTPUT_SUBQUERY_MERGE;
        } else if (!strcmp(argv[i], "--split-by-hour")) {
            split_by_hour = 1;
        } else if (!strcmp(argv[i], "--output-prefix")) {
            i++;
            output_prefix = argv[i];
        } else if (!strcmp(argv[i], "--output-suffix")) {
            i++;
            output_suffix = argv[i];
        } else if (!strcmp(argv[i], "--delete-merge-input")) {
            args_delete_merge_input = 1;
        } else if (!strcmp(argv[i], "--mpack")) {
            /* use MessagePack output format */
            args_use_mpack_output_format = 1;
        } else if (!strcmp(argv[i], "--json")) {
            /* use json output format (default) */
            args_use_json_output_format = 1;
        } else if (!strcmp(argv[i], "--random-seed")) {
            i++;
            random_seed_value = atoi(argv[i]);
            random_seed = &random_seed_value;
        } else if (!strcmp(argv[i], "-R")) {
            read_notify = 1;
        } else if (!strcmp(argv[i], "--debug")) {
            args_debug = 1;
            // enable core dumps
            struct rlimit core_limits;
            core_limits.rlim_cur = core_limits.rlim_max = RLIM_INFINITY;
            setrlimit(RLIMIT_CORE, &core_limits);
        } else if (!strcmp(argv[i], "-o")) {
            i++;
            output_filename = argv[i];
        } else if (!strcmp(argv[i], "--nice")) {
            args_nice = 1;
#ifdef __FreeBSD__
            /* drop priority from real-time (as we are called from real-time beam) to
             * normal so that running queries doesn't interfere with whatever runs in
             * Erlang
             *
             * XXX: perhaps, idle priority is a better choice, but unfortunately
             * FreeBSD doesn't let non-root users to change priority to idle */
            static struct rtprio rtp;
            rtp.type = RTP_PRIO_NORMAL;
            rtp.prio = 20;  /* nice -- NOTE: for some reason this has no effect and nice is set to 0 instead */
            rtprio(RTP_SET, 0, &rtp);

            /* setting nice manually to the lowest priority */
            setpriority(PRIO_PROCESS, 0, 20);
#endif
        } else if (!strcmp(argv[i], "--direct-io")) {
            args_direct_io = 1;
        } else {  // unknown option or positional argument
            break;
        }
    }

    output_file = stdout;
    if (output_filename && output_mode != OUTPUT_SUBQUERY_MERGE) {
        output_file = wam_fopen_w(output_filename, args_direct_io);
        assert(output_file);
    }

    /* library initialization */
    wam_random_init(random_seed);
    wam_smptbl_init();
    wam_groupby_init(groupby_key_size, sizeof(tuple_t));

    /* among other things, sets is_stream_mode used right below */
    query_init();
    if (!is_stream_mode) {
        wam_strtbl_init();
    } else {
       /* targeting max 102400 strings with average size of 64 bytes -- this is
        * per buffer, which should be plenty; NOTE: implementation doesn't have
        * specific limits, anything larger will just trigger dynamic memory
        * allocations as needed */
        wam_strtbl_init_without_dedup(102400, 64);
    }
    wam_output_init(output_file, args_use_mpack_output_format? WAM_OUTPUT_MPACK: WAM_OUTPUT_JSON);

    /* use JSON output format by default */
    if (!args_use_mpack_output_format) {
        args_use_json_output_format = 1;
    }

    if (output_mode == OUTPUT_SUBQUERY_MERGE) {

        if (!output_filename) output_filename = merged_filename;

        merge_precomputed_results(merged_filename, output_filename, argc - i, &argv[i]);

    } else if (i < argc) {  // more command-line args?
        for (; i < argc && !is_eoi; i++) {
            //LOG("processing file: %s\n", argv[i]);
            process_file(argv[i]);
        }
    } else {
        /* reading filenames from stdin */
        char filename[1024];

        while (fgets(filename, sizeof(filename), stdin)) {
            filename[strlen(filename) - 1] = '\0';  /* chop trailing \n */

            if (!filename[0]) break; /* treat empty string as EOF */

            /* make sure we read all input filenames to prevent a deadlock */
            if (!is_eoi) process_file(filename);

            if (read_notify) LOG("done\n");
        }
    }

    if (output_mode != OUTPUT_SUBQUERY && output_mode != OUTPUT_SUBQUERY_MERGE) {
        query_postprocess();
    }

    LOG("wam-query:\n");
    LOG("total_results = %d\n", result_count);
    LOG("file_count = %" PRIu64 ", buffer_count = %" PRIu64 ", record_count = %" PRIu64 ", size = %" PRIu64 ", field_count = %" PRIu64 ", event_count = %" PRIu64 "\n",
        total_file_count,
        buffer_count - 1,
        total_record_count, total_size,
        total_field_count, event_count - 1
    );
    LOG("string_count = %" PRIu64 ", string_size = %" PRIu64 ", read_string_count = %" PRIu64 ", read_string_size = %" PRIu64 "\n",
        total_string_count, total_string_size,
        read_string_count, read_string_size
    );
    LOG("wms_file_count = %" PRIu64 ", wms_record_count = %" PRIu64 "\n",
        total_wms_file_count,
        total_wms_record_count
    );

    if (output_mode != OUTPUT_SUBQUERY && output_mode != OUTPUT_SUBQUERY_MERGE) {
        /* output final or partial query results */
        query_output();
    } else if (output_mode == OUTPUT_SUBQUERY && matching_count) {
        /* subquery (i.e. summarized) query results
         *
         * NOTE: not storing anything if there are't any matching rows (i.e.
         * results) */
        int result_count = wam_groupby_get_tuple_count();

        /* materialized views must have their output sorted by group key; and
         * group key at least contains 'ts' */
        wam_orderby_init(result_count);
        wam_groupby_foreach_tuple(wam_orderby_visit, result_count);
        tuple_t **ordered_tuples = wam_orderby_group_key();

        int i = 0;
        if (!split_by_hour) {
            /* plain subquery output */
            store_subquery_header(output_file, 0 /* merge generation */);
            wam_strtbl_store(output_file);
            for (i = 0; i < result_count; i++) {
                store_query_tuple(ordered_tuples[i]);
            }
        } else {
            /* split output by UTC hour */
            int prev_hour = 0;
            char output_filename[1024];

            output_file = NULL;
            for (i = 0; i < result_count; i++) {
                /* timestamp is always the first 64-bit field of the output
                 * tuple
                 *
                 * NOTE: can't always access it by name (ordered_tuples[i]->ts),
                 * because other queries may not request 'ts' field at all */
                int current_ts = be64toh(*(uint64_t *)(ordered_tuples[i]));
                int hour = current_ts / 3600;
                if (hour != prev_hour) {
                    prev_hour = hour;

                    /* close previous output file */
                    if (output_file)
                        fclose(output_file);

                    /* create a new output file */
                    sprintf(output_filename, "%s_%d-%d.wms%s", output_prefix, hour * 3600, hour * 3600 + 3600 - 1, output_suffix);
                    output_file = wam_fopen_w(output_filename, args_direct_io);
                    if (!output_file) {
                        LOG("failed to open output file %s: %s\n", output_filename, strerror(errno));
                        exit(1);
                    }

                    /* first, write the header and the string table */
                    store_subquery_header(output_file, 0 /* merge generation */);
                    wam_strtbl_store(output_file);
                }

                store_query_tuple(ordered_tuples[i]);
            }
            /* close the last output file */
            fclose(output_file);
            output_file = NULL;
        }
    }

    /* close the last output file */
    if (output_file && output_file != stdout) {
        fclose(output_file);
    }

    /* log memory usage */
    struct rusage r_usage;
    assert(getrusage(RUSAGE_SELF, &r_usage) == 0);
#ifdef __FreeBSD__ // in FreeBSD it is measured in KB
    r_usage.ru_maxrss *= 1024;
#endif
    LOG("max_rss = %ldM\n",
        r_usage.ru_maxrss / 1024 / 1024
    );

    return 0;
}
