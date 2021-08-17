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

/* JSON and MessagePack query output
 *
 * TODO,XXX: buffered (line-buffered?) JSON output
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <math.h> // isfinite
#include <inttypes.h>  // portable printf format for ints

#include "wam_output.h"


/* contiguous and automatically expanding byte buffer */
typedef
struct buf {
    int cap;  /* capactity, i.e. allocated space */
    int size;  /* size of the buffer contents */
    char *data;  /* the actual buffer space */
} buf_t;


/* min unit for malloc'ing or realloc'ing the buffer */
#define BUF_ALLOC_UNIT_SIZE 4096

/* round up (x / size), i.e. add padding after fitting x mod size elements */
#define BUF_ALLOC_SIZE_2(min_size, unit_size) (((min_size) + (unit_size) - 1) / (unit_size) * (unit_size))

#define BUF_ALLOC_SIZE(min_size) BUF_ALLOC_SIZE_2(min_size, BUF_ALLOC_UNIT_SIZE)


static
char *buf_alloc(buf_t *buf, int size)
{
    char *res;

    if (!buf->data) {
        /* allocate buffer lazily when called for the first time */
        int init_cap = BUF_ALLOC_SIZE(size);

        buf->data = malloc(init_cap);
        assert(buf->data);

        buf->cap = init_cap;
        buf->size = 0;
    } else if (buf->size + size > buf->cap) {
        /* re-allocate existing buffer if it doesn't have enough capacity */
        int new_cap = BUF_ALLOC_SIZE(buf->size + size);

        buf->data = realloc(buf->data, new_cap);
        assert(buf->data);

        buf->cap = new_cap;
    }

    res = buf->data + buf->size;
    buf->size += size;

    return res;
}

static
void buf_init(buf_t *buf)
{
    memset(buf, 0, sizeof(*buf));
}

static
void buf_reset(buf_t *buf)
{
    buf->size = 0;
}

/* NOTE: unused
static
void buf_free(buf_t *buf)
{
    buf_reset(buf);

    free(buf->data);
    buf->data = NULL;

    buf->cap = 0;
}
*/


enum output_level_type {
    LEVEL_TYPE_ARRAY = 1,
    LEVEL_TYPE_MAP = 2
};


typedef
struct level_info {
    enum output_level_type type;
    int count;
    int count_offset;
} level_info_t;


struct wam_output {
    enum wam_output_format format;
    FILE *file;
    uint64_t written_size;

    buf_t buf;

    int level;
    level_info_t level_info[4];  /* nesting is allowed up to 4 levels */
} output;


void wam_output_init(FILE *output_file, enum wam_output_format output_format)
{
    output.file = output_file;
    output.format = output_format;
    output.level = 0;
    output.written_size = 0;

    buf_init(&output.buf);
}


static inline
level_info_t *get_level_info(void)
{
    assert(output.level > 0);  /* can't be called at top-level */

    return &output.level_info[output.level-1];
}


int wam_output_get_current_level(void)
{
    return output.level;
}


uint64_t wam_output_get_current_offset(void)
{
    //assert(output.written_size == ftell(output.file));

    /* written + buffered size */
    return output.written_size + output.buf.size;
}


/* called only for buffered mpack output */
uint32_t wam_output_get_current_buffer_size(void)
{
    return output.buf.size;
}


static inline
void buffer_bytes(char *str, int len)
{
    char *dst = buf_alloc(&output.buf, len);
    memcpy(dst, str, len);
}


static inline
void buffer_byte(int c)
{
    uint8_t *dst = (uint8_t *)buf_alloc(&output.buf, 1);
    *dst = c;
}


static
void write_bytes(const char *data, int size)
{
    int res = fwrite(data, size, 1, output.file);
    assert(res == 1);

    output.written_size += size;

    //assert(output.written_size == ftell(output.file));
}


static
void write_char(int c)
{
    putc(c, output.file);
    output.written_size++;

    //assert(output.written_size == ftell(output.file));
}


// msapack tags, see spec below
// https://github.com/msgpack/msgpack/blob/master/spec.md#types
#define TAG_NULL              (0xc0)
#define TAG_BOOL(b)           ((b) ? 0xc3 : 0xc2)

#define TAG_UINT_8            (0xcc)
#define TAG_UINT_16           (0xcd)
#define TAG_UINT_32           (0xce)
#define TAG_UINT_64           (0xcf)

#define TAG_INT_8             (0xd0)
#define TAG_INT_16            (0xd1)
#define TAG_INT_32            (0xd2)
#define TAG_INT_64            (0xd3)

#define TAG_FLOAT_64          (0xcb)

#define TAG_FIXED_ARRAY(n)    (0x90 | (n))
#define TAG_ARRAY_16          (0xdc)
#define TAG_ARRAY_32          (0xdd)

#define TAG_FIXED_MAP(n)      (0x80 | (n))
#define TAG_MAP_16            (0xde)
#define TAG_MAP_32            (0xdf)

#define TAG_FIXED_STR(n)      ((uint8_t)(0xa0 + (n)))
#define TAG_STR_8             (0xd9)
#define TAG_STR_16            (0xda)
#define TAG_STR_32            (0xdb)


// TODO optimization later cause know byte order
#define store8(p, d) do{*(p) = (uint8_t)(d);} while(0);

#define store16(p, d) do{                 \
    (p)[0] = (uint8_t)((d) >> 8);         \
    (p)[1] = (uint8_t)(d);                \
  } while(0);

#define store32(p, d) do{                 \
    (p)[0] = (uint8_t)((d) >> 24);        \
    (p)[1] = (uint8_t)((d) >> 16);        \
    (p)[2] = (uint8_t)((d) >> 8);         \
    (p)[3] = (uint8_t)(d);                \
  } while(0);

#define store64(p, d) do{                 \
    (p)[0] = (uint8_t)((d) >> 56);        \
    (p)[1] = (uint8_t)((d) >> 48);        \
    (p)[2] = (uint8_t)((d) >> 40);        \
    (p)[3] = (uint8_t)((d) >> 32);        \
    (p)[4] = (uint8_t)((d) >> 24);        \
    (p)[5] = (uint8_t)((d) >> 16);        \
    (p)[6] = (uint8_t)((d) >> 8);         \
    (p)[7] = (uint8_t)(d);                \
  } while(0);


// buffer mpack with tag and content
#define buffer_mpack_0(t)                                 \
{                                                         \
    uint8_t *dst = (uint8_t *)buf_alloc(&output.buf, 1);  \
    store8(dst, t);                                       \
}

#define buffer_mpack_1(t, d)                              \
{                                                         \
    uint8_t *dst = (uint8_t *)buf_alloc(&output.buf, 2);  \
    store8(dst, t);                                       \
    store8(dst + 1, d);                                   \
}

#define buffer_mpack_2(t, d)                              \
{                                                         \
    uint8_t *dst = (uint8_t *)buf_alloc(&output.buf, 3);  \
    store8(dst, t);                                       \
    store16(dst + 1, d);                                  \
}

#define buffer_mpack_4(t, d)                              \
{                                                         \
    uint8_t *dst = (uint8_t *)buf_alloc(&output.buf, 5);  \
    store8(dst, t);                                       \
    store32(dst + 1, d);                                  \
}

#define buffer_mpack_8(t, d)                              \
{                                                         \
    uint8_t *dst = (uint8_t *)buf_alloc(&output.buf, 9);  \
    store8(dst, t);                                       \
    store64(dst + 1, d);                                  \
}


void wam_output_flush(void)
{
    assert(output.level == 0);

    /* NOTE: we don't implement buffering for WAM_OUTPUT_JSON format */
    if (output.format == WAM_OUTPUT_MPACK) {
        write_bytes(output.buf.data, output.buf.size);
        buf_reset(&output.buf);
    }
}


/* NOTE: unused
static
void write_cstring(char *str)
{
    write_bytes(str, strlen(str));
}
*/


static
void write_quoted_string(char *str, int len)
{
    write_char('"');
    write_bytes(str, len);
    write_char('"');
}


static
void mpack_u32(char *ptr, uint32_t x)
{
    store32(ptr, x);
}


static
void write_quoted_cstring(char *str)
{
    write_quoted_string(str, strlen(str));
}


static inline
void output_level_begin(enum output_level_type level_type, int count_offset)
{
    output.level++;

    level_info_t *level_info = get_level_info();

    level_info->type = level_type;
    level_info->count = 0;
    level_info->count_offset = count_offset;
}


inline static
void output_level_end(void)
{
    if (output.format == WAM_OUTPUT_MPACK) {
        level_info_t *level_info = get_level_info();
        if (level_info->count_offset >= 0) {  /* counting mode */
            /* fill in actual count in the reserved placeholder */
            mpack_u32(&output.buf.data[level_info->count_offset], level_info->count);
        }
    }

    output.level--;
}


static
void output_level_add_array_item(void)
{
    if (output.level == 0) {  /* top-level => nothing to do */
        return;
    }

    level_info_t *level_info = get_level_info();
    if (level_info->type == LEVEL_TYPE_ARRAY) {
        if (output.format == WAM_OUTPUT_JSON && level_info->count) {
            write_char(',');  /* write a separator unless the first item */
        }
        level_info->count++;
    }
}


static
int buffer_u32_placeholder(void)
{
    int offset = output.buf.size;
    buf_alloc(&output.buf, 4);
    return offset;
}


static
int mpack_array_begin(int count)
{
    int count_offset = -1;

    if (count >= 0) {  /* count is known upfront */
        if (count < 16) {
            buffer_mpack_0(TAG_FIXED_ARRAY(count));
        } else if (count < 65536) {
            buffer_mpack_2(TAG_ARRAY_16, count);
        } else {
            buffer_mpack_4(TAG_ARRAY_32, count);
        }
    } else {  /* count is unknown upfront, count the items and backfill once all items are written */
        buffer_mpack_0(TAG_ARRAY_32);

        /* buffer 32-bit count placeholder */
        count_offset = buffer_u32_placeholder();
    }

    return count_offset;
}


static
int mpack_map_begin(int count)
{
    int count_offset = -1;

    if (count >= 0) {  /* count is known upfront */
        if (count < 16) {
            buffer_mpack_0(TAG_FIXED_MAP(count));
        } else if (count < 65536) {
            buffer_mpack_2(TAG_MAP_16, count);
        } else {
            buffer_mpack_4(TAG_MAP_32, count);
        }
    } else {  /* count is unknown upfront, count the items and backfill once all items are written */
        buffer_mpack_0(TAG_MAP_32);

        /* buffer 32-bit count placeholder */
        count_offset = buffer_u32_placeholder();
    }

    return count_offset;
}


void wam_output_array_begin(int count)
{
    output_level_add_array_item();

    int count_offset = -1;

    if (output.format == WAM_OUTPUT_JSON) {
        write_char('[');
    } else if (output.format == WAM_OUTPUT_MPACK) {
        count_offset = mpack_array_begin(count);
    }

    output_level_begin(LEVEL_TYPE_ARRAY, count_offset);
}


void wam_output_map_begin(int count)
{
    output_level_add_array_item();

    int count_offset = -1;

    if (output.format == WAM_OUTPUT_JSON) {
        write_char('{');
    } else if (output.format == WAM_OUTPUT_MPACK) {
        count_offset = mpack_map_begin(count);
    }

    output_level_begin(LEVEL_TYPE_MAP, count_offset);
}


void wam_output_array_begin_counting(void)
{
    wam_output_array_begin(-1);
}


void wam_output_map_begin_counting(void)
{
    wam_output_map_begin(-1);
}


void wam_output_array_end(void)
{
    if (output.format == WAM_OUTPUT_JSON) {
        write_char(']');
    } else if (output.format == WAM_OUTPUT_MPACK) {
        /* nothing to do */
    }
    output_level_end();
}


void wam_output_map_end(void)
{
    if (output.format == WAM_OUTPUT_JSON) {
        write_char('}');
    } else if (output.format == WAM_OUTPUT_MPACK) {
        /* nothing to do */
    }
    output_level_end();
}


static
void mpack_string(char *str, int len)
{
    if (len < 0) {
        return;
    } else if (len < 32) {
        buffer_mpack_0(TAG_FIXED_STR(len));
    } else if (len < 256) {
        buffer_mpack_1(TAG_STR_8, len);
    } else if (len < 65536) {
        buffer_mpack_2(TAG_STR_16, len);
    } else {
        buffer_mpack_4(TAG_STR_32, len);
    }

    buffer_bytes(str, len);
}


static
void mpack_cstring(char *str)
{
    mpack_string(str, strlen(str));
}


static
void mpack_double(double x)
{
    uint64_t tmp = *((uint64_t*)&x);
    buffer_mpack_8(TAG_FLOAT_64, tmp);
}


static
void mpack_int64(int64_t x)
{
    if (x >127)
    {
        if (x < 256) {
            buffer_mpack_1(TAG_UINT_8, x);
        } else if(x < 0x10000L) {
            buffer_mpack_2(TAG_UINT_16, x);
        } else if (x < 0x100000000LL) {
            buffer_mpack_4(TAG_UINT_32, x);
        } else {
            buffer_mpack_8(TAG_UINT_64, x);
        }
    } else {
        if (x >= -32) {
            buffer_mpack_0(x);
        } else if(x >= -128) {
            buffer_mpack_1(TAG_INT_8, x);
        } else if (x >= -32768) {
            buffer_mpack_2(TAG_INT_16, x);
        } else if (x >= (int64_t)0xffffffff80000000LL) {
            buffer_mpack_4(TAG_INT_32, x);
        } else {
            buffer_mpack_8(TAG_INT_64, x);
        }
    }
}


static
void mpack_bool(int64_t x)
{
    buffer_mpack_0(TAG_BOOL(x));
}


static
void mpack_null(void)
{
    buffer_mpack_0(TAG_NULL);
}


void wam_output_map_key(char *str)
{
    level_info_t *level_info = get_level_info();

    if (output.format == WAM_OUTPUT_JSON) {
        if (level_info->count) {
            write_char(',');  /* write a separator unless the first item */
        }
        write_quoted_cstring(str);
        write_char(':');
    } else if (output.format == WAM_OUTPUT_MPACK) {
        mpack_cstring(str);
    }

    level_info->count++;
}


void wam_output_map_string_field(char *key, char *value)
{
    wam_output_map_key(key);
    wam_output_string(value, strlen(value));
}


void wam_output_cstring(char *str)
{
    output_level_add_array_item();

    if (output.format == WAM_OUTPUT_JSON) {
        write_quoted_cstring(str);
    } else if (output.format == WAM_OUTPUT_MPACK) {
        mpack_cstring(str);
    }
}


#define write_literal(s) write_bytes(s, sizeof(s)-1)
#define write_printf(...) do{ output.written_size += fprintf(output.file, __VA_ARGS__); }while(0)
#define write_quoted_printf(...) do{ write_char('"'); write_printf(__VA_ARGS__); write_char('"'); }while(0)

/* NOTE, TODO: these are JSON only */
#define wam_output_printf(...) do{ output_level_add_array_item(); write_printf(__VA_ARGS__); }while(0)
#define wam_output_quoted_printf(...) do{ output_level_add_array_item(); write_quoted_printf(__VA_ARGS__); }while(0)


#define wam_output_literal(s) wam_output_literal_string(s, sizeof(s)-1)
void wam_output_literal_string(char *str, int len)
{
    output_level_add_array_item();

    if (output.format == WAM_OUTPUT_JSON) {
        write_quoted_string(str, len);
    } else if (output.format == WAM_OUTPUT_MPACK) {
        mpack_string(str, len);
    }
}


void wam_output_null(void)
{
    output_level_add_array_item();

    if (output.format == WAM_OUTPUT_JSON) {
        write_literal("null");
    } else if (output.format == WAM_OUTPUT_MPACK) {
        mpack_null();
    }
}


void wam_output_int64(int64_t x)
{
    output_level_add_array_item();

    if (output.format == WAM_OUTPUT_JSON) {
        write_printf("%" PRId64, x);
    } else if (output.format == WAM_OUTPUT_MPACK) {
        mpack_int64(x);
    }
}


void wam_output_int64_as_string(int64_t x)
{
    output_level_add_array_item();

    if (output.format == WAM_OUTPUT_JSON) {
        write_quoted_printf("%" PRId64, x);
    } else if (output.format == WAM_OUTPUT_MPACK) {
        static char str[32];
        int len = snprintf(str, sizeof(str), "%" PRId64, x);
        mpack_string(str, len);
    }
}


void wam_output_double(double x)
{
    output_level_add_array_item();

    if (output.format == WAM_OUTPUT_JSON) {
        if (!isfinite(x)) {
            write_literal("null");
        } else {
            // NOTE: 17 digits guarantees us full double precision in decimal
            // representation
            write_printf("%.17g", x);
        }
    } else if (output.format == WAM_OUTPUT_MPACK) {
        mpack_double(x);
    }
}


void wam_output_bool(int64_t x)
{
    output_level_add_array_item();

    if (output.format == WAM_OUTPUT_JSON) {
        if (x) {
            write_literal("true");
        } else {
            write_literal("false");
        }
    } else if (output.format == WAM_OUTPUT_MPACK) {
        mpack_bool(x);
    }
}


/* if ut8 sequence is correct: return 1 and the length of correct sequence
 * else: return 0 and the length of incorrect sequence
 */
static
int check_utf8(unsigned char *s, int max_len, int *out_len)
{
    int len;
    int c = s[0];

    /* TODO: optimize (count the number of leading ones or use a table) */
    if ((c & 0xe0) == 0xc0) {  // 110x xxxx
        len = 2;
    } else if ((c & 0xf0) == 0xe0) {  // 1110 xxxx
        len = 3;
    } else if ((c & 0xf8) == 0xf0) {  // 1111 0xxx
        len = 4;
    } else if ((c & 0xfc) == 0xf8) {  // 1111 10xx
        len = 5;
    } else if ((c & 0xfe) == 0xfc) {  // 1111 110x
        len = 6;
    } else {  // none of the above
        *out_len = 1;
        return 0;  // failure
    }

    if (len > max_len) {
        *out_len = max_len;
        return 0;  // failure
    }

    // check that all characters 1..(len-1) are 10xx xxxx
    int i;
    for (i = 1; i < len; i++) {
        c = s[i];
        if ((c & 0xc0) != 0x80) {
            *out_len = i;  // failure
            return 0;
        }
    }

    *out_len = len;
    return 1;  // success
}


static
void write_json_string_contents(char *str, int str_len)
{
    int i, c;
    for (i = 0; i < str_len; i++) {
        c = ((unsigned char *)str)[i];
        if (c >= 0x20 && c <= 0x7E /* is_print(c)? */ && c != '"' && c != '\\') {
            write_char(c);
        } else {  /* escape non-printable characters */
            switch (c) {
                /* escape \ and " characters */
                case '\\': write_char('\\'); write_char('\\'); break;
                case '"':  write_char('\\'); write_char('"'); break;
                /* some other escapes supported by JSON */
                case '\t': write_literal("\\t"); break;
                case '\n': write_literal("\\n"); break;
                case '\r': write_literal("\\r"); break;
                case '\b': write_literal("\\b"); break;
                case '\f': write_literal("\\f"); break;
                default:
                    if (c < 0x20) {
                        /* turn ascii control characters into unicode \u00XX
                         * where XX is the hex code of the character */
                        write_literal("\\u00"); write_printf("%x%x", c >> 4, c & 0xf);
                    } else {
                        /* include upper-range utf-8 encoded characters "as is"
                         * without escaping (starting with the current one)
                         *
                         * NOTE: checking for correctness and replacing
                         * incorrect sub-sequences with hex-formatted codes */
                        int len;
                        int is_good_utf8 = check_utf8((unsigned char *)str + i, str_len - i, &len);

                        // NOTE: len is >= 1
                        do {
                            c = ((unsigned char *)str)[i];

                            if (is_good_utf8) {
                                write_char(c);
                            } else {
                                write_printf("<0x%x%x>", c >> 4, c & 0xf);
                            }

                            len--;
                            i++;
                        } while(len);

                        // need to step back, because i is a for-loop variable
                        // and it will be incremented automatically
                        i--;
                    }
            }
        }
    }
}


void wam_output_string(char *str, int len)
{
    output_level_add_array_item();

    if (output.format == WAM_OUTPUT_JSON) {
        write_char('"');
        write_json_string_contents(str, len);
        write_char('"');
    } else if (output.format == WAM_OUTPUT_MPACK) {
        /* TODO, XXX: check for utf8 validity and make it valid similar to
         * write_json_string_contents() ? */
        mpack_string(str, len);
    }
}
