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
#include <stdlib.h>
#include <assert.h>

#include "wam.h"
#include "wam_ucount.h"
#include "hll.h"


/* exact count per hashtable after which we switch to an approximate ucount
 * calculation based on the HyperLogLog cardinality estimator
 *
 * XXX: should it be configurable?
 *
 * XXX: alternaitvely, set it so that the size of serialized (or runtime)
 * repesentation doesn't exceed that of the HLL state (given its percision) */

// NOTE: previous implementation used 16K
#define MAX_LOCAL_EXACT_ITEMS 4 * 1024

/* we gradually decrease the above local max exact count threshold by cutting it
 * in half until we reach this number -- this roughly corresponds to one 4KB
 * block worth of runtime hashtbl space ((64-bit hashes + 64-bit chain pointer)
 * x 256 = 4KB) */
#define MIN_LOCAL_EXACT_ITEMS 256

/* the current value of the local max exact count threshold */
static uint64_t max_local_exact_items = MAX_LOCAL_EXACT_ITEMS;


/* how many items can be counted exactly across all ucount instances -- this
 * puts an upper bound on how much memory can be used by all hashtable-based
 * ucounts */
#define MAX_GLOABAL_EXACT_ITEMS 1 * 1024 * 1024
/* current global number of exactly counted items */
static uint64_t num_exact_items = 0;


/* hll setting from 4 to 18 responsible for estimation accuracy; the higher the
 * value the less the error and the more memory it takes
 *
 * memory usage is proportional to the 2^precision;
 *
 * Error of HLL is 1.04 / sqrt(2^p)
 *
 * For example, precision 12 (4096 registers) corresponds to 1.625% error
 *
 * XXX: should it be configurable? */
//#define HLL_PRECISION 12  // 4096 registers => 1.625% error
#define HLL_PRECISION 13  // 8192 registers => 1.149% error
//#define HLL_PRECISION 14  // 16384 registers => 0.8125% error


/* until we exceed MAX_LOCAL_EXACT_ITEMS, we store unique items in a hash table
 *
 * we are storing 64-bit hashes of the UCOUNT-ed items without separate exact
 * keys -- this should be fined for our very limited cardinality goal (several
 * K) until we swtich to HLL-based UCOUNT */
typedef uint64_t hash_t;
typedef int key_t;  /* NOTE: unused */


/* items stored in hashtbl */
typedef
struct item {
    struct item *next;
    hash_t hash;
} item_t;


static inline
int get_item_size(item_t *item)
{
    return sizeof(struct item);
}


static inline
int get_new_item_size(int key_len, int value_len)
{
    return sizeof(struct item);
}


static inline
void init_new_item(item_t *item, key_t key, int key_len, int value_len)
{
}


static inline
bool is_item_key_equal(item_t *item, hash_t hash, key_t key, int key_len)
{
    return (item->hash == hash);
}


#define WAM_HASHTBL_BUCKET_PTRS 1
#include "wam_hashtbl2.h"


struct wam_ucount {
    /* whether we use exact hashtbl-based ucount or hll-based approximation */
    bool is_exact;
    bool is_merge_mode;

    int64_t hll_size;  /* cached hll estimate */
    struct wam_ucount *next;

    wam_hashtbl_t *hashtbl;
    hll_t hll;
};


/* the list of all ucount states;
 *
 * if we start hitting memory limits due to lots of small hashtables, we need to
 * go over the existing states and convert them to hll */
static struct wam_ucount *ucount_head = NULL;


wam_ucount_t *wam_ucount_init(void)
{
    wam_ucount_t *u = new wam_ucount;
    u->is_merge_mode = false;

    /* we always start with exact ucount and may switch to the approximate
     * method later upon reaching the threshold */
    u->is_exact = true;
    u->hashtbl = NULL;  /* allocated lazily below */

    u->hll_size = -1;  /* -1 for undefined */

    /* add to the list of ucount states */
    u->next = ucount_head;
    ucount_head = u;

    return u;
}


/* used for merging sorted subquery results; the goal to avoid dynamic
 * allocations on susequent merges, so we preallocate everything here and just
 * reset it in wam_ucount_reset_merge() below -- instead of free'ing, allocating
 * and reallocating over and over again */
wam_ucount_t *wam_ucount_init_merge(void)
{
    wam_ucount_t *u = wam_ucount_init();
    u->is_merge_mode = true;

    /* pre-allocate hashtable large enought to be able to fit MAX_LOCAL_EXACT_ITEMS */
    u->hashtbl = wam_hashtbl_alloc_with_size(MAX_LOCAL_EXACT_ITEMS);

    /* pre-allocate hll state */
    int res = hll_init(HLL_PRECISION, &u->hll);
    assert(!res);

    return u;
}


static inline
void hll_reset(hll_t *hll)
{
    memset(hll->registers, 0, hll->size);
}


/* used for resetting sorted subquery results; the goal to avoid dynamic
 * allocations on susequent merges */
void wam_ucount_reset_merge(wam_ucount_t *u)
{
    /* reset hll state if it was used */
    if (!u->is_exact) {
        hll_reset(&u->hll);
    }

    /* next merge state starts as exact */
    u->is_exact = true;

    /* reset hashtbl
     *
     * NOTE: this has practically zero cost regardless of whether it was used */
    wam_hashtbl_reset(u->hashtbl);
}


static inline
void hll_update(wam_ucount_t *u, uint64_t hash)
{
    hll_add_hash(&u->hll, hash);
}


/* merge the current hashtable into the current hll */
static inline
void hll_merge_hashtbl(wam_ucount_t *u)
{
    if (!u->hashtbl) return;  /* hashtbl is non-existent, i.e. empty as it is allocated on demand */

    wam_hashtbl_iterator_t *it = wam_hashtbl_first(u->hashtbl);
    for (; it; it = wam_hashtbl_next(it)) {
        item_t *item = wam_hashtbl_get_item(it);
        hll_update(u, item->hash);
    }

    /* free the hashtbl, we are not going to use it ever again (unless we are in
     * merge mode) */
    if (!u->is_merge_mode) {
        /* decrement the number of exactly counted items */
        num_exact_items -= wam_hashtbl_get_unique_item_count(u->hashtbl);

        wam_hashtbl_free(u->hashtbl);
        u->hashtbl = NULL;
    }
}


static
void convert_hashtbl_to_hll(wam_ucount_t *u)
{
    /* convert from hashtbl-based ucount to hll-based */
    u->is_exact = false;

    /* allocate hll state (already allocated in merge mode) */
    if (!u->is_merge_mode) {
        int res = hll_init(HLL_PRECISION, &u->hll);
        assert(!res);
    }

    //LOG("ucount: switching to hll opon reaching %llu\n", wam_hashtbl_get_unique_item_count(u->hashtbl));

    hll_merge_hashtbl(u);
}


static
void maybe_convert_hashtbl_to_hll(wam_ucount_t *u)
{
    /* check if we've exceeded the local maximum of uniquely counted items for
     * this hashtable
     *
     * NOTE: as we don't check whether we about to exceed the maximum before
     * adding to the table, we enforce the limit here instead -- proactively,
     * upon reaching the limit even if not yet exceeding it yet */
    if (wam_hashtbl_get_unique_item_count(u->hashtbl) >= max_local_exact_items) {
        convert_hashtbl_to_hll(u);

    /* check if we've exceeded the global maximum of uniquely counted items
     *
     * NOTE: unless this happens during summary resampling in which case we
     * don't do this to prevent reduction in accuracy (e.g. why downsampled
     * query returns different ucounts with no apparent reason?) */
    } else if (num_exact_items > MAX_GLOABAL_EXACT_ITEMS && max_local_exact_items > MIN_LOCAL_EXACT_ITEMS && !wam_summary_resampling_mode) {

        /* decrease the per-table threshold */
        max_local_exact_items /= 2;

        /* go over all hashtbl-based ucounts and convert those that meet the
         * updated threshold to hll */
        u = ucount_head;
        while (u) {
            if (u->is_exact && u->hashtbl) {
                if (wam_hashtbl_get_unique_item_count(u->hashtbl) > max_local_exact_items) {
                    convert_hashtbl_to_hll(u);
                }
            }
            u = u->next;
        }
    }
}


static inline
void exact_update(wam_ucount_t *u, uint64_t hash)
{
    if (!u->hashtbl) {  /* allocate hashtbl on-demand -- often times it won't be needed during merges */
        u->hashtbl = wam_hashtbl_alloc();
    }

    bool is_new_item;
    wam_hashtbl_find_or_alloc_item(u->hashtbl, hash, 0 /* = key */, 0 /* = key_len */, 0 /* = value_len */, &is_new_item);

    /* update the global number of exactly counter unique items (don't care
     * about this when in merge mode)  */
    if (!u->is_merge_mode) {
        num_exact_items += is_new_item;
    }

    /* check if we've exceeded the thresholds as a result of the update; if yes,
     * convert to hll */
    maybe_convert_hashtbl_to_hll(u);
}


// link the external murmur hash in
extern "C" {
extern void MurmurHash3_x64_128(const void * key, const int len, const uint32_t seed, void *out);
}


/* TODO, XXX: consider using a faster integer hash function instead of a generic
 * one
 *
 * XXX: replace with XXHASH64?
 *
 * TODO, XXX: in fact, with cardinality > 600M probability of hash collision is
 * 1 in 100, and with 1.2B it grows to 1 in 10... something to think about...
 */
static inline
uint64_t hash_value(uint64_t value)
{
    uint64_t out[2];
    MurmurHash3_x64_128(&value, sizeof(value), 0, &out);

    return out[1];
}


static inline
uint64_t hash_bytes(void *buf, int len)
{
    uint64_t out[2];
    MurmurHash3_x64_128(buf, len, 0, &out);

    return out[1];
}


static
void update_hash(wam_ucount_t *u, uint64_t hash)
{
    if (u->is_exact) {
        exact_update(u, hash);
    } else {
        hll_update(u, hash);
    }
}


void wam_ucount_update_uint64(wam_ucount_t *u, uint64_t value)
{
    /* NOTE: we deal with uint64 hashes instead of integers (even for
     * hashbtl-based exact method) which should be fine for our use-cases and
     * cardinality ranges */
    hash_t hash = hash_value(value);
    //LOG("UCOUNT UPDATE: %llu -> %llx\n", value, hash);
    update_hash(u, hash);
}


void wam_ucount_update_double(wam_ucount_t *u, double value)
{
    wam_ucount_update_uint64(u, double_to_uint64(value));
}


#include <stdarg.h>
void wam_ucount_update_multi(wam_ucount_t *u, int n, ...)
{
    uint64_t buf[n];

    va_list ap;
    va_start(ap, n);

    for (int i = 0; i < n; i++) {
        buf[i] = va_arg(ap, uint64_t);
    }

    va_end(ap);

    uint64_t hash = hash_bytes(buf, sizeof(buf));

    update_hash(u, hash);
}


#define HLL_VERSION 1
static uint32_t hll_version;

static
void hll_store(hll_t *hll, FILE *f)
{
    hll_version = HLL_VERSION;
    if (fwrite(&hll_version, sizeof(hll_version), 1, f) != 1)
        handle_io_error("storing hll ucount version", f);

    //LOG("hll: %llx ", hash_bytes(hll->registers, hll->size));
    if (fwrite(hll->registers, hll->size, 1, f) != 1)
        handle_io_error("storing hll ucount", f);
}


static
uint64_t hll_get_size(hll_t *hll)
{
    return hll->size + sizeof(hll_version);
}


static
void hll_load(hll_t *hll, FILE *f)
{
    if (fread(&hll_version, sizeof(hll_version), 1, f) != 1)
        handle_io_error("loading hll ucount version", f);
    assert(hll_version == HLL_VERSION);

    /* this should typically be only a few K but has a potential to be up to
     * a 100K, because of that, allocating the buffer in the heap */
    static char *ibuf = NULL;
    if (!ibuf) {
        ibuf = (char *)malloc(hll->size);
    }

    hll_t new_hll;
    new_hll.registers = (uint32_t *)ibuf;

    if (fread(new_hll.registers, hll->size, 1, f) != 1)
        handle_io_error("loading hll ucount", f);

    /* merge a new hll into the current hll (or skip it if there are no
     * registers) */
    if (hll->registers) {
        //LOG("hll: %llx ", hash_bytes(new_hll.registers, hll->size));
        hll_add_hll(hll, &new_hll);
    }
}


#define EXACT_VERSION 1
static uint32_t exact_version;


static uint64_t *iobuf = NULL;
static int iobuf_cap = 0;
/* dynamically allocated buffer for loading and storing exact hashes */


static
void check_iobuf_cap(int num_hashes)
{
    if (iobuf_cap >= num_hashes) return;

#define MAX(a, b) (((a) > (b)) ? (a): (b))

    /* NOTE: it may exceed MAX_LOCAL_EXACT_ITEMS but only for data that was
     * written with the old value of this constant */
    iobuf_cap = MAX(num_hashes, MAX_LOCAL_EXACT_ITEMS);
    int alloc_size = iobuf_cap * sizeof(uint64_t);

    if (!iobuf) {
        iobuf = (uint64_t *) malloc(alloc_size);
    } else {
        iobuf = (uint64_t *) realloc(iobuf, alloc_size);
    }
}


static inline
uint64_t exact_get_ucount(wam_hashtbl_t *t)
{
    /* as we allocate hashtbl on demand, in can be completely empty in case of
     * empty results */
    if (!t) return 0;
    return wam_hashtbl_get_unique_item_count(t);
}


static
void exact_store(wam_hashtbl_t *t, FILE *f)
{
    exact_version = EXACT_VERSION;
    if (fwrite(&exact_version, sizeof(exact_version), 1, f) != 1)
        handle_io_error("storing exact ucount version", f);

    uint64_t ucount = exact_get_ucount(t);
    if (fwrite(&ucount, sizeof(ucount), 1, f) != 1)
        handle_io_error("storing exact ucount count", f);

    //LOG("STORE EXACT UCOUNT: %llu\n", ucount);
    //LOG("exact: %llu ", ucount);
    if (ucount) {
        check_iobuf_cap(ucount);

        /* write all hashes to a temporary buffer */
        wam_hashtbl_iterator_t *it = wam_hashtbl_first(t);
        uint64_t i = 0;
        for (; it; it = wam_hashtbl_next(it), i++) {
            item_t *item = wam_hashtbl_get_item(it);
            iobuf[i] = item->hash;
        }
        assert(i == ucount);

        if (fwrite(iobuf, sizeof(uint64_t), ucount, f) != ucount)
            handle_io_error("storing exact ucount", f);
    }
}


static
uint64_t exact_get_size(wam_hashtbl_t *t)
{
    return
        sizeof(exact_version) +
        sizeof(uint64_t) +  /* number of hashes */
        + exact_get_ucount(t) * sizeof(uint64_t);  /* hashes themselves */
}


static
void exact_load(wam_ucount_t *u, FILE *f)
{
    if (fread(&exact_version, sizeof(exact_version), 1, f) != 1)
        handle_io_error("loading exact version", f);
    assert(exact_version == EXACT_VERSION);

    uint64_t ucount;
    if (fread(&ucount, sizeof(ucount), 1, f) != 1)
        handle_io_error("loading exact ucount count", f);

    //LOG("LOAD EXACT UCOUNT: %llu\n", ucount);
    /* read all hashes into a temporary buffer */
    if (ucount) {
        check_iobuf_cap(ucount);
        if (fread(iobuf, sizeof(uint64_t), ucount, f) != ucount)
            handle_io_error("loading exact ucount", f);

        if (u) {
            //LOG("exact: %llu ", ucount);
            /* finally, apply them one by one */
            for (unsigned i = 0; i < ucount; i++) {
                update_hash(u, iobuf[i]);
            }
        }
    }
}

static char is_exact;

void wam_ucount_store(wam_ucount_t *u, FILE *f)
{
    is_exact = u->is_exact;

    if (fwrite(&is_exact, sizeof(is_exact), 1, f) != 1)
        handle_io_error("storing ucount is_exact", f);

    if (u->is_exact) {
        exact_store(u->hashtbl, f);
    } else {
        hll_store(&u->hll, f);
    }
}


uint64_t wam_ucount_get_size(wam_ucount_t *u)
{
    uint64_t size;

    if (u->is_exact) {
        size = exact_get_size(u->hashtbl);
    } else {
        size = hll_get_size(&u->hll);
    }

    return size + sizeof(is_exact);
}


void wam_ucount_load(wam_ucount_t *u, FILE *f)
{
    if (fread(&is_exact, sizeof(is_exact), 1, f) != 1)
        handle_io_error("loading ucount is_exact", f);

    wam_ucount_load_common(u, is_exact, f);
}


/* also used by wam_weighted_ucount when converting ucount into weighted ucount
 * upon loading */
void wam_ucount_load_common(wam_ucount_t *u, int is_exact, FILE *f)
{
    if (is_exact) {  /* the new ucount is exact */
        /* load stored exact representation and apply it towards the existing
         * state -- regardles of whether it is exact or hll */
        exact_load(u, f);
    } else {  /* the new ucount is a hll */

        hll_t *hll = NULL;
        if (u) {
            if (u->is_exact)  /* the current ucount is a hashbl -- converting it to hll as a first step */
                convert_hashtbl_to_hll(u);
            hll = &u->hll;
        } else {
            /* skipping instead of loading */
            static hll_t fake_hll = {0};
            hll = &fake_hll;
            if (!hll->size) {
                hll->size = hll_bytes_for_precision(HLL_PRECISION);
            }
        }

        hll_load(hll, f);
    }
}


uint64_t wam_ucount_get_count(wam_ucount_t *u)
{
    if (u->is_exact) {
        return exact_get_ucount(u->hashtbl);
    } else {
        /* caching the returned value, because this function may be called from
         * order-by and other post-processing functions many times
         *
         * WARNING: because of caching, IT SHOULD NEVER BE CALLED IN THE MIDDLE
         * OF THE PROCESSING -- ONLY AT THE END */
        if (u->hll_size == -1)  /* undefined? */
            u->hll_size = hll_size(&u->hll);
        return u->hll_size;
    }
}
