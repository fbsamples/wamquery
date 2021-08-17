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
#include "wam_hashtbl.h"
#include "hll.h"


/* exact count per hashtable after which we switch to an approximate ucount
 * calculation based on the HyperLogLog cardinality estimator
 *
 * XXX: should it be configurable? */
#define MAX_LOCAL_EXACT_ITEMS 16 * 1024

/* we gradually decrease the above local max exact count threshold by cutting it
 * in half until we reach this number -- this roughly corresponds to 1 4k block
 * worth of hashtbl space */
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


//#include <tr1/unordered_set>
//typedef std::tr1::unordered_set<uint64_t> hashtbl_t;
//hashtbl_t hashtbl;


struct wam_ucount {
    /* whether we use exact hashtbl-based ucount or hll-based approximation */
    bool is_exact;
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


wam_ucount_t *wam_ucount_init()
{
    wam_ucount_t *u = new wam_ucount;

    /* we always start with exact ucount and may switch to the approximate
     * method later upon reaching the threshold */
    u->is_exact = true;
    u->hashtbl = wam_hashtbl_init_ucount();

    u->hll_size = -1;  /* -1 for undefined */

    /* add to the list of ucount states */
    u->next = ucount_head;
    ucount_head = u;

    return u;
}


static inline
void hll_update(wam_ucount_t *u, uint64_t value)
{
    hll_add(&u->hll, &value, sizeof(uint64_t));
}


/* merge the current hashtable into the current hll */
static inline
void hll_merge_hashtbl(wam_ucount_t *u)
{
    uint64_t i;
    wam_hashtbl_iterator_t *it = wam_hashtbl_first(u->hashtbl);
    for (i = 0; it; i++, it = wam_hashtbl_next(it)) {
        uint64_t value = *(uint64_t *)wam_hashtbl_get_item(it);
        hll_update(u, value);
    }

    /* decrement the number of exactly counted items */
    num_exact_items -= wam_hashtbl_get_unique_item_count(u->hashtbl);

    /* remove all elements from the hashtbl after it has been merged
     *
     * NOTE: clearing the hashtbl, but keeping it around in case we need it
     * later for future merges */
    wam_hashtbl_clear(u->hashtbl);
}


static
void convert_hashtbl_to_hll(wam_ucount_t *u)
{
    /* convert from hashtbl-based ucount to hll-based */
    u->is_exact = false;
    assert(!hll_init(HLL_PRECISION, &u->hll));
    //LOG("ucount: switching to hll opon reachng %llu\n", ucount);

    hll_merge_hashtbl(u);
}


static
void maybe_convert_hashtbl_to_hll(wam_ucount_t *u)
{
    /* check if we've exceeded the local maximum of uniquely counted items for
     * this hashtable */
    if (wam_hashtbl_get_unique_item_count(u->hashtbl) > max_local_exact_items) {
        convert_hashtbl_to_hll(u);

    /* check if we've exceeded the global maximum of uniquely counted items */
    } else if (num_exact_items > MAX_GLOABAL_EXACT_ITEMS && max_local_exact_items > MIN_LOCAL_EXACT_ITEMS) {

        /* decrease the per-table threshold */
        max_local_exact_items /= 2;

        /* go over all hashtbl-based ucounts and convert those that meet the
         * updated threshold to hll */
        u = ucount_head;
        while (u) {
            if (u->is_exact) {
                if (wam_hashtbl_get_unique_item_count(u->hashtbl) > max_local_exact_items) {
                    convert_hashtbl_to_hll(u);
                }
            }
            u = u->next;
        }
    }
}


void wam_ucount_update_uint64(wam_ucount_t *u, uint64_t value)
{
    //hashtbl.insert(value);

    if (u->is_exact) {
        uint64_t prev_ucount = wam_hashtbl_get_unique_item_count(u->hashtbl);
        uint64_t new_ucount = wam_hashtbl_update_ucount(u->hashtbl, value);

        /* update the global number of exactly counter unique items */
        num_exact_items += (prev_ucount != new_ucount);

        /* check if we've exceeded the thresholds as a result of the load;
         * if yes, convert to hll */
        maybe_convert_hashtbl_to_hll(u);
    } else {
        hll_update(u, value);
    }
}


void wam_ucount_update_double(wam_ucount_t *u, double value)
{
    wam_ucount_update_uint64(u, double_to_uint64(value));
}


uint64_t wam_ucount_get_count(wam_ucount_t *u)
{
    //LOG("real ucount: %d\n", (int)hashtbl.size());
    if (u->is_exact) {
        return wam_hashtbl_get_unique_item_count(u->hashtbl);
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

#define HLL_VERSION 1
static uint32_t hll_version = HLL_VERSION;

static
void hll_store(hll_t *hll, FILE *f)
{
    if (fwrite(&hll_version, sizeof(hll_version), 1, f) != 1)
        handle_io_error("storing hll version", f);

    if (fwrite(hll->registers, hll->size, 1, f) != 1)
        handle_io_error("storing hll", f);
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

    /* merge a new hll into the current hll */
    hll_add_hll(hll, &new_hll);
}


void wam_ucount_store(wam_ucount_t *u, FILE *f)
{
    if (fwrite(&u->is_exact, sizeof(u->is_exact), 1, f) != 1)
        handle_io_error("storing ucount is_exact", f);

    if (u->is_exact) {
        wam_hashtbl_store(u->hashtbl, f);
    } else {
        hll_store(&u->hll, f);
    }
}


uint64_t wam_ucount_get_size(wam_ucount_t *u)
{
    uint64_t size;

    if (u->is_exact) {
        size = wam_hashtbl_get_size(u->hashtbl);
    } else {
        size = hll_get_size(&u->hll);
    }

    return size + sizeof(u->is_exact);
}


void wam_ucount_load(wam_ucount_t *u, FILE *f)
{
    bool is_exact;
    if (fread(&is_exact, sizeof(u->is_exact), 1, f) != 1)
        handle_io_error("loading ucount is_exact", f);

    /* loading */
    if (is_exact) {  /* thew new ucount is exact */
        /* load the new hashtbl and merge it with the current one
         *
         * NOTE: make sure num_exact_items is consistently updated upon merging
         * by subtracting the current count and adding the resulting count after
         * the merge */
        num_exact_items -= wam_hashtbl_get_unique_item_count(u->hashtbl);
        wam_hashtbl_load(u->hashtbl, f, NULL /* merge callback */);
        num_exact_items += wam_hashtbl_get_unique_item_count(u->hashtbl);

        if (!u->is_exact) {  /* the current ucount is a hll */
            /* TODO: we are doing extra work above -- can completely optimize
             * out the hashtable building and just scan raw data */
            hll_merge_hashtbl(u);
        } else {  /* the current ucount is a hashtable */
            /* check if we've exceeded the thresholds as a result of the load;
             * if yes, convert to hll */
            maybe_convert_hashtbl_to_hll(u);
        }
    } else {  /* the new ucount is a hll */
        if (u->is_exact)  /* the current ucount is a hashbl -- converting it to hll as a first step */
            convert_hashtbl_to_hll(u);

        hll_load(&u->hll, f);
    }
}
