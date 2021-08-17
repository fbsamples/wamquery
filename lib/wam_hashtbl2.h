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
#ifndef __WAM_HASHTBL_H__
#define __WAM_HASHTBL_H__

/* this is a mix of a hashtbl that never shrinks and grows by doubling and a
 * dead-simple arena allocator (that never have to deallocate)
 *
 * the are two main reasons for writing this:
 *
 * - avoid excessive (separate) allocation for the data itself and hashtbl
 *   data structures
 * - make it as simple and as fast as possible for our use-case: single
 *   threaded, no deletes, no shrinking, no deallocation, find_or_insert, ...
 */
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <errno.h>
#include <assert.h>

#include "wam.h"


#ifdef __cplusplus
//extern "C" {
#endif


/* HASHTBL API */

struct wam_hashtbl;
typedef struct wam_hashtbl wam_hashtbl_t;


static void wam_hashtbl_init(wam_hashtbl_t *t);
static void wam_hashtbl_clear(wam_hashtbl_t *t);

/* NOTE: although not necessary, defining these two as inline to suppress
 * "unused function" warnings */
static inline wam_hashtbl_t *wam_hashtbl_alloc(void);
static inline void wam_hashtbl_free(wam_hashtbl_t *t);

/* for working with size-preallocated hash tables; they are allocated once and
 * just get reset for subsequent runs */
static void wam_hashtbl_init_with_size(wam_hashtbl_t *t, int size);
static inline wam_hashtbl_t *wam_hashtbl_alloc_with_size(int size);
static inline void wam_hashtbl_reset(wam_hashtbl_t *t);

/* lookup a hashtbl item */
static inline item_t *wam_hashtbl_find_item(wam_hashtbl_t *t, hash_t hash, key_t key, int key_len);

/* return existing item or create (placeholder for) a new item */
static inline item_t *wam_hashtbl_find_or_alloc_item(wam_hashtbl_t *t, hash_t hash, key_t key, int key_len, int value_len, bool *is_new_item);

static inline uint64_t wam_hashtbl_get_unique_item_count(wam_hashtbl_t *t);
static inline uint64_t wam_hashtbl_get_data_size(wam_hashtbl_t *t);


/* HASHTBL PARAMETERS */

/* types -- MUST BE DEFINED UPFRONT
 *
 * for example: */
#if 0
typedef uint64_t hash_t;
typedef hash_t key_t;

/* items stored in hashtbl */
typedef
struct item {
    struct item *next;  /* singly-linked list for hashtbtl's chaining on collisions */

    /* hash(key) -- we chose to store it to avoid recalculation on table
     * resizing and on deserialization */
    hash_t hash;
} item_t;
#endif

static inline int get_item_size(item_t *item);
static inline int get_new_item_size(int key_len, int value_len);
static inline void init_new_item(item_t *item, key_t key, int key_len, int value_len);
static inline bool is_item_key_equal(item_t *item, hash_t hash, key_t key, int key_len);


/* initial hashtable size defined as 2^INIT_TABLE_POWER */
#define INIT_TABLE_POWER 7

/* miniumum load factor (defined as the by the number of item stored divided
 * by the number of buckets (t->unique_item_count/t->bucket_count); upon
 * reaching this maximum we grow the table by doubling its size */
#define MAX_LOAD_FACTOR 0.75

/* how much buffer space to allocate at a time */
#define BUF_ALLOC_PAGE_SIZE 4096


/* buffers for storing items */
typedef
struct wam_hashtbl_buf {
    /* buffer capactity, i.e. how many bytes can store */
    uint64_t cap;

    /* bytes stored (having both cap and size is redundant, but convenient) */
    uint64_t size;

    /* singly-linked list pointer to the next allocated buffer */
    struct wam_hashtbl_buf *next;

    /* the actual buffer space for holding the data */
    char data[];
} buf_t;


/* hashtable item iterator */
typedef
struct wam_hashtbl_iterator {
    struct wam_hashtbl *t;
    buf_t *buf;
    item_t *item;
} wam_hashtbl_iterator_t;


typedef
struct wam_hashtbl {
    /* first buffer */
    buf_t *first_buf;
    /* current buffer from which we allocate regions */
    buf_t *current_buf;

    /* total data size in all buffers
     * XXX: keep track of allocated but unused space as well? i.e. how much we
     * loose due to our fixed-size allocation units? */
    uint64_t data_size;

    /* the table addressable by hash(key) and pointing into the allocated
     * buffers */
    item_t **lookup_table;

    /* total number of buckets in the table ( = 2^power) */
    int power;
    uint64_t bucket_count;
    uint64_t mask;
    uint64_t prime_factor;

    /* the number of unique keys stored in the table (our table doesn't allow
     * multiple values under with the same key) */
    uint64_t unique_item_count;

    /* references from stored items to lookup table buckets -- we rely on this
     * when checking for bucket validity (instead of zeroing lookup table which
     * can be very expensive); the clever trick we use is described here:
     * http://research.swtch.com/sparse
     */
    item_t ***item_buckets;

    /* our hashtbl is never accessed concurrently and interations always
     * performed from start to finish, so it is safe to allocate only one
     * iterator per table */
    struct wam_hashtbl_iterator it;

    int use_sequential_ids;
} wam_hashtbl_t;


/* return the current item pointed at by the iterator */
static inline
item_t *wam_hashtbl_get_item(wam_hashtbl_iterator_t *it)
{
    return it->item;
}


/* initialize and return table iterator */
static
wam_hashtbl_iterator_t *wam_hashtbl_first(wam_hashtbl_t *t) __attribute__ ((unused));
static
wam_hashtbl_iterator_t *wam_hashtbl_first(wam_hashtbl_t *t)
{
    if (!t->unique_item_count) return NULL;  /* no items */

    t->it.buf = t->first_buf;
    t->it.item = (item_t *)t->first_buf->data;  /* first item of the first buffer */
    return &t->it;
}


/* iterate to the next item */
static
wam_hashtbl_iterator_t *wam_hashtbl_next(wam_hashtbl_iterator_t *it) __attribute__ ((unused));
static
wam_hashtbl_iterator_t *wam_hashtbl_next(wam_hashtbl_iterator_t *it)
{
    item_t *next_item = (item_t *)((char *)it->item + get_item_size(it->item));
    char *end = it->buf->data + it->buf->size; /* pointer to the end of the current buffer */

    if ((char *)next_item >= end) {  /* this was the last item in the current buffer */
        it->buf = it->buf->next;
        if (!it->buf) return NULL;  /* this was the last buffer -- no more buffers or items */
        next_item = (item_t *)it->buf->data;
    }

    it->item = next_item;
    return it;
}


/* callback-style hashtable iteration */
typedef void (*foreach_callback_t)(wam_hashtbl_t *, item_t *, void *);


static
void wam_hashtbl_foreach_item(wam_hashtbl_t *t, foreach_callback_t f, void *p)
{
    if (!t->unique_item_count) return;  /* no items */

    item_t *item;

    buf_t *buf = t->first_buf;
    for (; buf; buf = buf->next) {
        char *end = buf->data + buf->size; /* pointer to the end of buffer */
        item = (item_t *)buf->data;  /* first item */
        while ((char *)item < end) {
            f(t, item, p);
            item = (item_t *)((char *)item + get_item_size(item));
        }
    }
}


/* prime factors closest to table size indexed by power: t->bucket_count = 2^t->power */
static uint32_t wam_hashtbl_primes[] = {
    0, // 0, 1
    1, // 1, 2
    3, // 2, 4
    7, // 3, 8
    13, // 4, 16
    31, // 5, 32
    61, // 6, 64
    127, // 7, 128
    251, // 8, 256
    509, // 9, 512
    1021, // 10, 1024
    2039, // 11, 2048
    4093, // 12, 4096
    8191, // 13, 8192
    16381, // 14, 16384
    32749, // 15, 32768
    65521, // 16, 65536
    131071, // 17, 131072
    262139, // 18, 262144
    524287, // 19, 524288
    1048573, // 20, 1048576
    2097143, // 21, 2097152
    4194301, // 22, 4194304
    8388593, // 23, 8388608
    16777213, // 24, 16777216
    33554393, // 25, 33554432
    67108859, // 26, 67108864
    134217689, // 27, 134217728
    268435399, // 28, 268435456
    536870909, // 29, 536870912
    1073741789, // 30, 1073741824
    2147483647, // 31, 2147483648
    4294967291, // 32, 4294967296
};


static inline
size_t wam_hashtbl_get_lookup_table_size(wam_hashtbl_t *t)
{
    return t->bucket_count * sizeof(t->lookup_table[0]);
}


static inline
void wam_hashtbl_init_lookup_table(wam_hashtbl_t *t)
{
    /* free the previous tab before allocating a new one below */
    if (t->lookup_table) free(t->lookup_table);

    /* note, as we reinsert all itesm from scratch, there is no point in using
     * realloc() */
    t->bucket_count = 1ULL << t->power;
    t->mask = t->bucket_count - 1;  /* set (t->power) lower bits to 1s */
    t->prime_factor = wam_hashtbl_primes[t->power];

    /* allocate a new zero'd (i.e. empty) lookup table
     *
     * NOTE: on some platforms, calloc() can be faster than malloc() and memset(0) -- see
     * http://stackoverflow.com/a/2688522/618259
     */
    t->lookup_table = (item_t **)calloc(1, wam_hashtbl_get_lookup_table_size(t));
}


static inline
void wam_hashtbl_reset_lookup_table(wam_hashtbl_t *t)
{
    memset(t->lookup_table, 0, wam_hashtbl_get_lookup_table_size(t));
}


/* find the hashtbl bucket given a hash */
static inline
item_t **wam_hashtbl_find_bucket(wam_hashtbl_t *t, hash_t hash)
{
    return &t->lookup_table[hash & t->mask];  /* equivalent to hash % t->bucket_count but faster */

    // this results in roughly the same number of collisions, but works slower
    //return &t->lookup_table[hash % t->prime_factor];
}


/* insert item to the head of the singly-link list chain that starts from the
 * table bucket */
static inline
void wam_hashtbl_add_item(item_t **bucket, item_t *item)
{
    item->next = *bucket;
    *bucket = item;
}


static inline
void wam_hashtbl_insert_existing_item(wam_hashtbl_t *t, item_t *item, void *)
{
    item_t **bucket = wam_hashtbl_find_bucket(t, item->hash);
    wam_hashtbl_add_item(bucket, item);
}


static inline
void wam_hashtbl_insert_existing_items(wam_hashtbl_t *t)
{
    wam_hashtbl_foreach_item(t, wam_hashtbl_insert_existing_item, NULL);
}


static inline
void wam_hashtbl_grow_lookup_table(wam_hashtbl_t *t)
{
    /* double the size of the table */
    t->power++;
    wam_hashtbl_init_lookup_table(t);

    /* re-insert all the existing items to the newly allocated table */
    wam_hashtbl_insert_existing_items(t);

    /*
    int i;
    int used = 0;
    for (i = 0; i < t->bucket_count; i++) {
        if (t->lookup_table[i]) used++;
    }
    LOG("grow_tab: power = %d, bucket_count = %u, mask = %x, ucount = %u, collisions = %d\n",
        t->power, t->bucket_count, t->mask, t->unique_item_count, t->unique_item_count - used
    );
    */
}


static inline
void wam_hashtbl_insert_new_item(wam_hashtbl_t *t, item_t **bucket, item_t *item)
{
    wam_hashtbl_add_item(bucket, item);

#ifdef WAM_HASHTBL_BUCKET_PTRS
    if (t->item_buckets) {
        t->item_buckets[t->unique_item_count] = bucket;
    }
#endif

    /* for the new items, we need to increment the item counter */
    t->unique_item_count++;

    /* check if we need to grow the table upon reaching the maximum load factor
     * threshold */
    double load_factor = (double)t->unique_item_count / t->bucket_count;
    if (load_factor > MAX_LOAD_FACTOR) wam_hashtbl_grow_lookup_table(t);
}


/* add a buffer to the list of table's buffers */
static inline
void wam_hashtbl_add_buf(wam_hashtbl_t *t, buf_t *buf)
{
    if (!t->first_buf) {
        t->first_buf = t->current_buf = buf;
    }
    else {
        t->current_buf->next = buf;
        t->current_buf = buf;
    }
}


/* round up (x / size), i.e. add padding after fitting x mod size elements */
#define ALIGN(x, size) (((x) + (size) - 1) / (size) * (size))

static inline
buf_t *wam_hashtbl_alloc_buf(wam_hashtbl_t *t, uint64_t data_size)
{
    /* allocate header + data size bytes; also pad it so that we alocated a
     * whole number of pages */
    size_t alloc_size = ALIGN(sizeof(struct wam_hashtbl_buf) + data_size, BUF_ALLOC_PAGE_SIZE);
    buf_t *buf = (buf_t *) malloc(alloc_size);
    assert(buf);

    buf->cap = alloc_size - sizeof(struct wam_hashtbl_buf);
    buf->size = 0;
    buf->next = NULL;

    return buf;
}


static inline
void wam_hashtbl_reset_buf(buf_t *buf)
{
    /* reclaim used space */
    buf->cap += buf->size;
    buf->size = 0;
}


static
item_t *wam_hashtbl_alloc_item(wam_hashtbl_t *t, int item_size)
{
    buf_t *buf = t->current_buf;

    if (!buf || buf->cap < (unsigned)item_size) {
        /* there's no current buffer or there's not enough free space in the
         * current buffer, so we need to allocate a new one */
        buf = wam_hashtbl_alloc_buf(t, item_size);
        wam_hashtbl_add_buf(t, buf);
    }

    item_t *res = (item_t *) &buf->data[buf->size];
    buf->cap -= item_size;
    buf->size += item_size;

    /* increment the total data size by size of item, less item's chain pointer */
    t->data_size += item_size - sizeof(item_t *);

    return res;
}


/* used internally: finding an item in the singly-linked list of items */
static inline
item_t *wam_hashtbl_find_chained_item(wam_hashtbl_t *t, item_t **bucket, hash_t hash, key_t key, int key_len)
{
    item_t *item = *bucket;

#ifdef WAM_HASHTBL_BUCKET_PTRS
    /* check if this is a valid pointer */
    if (t->item_buckets && item) {
        /* NOTE: this functionality should be used only for preallocated hash
         * tables, i.e. those created/initialized by
         * wam_hashtbl_alloc_with_size/wam_hashtbl_init_with_size */

        unsigned item_index = ((char *)item - t->first_buf->data) / get_item_size(NULL);

        /* item pointer is stale when
         *
         * a) it is outside of allocated range
         *
         * OR
         *
         * b) it is inside the allocated range but it doesn't belong to this
         * bucket */
        if (item_index >= t->unique_item_count || t->item_buckets[item_index] != bucket) {
            item = *bucket = NULL;
        }
    }
#endif /* WAM_HASHTBL_BUCKET_PTRS */

    while (item) {
        if (is_item_key_equal(item, hash, key, key_len)) break; /* found! */
        item = item->next;
    }

    return item;  /* returning NULL when not found */
}


/* find existing item */
static inline
item_t *wam_hashtbl_find_item(wam_hashtbl_t *t, hash_t hash, key_t key, int key_len)
{
    item_t **bucket = wam_hashtbl_find_bucket(t, hash);

    return wam_hashtbl_find_chained_item(t, bucket, hash, key, key_len);
}


/* find existing item or create a new item */
static inline
item_t *wam_hashtbl_find_or_alloc_item(wam_hashtbl_t *t, hash_t hash, key_t key, int key_len, int value_len, bool *is_new_item)
{
    item_t **bucket = wam_hashtbl_find_bucket(t, hash);

    item_t *item;

    if (!t->use_sequential_ids) {  /* normal hashtbl */
        /* try finding the item in the singly-linked list of items */
        item = wam_hashtbl_find_chained_item(t, bucket, hash, key, key_len);
    } else {
        /* we are using hashtable as a sequential array which means that each
         * newly added item will occupy the next available slot -- this is
         * caller's responsibility to do this, we are just assuming it is done
         * correctly here and resetting whatever value/values was present in
         * this bucket */
        item = *bucket = NULL;
    }

    if (item) {  /* found existing item */
        *is_new_item = false;
    } else {
        /* not found -- creating a new one (allocate space) */
        *is_new_item = true;

        item = wam_hashtbl_alloc_item(t, get_new_item_size(key_len, value_len));

        /* populate item's hash, key & length
         *
         * NOTE: hash and key must be set before insertion, because insertion
         * can trigger table expansion and hash-based reshuffling */
        item->hash = hash;
        init_new_item(item, key, key_len, value_len);

        wam_hashtbl_insert_new_item(t, bucket, item);
    }

    return item;
}


static
void wam_hashtbl_init_common(wam_hashtbl_t *t, buf_t *buf, int power)
{
    /* buffers for data will be allocated on demand */
    t->first_buf = t->current_buf = buf;
    t->data_size = 0;

    t->lookup_table = NULL;
    t->power = power;
    t->unique_item_count = 0;

    t->item_buckets = NULL;  /* allocated in wam_hashtbl_init_with_size */

    wam_hashtbl_init_lookup_table(t);  /* initialize the lookup table */

    t->use_sequential_ids = 0;
}


/* quick and dirty way to enable this mode */
static
void wam_hashtbl_set_use_sequential_ids(wam_hashtbl_t *t, int setting) __attribute__ ((unused));
static
void wam_hashtbl_set_use_sequential_ids(wam_hashtbl_t *t, int setting)
{
    t->use_sequential_ids = setting;
}


static
void wam_hashtbl_init(wam_hashtbl_t *t)
{
    wam_hashtbl_init_common(t, NULL, INIT_TABLE_POWER);
}


/* preallocate lookup table and data buffer assuming size as number of items */
static
void wam_hashtbl_init_with_size(wam_hashtbl_t *t, int size)
{
    /* NOTE: this will work only for fixed-size records */
    int max_data_size = size * get_item_size(NULL);
    buf_t *buf = wam_hashtbl_alloc_buf(t, max_data_size);

    double lookup_table_size = ceil(size / MAX_LOAD_FACTOR);
    int lookup_table_power = (int)ceil(log2(lookup_table_size));

    wam_hashtbl_init_common(t, buf, lookup_table_power);

#ifdef WAM_HASHTBL_BUCKET_PTRS
    t->item_buckets = new item_t**[size];
#endif
}


static inline
wam_hashtbl_t *wam_hashtbl_alloc(void)
{
    wam_hashtbl_t *t = new wam_hashtbl_t;

    wam_hashtbl_init(t);

    return t;
}


static inline
wam_hashtbl_t *wam_hashtbl_alloc_with_size(int size)
{
    wam_hashtbl_t *t = new wam_hashtbl_t;

    wam_hashtbl_init_with_size(t, size);

    return t;
}


static inline
void wam_hashtbl_reset(wam_hashtbl_t *t)
{
    /* NOTE: this function should be called only for preallocated hash tables,
     * i.e. those created/initialized by
     * wam_hashtbl_alloc_with_size/wam_hashtbl_init_with_size */

    if (t->unique_item_count) {
        wam_hashtbl_reset_buf(t->first_buf);

        /* reset all chained buffers, if any
         *
         * XXX: we should probably realloc the first_buf on expansion instead
         * of adding more buffer, then freeing them and so on; but this means
         * we need to use relative pointers to hashtbl items */
        if (t->first_buf->next) {
            buf_t *buf = t->first_buf->next;
            while (buf) {
                buf_t *next = buf->next;
                free(buf);
                buf = next;
            }
            t->first_buf->next = NULL;
        }

        t->current_buf = t->first_buf;

#ifndef WAM_HASHTBL_BUCKET_PTRS
        if (!t->use_sequential_ids) {
            /* NOTE: this can be an extremely expensive operation for preallocated
             * ~64K lookup tables used by wam_ucount, essentially we used to spend
             * up to 70% of time doing this on some summary merges (before we'd
             * implemented WAM_HASHTBL_BUCKET_PTRS)
             *
             * NOTE: with bucket ptrs we don't need zeroing */
            wam_hashtbl_reset_lookup_table(t);
        }
#endif

        t->data_size = 0;
        t->unique_item_count = 0;
    }
}


/* remove all hashtbl entries -- in order to continue using the hashtable, one
 * must call wam_hashtbl_init(t) */
static
void wam_hashtbl_clear(wam_hashtbl_t *t)
{
    /* free the buffers */
    buf_t *buf = t->first_buf;
    while (buf) {
        buf_t *next = buf->next;
        free(buf);
        buf = next;
    }

    /* free the lookup table */
    if (t->lookup_table) free(t->lookup_table);

#ifdef WAM_HASHTBL_BUCKET_PTRS
    if (t->item_buckets) delete t->item_buckets;
#endif
}


static inline
void wam_hashtbl_free(wam_hashtbl_t *t)
{
    wam_hashtbl_clear(t);

    /* free the container */
    delete t;
}


static inline
uint64_t wam_hashtbl_get_unique_item_count(wam_hashtbl_t *t)
{
    return t->unique_item_count;
}


static inline
uint64_t wam_hashtbl_get_data_size(wam_hashtbl_t *t)
{
    return t->data_size;
}


#ifdef __cplusplus
//}
#endif

#endif /* __WAM_HASHTBL_H__ */
