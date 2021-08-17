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

/* this is a mix of a hashtbl that never shrinks and grows by doubling and a
 * dead-simple arena allocator (that never have to deallocate)
 *
 * the are two main reasons for writing this:
 *
 * - avoid excessive (separate) allocation for the data itself and hashtbl
 *   data structures
 * - fast searialization/deserialization for storing and loading intermediate
 *   query restults
 * - make it as simple and as fast as possible for our use-case: single
 *   threaded, no deletes, no shrinking, no deallocation, find_or_insert, ...
 *
 * TODO, XXX: consider using a 64-bit hash for certain (very) high-cardinality
 * use-cases, essentially that means parameterizing the implementation with
 * whether to use hash32 or hash64
 */
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>

#include "wam.h"
#include "wam_hashtbl.h"
#include "xxhash.h"


/* initial hashtable size defined as 2^INIT_TABLE_POWER */
#define INIT_TABLE_POWER 7

/* miniumum load factor (defined as the by the number of item stored divided
 * by the number of buckets (t->unique_item_count/t->bucket_count); upon
 * reaching this maximum we grow the table by doubling its size */
#define MAX_LOAD_FACTOR 0.75

/* how much buffer space to allocate at a time */
#define BUF_ALLOC_PAGE_SIZE 4096


typedef uint32_t hash_t;

typedef void (*foreach_visit_callback_t)(void *);
typedef void (*foreach_merge_callback_t)(void *, void *);


/* items stored in hashtbl */
typedef
struct item {
    struct item *next;  /* singly-linked list for hastbtl's chaining on collisions */

    /* hash(key) -- we chose to store it to avoid recalculation on table
     * resizing and on deserialization */
    hash_t hash;

    /* combination of key and value layed out one after another; in case of a
     * variable-lengh they, it is prefixed with uint32_t lengh */
    char data[];
} item_t;


/* buffers for storing items */
typedef
struct buf {
    /* buffer capactity, i.e. how many bytes can store */
    uint64_t cap;

    /* bytes stored (having both cap and size is redundant, but convenient) */
    uint64_t size;

    /* singly-linked list pointer to the next allocated buffer */
    struct buf *next;

    /* the actual buffer space for holding the data */
    char data[];
} buf_t;


/* item iterator */
typedef
struct wam_hashtbl_iterator {
    struct wam_hashtbl *t;
    buf_t *buf;
    item_t *item;
} iterator_t;


typedef
struct wam_hashtbl {
    /* hash table config */
    int key_len;
    int value_len;
    int item_size;  /* size of the items excluding the variable-lengh key */

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
    item_t **tab;

    /* total number of buckets in the table ( = 2^power) */
    int power;
    hash_t bucket_count;
    hash_t mask;
    hash_t prime_factor;

    /* the number of unique keys stored in the table (our table doesn't allow
     * multiple values under with the same key) */
    hash_t unique_item_count;

    /* our hashtbl is never accessed concurrently and interations always
     * performed from start to finish, so it is safe to allocate only one
     * iterator per table */
    iterator_t it;
} hashtbl_t;


/* prime factors closest to table size indexed by power: t->bucket_count = 2^t->power */
static uint32_t primes[] = {
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
void init_tab(hashtbl_t *t)
{
    /* free the previous tab before allocating the new one */
    if (t->tab) free(t->tab);

    /* note, as we reinsert all itesm from scratch, there is no point in using
     * realloc() */
    t->bucket_count = 1ULL << t->power;
    t->mask = t->bucket_count - 1;  /* set (t->power) lower bits to 1s */
    t->prime_factor = primes[t->power];

    size_t alloc_size = t->bucket_count * sizeof(item_t *);
    t->tab = (item_t **)malloc(alloc_size);
    memset(t->tab, 0, alloc_size);  /* set all chaining pointers to 0 */
}


typedef void (*foreach_callback_t)(hashtbl_t *, item_t *, void *);


static inline
void foreach_buf_item(hashtbl_t *t, buf_t *buf, hash_t visit_count, foreach_callback_t f, void *p)
{
    item_t *item;
    hash_t i = 0;

#define LOOP(key_len) \
    for (; buf; buf = buf->next) { \
        char *end = buf->data + buf->size; /* pointer to the end of buffer */ \
        item = (item_t *)buf->data;  /* first item */ \
        while ((char *)item < end) { \
            if (i++ == visit_count) return; /* return from both loops */ \
            f(t, item, p); \
            item = (item_t *)((char *)item + t->item_size + (key_len)); \
        } \
    }

    /* using slightly different code for fixed-length keys and variable-length
     * keys, because we need to extract the key length from the item itself
     * (here, we apply branch optimization, branching once at the very top
     * instead of pushing down branching inside the loop) */
    if (t->key_len) {  /* fixed-length keys */
        LOOP(0);
    } else {  /* variable-length keys */
        LOOP(*(uint32_t *)item->data);
    }
#undef LOOP
}


static
void foreach_item(hashtbl_t *t, hash_t visit_count, foreach_callback_t f, void *p)
{
    foreach_buf_item(t, t->first_buf, visit_count, f, p);
}


/* initialize and return table iterator */
wam_hashtbl_iterator_t *wam_hashtbl_first(wam_hashtbl_t *t)
{
    if (!t->unique_item_count) return NULL;  /* no items */

    t->it.t = t;
    t->it.buf = t->first_buf;
    t->it.item = (item_t *)t->first_buf->data;  /* first item of the first buffer */
    return &t->it;
}


wam_hashtbl_iterator_t *wam_hashtbl_next(wam_hashtbl_iterator_t *it)
{
    /* in case of variable-length keys, we need to add the item's key length to
     * obtain the size of the item */
    uint32_t key_len;
    if (it->t->key_len) key_len = 0;
    else key_len = *(uint32_t *)it->item->data;

    item_t *next_item = (item_t *)((char *)it->item + it->t->item_size + key_len);
    char *end = it->buf->data + it->buf->size; /* pointer to the end of the current buffer */

    if ((char *)next_item >= end) {  /* this was the last item in the current buffer */
        it->buf = it->buf->next;
        if (!it->buf) return NULL;  /* this was the last buffer -- no more buffers or items */
        next_item = (item_t *)it->buf->data;
    }

    it->item = next_item;
    return it;
}


void *wam_hashtbl_get_item(wam_hashtbl_iterator_t *it)
{
    return it->item->data;
}


/* find the hashtbl bucket given a hash */
static inline
item_t **find_bucket(hashtbl_t *t, hash_t hash)
{
    return &t->tab[hash & t->mask];  /* equivalent to hash % t->bucket_count but faster */

    // this results in roughly the same number of collisions, but works slower
    //return &t->tab[hash % t->prime_factor];
}


/* insert item to the head of the singly-link list chain that starts from the
 * table bucket */
static inline
void add_item(item_t **bucket, item_t *item)
{
    item->next = *bucket;
    *bucket = item;
}


static inline
void insert_existing_item(hashtbl_t *t, item_t *item, void *)
{
    item_t **bucket = find_bucket(t, item->hash);
    add_item(bucket, item);
}


static inline
void insert_existing_items(hashtbl_t *t)
{
    foreach_item(t, t->unique_item_count, insert_existing_item, NULL);
}


static inline
void grow_tab(hashtbl_t *t)
{
    /* double the size of the table */
    t->power++;
    init_tab(t);

    /* re-insert all the existing items to the newly allocated table */
    insert_existing_items(t);

    /*
    int i;
    int used = 0;
    for (i = 0; i < t->bucket_count; i++) {
        if (t->tab[i]) used++;
    }
    LOG("grow_tab: power = %d, bucket_count = %u, mask = %x, ucount = %u, collisions = %d\n",
        t->power, t->bucket_count, t->mask, t->unique_item_count, t->unique_item_count - used
    );
    */
}


static inline
void insert_new_item(hashtbl_t *t, item_t **bucket, item_t *item)
{
    add_item(bucket, item);

    /* for the new items, we need to increment the item counter */
    t->unique_item_count++;

    /* check if we need to grow the table upon reaching the maximum load factor
     * threshold */
    double load_factor = (double)t->unique_item_count / t->bucket_count;
    if (load_factor > MAX_LOAD_FACTOR) grow_tab(t);
}


/* add a buffer to the list of table's buffers */
static inline
void add_buf(hashtbl_t *t, buf_t *buf)
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
buf_t *alloc_buf(hashtbl_t *t, uint64_t data_size)
{
    /* allocate header + data size bytes; also pad it so that we alocated a
     * whole number of pages */
    size_t alloc_size = ALIGN(sizeof(struct buf) + data_size, BUF_ALLOC_PAGE_SIZE);
    buf_t *buf = (buf_t *) malloc(alloc_size);
    assert(buf);

    buf->cap = alloc_size - sizeof(struct buf);
    buf->size = 0;
    buf->next = NULL;

    return buf;
}


static
item_t *alloc_item(hashtbl_t *t, int item_size)
{
    buf_t *buf = t->current_buf;

    if (!buf || buf->cap < (unsigned)item_size) {
        /* there's no current buffer or there's not enough free space in the
         * current buffer, so we need to allocate a new one */
        buf = alloc_buf(t, item_size);
        add_buf(t, buf);
    }

    item_t *res = (item_t *) &buf->data[buf->size];
    buf->cap -= item_size;
    buf->size += item_size;

    t->data_size += item_size;  /* increment the total data size */

    return res;
}


static inline
hash_t generic_hash_function(void *data, int len)
{
    /* XXX: inline */
    return XXH32(data, len, /* seed = */ 0);
}


static inline
hash_t uint64_hash_function(uint64_t value)
{
    /* TODO, XXX: optimize by using a 64-bit integer hash function instead of a
     * generic one;
     *
     * NOTE: can't use stdlib's std::tr1::hash<uint64_t> from <tr1/functional>,
     * because it is a 64-bit (FNV) hash */
    return generic_hash_function(&value, sizeof(value));
}


static inline
hash_t double_hash_function(double value)
{
    return generic_hash_function(&value, sizeof(value));
}


static inline
bool is_fixed_length_key_equal(item_t *item, void *key, int key_len)
{
    void *item_key = item->data;
    return !memcmp(item_key, key, key_len);
}


static inline
bool is_variable_length_key_equal(item_t *item, void *key, int key_len)
{
    /* there are cases when the key may be unknown at this point */
    if (!key) return false;

    /* comparing the key lengths first */
    int item_key_len = *(uint32_t *)item->data;
    if (item_key_len != key_len) return false;

    void *item_key = (char *)item->data + sizeof(uint32_t);
    return !memcmp(item_key, key, key_len);
}


static inline
item_t *find_or_alloc_item(hashtbl_t *t, hash_t hash, void *key, int real_key_len, bool is_fixed_length_key, bool *is_new_item)
{
    *is_new_item = false;
    item_t **bucket = find_bucket(t, hash);

// XXX: compare hashes first?
#define LOOP(is_item_key_equal) \
    while (item) { \
        if (is_item_key_equal(item, key, real_key_len)) \
            return item; /* found! */ \
        item = item->next; \
    }

    /* try finding the item in the singly-linked list of items */
    item_t *item = *bucket;
    if (is_fixed_length_key) {  /* fixed-length key */
        LOOP(is_fixed_length_key_equal);
        item = alloc_item(t, t->item_size);  /* not found */
    } else {  /* variable-length key */
        LOOP(is_variable_length_key_equal);
        item = alloc_item(t, t->item_size + real_key_len);  /* not found */
        *(uint32_t *)item->data = real_key_len;  /* write the length of the key */
    }
#undef LOOP

    item->hash = hash;
    insert_new_item(t, bucket, item);
    *is_new_item = true;

    return item;
}


static
void merge_or_alloc_item(hashtbl_t *t, item_t *src_item, void *p)
{
    item_t **bucket = find_bucket(t, src_item->hash);
    foreach_merge_callback_t merge = (foreach_merge_callback_t)p;

// XXX: compare hashes first?
#define LOOP(is_item_key_equal, real_key_len) \
    while (item) { \
        if (is_item_key_equal(item, key, real_key_len)) { \
            if (merge) merge(item->data, src_item->data); \
            return; \
        } \
        item = item->next; \
    }

    /* try finding the item in the singly-linked list of items */
    item_t *item = *bucket;
    int item_size = t->item_size;
    if (t->key_len) {  /* fixed-length key */
        void *key = src_item->data;
        LOOP(is_fixed_length_key_equal, t->key_len);
    } else {  /* variable-length key */
        int key_len = *(uint32_t *)src_item->data;
        void *key = (char *)src_item->data + sizeof(uint32_t);
        LOOP(is_variable_length_key_equal, key_len);
        item_size += key_len;
    }
#undef LOOP

    /* not found -- allocate, copy and insert the item to the hashtbl */
    item = alloc_item(t, item_size);
    memcpy(item, src_item, item_size);
    insert_new_item(t, bucket, item);
}


/* used by wam_hashtbl_get_string() -- see comments below */
static inline
item_t *find_item_by_hash(hashtbl_t *t, hash_t hash)
{
    item_t **bucket = find_bucket(t, hash);

    /* try finding the item in the singly-linked list of items */
    item_t *item = *bucket;
    while (item) {
        if (item->hash == hash)
            return item; /* returning the first entry with the matching hash */
        else
            item = item->next;
    }

    return NULL;
}


/* TODO, XXX: in many cases, the tuple key is ia 64-bit word, consider
 * specializing the implementation */
static inline
void *find_or_alloc_tuple(hashtbl_t *t, void *tuple, bool *is_new_item)
{
    hash_t hash = generic_hash_function(tuple, t->key_len);
    item_t *item = find_or_alloc_item(t, hash, tuple, t->key_len, true, is_new_item);

    /* initialize the new tuple by copying it from the original tuple */
    if (*is_new_item) memcpy(item->data, tuple, t->key_len + t->value_len /* = tuple_size */);

    return item->data;
}


/* NOTE: unused
static inline
string_value_t *find_or_alloc_string(hashtbl_t *t, char *str, int len)
{
    bool is_new_item;
    hash_t hash = generic_hash_function(str, len);
    item_t *item = find_or_alloc_item(t, hash, str, len, false, &is_new_item);
    string_value_t *s = (string_value_t *)item->data;

    // initialize the new string
    if (is_new_item) memcpy(s->str, str, len);

    return s;
}
*/


static inline
unique_value_t *find_or_alloc_bucket(hashtbl_t *t, double value)
{
    bool is_new_item;
    hash_t hash = double_hash_function(value);
    item_t *item = find_or_alloc_item(t, hash, &value, sizeof(value), true, &is_new_item);
    unique_value_t *b = (unique_value_t *)item->data;

    /* initialize the new bucket */
    if (is_new_item) {
        b->value = value;
        b->count = 0LL;
    }

    return b;
}


void wam_hashtbl_update_unique_values(wam_hashtbl_t *t, double value, uint64_t count)
{
    unique_value_t *bucket = find_or_alloc_bucket(t, value);
    bucket->count += count;
}


uint64_t wam_hashtbl_update_ucount(wam_hashtbl_t *t, uint64_t value)
{
    bool is_new_item;
    hash_t hash = uint64_hash_function(value);
    item_t *item = find_or_alloc_item(t, hash, &value, sizeof(value), true, &is_new_item);
    if (is_new_item) *(uint64_t *)item->data = value;

    /* returning the numerber of unique entries */
    return t->unique_item_count;
}


/* note that in our case, the hash is known upfront */
string_value_t *wam_hashtbl_add_string(wam_hashtbl_t *t, uint64_t hash, char *str, int len)
{
    bool is_new_item;
    item_t *item = find_or_alloc_item(t, hash, str, len, false, &is_new_item);
    string_value_t *s = (string_value_t *)item->data;

    /* initialize the new string (str can be NULL in some cases) */
    if (is_new_item && str) memcpy(s->str, str, len);

    return s;
}


/* currently, we lookup string values by their known hashes: although we can get
 * a hash conflict this is highly unlikely and (hopefully) not very critical for
 * our application */
string_value_t *wam_hashtbl_find_string(wam_hashtbl_t *t, uint64_t hash)
{
    item_t *item = find_item_by_hash(t, hash);
    if (!item) return NULL;
    else return (string_value_t *)item->data;
}


void *wam_hashtbl_find_or_alloc_tuple(hashtbl_t *t, void *tuple, int *is_new_item)
{
    return find_or_alloc_tuple(t, tuple, (bool *)is_new_item); /* OK on little-endian */
}


/* key_len = 0 means variable-length keys;
 *
 * size of our data is always fixed: 0 for string table, 64-bit for buckets and
 * some compile-time constant for tuples */
static
hashtbl_t *init_hashtbl(int key_len, int value_len)
{
    hashtbl_t *t = new hashtbl_t;

    /* config */
    t->key_len = key_len;
    t->value_len = value_len;

    t->item_size = sizeof(struct item) + t->key_len + t->value_len;
    /* variable-length keys? each variable-length key is prefixed with its
     * length encoded as a 32-bit integer */
    if (t->key_len == 0) t->item_size += sizeof(uint32_t);

    /* buffers for data will be allocated on demand */
    t->first_buf = t->current_buf = NULL;
    t->data_size = 0;

    t->tab = NULL;
    t->power = INIT_TABLE_POWER;
    t->unique_item_count = 0;
    init_tab(t);

    return t;
}


/* remove all hashtbl entries */
void wam_hashtbl_clear(wam_hashtbl_t *t)
{
    /* free the buffers */
    buf_t *buf = t->first_buf;
    while (buf) {
        buf_t *next = buf->next;
        free(buf);
        buf = next;
    }

    /* TODO: remove code duplication */
    t->first_buf = t->current_buf = NULL;
    t->data_size = 0;

    t->power = INIT_TABLE_POWER;
    t->unique_item_count = 0;

    init_tab(t);  /* the table itself will be freed here */
}


/* group-by tuples */
wam_hashtbl_t *wam_hashtbl_init_tuples(int key_size, int tuple_size)
{
    int value_size = tuple_size - key_size;
    return init_hashtbl(key_size, value_size);
}


/* string dictionary */
wam_hashtbl_t *wam_hashtbl_init_strings(void)
{
    /* key_len = 0 meanning variable-length keys
     *
     * value_len = 0, because there's no extra value associated with the string
     * in the string dictionary (i.e. our string dictionary is a hashset) */
    return init_hashtbl(0, 0);
}


/* unique values used for histograms and ntiles */
wam_hashtbl_t *wam_hashtbl_init_unique_values(void)
{
    /* a map of double -> uint64_t counter */
    return init_hashtbl(sizeof(double), sizeof(uint64_t));
}


/* *exact* ucount aka COUNT DISTINCT (only for 64-bit scalar values) */
wam_hashtbl_t *wam_hashtbl_init_ucount(void)
{
    /* for ucount we just count the unique value instances and don't care about
     * the values */
    return init_hashtbl(sizeof(uint64_t), 0);
}


uint64_t wam_hashtbl_get_unique_item_count(wam_hashtbl_t *t)
{
    return t->unique_item_count;
}


static
void foreach_visitor(wam_hashtbl_t *t, item_t *item, void *p)
{
    foreach_visit_callback_t f = (foreach_visit_callback_t)p;
    f(item->data);
}


void wam_hashtbl_foreach_item(wam_hashtbl_t *t, uint64_t visit_count, void (*user_callback)(void *))
{
    foreach_item(t, visit_count, foreach_visitor, (void *)user_callback);
}


#define HASHTBL_VERSION 1
static uint32_t hashtbl_version = HASHTBL_VERSION;


/* TODO: optimize: there's no need to store the chaining pointers (first 64-bit
 * of each tuple */
void wam_hashtbl_store(wam_hashtbl_t *t, FILE *f)
{
    if (fwrite(&hashtbl_version, sizeof(hashtbl_version), 1, f) != 1)
        handle_io_error("storing hashtbl version", f);

    uint64_t unique_item_count = t->unique_item_count; // uint32_t originally
    uint64_t data_size = t->data_size;
    if (fwrite(&unique_item_count, sizeof(unique_item_count), 1, f) != 1)
        handle_io_error("storing hashtbl item count", f);

    if (fwrite(&data_size, sizeof(data_size), 1, f) != 1)
        handle_io_error("storing hashtbl data size", f);

    //LOG("stored hashtbl: item_count = %u, size = %llu\n", t->unique_item_count, t->data_size);

    buf_t *buf;
    for (buf = t->first_buf; buf; buf = buf->next) {
        if (fwrite(buf->data, buf->size, 1, f) != 1)
            handle_io_error("storing hashtbl data", f);
    }
}


uint64_t wam_hashtbl_get_size(wam_hashtbl_t *t)
{
    return sizeof(hashtbl_version)
        + sizeof(uint64_t) /* unique_item_count */
        + sizeof(uint64_t) /* data_size */
        + t->data_size;
}


void wam_hashtbl_load(wam_hashtbl_t *t, FILE *f, void (*merge_callback)(void *, void *))
{
    if (fread(&hashtbl_version, sizeof(hashtbl_version), 1, f) != 1)
        handle_io_error("loading hashtbl version", f);
    assert(hashtbl_version == HASHTBL_VERSION);

    uint64_t unique_item_count;
    uint64_t data_size;
    if (fread(&unique_item_count, sizeof(unique_item_count), 1, f) != 1)
        handle_io_error("loading hashtbl item count", f);

    if (fread(&data_size, sizeof(data_size), 1, f) != 1)
        handle_io_error("loading hashtbl data size", f);

    //LOG("loading hashtbl: item_count = %llu, size = %llu\n", unique_item_count, data_size);

    if (data_size) {
        /* TODO: do several smaller reads in case of a large input (we'll need
         * to iterate over the records here anyway once we drop the pointers in
         * front of each record) */
        buf_t *buf = NULL;
        buf = alloc_buf(t, data_size);
        if (fread(buf->data, data_size, 1, f) != 1)
            handle_io_error("loading hashtbl data", f);
        buf->size = data_size;

        /* iterate over the items stored in the buffer we've just loaded and insert
         * them into the hashtbl */
        foreach_buf_item(t, buf, unique_item_count, merge_or_alloc_item, (void *)merge_callback);
        free(buf);
    }
    //LOG("loaded hashtbl: item_count = %u, size = %llu\n", t->unique_item_count, t->data_size);
}


static
void merge_unique_values_callback(void *p1, void *p2)
{
    unique_value_t *a = (unique_value_t *)p1;
    unique_value_t *b = (unique_value_t *)p2;
    a->count += b->count;
}


void wam_hashtbl_store_unique_values(wam_hashtbl_t *t, FILE *f)
{
    wam_hashtbl_store(t, f);
}


void wam_hashtbl_load_unique_values(wam_hashtbl_t *t, FILE *f)
{
    wam_hashtbl_load(t, f, merge_unique_values_callback);
}
