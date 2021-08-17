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
#include "wam.h"
#include "wam_strtbl.h"


/* parameters for wam_hashtbl storing unique associations of
 *
 * hash64(string) -> string_value:
 *
 *     struct string_value {
 *         uint32_t len;
 *         char str[];
 *     };
 *
 * NOTE: hash(string) is already precomputed and used as string references in
 * the storage format, therefore they become actual hastbl keys
 */
typedef uint64_t hash_t;
typedef int key_t;  /* NOTE: unused */


/* items stored in hashtbl
 *
 * NOTE: we want packed layout to avoid extra external alightment bytes added
 * after the variable-length and length-prefixed value field */
typedef
struct __attribute__ ((__packed__)) item {
    struct item *next;
    hash_t hash;
    struct string_value value;
} item_t;


static int average_string_length = 0;  /* set by wam_strtbl_init_with_size() below */

static inline
int get_item_size(item_t *item)
{
    /* estimate for an average string size -- could be completely wrong, but
     * should be good enough for our immediate needs */
    if (!item) return (sizeof(struct item) + average_string_length);

    return sizeof(struct item) + item->value.len;
}


static inline
int get_new_item_size(int key_len, int value_len)
{
    return sizeof(struct item) + value_len;
}


static inline
void init_new_item(item_t *item, key_t key, int key_len, int value_len)
{
    item->value.len = value_len;
}


static inline
bool is_item_key_equal(item_t *item, hash_t hash, key_t key, int key_len)
{
    return (item->hash == hash);
}


#include "wam_hashtbl2.h"


static wam_hashtbl_t hashtbl;


void wam_strtbl_init(void)
{
    wam_hashtbl_init(&hashtbl);

}


void wam_strtbl_init_without_dedup(int num_strings, int average_length)
{
    average_string_length = average_length;
    wam_hashtbl_init_with_size(&hashtbl, num_strings);

    /* in this mode, we are using hashtable similar to a sequential array, when
     * new items are added one by one at the end, and then quickly freed all at
     * once without memory zeroing (which adds massive overhead on a regular
     * hashtbl) -- see wam_hashtbl2.h for details */
    wam_hashtbl_set_use_sequential_ids(&hashtbl, 1);
}


/* strtbl is reset after each buffer in streaming queries -- this is supposed
 * to be a very cheap operation */
void wam_strtbl_reset(void)
{
    wam_hashtbl_reset(&hashtbl);
}


string_value_t *wam_strtbl_find(uint64_t hash)
{
    item_t *item = wam_hashtbl_find_item(&hashtbl, hash, 0 /* = key */, 0 /* = key_len */);
    if (!item) return NULL;  /* not found */

    return &item->value;
}


/* create a new string entry or return an existign one */
string_value_t *wam_strtbl_insert(uint64_t hash, char *str, int len)
{
    bool is_new_item;
    item_t *item = wam_hashtbl_find_or_alloc_item(&hashtbl, hash, 0 /* = key */, 0 /* = key_len */, len /* = value_len */, &is_new_item);
    string_value_t *value = &item->value;

    /* initialize a new string */
    if (is_new_item) {
        /* NOTE: str can be NULL initially and will be filled later by the caller */
        if (str) memcpy(value->str, str, len);
        //LOG("INSERTED STRING VALUE (%u): %llx\n", item->value.len, item->hash);
    }

    return value;
}


#define STRTBL_VERSION 1
static uint32_t strtbl_version;


void wam_strtbl_store(FILE *f)
{
    strtbl_version = STRTBL_VERSION;
    if (fwrite(&strtbl_version, sizeof(strtbl_version), 1, f) != 1)
        handle_io_error("storing strtbl version", f);

    uint64_t data_size = wam_hashtbl_get_data_size(&hashtbl);
    if (fwrite(&data_size, sizeof(data_size), 1, f) != 1)
        handle_io_error("storing strtbl data size", f);
    //LOG("STORING STRTBL: %llu\n", data_size);

    if (data_size) {
        /* create a temporary output buffer
         *
         * NOTE: don't even have to free it, because this function is called at
         * the end of the query*/
        static char *buf = NULL;
        if (!buf) {
            buf = (char *)malloc(data_size);
        }

        /* populate the output buffer with length-prefixed strings by writing
         * them one by one */
        wam_hashtbl_iterator_t *it = wam_hashtbl_first(&hashtbl);
        char *p = buf;
        for (; it; it = wam_hashtbl_next(it)) {
            item_t *item = wam_hashtbl_get_item(it);

            //string_value_t *value = &item->value;
            //LOG("STORING STRING VALUE (%d): %llx, %.*s\n", value->len, item->hash, value->len, value->str);

            /* write the item without the chain pointer, i.e. starting from the
             * hash */
            int entry_size = get_item_size(item) - sizeof(item_t *);
            memcpy(p, &item->hash, entry_size);
            p += entry_size;
        }
        assert((p - buf) == (int64_t)data_size);

        /* finally, store the buffer */
        if (fwrite(buf, data_size, 1, f) != 1)
            handle_io_error("storing strtbl data", f);
    }
}


void wam_strtbl_load(FILE *f)
{
    if (fread(&strtbl_version, sizeof(strtbl_version), 1, f) != 1)
        handle_io_error("loading strtbl version", f);
    assert(strtbl_version == STRTBL_VERSION);

    uint64_t data_size;
    if (fread(&data_size, sizeof(data_size), 1, f) != 1)
        handle_io_error("loading strtbl data size", f);
    //LOG("LOADING STRTBL: %llu\n", data_size);

    if (data_size) {
        char *buf = (char *)malloc(data_size);  /* create a temporary input buffer */

        /* read the data */
        if (fread(buf, data_size, 1, f) != 1)
            handle_io_error("loading strtbl data", f);

        /* iterate over the items stored in the buffer we've just loaded and
         * insert them into the hashtbl */
        char *p = buf;
        while (p < buf + data_size) {
            /* read the item without the chain pointer, i.e. starting from the
             * hash */
            item_t *item = (item_t *) (p - sizeof(item_t *));
            int entry_size = get_item_size(item) - sizeof(item_t *);

            //string_value_t *value = &item->value;
            //LOG("LOADING STRING VALUE (%d): %llx, %.*s\n", value->len, item->hash, value->len, value->str);

            wam_strtbl_insert(item->hash, item->value.str, item->value.len);
            p += entry_size;
        }
        assert((p - buf) == (int64_t)data_size);

        free(buf);  /* free temporary buffer */
    }
}
