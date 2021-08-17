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
#include "wam_smptbl.h"

#define MAX(a, b) (((a) > (b)) ? (a): (b))

typedef uint64_t hash_t;
typedef int key_t;  /* NOTE: unused */


/*
 * we store stratified sample rates in a hashtable. to simplify design, we store
 * hash as key, similar to wam_strtbl. if there's collision we pick the max and
 * print warning message.
 *
 * one more trick we do is when there is collision, we resolve collision by picking
 * the larger value stored, while log some warning. or we can try different hash value
 * when using different hc until there is no collision. but that involves
 * http://myeyesareblind.com/2017/02/06/Combine-hash-values/ has several ideas how to
 * combine hashes.
 */
typedef
struct __attribute__ ((__packed__)) item {
    struct item *next;
    hash_t hash;
    float value;
} item_t;


static inline
int get_item_size(item_t *item)
{
    return sizeof(struct item);
}


static inline
int get_new_item_size(int key_len, int value_len)
{
    return sizeof(struct item) + value_len;
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


#include "wam_hashtbl2.h"


static wam_hashtbl_t hashtbl;


void wam_smptbl_init(void)
{
    wam_hashtbl_init(&hashtbl);
}


float *wam_smptbl_find(uint64_t hash)
{
    item_t *item = wam_hashtbl_find_item(&hashtbl, hash, 0 /* = key */, 0 /* = key_len */);
    if (!item) return NULL;  /* not found */

    return &item->value;
}


float wam_smptbl_get(uint64_t hash, float default_val)
{
    item_t *item = wam_hashtbl_find_item(&hashtbl, hash, 0 /* = key */, 0 /* = key_len */);
    if (!item) return default_val;

    return item->value;
}


/* insert and merge */
float *wam_smptbl_insert(uint64_t hash, float value)
{
    bool is_new_item;
    item_t *item = wam_hashtbl_find_or_alloc_item(&hashtbl, hash, 0 /* = key */, 0 /* = key_len */, 0 /* = value_len */, &is_new_item);

    if (is_new_item) {
      item->value = value;
    } else {
      LOG("hash collision in wam_smptbl_insert hash: %ld (%f, %f). hicked larger value\n", hash, item->value, value);
      item->value = MAX(value, item->value);
    }

    return &item->value;
}
