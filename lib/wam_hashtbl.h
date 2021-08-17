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

#include <stdint.h>
#include <stdio.h>  // load & store

#include "wam_strtbl.h"  // defines string_value_t

#ifdef __cplusplus
extern "C" {
#endif


struct wam_hashtbl;
typedef struct wam_hashtbl wam_hashtbl_t;


/* internal representation of deduplicated value and its duplicate counts (used
 * by histograms and ntiles */
typedef
struct unique_value {
    double value;
    uint64_t count;
} unique_value_t;


/* table initializers */
extern wam_hashtbl_t *wam_hashtbl_init_strings(void);
extern wam_hashtbl_t *wam_hashtbl_init_unique_values(void);
extern wam_hashtbl_t *wam_hashtbl_init_ucount(void);
extern wam_hashtbl_t *wam_hashtbl_init_tuples(int key_size, int tuple_size);

/* remove all hashtbl entries */
extern void wam_hashtbl_clear(wam_hashtbl_t *t);

/* lookup and insertion */
extern void wam_hashtbl_update_unique_values(wam_hashtbl_t *t, double value, uint64_t count);
extern uint64_t wam_hashtbl_update_ucount(wam_hashtbl_t *t, uint64_t value);

extern string_value_t *wam_hashtbl_add_string(wam_hashtbl_t *t, uint64_t hash, char *str, int len);
extern string_value_t *wam_hashtbl_find_string(wam_hashtbl_t *t, uint64_t hash);

extern void *wam_hashtbl_find_or_alloc_tuple(wam_hashtbl_t *t, void *tuple, int *is_new_item);

/* iteration */
extern uint64_t wam_hashtbl_get_unique_item_count(wam_hashtbl_t *t);
extern void wam_hashtbl_foreach_item(wam_hashtbl_t *t, uint64_t visit_count, void (*user_callback)(void *));

struct wam_hashtbl_iterator;
typedef struct wam_hashtbl_iterator wam_hashtbl_iterator_t;

extern wam_hashtbl_iterator_t *wam_hashtbl_first(wam_hashtbl_t *t);
wam_hashtbl_iterator_t *wam_hashtbl_next(wam_hashtbl_iterator_t *it);
extern void *wam_hashtbl_get_item(wam_hashtbl_iterator_t *it);

/* serialization/deserialization: return 1 on success; 0 on error */
extern void wam_hashtbl_store(wam_hashtbl_t *t, FILE *f);
extern void wam_hashtbl_load(wam_hashtbl_t *t, FILE *f, void (*merge_callback)(void *, void *));

extern uint64_t wam_hashtbl_get_size(wam_hashtbl_t *t);
extern void wam_hashtbl_store_unique_values(wam_hashtbl_t *t, FILE *f);
extern void wam_hashtbl_load_unique_values(wam_hashtbl_t *t, FILE *f);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_HASHTBL_H__ */
