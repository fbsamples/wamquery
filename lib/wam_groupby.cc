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
#include "wam_groupby.h"
#include "wam_hashtbl.h"


static wam_hashtbl_t *hashtbl;


void wam_groupby_init(int key_size, int tuple_size)
{
    hashtbl = wam_hashtbl_init_tuples(key_size, tuple_size);
}


tuple_t *wam_groupby_find_or_alloc(tuple_t *tuple, int *is_new_tuple)
{
    return (tuple_t *)wam_hashtbl_find_or_alloc_tuple(hashtbl, tuple, is_new_tuple);
}


int wam_groupby_get_tuple_count(void)
{
    return wam_hashtbl_get_unique_item_count(hashtbl);
}


typedef void (*void_callback_t)(void *);

void wam_groupby_foreach_tuple(void (*f)(tuple_t *), int visit_count)
{
    wam_hashtbl_foreach_item(hashtbl, visit_count, (void_callback_t)f);
}
