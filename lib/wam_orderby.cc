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

/* orderby + limit implementation */

#include <algorithm>

#include "wam_orderby.h"


/* array to sort (maybe be partially sort) */
static tuple_t **tuples = NULL;
typedef tuple_t *tuple_p;


static uint64_t size = 0;


void wam_orderby_init(uint64_t input_size)
{
    tuples = new tuple_p[input_size];
}


void wam_orderby_free(void)
{
    if (!tuples) return;

    delete []tuples;
    tuples = NULL;
}


void wam_orderby_visit(tuple_t *t)
{
    tuples[size++] = t;
}


/* NOTE: the same as wam_query_orderby_compare(a, b) but returns bool instead of
 * int 0|1 */
static inline
bool compare_tuple(tuple_t *a, tuple_t *b)
{
    return wam_query_orderby_compare(a, b);
}


tuple_t **wam_orderby_function(uint64_t limit)
{
    if (limit >= size) // full sort
        std::sort(tuples, tuples + size, compare_tuple);
    else // partial sort
        std::partial_sort(tuples, tuples + limit, tuples + size, compare_tuple);

    return tuples;
}


/* NOTE: the same as wam_query_groupby_compare(a, b) but returns bool instead of
 * int 0|1 */
static inline
bool compare_groupby_key(tuple_t *a, tuple_t *b)
{
    return wam_query_groupby_compare(a, b);
}


tuple_t **wam_orderby_group_key(void)
{
    std::sort(tuples, tuples + size, compare_groupby_key);

    return tuples;
}
