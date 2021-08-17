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
#ifndef __WAM_ORDERBY_H__
#define __WAM_ORDERBY_H__

#include "wam.h"


#ifdef __cplusplus
extern "C" {
#endif


/* intialize orderby state */
extern void wam_orderby_init(uint64_t input_size);
extern void wam_orderby_free(void);

/* copy tuple to a temporary array so that it could be sorted by
 * wam_orderby_*() */
extern void wam_orderby_visit(tuple_t *t);

/* sort the array of tuples using wam_query_orderby_compare() and return sorted
 * array or tuple pointers; uses partial_sort when limit < input_size */
extern tuple_t **wam_orderby_function(uint64_t limit);

/* sort the array of tuples using wam_query_groupby_compare() and return sorted
 * array or tuple pointers */
extern tuple_t **wam_orderby_group_key(void);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_ORDERBY_H__ */
