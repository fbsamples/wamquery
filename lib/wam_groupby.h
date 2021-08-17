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
#ifndef __WAM_GROUPBY_H__
#define __WAM_GROUPBY_H__

#include "wam.h"

#ifdef __cplusplus
extern "C" {
#endif


extern void wam_groupby_init(int key_size, int tuple_size);

extern tuple_t *wam_groupby_find_or_alloc(tuple_t *tuple, int *is_new_tuple);

extern int wam_groupby_get_tuple_count(void);

extern void wam_groupby_foreach_tuple(void (*f)(tuple_t *), int visit_count);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_GROUPBY_H__ */
