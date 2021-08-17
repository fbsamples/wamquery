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
#ifndef __WAM_PQUEUE_H__
#define __WAM_PQUEUE_H__

#include "wam.h"


#ifdef __cplusplus
extern "C" {
#endif


typedef wam_input_t pqueue_item_t;


/* intialize priority queue state */
extern void wam_pqueue_init(void);
extern void wam_pqueue_free(void);


extern void wam_pqueue_push(pqueue_item_t *item);
extern pqueue_item_t *wam_pqueue_pop(void);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_PQUEUE_H__ */
