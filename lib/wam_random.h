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
#ifndef __WAM_RANDOM_H__
#define __WAM_RANDOM_H__

#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif

/* pass NULL if you want to use a random seed */
extern void wam_random_init(uint64_t *seed);

extern uint64_t wam_random_uint64(uint64_t range_start, uint64_t range_end);


#ifdef __cplusplus
}
#endif


#endif /* __WAM_RANDOM_H__ */
