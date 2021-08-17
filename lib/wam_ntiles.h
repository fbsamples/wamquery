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
#ifndef __WAM_NTILES_H__
#define __WAM_NTILES_H__

#include <stdio.h>


#ifdef __cplusplus
extern "C" {
#endif


struct wam_ntiles;
typedef struct wam_ntiles wam_ntiles_t;


extern wam_ntiles_t *wam_ntiles_init(void);

extern void wam_ntiles_update(wam_ntiles_t *nt, double value, int weight);

// return the ntile for n in [0, 1]
extern double wam_ntile(wam_ntiles_t *nt, double n);

extern void wam_ntiles_store(wam_ntiles_t *u, FILE *f);
extern void wam_ntiles_load(wam_ntiles_t *u, FILE *f);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_NTILES_H__ */
