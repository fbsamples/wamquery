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
#ifndef __WAM_BINS_H__
#define __WAM_BINS_H__

#include "wam_hist.h"  // binned_result_t, output_bin_t


#ifdef __cplusplus
extern "C" {
#endif


struct wam_bins;
typedef struct wam_bins wam_bins_t;


extern wam_bins_t *wam_bins_init(int num_separators, ...);

extern void wam_bins_free(wam_bins_t *b);

extern void wam_bins_update(wam_bins_t *b, double value, int weight);

extern binned_result_t *wam_bins_return(wam_bins_t *b);

extern void wam_bins_store(wam_bins_t *b, FILE *f);
extern void wam_bins_load(wam_bins_t *b, FILE *f);

#ifdef __cplusplus
}
#endif

#endif /* __WAM_BINS_H__ */
