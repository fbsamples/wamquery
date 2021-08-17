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
#ifndef __WAM_HIST_H__
#define __WAM_HIST_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>


struct wam_hist;
typedef struct wam_hist wam_hist_t;


struct wam_distr;
typedef struct wam_distr wam_distr_t;


typedef
struct output_bin {
    uint64_t count;

    // [start, stop) bin value range; [start, stop] for the last bin
    double start;
    double stop;
} output_bin_t;


typedef
struct binned_result {
    // consider returning the actual non-0 value range and stripping leading and
    // trailing 0-weight bins automatically;
    // if we do this, we may need to return as well query_range...

    // counts outside of the histogram range
    uint64_t left_count, right_count;

    // total (u)count and range (u)count
    uint64_t total_count, range_count;

    int num_bins;
    output_bin_t bins[];
} binned_result_t;


extern wam_hist_t *wam_hist_init(int size, double config_min, double config_max, double bin_width);

extern void wam_hist_update(wam_hist_t *h, double value, int weight);

wam_distr_t *wam_hist_update_distr(wam_hist_t *h, wam_distr_t *accu);

extern binned_result_t *wam_hist_return(wam_hist_t *h);


extern void wam_hist_store(wam_hist_t *h, FILE *f);
extern void wam_hist_load(wam_hist_t *h, FILE *f);

extern void wam_hist_store_bins(uint64_t *bins, int count, FILE *f);
extern void wam_hist_load_bins(uint64_t *bins, int count, FILE *f);

extern void wam_hist_store_outliers(uint64_t left_count, uint64_t right_count, FILE *f);
extern void wam_hist_load_outliers(uint64_t *left_count, uint64_t *right_count, FILE *f);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_HIST_H__ */
