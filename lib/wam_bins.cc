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
#include <stdarg.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>  // memset
#include <float.h>

#include <algorithm>   // max

#include "wam_hist.h"  // output_bin_t
#include "wam_bins.h"


// bins results (lazily allocated below)
// TODO: this is never free'd
static binned_result *res = NULL;

// capacity: max number of bins across all requested histograms; making sure
// we have enough bins in the output array
static int bins_cap = 0;


struct wam_bins {
    // config
    double *separators;  // num_separators = num_bins + 1
    int num_bins;

    uint64_t left_count, right_count;  // counts of values falling outside of the bins range
    uint64_t *bins;
};


wam_bins_t *wam_bins_init(int num_separators, ...)
{
    wam_bins_t *b = new wam_bins();

    int num_bins = num_separators - 1;
    b->separators = new double[num_separators];
    b->bins = new uint64_t[num_bins];
    memset(b->bins, 0, num_bins*sizeof(uint64_t));

    b->num_bins = num_bins;
    b->left_count = b->right_count = 0;

    va_list vl;
    va_start(vl, num_separators);
    for (int i = 0; i < num_separators; i++)
    {
        double n = va_arg(vl, double);
        b->separators[i] = n;
    }
    va_end(vl);

    // update the max capacity value
    bins_cap = std::max(bins_cap, num_bins);

    return b;
}


void wam_bins_free(wam_bins_t *b)
{
    delete []b->separators;
    delete []b->bins;
    delete b;
}


void wam_bins_update(wam_bins_t *b, double value, int weight)
{
    // keep track of outliers, i.e. points falling outside of the binned range
    if (value > b->separators[b->num_bins]) {
        b->right_count++;
        return;
    }

    if (value < b->separators[0]) {
        b->left_count++;
        return;
    }

    // XXX: do bsearch when the number of spearators is large?
    for (int i = 0; i < b->num_bins; i++) {
        if (value < b->separators[i+1]) {
            b->bins[i] += weight;
            return;
        }
    }
}


binned_result_t *wam_bins_return(wam_bins_t *b)
{
    /* lazily allocate the output record which includes the array of bins */
    if (!res) {
        res = (binned_result_t *)malloc(sizeof(struct binned_result) + bins_cap * sizeof(output_bin_t));
    }
    /* output bins to fill */
    output_bin_t *bins = res->bins;

    // populate the output bins
    int i;
    bins[0].start = b->separators[0];
    bins[b->num_bins-1].stop = b->separators[b->num_bins];  // num_bins = num_separators - 1
    for (i = 1; i < b->num_bins; i++) {
        bins[i].start = bins[i-1].stop = b->separators[i];
    }
    uint64_t range_count = 0;
    for (i = 0; i < b->num_bins; i++) {
        bins[i].count = b->bins[i];
        range_count += bins[i].count;
    }

    /* populate the remaining output values */
    res->num_bins = b->num_bins;
    res->left_count = b->left_count;
    res->right_count = b->right_count;
    res->range_count = range_count;
    res->total_count = range_count + b->left_count + b->right_count;

    return res;
}


void wam_bins_store(wam_bins_t *b, FILE *f)
{
    wam_hist_store_outliers(b->left_count, b->right_count, f);
    wam_hist_store_bins(b->bins, b->num_bins, f);
}


void wam_bins_load(wam_bins_t *b, FILE *f)
{
    wam_hist_load_outliers(&b->left_count, &b->right_count, f);
    wam_hist_load_bins(b->bins, b->num_bins, f);
}
