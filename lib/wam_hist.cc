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

/* numeric histograms */

#include <stdint.h>
#include <stdlib.h>  // malloc()
#include <float.h>
#include <string.h>  // memset()

// see comment about isfinite() below
#ifdef USE_CXX11
#include <cmath>
#else
#include <math.h>
#endif

#include <errno.h>

#include <algorithm>   // min, max

#include "wam.h"
#include "wam_hist.h"
#include "wam_hashtbl.h"


// histogram results (lazily allocated below)
// TODO: this is never free'd
static binned_result *res = NULL;

// capacity: max number of bins across all requested histograms; making sure
// we have enough bins in the output array
static int bins_cap = 0;


// data distribution info shared between several histograms, e.g. those that
// belong to different group-by groups; we need it so that we could align
// bins of group'd histograms for visual comparison
struct wam_distr {
    uint64_t count;
    double min, max;
};


struct wam_hist {
    // config
    int size;
    double bin_width;  // 0 means undefined
    double config_min, config_max;
    bool is_fixed_bins;

    // runtime
    wam_distr_t distr;
    wam_distr_t *merged_distr;  // distribution info merged across all group-by groups
    uint64_t left_count, right_count;  // counts of values falling outside of the histogram range

    // used when is_fixed_bins = false
    wam_hashtbl_t *hashtbl;

    // used when is_fixed_bins = true
    uint64_t *bins;
};


static inline
void distr_init(wam_distr_t *d)
{
    d->count = 0;
    d->min = DBL_MAX;
    d->max = -DBL_MAX;
}


static inline
void distr_update(wam_distr_t *d, double value)
{
    d->count++;
    d->min = std::min(d->min, value);
    d->max = std::max(d->max, value);
}


static inline
void distr_merge(wam_distr_t *d, wam_distr_t *accu)
{
    accu->count += d->count;
    accu->min = std::min(accu->min, d->min);
    accu->max = std::max(accu->max, d->max);
}


wam_distr_t *wam_hist_update_distr(wam_hist_t *h, wam_distr_t *accu)
{
    // lazily allocate
    if (!accu) {
        // TODO: this is never free'd
        accu = new wam_distr;
        distr_init(accu);
    }
    distr_merge(&h->distr, accu);
    h->merged_distr = accu;

    return accu;
}


wam_hist_t *wam_hist_init(int size, double config_min, double config_max, double bin_width)
{
    wam_hist_t *h = new wam_hist;

    h->size = size;
    h->bin_width = bin_width;
    h->config_min = config_min;
    h->config_max = config_max;

    distr_init(&h->distr);
    h->merged_distr = NULL;

    h->left_count = h->right_count = 0;

    // the historgram's bins are known if both MIN and MAX bounds are defined
    if (config_min != -DBL_MAX && config_max != DBL_MAX) {
        h->is_fixed_bins = true;
        h->bins = new uint64_t[size];
        if (h->bin_width == 0.0)
            h->bin_width = (config_max - config_min) / size;
        memset(h->bins, 0, size*sizeof(uint64_t));
    } else {
        h->is_fixed_bins = false;
        h->hashtbl = wam_hashtbl_init_unique_values();
    }

    // update the max capacity value
    bins_cap = std::max(bins_cap, size);

    return h;
}


// calculate bin index
inline static
int bin_index(int size, double bin_width, double min, double max, double value)
{
    if (value == max)
        // XXX: max belongs to the last bin, not last + 1
        return size - 1;
    else
        return (value - min) / bin_width;
}


void wam_hist_update(wam_hist_t *h, double value, int weight)
{
    // XXX: discarding NaNs and Infinities as there's no reasonable way to
    // interpret them in histogram; in fact, there shouldn't be any of these in
    // client's data
#ifdef USE_CXX11
    /* make gcc on FB devserver happy, not sure why it is complaining about
     * standard C function */
    if (!std::isfinite(value)) return;
#else
    if (!isfinite(value)) return;
#endif

    // keep track of outliers, i.e. points falling outside of the histogram
    // range
    if (value > h->config_max) {
        h->right_count += weight;
        return;
    }

    if (value < h->config_min) {
        h->left_count += weight;
        return;
    }

    if (h->is_fixed_bins) {
        int i = bin_index(h->size, h->bin_width, h->config_min, h->config_max, value);
        h->bins[i] += weight;
    } else {
        distr_update(&h->distr, value);
        wam_hashtbl_update_unique_values(h->hashtbl, value, weight);
    }
}


binned_result_t *wam_hist_return(wam_hist_t *h)
{
    wam_distr_t *distr = (h->merged_distr)? h->merged_distr : &h->distr;

    if (!distr->count && !h->is_fixed_bins) return NULL; // no values - empty histogram

    /* lazily allocate the output record which includes the array of bins */
    if (!res) {
        res = (binned_result_t *)malloc(sizeof(struct binned_result) + bins_cap * sizeof(output_bin_t));
    }
    /* output bins to fill */
    output_bin_t *bins = res->bins;

    /* override the real min and max if they where configured upfront */
    double min = (h->config_min == -DBL_MAX)? distr->min: h->config_min;
    double max = (h->config_max == DBL_MAX)? distr->max: h->config_max;

    double bin_width = (h->bin_width != 0.0) ? h->bin_width : (max - min) / h->size;
    double bin_max = min + bin_width * h->size;
    if (max > bin_max) max = bin_max;

    // initialize histogram bins
    int i;
    for (i = 0; i < h->size; i++) {
        output_bin_t *b = &bins[i];

        b->start = min + i * bin_width;
        b->stop = b->start + bin_width;
        b->count = 0;
    }
    /* set the correct upper bound on the last bin (account for possible fp
     * error) */
    bins[h->size - 1].stop = max;

    uint64_t range_count = 0;
    if (h->is_fixed_bins) {
        for (i = 0; i < h->size; i++) {
            bins[i].count = h->bins[i];
            range_count += bins[i].count;
        }
    } else {
        wam_hashtbl_iterator_t *it = wam_hashtbl_first(h->hashtbl);
        for (; it; it = wam_hashtbl_next(it)) {
            unique_value_t *v = (unique_value_t *)wam_hashtbl_get_item(it);
            if (v->value <= max) {
                int i = bin_index(h->size, bin_width, min, max, v->value);
                bins[i].count += v->count;
                range_count += bins[i].count;
            }
        }
    }

    /* populate the remaining output values */
    res->num_bins = h->size;
    res->left_count = h->left_count;
    res->right_count = h->right_count;
    res->range_count = range_count;
    res->total_count = range_count + h->left_count + h->right_count;

    // TODO: don't do the actual work of binning the data if there was not a
    // single data point
    if (!res->total_count) return NULL; // no values - empty histogram

    return res;
}


void wam_hist_store_bins(uint64_t *bins, int count, FILE *f)
{
    if (fwrite(bins, sizeof(*bins), count, f) != (size_t)count)
        handle_io_error("storing bins", f);
}


void wam_hist_load_bins(uint64_t *bins, int count, FILE *f)
{
    uint64_t *bins_buf = new uint64_t[count];
    if (fread(bins_buf, sizeof(*bins), count, f) != (size_t)count)
        handle_io_error("loading bins", f);

    for (int i = 0; i < count; i++) bins[i] += bins_buf[i];

    delete []bins_buf;
}


void wam_hist_store_outliers(uint64_t left_count, uint64_t right_count, FILE *f)
{
    if (fwrite(&left_count, sizeof(left_count), 1, f) != 1)
        handle_io_error("storing bins outliers", f);
    if (fwrite(&right_count, sizeof(right_count), 1, f) != 1)
        handle_io_error("storing bins outliers", f);
}


void wam_hist_load_outliers(uint64_t *left_count, uint64_t *right_count, FILE *f)
{
    uint64_t l, r;

    if (fread(&l, sizeof(l), 1, f) != 1)
        handle_io_error("loading bins outliers", f);
    if (fread(&r, sizeof(r), 1, f) != 1)
        handle_io_error("loading bins outliers", f);

    *left_count += l;
    *right_count += r;
}


void wam_hist_store(wam_hist_t *h, FILE *f)
{
    if (fwrite(&h->distr, sizeof(wam_distr_t), 1, f) != 1)
        handle_io_error("storing histogram", f);

    wam_hist_store_outliers(h->left_count, h->right_count, f);

    if (h->is_fixed_bins)
        return wam_hist_store_bins(h->bins, h->size, f);
    else
        return wam_hashtbl_store_unique_values(h->hashtbl, f);
}


void wam_hist_load(wam_hist_t *h, FILE *f)
{
    wam_distr_t distr;
    if (fread(&distr, sizeof(wam_distr_t), 1, f) != 1)
        handle_io_error("loading histogram", f);
    distr_merge(&distr, &h->distr);

    wam_hist_load_outliers(&h->left_count, &h->right_count, f);

    if (h->is_fixed_bins)
        wam_hist_load_bins(h->bins, h->size, f);
    else
        wam_hashtbl_load_unique_values(h->hashtbl, f);
}
