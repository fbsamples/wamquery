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

/* numeric quantiles
 *
 * This implementation uses base-2 logarithmic binning with base-2 linear
 * sub-binning over the full IEEE 754 double value range including subnormals
 * and infinities.
 *
 * This approach guarantees an upper bound for relative error for estimated
 * quantile values as 1/2^precision. In other words, the maximum difference
 * between real and estimated quantile value never exceeds a factor of
 * 1/2^precision. For example, for binary precision 10, the relative error is
 * 1/2^10 which approximately equals to 0.001
 *
 * The implementation is based on a hash table. Updates are O(1) in time. Space
 * used is O(|<input double values trimmed to specified precicision>|).
 *
 * Returning quantiles is logarithmic to the size of the state.
 *
 * TODO: unify with wam_hist.cc when used with non-fixed bins
 *
 *
 * 1. Overview
 *
 *    You can read an overview of this approach at "High Dynamic Range (HDR)
 *    Histogram":
 *
 *         https://github.com/HdrHistogram/HdrHistogram
 *
 *    They also have a direct C port:
 *
 *         https://github.com/HdrHistogram/HdrHistogram_c
 *
 *    Their main implementation is in Java. It is 3 orders of magnitude more
 *    complex than ours (a lot of its complexity is incidental, i.e. code is not
 *    very good), doesn't work on a full double range, and has an ennormous
 *    space overhead for real-life distributions we are dealing with. It was
 *    originally written for histogramming integer values with ranges known
 *    upfront, e.g. for server-side request latencies.
 *
 *    Potentially, the HDR implementation can have smaller constant factors for
 *    O(1) updates that ours as it is array-based, but with large arrays for
 *    wide value order ranges it really depends on an the overall cache usage
 *    profile.
 *
 *    The HDR memory footprint can be improved using various approaches. Here's
 *    one of them: https://github.com/HdrHistogram/HdrHistogram/issues/54
 *
 *    Another approach would be to allocate memory regions (e.g. linear
 *    sub-bins) dynamically as soon as we get at least one value falling in the
 *    logarithmic bin.
 *
 *    In our implementation, we minimize memory usage by using hashtable for
 *    storing unique bins.
 *
 *
 * 2. Alternative methods
 *
 *    Approximate quantiles is a heavily researched topic. Here's a overiew of
 *    what is relevant to our user case.
 *
 *    Most scientific approaches tend to focus on estimating quantiles with a
 *    relative error bound on rank and not on input values. For example, when
 *    asked for a 99-th percentile, with a 0.1 precision, they may return a
 *    value for any percentile in the 89.1 and 99.1 range. This doesn't say
 *    anything about how far the returned percentile value is from the true one.
 *    On the other hand, most algorithms from this category work on all
 *    histograms, not limited to numeric histograms we are dealing with here.
 *
 *    These methods vary in accuracy, space and time-complexity. State of the
 *    art algorithms tend to require space log-linear or worse to the 1/error,
 *    and their update complexity is logarithmic or worse to the size of the
 *    space.
 *
 *    It took at least several decades to come up with modern-day practical
 *    algorithms and provide theoretically optimal bounds for the problem.
 *    Overall, these results represent an ennormous improvement over ad-hoc
 *    approaches based on a random sampling (used, for example, in Cloudera
 *    Impala) and some earlier algorithms (used, for example, in Google's
 *    open-sourced Sawzall).
 *
 * 2.1. Best in class randomized algorithm is described in this paper. The paper
 *      provides an overview and comparative evaluation of various methods and
 *      describes a new randomized algorithm called Random. The newly presented
 *      algorithm provides optimal theoretical performance and it is relatively
 *      simple to implement.
 *
 *      "Quantiles over Data Streams: An Experimental Study" (SIGMOD 2013) by Lu
 *      Wang et al.
 *      (http://dimacs.rutgers.edu/~graham/pubs/papers/nquantiles.pdf)
 *
 *      Presentation slides and implementation (not-production quality) for the
 *      Random algorithm are available here:
 *
 *          https://github.com/coolwanglu/quantile-alg
 *
 *
 * 2.2. Efficient and very popular method for so called "biased quantiles",
 *      providing higher accuracy for higher quantile ranks (e.g. estimate for
 *      99.9 perentile would be a lot more accurate than for 50-th percentile):
 *
 *      Effective computation of biased quantiles over data streams (2005) by by
 *      Graham Cormode, S. Muthukrishnan, et al.
 *      (http://dimacs.rutgers.edu/~graham/pubs/papers/bquant-icde.pdf)
 *
 *      Some practical implementations:
 *
 *      in C: https://github.com/armon/statsite
 *
 *          -- there may be some problems with this implementation , e.g. it may
 *          not offer higher accuracy for higher ranks in pracice
 *
 *      in Go: https://github.com/bmizerany/perks
 *
 *
 * 2.3. A bit dated but still very helpful survey of various algorithms:
 *
 *      "Quantiles on Streams" (https://www.cs.ucsb.edu/~suri/psdir/ency.pdf)
 *
 *
 * 2.4. Practically none of the papers suggest ways to distribute described
 *      algorithms in a practical way and with error bounds. This paper pretty
 *      much solves this problem:
 *
 *      Holistic Aggregates in a Networked World: Distributed Tracking of
 *      Approximate Quantiles (2005) by Graham Cormode, Minos Garofalakis
 *      (http://dimacs.rutgers.edu/~graham/pubs/papers/cdquant.pdf)
 *
 *      Note that there is a more modern method that further minimizes
 *      communication overhead for online distributed streaming quantiles by
 *      using 2-way communication between coordinator and leaf nodes, but this
 *      is not relevant for us, because our computation is a) offline b) can't
 *      take advantage of 2-way communication which would be impractical for a
 *      MPP query system. But here's a reference just in case:
 *
 *          Optimal tracking of distributed heavy hitters and quantiles (2009)
 *          by Ke Yi, Qin Zhang (https://www.cse.ust.hk/~yike/pods09-hhquan.pdf)
 *
 *
 * 2.5. Finally, a lot of people use a heuristics-based method described in the
 *      following paper:
 *
 *         A streaming parallel decision tree algorithm (2008) by Yael
 *         Ben-haim , Elad Yom-tov
 *         (http://jmlr.csail.mit.edu/papers/volume11/ben-haim10a/ben-haim10a.pdf)
 *
 *      Users include:
 *
 *          - Hadoop/Hive (NumericHistogram.java)
 *          - hive/udf/lib/NumericHistogramImproved.java (and its C ports)
 *          - Metamarkets Druid:
 *                  They actually store distribution summaries and came up with
 *                  substantial optimizations for the merge stage. Description
 *                  along with some empirical evaluation is available at
 *                  https://metamarkets.com/2013/histograms/
 *
 *          - Go: https://github.com/VividCortex/gohistogram
 *          - Clojure/Java: https://github.com/bigmlcom/histogram
 *
 *      Main advantages:
 *
 *         - convenient: you just need to specify the number of bins (max size
 *           of algorithm's state), and implementation will take care of the
 *           rest.
 *
 *         - compact state: this number for a lot of practical use-cases can be
 *           as low as 100, which is suitable for storing aggregates.
 *
 *         - reasonably fast: updates are something in the order of log(size of
 *           the state), which is strictly bounded
 *
 *         - relatively simple to implement
 *
 *
 *      Main disadvantages:
 *
 *         - absolutely no guarantee on accuracy. Can be 5% off, can be 20% off,
 *           there is no way to tell. Using higher number of bins (e.g. 10000
 *           instead of 100), seem to help in practice, but still no guarantee.
 *
 *         - updated are still slower than O(1)
 */

#include <stdlib.h>
#include <math.h>      // isnan

#include <algorithm>   // min, max, sort, lower_bound

#include "wam.h"
#include "wam_ntiles.h"
#include "wam_hashtbl.h"


// 2^10 = 1024; which roughly corresponds to 1/1024 =~ 0.001 relative error in
// decimal
//
// TODO: make it configurable from the queries
#define BINARY_PRECISION 10


struct wam_ntiles {
    wam_hashtbl_t *hashtbl;

    uint64_t total_count;
    double min, max;

    // lazily allocated array of pointers to unique values stored in hashtbl
    unique_value_t **unique_values;
};


wam_ntiles_t *wam_ntiles_init(void)
{
    wam_ntiles_t *nt = new wam_ntiles;

    /* TODO, XXX: to provide better data locality, we can preallocate
     * anticipated frequent bins so that their counters are laid down closer to
     * each other. For example, we can preallocate bins for values in
     * [0--2^precision) range, or extend it even further
     *
     * To make it generic, we can periodically recalculate most frequently
     * updated buckets and reshuffle them in the allocated memory. We could even
     * do it online, but this could result in a lot higher overhead which would
     * negate the benefit of doing something like that in the first place */

    nt->hashtbl = wam_hashtbl_init_unique_values();

    nt->total_count = 0;

    nt->min = INFINITY;
    nt->max = -INFINITY;

    // allocated lazily below
    nt->unique_values = NULL;

    return nt;
}


/*
#include <stdio.h>
#include <stdint.h>
#include <math.h>      // isnan

static inline
double trim_double_to_precision(double value, unsigned precision);

int main()
{
    double bin = 0;
    double d;
    for (d = 127; d <= 1025; d++) {
        double t = trim_double_to_precision(d, 7);
        if (t == bin) {
            printf(" %.17g", d);
        }
        else {
            int exp;
            frexp(d, &exp);
            bin = t;
            printf("\nexp = %d, bin = %.17g: %.17g", exp, bin, d);
        }
    }
    return 0;
}
*/


static inline
double trim_double_to_precision(double value, unsigned precision)
{
    union {
        double d;
        uint64_t i;
    } u;

    // trim the double value's mantissa to the specified binary precision:
    //
    // keep leading sign(1 bit), exponent(11 bits), and precision bits
    // and zero all the lower bits beyond that
    const uint64_t mask = ~(~0ULL >> (1 + 11 + precision));

    u.d = value;
    u.i &= mask;

    return u.d;
}


void wam_ntiles_update(wam_ntiles_t *nt, double value, int weight)
{
    // discarding NaNs as there's no way to interpret them
    // XXX: should we at least count them and print a warning?
    //
    // NOTE: infinities and subnormals are OK
    if (isnan(value)) return;

    nt->total_count += weight;
    nt->min = std::min(nt->min, value);
    nt->max = std::max(nt->max, value);

    value = trim_double_to_precision(value, BINARY_PRECISION);

    wam_hashtbl_update_unique_values(nt->hashtbl, value, weight);
}


static inline
bool compare_unique_value(unique_value_t *a, unique_value_t *b)
{
    return a->value < b->value;
}


static inline
bool compare_unique_value_count(unique_value_t *a, double b)
{
    return a->count < b;
}


/* return n-th percentile; NOTE: n is from [0, 100] range */
double wam_ntile(wam_ntiles_t *nt, double n)
{
    uint64_t i;
    uint64_t unique_value_count = wam_hashtbl_get_unique_item_count(nt->hashtbl);

    // check no values -- NOTE: NaN will be formatted as JSON null by
    // format_float() in wam_query.c
    if (!unique_value_count) return NAN;

    // created and array of (value, count) pairs and sort it by value (this is
    // only done once, subsequent wam_ntile invocations will be reusing it)
    unique_value_t **unique_values = nt->unique_values;
    if (!unique_values) {
        nt->unique_values = unique_values = new unique_value_t*[unique_value_count];

        wam_hashtbl_iterator_t *it = wam_hashtbl_first(nt->hashtbl);
        for (i = 0; it; i++, it = wam_hashtbl_next(it)) {
            unique_value_t *v = (unique_value_t *)wam_hashtbl_get_item(it);
            unique_values[i] = v;
        }

        std::sort(unique_values, unique_values + unique_value_count, compare_unique_value);

        /* make a cumulative histogram */
        uint64_t count = 0;
        for (i = 0; i < unique_value_count; i++) {
            count += unique_values[i]->count;
            unique_values[i]->count = count;
        }
    }

    // now, calculate the ntile itself

    // special cases -- min() and max() are known upfront
    if (n == 0.0) return nt->min;
    if (n == 100.0) return nt->max;

    double target_count = nt->total_count * n / 100.0;

    /* older version that doesn't use binary search */
    /*
    uint64_t count = 0;
    for (i = 0; i < unique_value_count; i++) {
        count += unique_values[i]->count;

        if (target_count <= count) break;
    }
    */

    /* binary search: return the lowest bin such that target_count <= cumulative
     * bin count
     *
     * TODO: optimize subsequent wam_ntile() calls if requested ntile is greater
     * than the previously requested one, in which case we can use the new
     * lower_bound as the left starting point
     */
    unique_value_t **lower_bound = std::lower_bound(unique_values, unique_values + unique_value_count, target_count, compare_unique_value_count);
    i = lower_bound - unique_values;

    /* returnining an upper bound for all percentiles in (0, 100) range */
    if (i < unique_value_count - 1)
        return unique_values[i + 1]->value;
    else
        return nt->max;
}


void wam_ntiles_store(wam_ntiles_t *nt, FILE *f)
{
    if (fwrite(&nt->total_count, sizeof(nt->total_count), 1, f) != 1)
        handle_io_error("storing ntiles", f);
    if (fwrite(&nt->min, sizeof(nt->min), 1, f) != 1)
        handle_io_error("storing ntiles", f);
    if (fwrite(&nt->max, sizeof(nt->max), 1, f) != 1)
        handle_io_error("storing ntiles", f);

    wam_hashtbl_store_unique_values(nt->hashtbl, f);
}


void wam_ntiles_load(wam_ntiles_t *nt, FILE *f)
{
    uint64_t total_count;
    double min, max;

    if (fread(&total_count, sizeof(nt->total_count), 1, f) != 1)
        handle_io_error("loading ntiles", f);
    if (fread(&min, sizeof(nt->min), 1, f) != 1)
        handle_io_error("loading ntiles", f);
    if (fread(&max, sizeof(nt->max), 1, f) != 1)
        handle_io_error("loading ntiles", f);

    nt->total_count += total_count;
    nt->min = std::min(nt->min, min);
    nt->max = std::max(nt->max, max);

    wam_hashtbl_load_unique_values(nt->hashtbl, f);
}
