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
#include <stdlib.h>
#include <assert.h>

#include "wam.h"
#include "wam_ucount.h"
#include "wam_weighted_ucount.h"


/* ucount segment per weight */
struct segment {
    /* next segment in the list of segments */
    segment *next;

    double weight;  /* segment weight */

    /* keeping track of number of counts to bubble most frequently accessed
     * weights to the top; also to skip empty segments during serialization */
    uint64_t count;

    /* ucount itself */
    wam_ucount_t *ucount;
};


struct wam_weighted_ucount {
    bool is_merge_mode;

    int64_t cached_ucount;  /* cached ucount estimate (-1 for undefined) */

    /* list of segments */
    int segment_count;
    segment *segment_list;
};


/* TODO: pool allocator for weighted segments; also allocation strategy that
 * provides better cache locality for segments belonging to a single weighted
 * ucount */
static
segment *find_segment_by_weight(segment **segment_list, double weight, segment **last)
{
    segment *curr = *segment_list;
    segment *prev = NULL;  /* previous element in the list */
    segment *prev_prev = NULL;  /* previous to previous element in the list */

    while (curr) {
        if (curr->weight == weight) {  /* found */
            if (prev && prev->count < curr->count) {
                //LOG("W_UCOUNT: swapping weights: %g:%lld and %g:%lld\n", prev->weight, prev->count, curr->weight, curr->count);
                /* if this isn't the first element, and prev->count <
                 * this->count, change their order so that the segment with the
                 * higher count gets closer to the top of the list
                 *
                 * NOTE, XXX: such reordering may lead to non-identical results
                 * when merging a single file which makes debugging somewhat
                 * more difficult */
                prev->next = curr->next;
                curr->next = prev;
                if (prev_prev) {
                    prev_prev->next = curr;
                } else {
                    *segment_list = curr;  /* no previous to previous => updating head */
                }
            }
            break;
        }

        /* not found => keep searching */
        prev_prev = prev;
        prev = curr;

        curr = curr->next;
    }

    *last = prev;  /* return the last element if not found */
    return curr;
}


static
segment *find_or_alloc_segment(wam_weighted_ucount *u, double weight)
{
    segment *last;
    segment *s = find_segment_by_weight(&u->segment_list, weight, &last);
    if (!s) {
        /* not found => need to allocate and initialize a new one */
        s = new segment;

        /* push to back */
        s->next = NULL;
        if (!u->segment_list) {  /* segment_list is empty */
            u->segment_list = s;
        } else {  /* segment_list is not empty && last is its last element */
            last->next = s;
        }
        u->segment_count++;
        //LOG("W_UCOUNT: alloc_segment: %d, %g\n", u->segment_count, weight);

        /* init segment */
        s->weight = weight;
        s->count = 0;
        s->ucount = (u->is_merge_mode)? wam_ucount_init_merge() : wam_ucount_init();
    }

    s->count++;  /* increment number of samples */
    return s;
}


wam_weighted_ucount_t *wam_weighted_ucount_init(void)
{
    wam_weighted_ucount_t *u = new wam_weighted_ucount;

    u->is_merge_mode = false;
    u->cached_ucount = -1;  /* -1 for undefined */

    u->segment_count = 0;
    u->segment_list = NULL;

    return u;
}


/* see wam_ucount_init_merge() for details */
wam_weighted_ucount_t *wam_weighted_ucount_init_merge(void)
{
    wam_weighted_ucount_t *u = wam_weighted_ucount_init();
    u->is_merge_mode = true;

    return u;
}


static
void reset_segment(segment *s)
{
    if (s->count) {  /* reset if it was used */
        s->count = 0;
        wam_ucount_reset_merge(s->ucount);
    }
}


void wam_weighted_ucount_reset_merge(wam_weighted_ucount_t *u)
{
    segment *s = u->segment_list;
    for (; s; s = s->next) {
        reset_segment(s);
    }
}


void wam_weighted_ucount_update_uint64(wam_weighted_ucount_t *u, uint64_t value, double weight)
{
    segment *s = find_or_alloc_segment(u, weight);

    /* update ucount for this weight */
    wam_ucount_update_uint64(s->ucount, value);
}


void wam_weighted_ucount_update_double(wam_weighted_ucount_t *u, double value, double weight)
{
    wam_weighted_ucount_update_uint64(u, double_to_uint64(value), weight);
}


/* NOTE: using char and 3 here to be able to distingwish between ucount (beeing
 * converted to weighted_ucount) and weighted_ucount */
#define WEIGHTED_UCOUNT_VERSION 3
static unsigned char weighted_ucount_version;
static uint32_t segment_count;
double weight;


void store_segment(segment *s, FILE *f)
{
    weight = s->weight;
    if (fwrite(&weight, sizeof(weight), 1, f) != 1)
        handle_io_error("storing ucount segment weight", f);

    /* store ucount for this weight */
    wam_ucount_store(s->ucount, f);
}


void load_segment(wam_weighted_ucount_t *u, FILE *f)
{
    if (fread(&weight, sizeof(weight), 1, f) != 1)
        handle_io_error("loading ucount segment weight", f);

    /* load ucount for this weight */
    segment *s = find_or_alloc_segment(u, weight);
    wam_ucount_load(s->ucount, f);
}


void skip_segment(FILE *f)
{
    if (fread(&weight, sizeof(weight), 1, f) != 1)
        handle_io_error("loading ucount segment weight", f);

    /* skip ucount */
    wam_ucount_load(NULL, f);
}


uint64_t get_segment_size(segment *s)
{
    return sizeof(weight) + wam_ucount_get_size(s->ucount);
}


/* FIXME: this is error prone (and actually led to an error). things like that
 * MUST be always implemented as a dry-run of 'store' operation!!! */
uint64_t wam_weighted_ucount_get_size(wam_weighted_ucount_t *u)
{
    uint64_t size = sizeof(weighted_ucount_version) + sizeof(segment_count);

    segment *s = u->segment_list;
    for (; s; s = s->next) {
        if (s->count) size += get_segment_size(s);
    }

    return size;
}


void wam_weighted_ucount_store(wam_weighted_ucount_t *u, FILE *f)
{
    weighted_ucount_version = WEIGHTED_UCOUNT_VERSION;
    if (fwrite(&weighted_ucount_version, sizeof(weighted_ucount_version), 1, f) != 1)
        handle_io_error("storing weighted ucount version", f);

    /* get the number of non-empty segments */
    segment *s;
    segment_count = 0;
    for (s = u->segment_list; s; s = s->next) {
        if (s->count) segment_count++;
    }

    if (fwrite(&segment_count, sizeof(segment_count), 1, f) != 1)
        handle_io_error("stroing weighted ucount segment count", f);

    /* store segments one by one */
    //LOG("st: %u ", segment_count);
    for (s = u->segment_list; s; s = s->next) {
        if (s->count) {
            store_segment(s, f);
        }
    }
    //LOG("\n");
}


void wam_weighted_ucount_load(wam_weighted_ucount_t *u, FILE *f)
{
    if (fread(&weighted_ucount_version, sizeof(weighted_ucount_version), 1, f) != 1)
        handle_io_error("loading weighted ucount version", f);

    if (weighted_ucount_version <= 1) {
        /* simple (non-weighted) ucount => turning it into weighted ucount */
        bool is_exact = weighted_ucount_version;  // 0 | 1

        /* NOTE: assuming weight = 1 for unsampled queries */
        //LOG("W_UCOUNT: promoting ucount to weighted\n");
        segment *s = find_or_alloc_segment(u, 1 /* = weight */);

        wam_ucount_load_common(s->ucount, is_exact, f);
    } else {
        /* weighted ucount */
        assert(weighted_ucount_version == WEIGHTED_UCOUNT_VERSION);

        if (fread(&segment_count, sizeof(segment_count), 1, f) != 1)
            handle_io_error("loading weighted ucount segment count", f);

        /* load segments one by one */
        //LOG("ld: %u ", segment_count);
        while (segment_count--) {
            load_segment(u, f);
        }
        //LOG("\n");
    }
}


/* workaround for a bug in wam_weighted_ucount_get_size; the size was
 * miscalculated and we can't skip it over easily without trying to actually
 * load it */
void wam_weighted_ucount_skip(FILE *f)
{
    if (fread(&weighted_ucount_version, sizeof(weighted_ucount_version), 1, f) != 1)
        handle_io_error("loading weighted ucount version", f);

    if (weighted_ucount_version <= 1) {
        /* simple (non-weighted) ucount => turning it into weighted ucount */
        bool is_exact = weighted_ucount_version;  // 0 | 1

        wam_ucount_load_common(NULL, is_exact, f);
    } else {
        /* weighted ucount */
        assert(weighted_ucount_version == WEIGHTED_UCOUNT_VERSION);

        if (fread(&segment_count, sizeof(segment_count), 1, f) != 1)
            handle_io_error("loading weighted ucount segment count", f);

        /* skip actually written segments one by one */
        while (segment_count--) {
            skip_segment(f);
        }
    }
}


static
uint64_t get_count(wam_weighted_ucount_t *u)
{
    /* go over segments one by one and summarize their weights
     *
     * TODO: account for the fact that estimates from higher weights may may be
     * included in estimates in loser weights. E.g. a user_id that was once
     * reported in weight = 20 sample, may also be counted in weight = 1 sample
     *
     * in this case, we can apply inclusion/exclusion principle progressively to
     * to make a correction, e.g.
     *
     * U1 ucount is going to become (ucount(U1 union U20) - ucount(U20))
     *
     * we can apply the same idea progressively starting with max weights and
     * re-evaluating correction for lower rates one by one
     */
    double count = 0;  /* NOTE: using double here, because weights are double */
    segment *s = u->segment_list;
    for (; s; s = s->next) {
        //LOG("W_UCOUNT: get_count: %lld, %g\n", wam_ucount_get_count(s->ucount), s->weight);
        count += wam_ucount_get_count(s->ucount) * s->weight;
    }

    return (uint64_t)count;
}


uint64_t wam_weighted_ucount_get_count(wam_weighted_ucount_t *u)
{
    /* caching the returned value, because this function may be called from
     * order-by and other post-processing functions many times
     *
     * WARNING: because of caching, IT SHOULD NEVER BE CALLED IN THE MIDDLE
     * OF THE PROCESSING -- ONLY AT THE END */

    if (u->cached_ucount == -1)  /* undefined? */
        u->cached_ucount = get_count(u);

    return u->cached_ucount;
}
