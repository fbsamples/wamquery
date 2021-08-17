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

/* priority queue for incremental N-way summary file merge */

#include <queue>
#include <vector>

#include "wam_pqueue.h"


struct Compare
{
    inline
    bool operator() (const pqueue_item_t *a, const pqueue_item_t *b) const
    {
        /* NOTE: negating the result of the less comparison to turn max heap
         * (the default) to min heap */
        return !wam_query_groupby_compare(a->key, b->key);
    }
};


/* dynamically allocated storage for the priority_queue (min heap) */
typedef std::priority_queue<pqueue_item_t *, std::vector<pqueue_item_t *>, Compare> pqueue_t;
static pqueue_t *pqueue = NULL;


/* intialize priority queue state */
void wam_pqueue_init(void)
{
    pqueue = new pqueue_t();
}


void wam_pqueue_free(void)
{
    delete pqueue;
}


void wam_pqueue_push(pqueue_item_t *item)
{
    pqueue->push(item);
}


pqueue_item_t *wam_pqueue_pop(void)
{
    if (pqueue->empty()) return NULL;

    pqueue_item_t *res = pqueue->top();
    pqueue->pop();

    return res;
}
