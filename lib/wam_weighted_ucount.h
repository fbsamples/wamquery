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
#ifndef __WAM_WEIGHTED_UCOUNT_H__
#define __WAM_WEIGHTED_UCOUNT_H__

#ifdef __cplusplus
extern "C" {
#endif


struct wam_weighted_ucount;
typedef struct wam_weighted_ucount wam_weighted_ucount_t;


extern wam_weighted_ucount_t *wam_weighted_ucount_init(void);

extern wam_weighted_ucount_t *wam_weighted_ucount_init_merge(void);
extern void wam_weighted_ucount_reset_merge(wam_weighted_ucount_t *u);

extern void wam_weighted_ucount_update_uint64(wam_weighted_ucount_t *u, uint64_t value, double weight);
extern void wam_weighted_ucount_update_double(wam_weighted_ucount_t *u, double value, double weight);

extern uint64_t wam_weighted_ucount_get_count(wam_weighted_ucount_t *u);


extern uint64_t wam_weighted_ucount_get_size(wam_weighted_ucount_t *u);
extern void wam_weighted_ucount_store(wam_weighted_ucount_t *u, FILE *f);
extern void wam_weighted_ucount_load(wam_weighted_ucount_t *u, FILE *f);
extern void wam_weighted_ucount_skip(FILE *f);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_WEIGHTED_UCOUNT_H__ */
