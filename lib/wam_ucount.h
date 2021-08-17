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
#ifndef __WAM_UCOUNT_H__
#define __WAM_UCOUNT_H__

#ifdef __cplusplus
extern "C" {
#endif


struct wam_ucount;
typedef struct wam_ucount wam_ucount_t;


extern wam_ucount_t *wam_ucount_init(void);

extern wam_ucount_t *wam_ucount_init_merge(void);
extern void wam_ucount_reset_merge(wam_ucount_t *u);

extern void wam_ucount_update_uint64(wam_ucount_t *u, uint64_t value);
extern void wam_ucount_update_double(wam_ucount_t *u, double value);
extern void wam_ucount_update_multi(wam_ucount_t *u, int n, ...);

extern uint64_t wam_ucount_get_count(wam_ucount_t *u);


extern uint64_t wam_ucount_get_size(wam_ucount_t *u);
extern void wam_ucount_store(wam_ucount_t *u, FILE *f);
extern void wam_ucount_load(wam_ucount_t *u, FILE *f);
extern void wam_ucount_load_common(wam_ucount_t *u, int is_exact, FILE *f);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_UCOUNT_H__ */
