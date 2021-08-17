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
#ifndef __WAM_SMPTBL_H__
#define __WAM_SMPTBL_H__

#ifdef __cplusplus
extern "C" {
#endif


extern void wam_smptbl_init(void);

extern float *wam_smptbl_find(uint64_t hash);

extern float wam_smptbl_get(uint64_t hash, float default_val);

extern float *wam_smptbl_insert(uint64_t hash, float value);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_SMPTBL_H__ */
