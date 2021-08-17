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
#ifndef __WAM_STRTBL_H__
#define __WAM_STRTBL_H__

#ifdef __cplusplus
extern "C" {
#endif


/* internal representation of strings */
typedef
struct string_value {
    uint32_t len;
    char str[];
} string_value_t;


extern void wam_strtbl_init(void);
extern void wam_strtbl_init_without_dedup(int num_strings, int average_length);

extern void wam_strtbl_reset(void);

extern string_value_t *wam_strtbl_find(uint64_t hash);

extern string_value_t *wam_strtbl_insert(uint64_t hash, char *str, int len);


extern void wam_strtbl_store(FILE *f);
extern void wam_strtbl_load(FILE *f);


#ifdef __cplusplus
}
#endif

#endif /* __WAM_STRTBL_H__ */
