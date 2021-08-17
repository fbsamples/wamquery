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
#ifndef __WAM_OUTPUT_H__
#define __WAM_OUTPUT_H__

#include <stdio.h>
#include <stdint.h>


enum wam_output_format {
    WAM_OUTPUT_JSON = 1,
    WAM_OUTPUT_MPACK = 2
};

extern void wam_output_init(FILE *output_file, enum wam_output_format output_format);

extern int wam_output_get_current_level(void);
extern uint64_t wam_output_get_current_offset(void);
extern uint32_t wam_output_get_current_buffer_size(void);

extern void wam_output_flush(void);

extern void wam_output_array_begin(int count);
extern void wam_output_array_begin_counting(void);
extern void wam_output_array_end(void);

extern void wam_output_map_begin(int count);
extern void wam_output_map_begin_counting(void);
extern void wam_output_map_end(void);

extern void wam_output_map_key(char *str);
extern void wam_output_map_string_field(char *key, char *value);

extern void wam_output_cstring(char *str);
#define wam_output_literal(s) wam_output_literal_string(s, sizeof(s)-1)
extern void wam_output_literal_string(char *str, int len);
extern void wam_output_string(char *str, int len);
extern void wam_output_null(void);
extern void wam_output_int64(int64_t x);
extern void wam_output_int64_as_string(int64_t x);
extern void wam_output_double(double x);
extern void wam_output_bool(int64_t x);


#endif /* __WAM_OUTPUT_H__ */
