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
#ifndef __WAM_H__
#define __WAM_H__


#include <stdint.h>
#include <stdio.h>

#define LOG(...) fprintf(stderr, __VA_ARGS__)


/* groupby key is the exact prefix (with size = key_size) of the tuple */
struct tuple;
typedef struct tuple tuple_t;


/* input state */
typedef
struct wam_input {
    FILE *file;              /* input file */
    char *filename;          /* input filename */

    /* record descriptor */
    int tuple_header_len;
    int *tuple_descriptor;  /* dynamically-allocated of tuple_header_len */

    /*
     * current input record
     */

    /* original tuple header loaded from the input file */
    uint64_t *tuple_header;  /* dynamically allocated of tuple_header_len */

    /* fixed-size group key constructed by the query from the tuple header */
    tuple_t *key;  /* dynamically allocated of sizeof(tuple_t) */

    /*
     * current input field
     */
    int tuple_header_pos;  /* field number */
    uint64_t *tuple_header_ptr;  /* pointer to the field's value */
} wam_input_t;


/* utils */
static inline
uint64_t double_to_uint64(double d)
{
    union {
        double d;
        uint64_t i;
    } u;
    u.d = d;
    return u.i;
}


static inline
double uint64_to_double(uint64_t i)
{
    union {
        double d;
        uint64_t i;
    } u;
    u.i = i;
    return u.d;
}


#ifdef __cplusplus
extern "C" {
#endif


extern void handle_io_error(const char *where, FILE *f);


/* comparison operators defined by the query */
extern int wam_query_orderby_compare(tuple_t *a, tuple_t *b);
extern int wam_query_groupby_compare(tuple_t *a, tuple_t *b);


/* global variable indicating that this query performes summary resampling */
extern int wam_summary_resampling_mode;


#ifdef __cplusplus
}
#endif

#endif /* __WAM_H__ */
