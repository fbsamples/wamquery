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
#include "wam.h"
#include "wam_strtbl.h"
#include "wam_hashtbl.h"


static wam_hashtbl_t *hashtbl;


void wam_strtbl_init(void)
{
    hashtbl = wam_hashtbl_init_strings();
}


/* return the first matching string entry given its hash */
string_value_t *wam_strtbl_find(uint64_t hash)
{
    return wam_hashtbl_find_string(hashtbl, hash);
}


/* create a new string entry or return an existign one */
string_value_t *wam_strtbl_insert(uint64_t hash, char *str, int len)
{
    return wam_hashtbl_add_string(hashtbl, hash, str, len);
}


void wam_strtbl_store(FILE *f)
{
    wam_hashtbl_store(hashtbl, f);
}


void wam_strtbl_load(FILE *f)
{
    wam_hashtbl_load(hashtbl, f, NULL /* merge callback */);
    //LOG("loaded strtbl\n");
}
