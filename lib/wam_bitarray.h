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
#ifndef __WAM_BITARRAY_H__
#define __WAM_BITARRAY_H__


#define BITOP(a, i, op) (a[i/64] op (1ULL << (i%64)))
#define BITOP_COND(a, i, op, cond) (a[i/64] op (cond << (i%64)))

#define BITARRAY_WORD_SIZE(n) ((n + 63) / 64)
#define BITARRAY_BYTE_SIZE(n) (BITARRAY_WORD_SIZE(n) * sizeof(uint64_t))


static
inline
void set_bit(uint64_t *array, int i)
{
    BITOP(array, i, |=);
}


/* cond must be 0 or 1 */
static
inline
void set_bit_cond(uint64_t *array, int i, uint64_t cond)
{
    BITOP_COND(array, i, |=, cond);
}


static
inline
void clear_bit(uint64_t *array, int i)
{
    BITOP(array, i, &=~);
}


static
inline
int test_bit(uint64_t *array, int i)
{
    return BITOP(array, i, &) != 0ULL;
}


static
inline
void assign_bit(uint64_t *array, int i, int f)
{
    /* equivalent to if (f) set_bit() else clear_bit() -- see
     * http://graphics.stanford.edu/~seander/bithacks.html#ConditionalSetOrClearBitsWithoutBranching
     */
    uint64_t w = array[i/64];
    uint64_t m = 1ULL << (i%64);
    array[i/64] = (w & ~m) | (-f & m);
}


static
inline
void clear_bitarray(uint64_t *array, int size)
{
    memset(array, 0, BITARRAY_BYTE_SIZE(size));
}


static
uint64_t *alloc_bitarray(int size)
{
    return (uint64_t*) malloc(BITARRAY_BYTE_SIZE(size));
}


#endif /* __WAM_BITARRAY_H__ */
