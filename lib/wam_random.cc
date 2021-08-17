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
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>

#ifdef USE_CXX11
#include <random>
#else
#include <tr1/random>
#endif

#include "wam_random.h"


/* the current seed for the RNG */
static uint64_t rng_seed;

/* the RNG itself */

#ifdef USE_CXX11
std::mt19937_64 rng;
#else
std::tr1::mt19937 rng;
#endif


void wam_random_init(uint64_t *seed)
{
    /* used for choosing a new random seed */
    static uint32_t seedInput = 0;

    if (seed) {
        rng_seed = *seed;
    }
    else {
        /* this code was borrowed from
         * https://github.com/facebook/folly/blob/master/folly/Random.cpp
         * (licensed under Apache2) */
        struct timeval tv;
        gettimeofday(&tv, NULL);
        const uint32_t kPrime0 = 51551;
        const uint32_t kPrime1 = 61631;
        const uint32_t kPrime2 = 64997;
        const uint32_t kPrime3 = 111857;
        rng_seed = kPrime0 * (seedInput++)
            + kPrime1 * static_cast<uint32_t>(getpid())
            + kPrime2 * static_cast<uint32_t>(tv.tv_sec)
            + kPrime3 * static_cast<uint32_t>(tv.tv_usec);
    }

    /* (re-)seed the RNG */
    rng.seed(rng_seed);
}


uint64_t wam_random_uint64(uint64_t range_start, uint64_t range_end)
{
#ifdef USE_CXX11
    std::uniform_int_distribution<uint64_t> uniform_int(range_start, range_end);
#else
    std::tr1::uniform_int<uint64_t> uniform_int(range_start, range_end);
#endif
    return uniform_int(rng);
}
