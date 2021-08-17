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


/* utility for testing sequential file read performance */
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>


// Mac OS X and others without O_DIRECT
#ifndef O_DIRECT
#define O_DIRECT 0
#endif


//#define BLOCK_SIZE 4096
#define BLOCK_SIZE 65536


static int block_size = BLOCK_SIZE;
static int read_size = BLOCK_SIZE;
static int open_flags = 0;
static int stdio = 0;

static char *buf = NULL;


static volatile int ticked = 0;

static void handle_tick(int sig)
{
    ticked = 1;
    alarm(1);
}

static void start_ticker(void)
{
    signal(SIGALRM, handle_tick);
    alarm(1);
}


static unsigned long long last_read = 0;
static unsigned long long total_read = 0;

static void handle_read(int bytes_read)
{
    total_read += bytes_read;
    if (ticked) {
        ticked = 0;

        unsigned long long d = total_read - last_read;
        printf("read rate: %llu MB/s\n", d >> 20);

        last_read = total_read;
    }
}


static void do_gulp_file(char *filename)
{
    int fd = open(filename, O_RDONLY | open_flags);
    assert(fd >= 0);

    if (!stdio) {
        while (1) {
            int res = read(fd, buf, block_size);
            assert(res >= 0);

            if (!res) break; /* eof */

            handle_read(res);
        }

        close(fd);
    } else {
        FILE *f = fdopen(fd, "r");
        assert(f);

        static char *fread_buf = NULL;
        if (!fread_buf) {
            /* page-aligned fread buffer */
            assert(!posix_memalign((void **)&fread_buf, 4096, block_size));
        }

        assert(!setvbuf(f, fread_buf, _IOFBF, block_size));

        while (1) {
            int res = fread(buf, read_size, 1, f);
            if (res < 1) {
                assert(!ferror(f));
                break; /* eof */
            }
            handle_read(read_size);
        }

        fclose(f);
    }
}


static void gulp_file(char *filename)
{
    if (strcmp(filename, "-")) {
        do_gulp_file(filename);
    } else {
        char filename[1024];
        while (fgets(filename, sizeof(filename), stdin)) {
            filename[strlen(filename) - 1] = '\0';  /* chop trailing \n */

            if (!filename[0]) break; /* treat empty string as EOF */

            do_gulp_file(filename);
        }
    }
}


int main(int argc, char *argv[])
{
    int i;
    for (i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--bs")) {
            block_size = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--rs")) {
            read_size = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--direct")) {
             open_flags |= O_DIRECT;
        } else if (!strcmp(argv[i], "--stdio")) {
             stdio = 1;
        } else {
            break;
        }
    }

    /* page-aligned input buffer */
    //buf = (char *)malloc(block_size);
    assert(!posix_memalign((void **)&buf, 4096, block_size));

    start_ticker();

    if (i < argc) {
        for (; i < argc; i++) {
            gulp_file(argv[i]);
        }
    } else {
        gulp_file("-");
    }

    return 0;
}

