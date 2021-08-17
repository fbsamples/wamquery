## Copyright (c) Facebook, Inc. and its affiliates.
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

LIB = libwam.a
REL = libwam.o

OBJS = \
	wam_random.o \
	wam_hashtbl.o wam_groupby.o wam_strtbl2.o \
	wam_smptbl.o \
	wam_ucount2.o wam_hist.o wam_ntiles.o \
	wam_orderby.o wam_bins.o \
	wam_pqueue.o \
	wam_weighted_ucount.o \
	\
	xxhash.o \
	\
	MurmurHash3.o \
	hll.o hll_constants.o


ifdef DEBUG
CFLAGS   = -Wall -g
else
CFLAGS   = -Wall -O3
endif

CXXFLAGS = $(CFLAGS)
CXXFLAGS += -std=c++11
CFLAGS += -DUSE_CXX11=1


# to make sure we recompile the sources after *.h changes
SRCS = $(wildcard *.cc *.c)


all: $(LIB)

# NOTE: making sure $(LIB) is built strictly after $(REL) to work correctly with parallel builds
$(LIB):  $(REL)
	$(MAKE) DEBUG=1 $(OBJS)
	$(AR) -cr $@ $(OBJS)

# optimized relocatable object for faster linking in production
$(REL): $(OBJS) $(PRIV_DIR)
	$(LD) -r $(OBJS) -o $@

clean:
	rm -f $(OBJS) $(LIB) $(REL)


.PHONY: all clean
