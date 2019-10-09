##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2019-09-30 16:29:54 macan>
#
# This is the makefile for GK project.
#
# Armed with EMACS.

CC = gcc
LD = gcc
AR = ar
PYTHON = env python
GIT = env git
ECHO = /bin/echo

INC_PATH = $(HOME_PATH)/include
LIB_PATH = $(HOME_PATH)/lib
MDS = $(HOME_PATH)/mds
R2 = $(HOME_PATH)/r2
XNET = $(HOME_PATH)/xnet
TEST = $(HOME_PATH)/test
CONF = $(HOME_PATH)/conf
BIN = $(HOME_PATH)/bin
TRIGGERS = $(HOME_PATH)/triggers
BRANCH = $(HOME_PATH)/branch
FUSE = $(HOME_PATH)/fuse
OSD = $(HOME_PATH)/osd

ifdef USE_GIT_TAG
GIT_SHA = `$(GIT) rev-parse HEAD`
endif

COMPILE_DATE = `date`
COMPILE_HOST = `hostname`

# for now we know the number of cores of the target machines is 8, so just set
# it manually.
# GET_CORE = `grep -c processor /proc/cpuinfo`
GET_CORE = 8

ifeq ($(CPU_CORE),)
__CORES__=$(GET_CORE)
endif

# Debuging the memory and locks
XCFLAGS = -O2 -rdynamic -g -pg -fPIC
DEADCODESTRIP := -fdata-sections -ffunction-sections \
				 -Wl,--gc-sections
O3_OPT = -frename-registers -funroll-loops
W = -W -Wall -Wno-unused-parameter -Wbad-function-cast -Wuninitialized

CFLAGS += $(XCFLAGS) $(O3_OPT)
CFLAGS += -Wall -DCDATE="\"$(COMPILE_DATE)\"" -DGIT_SHA="\"$(GIT_SHA)\""\
			-DCHOST="\"$(COMPILE_HOST)\"" -I$(INC_PATH) \
			-I$(LIB_PATH) -I$(MDS) -I$(R2) \
			-D_GNU_SOURCE \
			-DGK_TRACING -DGK_DEBUG_MEMORY -DGK_DEBUG_LOCK_ \
			-D_USE_SPINLOCK_ -DGK_DEBUG_LATENCY_ -DXNET_BLOCKING \
			-DXNET_EAGER_WRITEV -DCPU_CORE=$(__CORES__) \
			-DFUSE_SAFE_OPEN -DXNET_CONN_EINTR \
			-DGK_OSD_STORE_DETECT_SLOW

ifndef JEMALLOC_HOME
ifeq ($(JEMALLOC),)
else
$(info USE JEMALLOC malloc function @ $(JEMALLOC).)
export JEMALLOC_HOME=$(JEMALLOC)
$(info ========================================================)
endif
endif

ifeq ($(JEMALLOC_HOME),)
else
CFLAGS += -DUSE_JEMALLOC=1 -I$(JEMALLOC_HOME)/include
LFLAGS += -L$(JEMALLOC_HOME)/lib -ljemalloc
endif

LFLAGS += -lrt -ldl -lpthread

# Region for depend files
TEST_MDS_SOURCE =
TEST_XNET_SOURCE = root.c client.c mds.c

MDS_AR_SOURCE = mds.c spool.c fe.c latency.c async.c prof.c conf.c \
                dispatch.c kvs.c cli.c
LIB_AR_SOURCE = lib.c time.c bitmap.c xlock.c segv.c conf.c md5.c \
                minilzo.c brtree.c crc32.c midl.c mdb.c
XNET_AR_SOURCE = xnet.c xnet_simple.c
R2_AR_SOURCE = root.c dispatch.c spool.c mgr.c bparser.c x2r.c cli.c \
               profile.c

INC_H_SOURCE = atomic.h err.h gk.h gk_const.h gk_k.h \
				gk_u.h memory.h site.h \
				tracing.h xhash.h xlist.h xlock.h xnet.h \
				xprof.h profile.h
MDS_H_SOURCE = mds.h async.h kvs.h prof.h
R2_H_SOURCE = root.h mgr.h root_config.h rprof.h
LIB_H_SOURCE = lib.h minilzo.h lmdb.h midl.h

inc_h_depend_files = $(patsubst %.h, $(INC_PATH)/%.h, $(INC_H_SOURCE)) \
						$(LIB_PATH)/hash.c
mds_h_depend_files = $(patsubst %.h, $(MDS)/%.h, $(MDS_H_SOURCE)) \
                        $(MDS)/latency.c
lib_h_depend_files = $(patsubst %.h, $(LIB_PATH)/%.h, $(LIB_H_SOURCE))
r2_h_depend_files = $(patsubst %.h, $(R2)/%.h, $(R2_H_SOURCE))
header_depend_files = $(inc_h_depend_files) $(mds_h_depend_files) \
						$(lib_h_depend_files) \
						$(r2_h_depend_files) \
						$(fuse_h_depend_files)

ut_depend_files = $(patsubst %.c, $(TEST)/mds/%.c, $(TEST_MDS_SOURCE)) \
					$(patsubst %.c, $(TEST)/xnet/%.c, $(TEST_XNET_SOURCE)) \
					$(header_depend_files)
mds_depend_files = $(patsubst %.c, $(MDS)/%.c, $(MDS_AR_SOURCE)) \
					$(header_depend_files)
lib_depend_files = $(patsubst %.c, $(LIB_PATH)/%.c, $(LIB_AR_SOURCE)) \
					$(header_depend_files)
xnet_depend_files = $(patsubst %.c, $(XNET)/%.c, $(XNET_AR_SOURCE)) \
					$(header_depend_files)
r2_depend_files = $(patsubst %.c, $(R2)/%.c, $(R2_AR_SOURCE)) \
					$(header_depend_files)

# shared libs
LIB_SO = $(LIB_PATH)/libgk.so.1.0
XNET_SO = $(XNET)/libxnet.so.1.0
MDS_SO = $(MDS)/libmds.so.1.0
R2_SO = $(R2)/libr2.so.1.0

GK_LIB = $(LIB_PATH)/libgk.a
MDS_LIB = $(MDS)/libmds.a
R2_LIB = $(R2)/libr2.a
XNET_LIB = $(XNET)/libxnet.a
