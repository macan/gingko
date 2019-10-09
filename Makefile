##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2019-08-19 15:56:37 macan>
#
# This is the makefile for GK project.
#
# Armed by EMACS.

HOME_PATH = $(shell pwd)

include Makefile.inc

RING_SOURCES = $(LIB_PATH)/ring.c $(LIB_PATH)/lib.c $(LIB_PATH)/hash.c \
				$(LIB_PATH)/xlock.c

all : unit_test lib

$(GK_LIB) : $(lib_depend_files)
	@$(ECHO) -e " " CD"\t" $(LIB_PATH)
	@$(ECHO) -e " " MK"\t" $@
	@$(MAKE) --no-print-directory -C $(LIB_PATH) -e "HOME_PATH=$(HOME_PATH)"

$(MDS_LIB) : $(mds_depend_files)
	@$(ECHO) -e " " CD"\t" $(MDS)
	@$(ECHO) -e " " MK"\t" $@
	@$(MAKE) --no-print-directory -C $(MDS) -e "HOME_PATH=$(HOME_PATH)"

$(R2_LIB) : $(r2_depend_files)
	@$(ECHO) -e " " CD"\t" $(R2)
	@$(ECHO) -e " " MK"\t" $@
	@$(MAKE) --no-print-directory -C $(R2) -e "HOME_PATH=$(HOME_PATH)"

$(XNET_LIB) : $(xnet_depend_files)
	@$(ECHO) -e " " CD"\t" $(XNET)
	@$(ECHO) -e " " MK"\t" $@
	@$(MAKE) --no-print-directory -C $(XNET) -e "HOME_PATH=$(HOME_PATH)"

clean :
	@$(MAKE) --no-print-directory -C $(LIB_PATH) -e "HOME_PATH=$(HOME_PATH)" clean
	@$(MAKE) --no-print-directory -C $(MDS) -e "HOME_PATH=$(HOME_PATH)" clean
	@$(MAKE) --no-print-directory -C $(R2) -e "HOME_PATH=$(HOME_PATH)" clean
	@$(MAKE) --no-print-directory -C $(XNET) -e "HOME_PATH=$(HOME_PATH)" clean
	@$(MAKE) --no-print-directory -C $(TEST)/xnet -e "HOME_PATH=$(HOME_PATH)" clean

help :
	@$(ECHO) "Environment Variables:"
	@$(ECHO) ""
	@$(ECHO) "1. DISABLE_PYTHON    if defined, do not compile w/ Python C API."
	@$(ECHO) "                     otherwise, compile and link with libpython."
	@$(ECHO) ""
	@$(ECHO) "2. JEMALLOC          Must defined w/ jemalloc install path prefix;"
	@$(ECHO) "                     otherwise, we can find the jemalloc lib path."
	@$(ECHO) ""
	@$(ECHO) "3. PYTHON_INC        python include path"
	@$(ECHO) ""

# Note: the following region is only for UNIT TESTing
# region for unit test
$(LIB_PATH)/ring : $(RING_SOURCES)
	@$(ECHO) -e " " CC"\t" $@
	@$(CC) $(CFLAGS) $^ -o $@ -DUNIT_TEST

lib : $(GK_LIB) $(MDS_LIB) $(XNET_LIB) $(MDSL_LIB) $(R2_LIB)
	@$(ECHO) -e " " Lib is ready.

unit_test : $(ut_depend_files) $(GK_LIB) $(MDS_LIB) $(XNET_LIB) \
			$(MDSL_LIB) $(R2_LIB)
	@$(ECHO) -e " " CD"\t" $(TEST)/xnet
	@$(MAKE) --no-print-directory -C $(TEST)/xnet -e "HOME_PATH=$(HOME_PATH)"
	@$(ECHO) "Targets for unit test are ready."

plot: 
	@$(ECHO) -e "Ploting ..."
	@$(MAKE) --no-print-directory -C $(TEST)/result -e "HOME_PATH=$(HOME_PATH)" plot
	@$(ECHO) -e "Done.\n"
