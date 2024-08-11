# source: https://github.com/enova/pgl_ddl_deploy/blob/master/Makefile

EXTENSION = pgcapture
TESTS = $(wildcard sql/*)
REGRESS = $(patsubst sql/%.sql,%,$(TESTS))
DATA = pgcapture--0.1.sql
MODULES = pgcapture

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Prevent unintentional inheritance of PGSERVICE while running regression suite
# with make installcheck.  We typically use PGSERVICE in our shell environment but
# not for dev. Require instead explicit PGPORT= or PGSERVICE= to do installcheck
unexport PGSERVICE