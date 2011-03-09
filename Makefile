# contrib/pgstats/Makefile

PGFILEDESC = "pgstats - examine the file structure"
PGAPPICON = win32

PROGRAM = pgstats
OBJS	= pgstats.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgstats
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifdef BEFORE_8_2
CFLAGS += -D BEFORE_8_2
endif
