# contrib/pgstats/Makefile

PGFILEDESC = "pgstats - grabs all statistics, and stores them in .csv files"
PGAPPICON = win32

PROGRAM = pgstats
OBJS	= pgstats.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
