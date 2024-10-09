PGFILEDESC = "Statistics utilities"
PGAPPICON = win32

PROGRAMS = pgcsvstat pgstat pgdisplay pgwaitevent pgreport
PGFELIBS = pgfe_connect_utils.o pgfe_query_utils.o pgfe_cancel.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)
SCRIPTS_built = pgcsvstat pgstat pgdisplay pgwaitevent pgreport
EXTRA_CLEAN = $(addsuffix .o, $(PROGRAMS)) $(PGFELIBS)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: $(PROGRAMS)

%: %.o $(WIN32RES)
	   $(CC) $(CFLAGS) $^ $(libpq_pgport) $(LDFLAGS) -lpgcommon -lpgport -lm -o $@$(X)

pgcsvstat: pgcsvstat.o $(PGFELIBS)
pgdisplay: pgdisplay.o $(PGFELIBS)
pgstat: pgstat.o $(PGFELIBS)
pgwaitevent: pgwaitevent.o $(PGFELIBS)
pgreport: pgreport.o $(PGFELIBS)
pgreport.o: pgreport_queries.h
