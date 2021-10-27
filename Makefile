PGFILEDESC = "Statistics utilities"
PGAPPICON = win32

PROGRAMS = pgcsvstat pgstat pgdisplay pgwaitevent pgreport

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)
SCRIPTS_built = pgcsvstat pgstat pgdisplay pgwaitevent pgreport
EXTRA_CLEAN = rm -f $(addsuffix $(X), $(PROGRAMS)) $(addsuffix .o, $(PROGRAMS))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: $(PROGRAMS)

pgreport.o: pgreport_queries.h
%: %.o $(WIN32RES)
	   $(CC) $(CFLAGS) $^ $(libpq_pgport) $(LDFLAGS) -lpgfeutils -lpgcommon -lm -o $@$(X)

pgcsvstat: pgcsvstat.o
pgdisplay: pgdisplay.o
pgstat: pgstat.o
pgwaitevent: pgwaitevent.o
pgreport: pgreport.o

