PGFILEDESC = "Statistics utilities"
PGAPPICON = win32

PROGRAMS = pgcsvstat pgstat pgdisplay

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: $(PROGRAMS)

%: %.o $(WIN32RES)
	   $(CC) $(CFLAGS) $^ $(libpq_pgport) $(LDFLAGS) -o $@$(X)

pgcsvstat: pgcsvstat.o
pgstat: pgstat.o

clean:
	rm -f $(addsuffix $(X), $(PROGRAMS)) $(addsuffix .o, $(PROGRAMS))
