/*
 * pgstat, a PostgreSQL app to gather statistical informations
 * from a PostgreSQL database, and act like a vmstat tool.
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Guillaume Lelarge, guillaume@lelarge.info, 2014-2026.
 *
 * pgstats/pgstat.c
 */


/*
 * System headers
 */
#include <sys/ioctl.h>


/*
 * PostgreSQL headers
 */
#include "postgres_fe.h"
#include "common/logging.h"
#include "fe_utils/connect_utils.h"
#include "libpq/pqsignal.h"


/*
 * Defines
 */
#define PGSTAT_VERSION "1.4.0"
#define PGSTAT_DEFAULT_LINES 20
#define PGSTAT_DEFAULT_STRING_SIZE 1024
#define PGSTAT_OLDEST_STAT_RESET "0001-01-01"
#define half_rounded(x)   (((x) + ((x) < 0 ? -1 : 1)) / 2)


/*
 * Structs and enums
 */

/* units enum */
typedef enum
{
  NO_UNIT = 0,
  ALL_UNIT,
  SIZE_UNIT
} unit_t;

/* stats enum */
typedef enum
{
  NONE = 0,
  ARCHIVER,
  BGWRITER,
  BUFFERCACHE,
  CHECKPOINTER,
  CONNECTION,
  DATABASE,
  TABLE,
  TABLEIO,
  INDEX,
  FUNCTION,
  STATEMENT,
  SLRU,
  XLOG,
  DEADLIVE,
  TEMPFILE,
  REPSLOTS,
  WAITEVENT,
  WAL,
  IO,
  PROGRESS_ANALYZE,
  PROGRESS_BASEBACKUP,
  PROGRESS_CLUSTER,
  PROGRESS_COPY,
  PROGRESS_CREATEINDEX,
  PROGRESS_VACUUM,
  PBPOOLS,
  PBSTATS
} stat_t;


/* these are the options structure for command line parameters */
struct options
{
  /* misc */
  bool   verbose;
  bool   dontredisplayheader;
  bool   addtimestamp;
  stat_t stat;
  char   *substat;
  char   *filter;
  bool   human_readable;

  /* connection parameters */
  char   *dbname;
  char   *hostname;
  char   *port;
  char   *username;

  /* version number */
  int    major;
  int    minor;

  /* extension namespace (pg_stat_statements or pg_buffercache) */
  char   *namespace;

  /* frequency */
  int    interval;
  int    count;
};

/* structs for pretty printing */
struct size_pretty_unit
{
  const char *name;
  long        limit;
  bool        round;
  long        unitbits;
};

struct nosize_pretty_unit
{
  const char *name;
  long        limit;
  bool        round;
  long        divider;
};

/* pg_stat_archiver struct */
struct pgstatarchiver
{
  long archived_count;
  /*
  we don't put these columns here because it makes no sense to get a diff between the new and the old values
  ? last_archived_wal;
  ? last_archived_time;
  */
  long failed_count;
  /*
  we don't put these columns here because it makes no sense to get a diff between the new and the old values
  ? last_failed_wal;
  ? last_failed_time;
  */
  char *stats_reset;
};

/* pg_stat_bgwriter struct */
struct pgstatbgwriter
{
  long buffers_clean;
  long maxwritten_clean;
  long buffers_alloc;
  char *stats_reset;
};

/* pg_stat_checkpointer struct */
struct pgstatcheckpointer
{
  long checkpoints_timed;        /* real name is num_timed */
  long checkpoints_requested;    /* real name is num_requested */
  long restartpoints_timed;
  long restartpoints_requested;  /* real name is restartpoints_req */
  long restartpoints_done;
  long write_time;
  long sync_time;
  long buffers_written;
  char *stats_reset;
};

/* pg_stat_database struct */
struct pgstatdatabase
{
  /*
  we don't put numbackends here because it makes no sense to get a diff between the new and the old values
  long  numbackends;
  */
  long  xact_commit;
  long  xact_rollback;
  long  blks_read;
  long  blks_hit;
  long  tup_returned;
  long  tup_fetched;
  long  tup_inserted;
  long  tup_updated;
  long  tup_deleted;
  long  conflicts;
  long  temp_files;
  long  temp_bytes;
  long  deadlocks;
  long  checksum_failures;
  /* checksum_last_failure */
  float blk_read_time;
  float blk_write_time;
  float session_time;
  float active_time;
  float idle_in_transaction_time;
  long  sessions;
  long  sessions_abandoned;
  long  sessions_fatal;
  long  sessions_killed;
  char  *stats_reset;
};

/* pg_stat_all_tables struct */
struct pgstattable
{
  long seq_scan;
  /*
  we don't put the timestamps here because it makes no sense to get a diff between the new and the old values
  ? last_seq_scan;
  */
  long seq_tup_read;
  long idx_scan;
  /*
  we don't put the timestamps here because it makes no sense to get a diff between the new and the old values
  ? last_idx_scan;
  */
  long idx_tup_fetch;
  long n_tup_ins;
  long n_tup_upd;
  long n_tup_del;
  long n_tup_hot_upd;
  long n_tup_newpage_upd;
  long n_live_tup;
  long n_dead_tup;
  long n_mod_since_analyze;
  long n_ins_since_vacuum;
  /*
  we don't put the timestamps here because it makes no sense to get a diff between the new and the old values
  ? last_vacuum;
  ? last_autovacuum;
  ? last_analyze;
  ? last_autoanalyze;
  */
  long vacuum_count;
  long autovacuum_count;
  long analyze_count;
  long autoanalyze_count;
};

/* pg_statio_all_tables struct */
struct pgstattableio
{
  long heap_blks_read;
  long heap_blks_hit;
  long idx_blks_read;
  long idx_blks_hit;
  long toast_blks_read;
  long toast_blks_hit;
  long tidx_blks_read;
  long tidx_blks_hit;
};

/* pg_stat_all_indexes struct */
struct pgstatindex
{
  long idx_scan;
  /*
  we don't put the timestamps here because it makes no sense to get a diff between the new and the old values
  ? last_idx_scan;
  */
  long idx_tup_read;
  long idx_tup_fetch;
};

/* pg_stat_user_functions struct */
struct pgstatfunction
{
  long  calls;
  float total_time;
  float self_time;
};

/* pg_stat_statements struct */
struct pgstatstatement
{
  /*
  long  userid;
  long  dbid;
  long  queryid;
  text  query;
  */
  long  plans;
  float total_plan_time;
  /*
  float min_plan_time;
  float max_plan_time;
  float mean_plan_time;
  float stddev_plan_time;
  */
  long  calls;
  float total_exec_time;
  /*
  float min_exec_time;
  float max_exec_time;
  float mean_exec_time;
  float stddev_exec_time;
  */
  long  rows;
  long  shared_blks_hit;
  long  shared_blks_read;
  long  shared_blks_dirtied;
  long  shared_blks_written;
  long  local_blks_hit;
  long  local_blks_read;
  long  local_blks_dirtied;
  long  local_blks_written;
  long  temp_blks_read;
  long  temp_blks_written;
  float shared_blk_read_time;  /* 9.2 - 16, blk_read_time  */
  float shared_blk_write_time; /* 9.2 - 16, blk_write_time */
  float local_blk_read_time;   /* 9.2 - 16, blk_read_time  */
  float local_blk_write_time;  /* 9.2 - 16, blk_write_time */
  float temp_blk_read_time;    /* 9.2 - 15, blk_read_time  */
  float temp_blk_write_time;   /* 9.2 - 15, blk_write_time */
  long  wal_records;
  long  wal_fpi;
  long  wal_bytes;
  long  jit_functions;
  float jit_generation_time;
  long  jit_inlining_count;
  float jit_inlining_time;
  long  jit_optimization_count;
  float jit_optimization_time;
  long  jit_emission_count;
  float jit_emission_time;
  long  jit_deform_count;
  float jit_deform_time;
  char  *stats_since;
  char  *minmax_stats_since;
};

/* pg_stat_slru struct */
struct pgstatslru
{
  long blks_zeroed;
  long blks_hit;
  long blks_read;
  long blks_written;
  long blks_exists;
  long flushes;
  long truncates;
  char *stats_reset;
};

/* pg_stat_wal struct */
struct pgstatwal
{
  long  wal_records;
  long  wal_fpi;
  long  wal_bytes;
  long  wal_buffers_full;
  long  wal_write;
  long  wal_sync;
  float wal_write_time;
  float wal_sync_time;
  char  *stats_reset;
};

/* pg_stat_io struct */
struct pgstatio
{
  /*
  char *backend_type;
  char *object;
  char *context;
  */
  long  reads;
  long  read_bytes;
  float read_time;
  long  writes;
  long  write_bytes;
  float write_time;
  long  writebacks;
  float writeback_time;
  long  extends;
  float extend_time;
  long  op_bytes;
  long  hits;
  long  evictions;
  long  reuses;
  float fsyncs;
  float fsync_time;
  char  *stats_reset;
};

/* deadlivestats struct */
struct deadlivestats
{
  long live;
  long dead;
};

/* repslots struct */
/* TODO : there is a lot of other informations, might want to check them */
struct repslots
{
  char *currentlocation;
  char *restartlsn;
  long restartlsndiff;
};

/* xlogstats struct */
struct xlogstats
{
  char *location;
  long locationdiff;
};

/* pgBouncer stats struct */
struct pgbouncerstats
{
  long total_request;
  long total_received;
  long total_sent;
  long total_query_time;
  /* not used yet
  float avg_req;
  float avg_recv;
  float avg_sent;
  float avg_query;
  */
};

/*
 * Global variables
 */
static PGconn                     *conn;
static struct options             *opts;
extern char                       *optarg;
static struct pgstatarchiver      *previous_pgstatarchiver;
static struct pgstatbgwriter      *previous_pgstatbgwriter;
static struct pgstatcheckpointer  *previous_pgstatcheckpointer;
static struct pgstatdatabase      *previous_pgstatdatabase;
static struct pgstattable         *previous_pgstattable;
static struct pgstattableio       *previous_pgstattableio;
static struct pgstatindex         *previous_pgstatindex;
static struct pgstatfunction      *previous_pgstatfunction;
static struct pgstatstatement     *previous_pgstatstatement;
static struct pgstatslru          *previous_pgstatslru;
static struct pgstatwal           *previous_pgstatwal;
static struct pgstatio            *previous_pgstatio;
static struct xlogstats           *previous_xlogstats;
static struct deadlivestats       *previous_deadlivestats;
static struct repslots            *previous_repslots;
static struct pgbouncerstats      *previous_pgbouncerstats;
static int                        hdrcnt = 0;
static volatile sig_atomic_t      wresized;
static int                 winlines = PGSTAT_DEFAULT_LINES;
static const struct        size_pretty_unit size_pretty_units[] = {
  {" b", 10 * 1024, false, 0},
  {"kB", 20 * 1024 - 1, true, 10},
  {"MB", 20 * 1024 - 1, true, 20},
  {"GB", 20 * 1024 - 1, true, 30},
  {"TB", 20 * 1024 - 1, true, 40},
  {"PB", 20 * 1024 - 1, true, 50},
  {NULL, 0, false, 0}
};
static const struct        nosize_pretty_unit nosize_pretty_units[] = {
  {" ", 10 * 1000, false, 1000},
  {"k", 20 * 1000 - 1, true, 1000},
  {"M", 20 * 1000 - 1, true, 1000},
  {"G", 20 * 1000 - 1, true, 1000},
  {"T", 20 * 1000 - 1, true, 1000},
  {"P", 20 * 1000 - 1, true, 1000},
  {NULL, 0, false, 0}
};


/*
 * Function prototypes
 */
static void help(const char *progname);
void        get_opts(int, char **);
#ifndef FE_MEMUTILS_H
void        *pg_malloc(size_t size);
char        *pg_strdup(const char *in);
#endif
char        *pg_size_pretty(long long size);
char        *pg_nosize_pretty(long long size);
void        format(char *r, long long value, long length, unit_t SIZE_UNIT);
void        format_time(char *r, float value, long length);
void        print_pgstatarchiver(void);
void        print_pgstatbgwriter(void);
void        print_pgstatcheckpointer(void);
void        print_pgstatconnection(void);
void        print_pgstatdatabase(void);
void        print_pgstattable(void);
void        print_pgstattableio(void);
void        print_pgstatindex(void);
void        print_pgstatfunction(void);
void        print_pgstatstatement(void);
void        print_pgstatslru(void);
void        print_pgstatwal(void);
void        print_pgstatio(void);
void        print_pgstatprogressanalyze(void);
void        print_pgstatprogressbasebackup(void);
void        print_pgstatprogresscluster(void);
void        print_pgstatprogresscopy(void);
void        print_pgstatprogresscreateindex(void);
void        print_pgstatprogressvacuum(void);
void        print_pgstatwaitevent(void);
void        print_buffercache(void);
void        print_deadlivestats(void);
void        print_repslotsstats(void);
void        print_tempfilestats(void);
void        print_xlogstats(void);
void        print_pgbouncerpools(void);
void        print_pgbouncerstats(void);
void        fetch_version(void);
char        *fetch_setting(char *name);
void        fetch_pgbuffercache_namespace(void);
void        fetch_pgstatstatements_namespace(void);
bool        backend_minimum_version(int major, int minor);
void        print_header(void);
void        print_line(void);
void        allocate_struct(void);
static void needhdr(int dummy);
static void needresize(int);
void        doresize(void);
static void quit_properly(SIGNAL_ARGS);

/*
 * Print help message
 */
static void
help(const char *progname)
{
  printf("%s gathers statistics from a PostgreSQL database.\n\n"
       "Usage:\n"
       "  %s [OPTIONS] [delay [count]]\n"
       "\nGeneral options:\n"
       "  -f FILTER              include only this object\n"
       "                         (only works for database, table, tableio,\n"
       "                          index, function, statement statistics,\n"
       "                          replication slots, and slru)\n"
       "  -H                     display human-readable values\n"
       "  -n                     do not redisplay header\n"
       "  -t                     add timestamp\n"
       "  -s STAT                stats to collect\n"
       "  -S SUBSTAT             part of stats to display\n"
       "                         (only works for database and statement)\n"
       "  -v                     verbose\n"
       "  -?|--help              show this help, then exit\n"
       "  -V|--version           output version information, then exit\n"
       "\nConnection options:\n"
       "  -h HOSTNAME            database server host or socket directory\n"
       "  -p PORT                database server port number\n"
       "  -U USER                connect as specified database user\n"
       "  -d DBNAME              database to connect to\n"
       "\nThe default stat is pg_stat_bgwriter, but you can change it with\n"
       "the -s command line option, and one of its value (STAT):\n"
       "  * archiver             for pg_stat_archiver (only for 9.4+)\n"
       "  * bgwriter             for pg_stat_bgwriter\n"
       "  * buffercache          for pg_buffercache (needs the extension)\n"
       "  * checkpointer         for pg_stat_bgwriter (<17) or\n"
       "                         for pg_stat_checkpointer (17+)\n"
       "  * connection           (only for 9.2+)\n"
       "  * database             for pg_stat_database\n"
       "  * table                for pg_stat_all_tables\n"
       "  * tableio              for pg_statio_all_tables\n"
       "  * index                for pg_stat_all_indexes\n"
       "  * function             for pg_stat_user_function\n"
       "  * statement            for pg_stat_statements (needs the extension)\n"
       "  * slru                 for pg_stat_slru (only for 13+)\n"
       "  * xlog                 for xlog writes (only for 9.2+)\n"
       "  * deadlive             for dead/live tuples stats\n"
       "  * repslots             for replication slots\n"
       "  * tempfile             for temporary file usage\n"
       "  * waitevent            for wait events usage\n"
       "  * wal                  for pg_stat_wal (only for 14+)\n"
       "  * io                   for pg_stat_io (only for 16+)\n"
       "  * progress_analyze     for analyze progress monitoring (only for\n"
       "                         13+)\n"
       "  * progress_basebackup  for base backup progress monitoring (only\n"
       "                         for 13+)\n"
       "  * progress_cluster     for cluster progress monitoring (only for\n"
       "                         12+)\n"
       "  * progress_copy        for copy progress monitoring (only for\n"
       "                         14+)\n"
       "  * progress_createindex for create index progress monitoring (only\n"
       "                         for 12+)\n"
       "  * progress_vacuum      for vacuum progress monitoring (only for\n"
       "                         9.6+)\n"
       "  * pbpools              for pgBouncer pools statistics\n"
       "  * pbstats              for pgBouncer statistics\n\n"
       "Report bugs to <guillaume@lelarge.info>.\n",
       progname, progname);
}

/*
 * Parse command line options and check for some usage errors
 */
void
get_opts(int argc, char **argv)
{
  int        c;
  const char *progname;

  progname = get_progname(argv[0]);

  /* set the defaults */
  opts->verbose = false;
  opts->dontredisplayheader = false;
  opts->addtimestamp = false;
  opts->stat = NONE;
  opts->substat = NULL;
  opts->filter = NULL;
  opts->human_readable = false;
  opts->dbname = NULL;
  opts->hostname = NULL;
  opts->port = NULL;
  opts->username = NULL;
  opts->namespace = NULL;
  opts->interval = 1;
  opts->count = -1;

  if (argc > 1)
  {
    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
    {
      help(progname);
      exit(0);
    }
    if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
    {
      puts("pgstats " PGSTAT_VERSION " (compiled with PostgreSQL " PG_VERSION ")");
      exit(0);
    }
  }

  /* get opts */
  while ((c = getopt(argc, argv, "h:Hp:U:d:f:nts:S:v")) != -1)
  {
    switch (c)
    {
        /* specify the database */
      case 'd':
        opts->dbname = pg_strdup(optarg);
        break;

        /* specify the filter */
      case 'f':
        opts->filter = pg_strdup(optarg);
        break;

        /* do not redisplay the header */
      case 'n':
        opts->dontredisplayheader = true;
        break;

        /* don't show headers */
      case 'v':
        opts->verbose = true;
        break;

        /* add timestamp */
      case 't':
        opts->addtimestamp = true;
        break;

        /* specify the stat */
      case 's':
        if (opts->stat != NONE)
        {
          pg_log_error("You can only use once the -s command line switch.\n");
          exit(EXIT_FAILURE);
        }

        if (!strcmp(optarg, "archiver"))
        {
          opts->stat = ARCHIVER;
        }
        else if (!strcmp(optarg, "bgwriter"))
        {
          opts->stat = BGWRITER;
        }
        else if (!strcmp(optarg, "buffercache"))
        {
          opts->stat = BUFFERCACHE;
        }
        else if (!strcmp(optarg, "checkpointer"))
        {
          opts->stat = CHECKPOINTER;
        }
        else if (!strcmp(optarg, "connection"))
        {
          opts->stat = CONNECTION;
        }
        else if (!strcmp(optarg, "database"))
        {
          opts->stat = DATABASE;
        }
        else if (!strcmp(optarg, "table"))
        {
          opts->stat = TABLE;
        }
        else if (!strcmp(optarg, "tableio"))
        {
          opts->stat = TABLEIO;
        }
        else if (!strcmp(optarg, "index"))
        {
          opts->stat = INDEX;
        }
        else if (!strcmp(optarg, "function"))
        {
          opts->stat = FUNCTION;
        }
        else if (!strcmp(optarg, "statement"))
        {
          opts->stat = STATEMENT;
        }
        else if (!strcmp(optarg, "slru"))
        {
          opts->stat = SLRU;
        }
        else if (!strcmp(optarg, "wal"))
        {
          opts->stat = WAL;
        }
        else if (!strcmp(optarg, "io"))
        {
          opts->stat = IO;
        }
        else if (!strcmp(optarg, "xlog"))
        {
          opts->stat = XLOG;
        }
        else if (!strcmp(optarg, "deadlive"))
        {
          opts->stat = DEADLIVE;
        }
        else if (!strcmp(optarg, "repslots"))
        {
          opts->stat = REPSLOTS;
        }
        else if (!strcmp(optarg, "tempfile"))
        {
          opts->stat = TEMPFILE;
        }
        else if (!strcmp(optarg, "waitevent"))
        {
          opts->stat = WAITEVENT;
        }
        else if (!strcmp(optarg, "progress_analyze"))
        {
          opts->stat = PROGRESS_ANALYZE;
        }
        else if (!strcmp(optarg, "progress_basebackup"))
        {
          opts->stat = PROGRESS_BASEBACKUP;
        }
        else if (!strcmp(optarg, "progress_cluster"))
        {
          opts->stat = PROGRESS_CLUSTER;
        }
        else if (!strcmp(optarg, "progress_copy"))
        {
          opts->stat = PROGRESS_COPY;
        }
        else if (!strcmp(optarg, "progress_createindex"))
        {
          opts->stat = PROGRESS_CREATEINDEX;
        }
        else if (!strcmp(optarg, "progress_vacuum"))
        {
          opts->stat = PROGRESS_VACUUM;
        }
        else if (!strcmp(optarg, "pbpools"))
        {
          opts->stat = PBPOOLS;
        }
        else if (!strcmp(optarg, "pbstats"))
        {
          opts->stat = PBSTATS;
        }
        else
        {
          pg_log_error("Unknown service \"%s\".\n", optarg);
          pg_log_info("Try \"%s --help\" for more information.\n", progname);
          exit(EXIT_FAILURE);
        }
        break;

        /* specify the substat */
      case 'S':
        opts->substat = pg_strdup(optarg);
        break;

        /* host to connect to */
      case 'h':
        opts->hostname = pg_strdup(optarg);
        break;

        /* display human-readable values */
      case 'H':
        opts->human_readable = true;
        break;

        /* port to connect to on remote host */
      case 'p':
        opts->port = pg_strdup(optarg);
        break;

        /* username */
      case 'U':
        opts->username = pg_strdup(optarg);
        break;

      default:
        pg_log_error("Try \"%s --help\" for more information.\n", progname);
        exit(EXIT_FAILURE);
    }
  }

  if (optind < argc)
  {
    opts->interval = atoi(argv[optind]);
    if (opts->interval == 0)
    {
      pg_log_error("Invalid delay.\n");
      pg_log_info("Try \"%s --help\" for more information.\n", progname);
      exit(EXIT_FAILURE);
    }
    optind++;
  }

  if (optind < argc)
  {
    opts->count = atoi(argv[optind]);
    if (opts -> count == 0)
    {
      pg_log_error("Invalid count.\n");
      pg_log_info("Try \"%s --help\" for more information.\n", progname);
      exit(EXIT_FAILURE);
    }
  }

  if (opts->stat == PBPOOLS || opts->stat == PBSTATS)
  {
    /*
     * Set (or override) database name.
     * It should always be pgbouncer
     */
    opts->dbname = pg_strdup("pgbouncer");
  }

  if (opts->dbname == NULL)
  {
    /*
     * We want to use dbname for possible error reports later,
     * and in case someone has set and is using PGDATABASE
     * in its environment preserve that name for later usage
     */
    if (!getenv("PGDATABASE"))
      opts->dbname = "postgres";
    else
      opts->dbname = getenv("PGDATABASE");
  }
}

#ifndef FE_MEMUTILS_H

/*
 * "Safe" wrapper around malloc().
 */
void *
pg_malloc(size_t size)
{
  void *tmp;

  /* Avoid unportable behavior of malloc(0) */
  if (size == 0)
    size = 1;
  tmp = malloc(size);
  if (!tmp)
  {
    pg_log_error("out of memory (pg_malloc)\n");
    exit(EXIT_FAILURE);
  }
  return tmp;
}

/*
 * "Safe" wrapper around strdup().
 */
char *
pg_strdup(const char *in)
{
  char *tmp;

  if (!in)
  {
    pg_log_error("cannot duplicate null pointer (internal error)\n");
    exit(EXIT_FAILURE);
  }
  tmp = strdup(in);
  if (!tmp)
  {
    pg_log_error("out of memory (pg_strdup)\n");
    exit(EXIT_FAILURE);
  }
  return tmp;
}
#endif

/*
 * Display metrics with a size unit
 */
char *pg_size_pretty(long long size)
{
  char    *buf;
  const struct size_pretty_unit *SIZE_UNIT;

  buf = malloc( sizeof(char) * (64+1));

  for (SIZE_UNIT = size_pretty_units; SIZE_UNIT->name != NULL; SIZE_UNIT++)
  {
    long      bits;
    long long abs_size = size < 0 ? 0 - size : size;

    if (SIZE_UNIT[1].name == NULL || abs_size < SIZE_UNIT->limit)
    {
      if (SIZE_UNIT->round)
        size = half_rounded(size);

      snprintf(buf, sizeof(buf), "%lld %s", size, SIZE_UNIT->name);
      break;
    }

    bits = (SIZE_UNIT[1].unitbits - SIZE_UNIT->unitbits - (SIZE_UNIT[1].round == true)
        + (SIZE_UNIT->round == true));
    size /= 1 << bits;
  }

  return(buf);
}

/*
 * Display metrics with a unit
 */
char *pg_nosize_pretty(long long size)
{
  char    *buf;
  const struct nosize_pretty_unit *SIZE_UNIT;


  buf = malloc( sizeof(char) * (64+1));

  for (SIZE_UNIT = nosize_pretty_units; SIZE_UNIT->name != NULL; SIZE_UNIT++)
  {
    if (SIZE_UNIT[1].name == NULL || size < SIZE_UNIT->limit)
    {
      snprintf(buf, sizeof(buf), "%lld %s", size, SIZE_UNIT->name);
      break;
    }
    size = size / SIZE_UNIT->divider;
  }

  return(buf);
}

/*
 * Format a long long value as a string
 */
void format(char *r, long long value, long length, unit_t unit)
{
  char v[64] = "";

  // check if pretty print
  if (unit == NO_UNIT)
  {
    sprintf(v, "%lld", value);
  }
  else
  {
    long long abs_value = value < 0 ? 0 - value : value;
    sprintf(v, "%s%s",
      value < 0    ? "-":"",
      unit == SIZE_UNIT ? pg_size_pretty(abs_value) : pg_nosize_pretty(abs_value)
      );
  }

  // check for overflow
  if (length < strlen(v))
  {
    // Overflow!
    sprintf(v, "!OF!");
  }

  // initialize with empty string
  strcpy(r, "");

  // add spaces
  for(long i=0; i<length-strlen(v); i++)
    strcat(r, " ");

  // add value
  strcat(r, v);
}

/*
 * Format a duration value as a string
 */
void format_time(char *r, float value, long length)
{
  long value_int;
  char v[64] = "";

  // format the value
  value_int = value*100;
  sprintf(v, "%ld.%d", value_int/100, abs(value_int)%100);

  // check for overflow
  if (length < strlen(v))
  {
    // Overflow!
    sprintf(v, "!OF!");
  }

  // allocate the string
  strcpy(r, "");

  // add spaces
  for(long i=0; i<length-strlen(v); i++)
    strcat(r, " ");

  // add value
  strcat(r, v);
}

/*
 * Dump all archiver stats.
 */
void
print_pgstatarchiver()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  long     archived_count;
  long     failed_count;
  char     *stats_reset;
  bool     has_been_reset;

  char *ts = NULL;
  char *r_archived_count = (char *)malloc(sizeof(char) * (8 + 1));
  char *r_failed_count = (char *)malloc(sizeof(char) * (8 + 1));

  /* grab the stats (this is the only stats on one line) */
  snprintf(sql, sizeof(sql),
    "SELECT %s archived_count, failed_count, stats_reset, stats_reset>'%s' "
    "FROM pg_stat_archiver ",
    opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
    previous_pgstatarchiver->stats_reset);

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    archived_count = atol(PQgetvalue(res, row, column++));
    failed_count = atol(PQgetvalue(res, row, column++));
    stats_reset = PQgetvalue(res, row, column++);
    has_been_reset = strcmp(PQgetvalue(res, row, column++), "f") && strcmp(previous_pgstatarchiver->stats_reset, PGSTAT_OLDEST_STAT_RESET);

    if (has_been_reset)
    {
      (void)printf("pg_stat_archiver has been reset!\n");
    }

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    format(r_archived_count, archived_count - previous_pgstatarchiver->archived_count, 8, NO_UNIT);
    format(r_failed_count, failed_count - previous_pgstatarchiver->failed_count, 8, NO_UNIT);
    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s %s\n", r_archived_count, r_failed_count);

    /* setting the new old value */
    previous_pgstatarchiver->archived_count = archived_count;
      previous_pgstatarchiver->failed_count = failed_count;
      previous_pgstatarchiver->stats_reset = stats_reset;
  }

  /* cleanup */
  free(r_archived_count);
  free(r_failed_count);
  PQclear(res);
}

/*
 * dump all bgwriter stats.
 */
void
print_pgstatbgwriter()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  long     buffers_clean = 0;
  long     maxwritten_clean = 0;
  long     buffers_alloc = 0;
  char     *stats_reset;
  bool     has_been_reset;

  char *ts = NULL;
  char *r_buffers_clean = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_maxwritten_clean = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_buffers_alloc = (char *)malloc(sizeof(char) * (10 + 1));

  /* grab the stats (this is the only stats on one line) */
  snprintf(sql, sizeof(sql),
    "select %s buffers_clean, maxwritten_clean, buffers_alloc, stats_reset, stats_reset>'%s' "
    "from pg_stat_bgwriter ",
    opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
    previous_pgstatbgwriter->stats_reset);

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    buffers_clean = atol(PQgetvalue(res, row, column++));
    maxwritten_clean = atol(PQgetvalue(res, row, column++));
    buffers_alloc = atol(PQgetvalue(res, row, column++));
    stats_reset = PQgetvalue(res, row, column++);
    has_been_reset = strcmp(PQgetvalue(res, row, column++), "f") && strcmp(previous_pgstatbgwriter->stats_reset, PGSTAT_OLDEST_STAT_RESET);

    if (has_been_reset)
    {
      (void)printf("pg_stat_bgwriter has been reset!\n");
    }

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    format(r_buffers_clean, buffers_clean - previous_pgstatbgwriter->buffers_clean, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_maxwritten_clean, buffers_alloc - previous_pgstatbgwriter->buffers_alloc, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_buffers_alloc, maxwritten_clean - previous_pgstatbgwriter->maxwritten_clean, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s  %s  %s\n", r_buffers_clean, r_maxwritten_clean, r_buffers_alloc);

    /* setting the new old value */
    previous_pgstatbgwriter->buffers_clean = buffers_clean;
    previous_pgstatbgwriter->maxwritten_clean = maxwritten_clean;
    previous_pgstatbgwriter->buffers_alloc = buffers_alloc;
    previous_pgstatbgwriter->stats_reset = stats_reset;
  }

  /* cleanup */
  free(r_buffers_clean);
  free(r_maxwritten_clean);
  free(r_buffers_alloc);
  PQclear(res);
}

/*
 * dump all checkpointer stats.
 */
void
print_pgstatcheckpointer()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  long     checkpoints_timed = 0;
  long     checkpoints_requested = 0;
  long     restartpoints_timed = 0;
  long     restartpoints_requested = 0;
  long     restartpoints_done = 0;
  long     write_time = 0;
  long     sync_time = 0;
  long     buffers_written = 0;
  char     *stats_reset;
  bool     has_been_reset;

  char *ts = NULL;
  char *r_checkpoints_timed = (char *)malloc(sizeof(char) * (9 + 1));
  char *r_checkpoints_requested = (char *)malloc(sizeof(char) * (9 + 1));
  char *r_restartpoints_timed = (char *)malloc(sizeof(char) * (9 + 1));
  char *r_restartpoints_requested = (char *)malloc(sizeof(char) * (9 + 1));
  char *r_restartpoints_done = (char *)malloc(sizeof(char) * (9 + 1));
  char *r_write_time = (char *)malloc(sizeof(char) * (6 + 1));
  char *r_sync_time = (char *)malloc(sizeof(char) * (6 + 1));
  char *r_buffers_written = (char *)malloc(sizeof(char) * (7 + 1));

  /* grab the stats (this is the only stats on one line) */
  if (backend_minimum_version(17, 0))
  {
    snprintf(sql, sizeof(sql),
      "select %snum_timed, num_requested, restartpoints_timed, restartpoints_req, "
      "restartpoints_done, write_time, sync_time, buffers_written, "
      "stats_reset, stats_reset>'%s' "
      "from pg_stat_checkpointer ",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), " : "",
      previous_pgstatcheckpointer->stats_reset);
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "select %scheckpoints_timed, checkpoints_req, %sbuffers_checkpoint, "
      "stats_reset, stats_reset>'%s' "
      "from pg_stat_bgwriter ",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), " : "",
      backend_minimum_version(9, 2) ? "checkpoint_write_time, checkpoint_sync_time, " : "",
      previous_pgstatcheckpointer->stats_reset);
  }

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    checkpoints_timed = atol(PQgetvalue(res, row, column++));
    checkpoints_requested = atol(PQgetvalue(res, row, column++));
    if (backend_minimum_version(17, 0))
    {
      restartpoints_timed = atol(PQgetvalue(res, row, column++));
      restartpoints_requested = atol(PQgetvalue(res, row, column++));
      restartpoints_done = atol(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(9, 2))
    {
      write_time = atol(PQgetvalue(res, row, column++));
      sync_time = atol(PQgetvalue(res, row, column++));
    }
    buffers_written = atol(PQgetvalue(res, row, column++));
    stats_reset = PQgetvalue(res, row, column++);
    has_been_reset = strcmp(PQgetvalue(res, row, column++), "f") && strcmp(previous_pgstatcheckpointer->stats_reset, PGSTAT_OLDEST_STAT_RESET);

    if (has_been_reset)
    {
      (void)printf("pg_stat_%s has been reset!\n",
        backend_minimum_version(17, 0) ? "checkpointer" : "bgwriter");
    }

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    format(r_checkpoints_timed, checkpoints_timed - previous_pgstatcheckpointer->checkpoints_timed, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_checkpoints_requested, checkpoints_requested - previous_pgstatcheckpointer->checkpoints_requested, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_restartpoints_timed, restartpoints_timed - previous_pgstatcheckpointer->restartpoints_timed, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_restartpoints_requested, restartpoints_requested - previous_pgstatcheckpointer->restartpoints_requested, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_restartpoints_done, restartpoints_done - previous_pgstatcheckpointer->restartpoints_done, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format_time(r_write_time, write_time - previous_pgstatcheckpointer->write_time, 6);
    format_time(r_sync_time, sync_time - previous_pgstatcheckpointer->sync_time, 6);
    format(r_buffers_written, buffers_written - previous_pgstatcheckpointer->buffers_written, 7, opts->human_readable ? ALL_UNIT : NO_UNIT);

    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s   %s",
      r_checkpoints_timed,
      r_checkpoints_requested);
    if (backend_minimum_version(17, 0))
    {
      (void)printf("   %s  %s  %s",
        r_restartpoints_timed,
        r_restartpoints_requested,
        r_restartpoints_done
      );
    }
    if (backend_minimum_version(9, 2))
    {
      (void)printf("   %s  %s",
        r_write_time,
        r_sync_time
      );
    }
    (void)printf("   %s\n",
      r_buffers_written
    );

    /* setting the new old value */
    previous_pgstatcheckpointer->checkpoints_timed = checkpoints_timed;
    previous_pgstatcheckpointer->checkpoints_requested = checkpoints_requested;
    previous_pgstatcheckpointer->restartpoints_timed = restartpoints_timed;
    previous_pgstatcheckpointer->restartpoints_requested = restartpoints_requested;
    previous_pgstatcheckpointer->restartpoints_done = restartpoints_requested;
    previous_pgstatcheckpointer->write_time = write_time;
    previous_pgstatcheckpointer->sync_time = sync_time;
    previous_pgstatcheckpointer->buffers_written = buffers_written;
    previous_pgstatcheckpointer->stats_reset = stats_reset;
  }

  /* cleanup */
  free(r_checkpoints_timed);
  free(r_checkpoints_requested);
  free(r_restartpoints_timed);
  free(r_restartpoints_requested);
  free(r_restartpoints_done);
  free(r_write_time);
  free(r_sync_time);
  free(r_buffers_written);
  PQclear(res);
}

/*
 * Dump all connection stats.
 */
void
print_pgstatconnection()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  long     total = 0;
  long     active = 0;
  long     lockwaiting = 0;
  long     idleintransaction = 0;
  long     idle = 0;

  char     *ts = NULL;
  char     *r_total = (char *)malloc(sizeof(char) * (5 + 1));
  char     *r_active = (char *)malloc(sizeof(char) * (5 + 1));
  char     *r_lockwaiting = (char *)malloc(sizeof(char) * (5 + 1));
  char     *r_idleintransaction = (char *)malloc(sizeof(char) * (5 + 1));
  char     *r_idle = (char *)malloc(sizeof(char) * (5 + 1));

  if (backend_minimum_version(10, 0))
  {
    snprintf(sql, sizeof(sql),
      "SELECT %scount(*) AS total, "
      "  sum(CASE WHEN state='active' AND wait_event IS NULL "
      "THEN 1 ELSE 0 END) AS active, "
      "  sum(CASE WHEN state='active' AND wait_event IS NOT NULL "
      "THEN 1 ELSE 0 END) AS lockwaiting, "
      "  sum(CASE WHEN state='idle in transaction' THEN 1 ELSE 0 END) AS idleintransaction, "
      "  sum(CASE WHEN state='idle' THEN 1 ELSE 0 END) AS idle "
      "FROM pg_stat_activity "
      "WHERE backend_type='client backend'",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");
  }
  else if (backend_minimum_version(9, 6))
  {
    snprintf(sql, sizeof(sql),
      "SELECT %scount(*) AS total, "
      "  sum(CASE WHEN state='active' AND wait_event IS NULL THEN 1 ELSE 0 END) AS active, "
      "  sum(CASE WHEN state='active' AND wait_event IS NOT NULL THEN 1 ELSE 0 END) AS lockwaiting, "
      "  sum(CASE WHEN state='idle in transaction' THEN 1 ELSE 0 END) AS idleintransaction, "
      "  sum(CASE WHEN state='idle' THEN 1 ELSE 0 END) AS idle "
      "FROM pg_stat_activity",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %scount(*) AS total, "
      "  sum(CASE WHEN state='active' AND NOT waiting THEN 1 ELSE 0 END) AS active, "
      "  sum(CASE WHEN waiting THEN 1 ELSE 0 END) AS lockwaiting, "
      "  sum(CASE WHEN state='idle in transaction' THEN 1 ELSE 0 END) AS idleintransaction, "
      "  sum(CASE WHEN state='idle' THEN 1 ELSE 0 END) AS idle "
      "FROM pg_stat_activity",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");
  }

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    total = atol(PQgetvalue(res, row, column++));
    active = atol(PQgetvalue(res, row, column++));
    lockwaiting = atol(PQgetvalue(res, row, column++));
    idleintransaction = atol(PQgetvalue(res, row, column++));
    idle = atol(PQgetvalue(res, row, column++));

    /* printing the actual values for once */
    format(r_total, total, 5, NO_UNIT);
    format(r_active, active, 5, NO_UNIT);
    format(r_lockwaiting, lockwaiting, 5, NO_UNIT);
    format(r_idleintransaction, idleintransaction, 5, NO_UNIT);
    format(r_idle, idle, 5, NO_UNIT);
    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf("   %s    %s         %s                 %s   %s\n",
        r_total, r_active, r_lockwaiting, r_idleintransaction, r_idle);
  }

  /* cleanup */
  free(r_total);
  free(r_active);
  free(r_lockwaiting);
  free(r_idleintransaction);
  free(r_idle);
  PQclear(res);
}

/*
 * Dump all database stats.
 */
void
print_pgstatdatabase()
{
  char       sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  const char *paramValues[1];
  int        nrows;
  int        row, column;

  long       numbackends = 0;
  long       xact_commit = 0;
  long       xact_rollback = 0;
  long       blks_read = 0;
  long       blks_hit = 0;
  long       tup_returned = 0;
  long       tup_fetched = 0;
  long       tup_inserted = 0;
  long       tup_updated = 0;
  long       tup_deleted = 0;
  long       conflicts = 0;
  long       temp_files = 0;
  long       temp_bytes = 0;
  long       deadlocks = 0;
  long       checksum_failures = 0;
  float      blk_read_time = 0;
  float      blk_write_time = 0;
  float      session_time = 0;
  float      active_time = 0;
  float      idle_in_transaction_time = 0;
  long       sessions = 0;
  long       sessions_abandoned = 0;
  long       sessions_fatal = 0;
  long       sessions_killed = 0;
  char       *stats_reset;
  bool       has_been_reset;
  float      hit_ratio;

  char       *ts = NULL;
  char       *r1 = (char *)malloc(sizeof(char) * (12 + 1));
  char       *r2 = (char *)malloc(sizeof(char) * (12 + 1));
  char       *r3 = (char *)malloc(sizeof(char) * (12 + 1));
  char       *r4 = (char *)malloc(sizeof(char) * (12 + 1));
  char       *r5 = (char *)malloc(sizeof(char) * (12 + 1));
  char       *r6 = (char *)malloc(sizeof(char) * (12 + 1));
  char       *r7 = (char *)malloc(sizeof(char) * (12 + 1));

  /*
   * With a filter, we assume we'll get only one row.
   * Without, we sum all the fields to get one row.
   */
  if (opts->filter == NULL)
  {
    snprintf(sql, sizeof(sql),
      "SELECT %ssum(numbackends), sum(xact_commit), sum(xact_rollback), sum(blks_read), sum(blks_hit)"
      ", max(stats_reset), max(stats_reset)>'%s'"
      "%s%s%s%s%s "
      "FROM pg_stat_database ",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      previous_pgstatdatabase->stats_reset,
      backend_minimum_version(8, 3) ? ", sum(tup_returned), sum(tup_fetched), sum(tup_inserted), sum(tup_updated), sum(tup_deleted)" : "",
      backend_minimum_version(9, 1) ? ", sum(conflicts)" : "",
      backend_minimum_version(9, 2) ? ", sum(temp_files), sum(temp_bytes), sum(deadlocks), sum(blk_read_time), sum(blk_write_time)" : "",
      backend_minimum_version(12, 0) ? ", sum(checksum_failures)" : "",
      backend_minimum_version(14, 0) ? ", sum(session_time), sum(active_time), sum(idle_in_transaction_time), sum(sessions), sum(sessions_abandoned), sum(sessions_fatal), sum(sessions_killed)" : "");

    res = PQexec(conn, sql);
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %snumbackends, xact_commit, xact_rollback, blks_read, blks_hit"
      ", stats_reset, stats_reset>'%s'"
      "%s%s%s%s%s "
      "FROM pg_stat_database "
      "WHERE datname=$1",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      previous_pgstatdatabase->stats_reset,
      backend_minimum_version(8, 3) ? ", tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted" : "",
      backend_minimum_version(9, 1) ? ", conflicts" : "",
      backend_minimum_version(9, 2) ? ", temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time" : "",
      backend_minimum_version(12, 0) ? ", checksum_failures" : "",
      backend_minimum_version(14, 0) ? ", session_time, active_time, idle_in_transaction_time, sessions, sessions_abandoned, sessions_fatal, sessions_killed" : "");

    paramValues[0] = pg_strdup(opts->filter);

    res = PQexecParams(conn,
                       sql,
                       1,       /* one param */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       NULL,    /* don't need param lengths since text */
                       NULL,    /* default to all text params */
                       0);      /* ask for text results */
    }

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    numbackends = atol(PQgetvalue(res, row, column++));
    xact_commit = atol(PQgetvalue(res, row, column++));
    xact_rollback = atol(PQgetvalue(res, row, column++));
    blks_read = atol(PQgetvalue(res, row, column++));
    blks_hit = atol(PQgetvalue(res, row, column++));
    stats_reset = PQgetvalue(res, row, column++);
    has_been_reset = strcmp(PQgetvalue(res, row, column++), "f") && strcmp(previous_pgstatdatabase->stats_reset, PGSTAT_OLDEST_STAT_RESET);
    if (backend_minimum_version(8, 3))
    {
      tup_returned = atol(PQgetvalue(res, row, column++));
      tup_fetched = atol(PQgetvalue(res, row, column++));
      tup_inserted = atol(PQgetvalue(res, row, column++));
      tup_updated = atol(PQgetvalue(res, row, column++));
      tup_deleted = atol(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(9, 1))
    {
      conflicts = atol(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(9, 2))
    {
      temp_files = atol(PQgetvalue(res, row, column++));
      temp_bytes = atol(PQgetvalue(res, row, column++));
      deadlocks = atol(PQgetvalue(res, row, column++));
      blk_read_time = atof(PQgetvalue(res, row, column++));
      blk_write_time = atof(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(12, 0))
    {
      checksum_failures = atol(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(14, 0))
    {
      session_time = atof(PQgetvalue(res, row, column++));
      active_time = atof(PQgetvalue(res, row, column++));
      idle_in_transaction_time = atof(PQgetvalue(res, row, column++));
      sessions = atol(PQgetvalue(res, row, column++));
      sessions_abandoned = atol(PQgetvalue(res, row, column++));
      sessions_fatal = atol(PQgetvalue(res, row, column++));
      sessions_killed = atol(PQgetvalue(res, row, column++));
    }

    if (has_been_reset)
    {
      (void)printf("pg_stat_database has been reset!\n");
    }

    // calculate hit ratio
    if (blks_hit - previous_pgstatdatabase->blks_hit + blks_read - previous_pgstatdatabase->blks_read > 0)
      hit_ratio = 100.*(blks_hit - previous_pgstatdatabase->blks_hit)/(blks_hit - previous_pgstatdatabase->blks_hit + blks_read - previous_pgstatdatabase->blks_read);
    else
      hit_ratio = 0;

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    if (opts->substat == NULL || strstr(opts->substat, "backends") != NULL)
    {
      format(r1, numbackends, 8, NO_UNIT);
      (void)printf("  %s", r1);
    }
    if (opts->substat == NULL || strstr(opts->substat, "xacts") != NULL)
    {
      format(r1, xact_commit - previous_pgstatdatabase->xact_commit, 8, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r2, xact_rollback - previous_pgstatdatabase->xact_rollback, 8, opts->human_readable ? ALL_UNIT : NO_UNIT);
      (void)printf("    %s %s", r1, r2);
    }
    if (opts->substat == NULL || strstr(opts->substat, "blocks") != NULL)
    {
      format(r1, blks_read - previous_pgstatdatabase->blks_read, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r2, blks_hit - previous_pgstatdatabase->blks_hit, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r3, hit_ratio, 5, NO_UNIT);
      (void)printf("   %s %s    %s", r1, r2, r3);
      if (backend_minimum_version(9, 2))
      {
        format_time(r4, blk_read_time - previous_pgstatdatabase->blk_read_time, 9);
        format_time(r5, blk_write_time - previous_pgstatdatabase->blk_write_time, 9);
        (void)printf(" %s  %s", r4, r5);
      }
    }
    if ((opts->substat == NULL || strstr(opts->substat, "tuples") != NULL) && backend_minimum_version(8, 3))
    {
      format(r1, tup_returned - previous_pgstatdatabase->tup_returned, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r2, tup_fetched - previous_pgstatdatabase->tup_fetched, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r3, tup_inserted - previous_pgstatdatabase->tup_inserted, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r4, tup_updated - previous_pgstatdatabase->tup_updated, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r5, tup_deleted - previous_pgstatdatabase->tup_deleted, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      (void)printf("   %s %s %s %s %s", r1, r2, r3, r4, r5);
    }
    if ((opts->substat == NULL || strstr(opts->substat, "temp") != NULL) && backend_minimum_version(9, 2))
    {
      format(r1, temp_files - previous_pgstatdatabase->temp_files, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r2, temp_bytes - previous_pgstatdatabase->temp_bytes, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      (void)printf("   %s  %s", r1, r2);
    }
    if ((opts->substat == NULL || strstr(opts->substat, "session") != NULL) && backend_minimum_version(14, 0))
    {
      format_time(r1, session_time - previous_pgstatdatabase->session_time, 11);
      format_time(r2, active_time - previous_pgstatdatabase->active_time, 11);
      format_time(r3, idle_in_transaction_time - previous_pgstatdatabase->idle_in_transaction_time, 11);
      format(r4, sessions - previous_pgstatdatabase->sessions, 7, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r5, sessions_abandoned - previous_pgstatdatabase->sessions_abandoned, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r6, sessions_fatal - previous_pgstatdatabase->sessions_fatal, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r7, sessions_killed - previous_pgstatdatabase->sessions_killed, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      (void)printf("   %s %s %s %s    %s  %s  %s  ", r1, r2, r3, r4, r5, r6, r7);
    }
    if ((opts->substat == NULL || strstr(opts->substat, "misc") != NULL) && backend_minimum_version(9, 1))
    {
      if (backend_minimum_version(9, 1))
      {
        format(r1, conflicts - previous_pgstatdatabase->conflicts, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
        (void)printf(" %s", r1);
      }
      if (backend_minimum_version(9, 2))
      {
        format(r2, deadlocks - previous_pgstatdatabase->deadlocks, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
        (void)printf(" %s", r2);
      }
      if (backend_minimum_version(12, 0))
      {
        format(r3, checksum_failures - previous_pgstatdatabase->checksum_failures, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
        (void)printf(" %s", r3);
      }
    }
    (void)printf("\n");

    /* setting the new old value */
    previous_pgstatdatabase->xact_commit = xact_commit;
    previous_pgstatdatabase->xact_rollback = xact_rollback;
    previous_pgstatdatabase->blks_read = blks_read;
    previous_pgstatdatabase->blks_hit = blks_hit;
    previous_pgstatdatabase->tup_returned = tup_returned;
    previous_pgstatdatabase->tup_fetched = tup_fetched;
    previous_pgstatdatabase->tup_inserted = tup_inserted;
    previous_pgstatdatabase->tup_updated = tup_updated;
    previous_pgstatdatabase->tup_deleted = tup_deleted;
    previous_pgstatdatabase->conflicts = conflicts;
    previous_pgstatdatabase->temp_files = temp_files;
    previous_pgstatdatabase->temp_bytes = temp_bytes;
    previous_pgstatdatabase->deadlocks = deadlocks;
    previous_pgstatdatabase->blk_read_time = blk_read_time;
    previous_pgstatdatabase->blk_write_time = blk_write_time;
    previous_pgstatdatabase->checksum_failures = checksum_failures;
    previous_pgstatdatabase->session_time = session_time;
    previous_pgstatdatabase->active_time = active_time;
    previous_pgstatdatabase->idle_in_transaction_time = idle_in_transaction_time;
    previous_pgstatdatabase->sessions = sessions;
    previous_pgstatdatabase->sessions_abandoned = sessions_abandoned;
    previous_pgstatdatabase->sessions_fatal = sessions_fatal;
    previous_pgstatdatabase->sessions_killed = sessions_killed;
    if (strlen(stats_reset) == 0)
      previous_pgstatdatabase->stats_reset = PGSTAT_OLDEST_STAT_RESET;
    else
      previous_pgstatdatabase->stats_reset = stats_reset;
  }

  /* cleanup */
  free(r1);
  free(r2);
  free(r3);
  free(r4);
  free(r5);
  free(r6);
  free(r7);
  PQclear(res);
}

/*
 * Dump all table stats.
 */
void
print_pgstattable()
{
  char       sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  const char *paramValues[1];
  int        nrows;
  int        row, column;

  long       seq_scan = 0;
  long       seq_tup_read = 0;
  long       idx_scan = 0;
  long       idx_tup_fetch = 0;
  long       n_tup_ins = 0;
  long       n_tup_upd = 0;
  long       n_tup_del = 0;
  long       n_tup_hot_upd = 0;
  long       n_tup_newpage_upd = 0;
  long       n_live_tup = 0;
  long       n_dead_tup = 0;
  long       n_mod_since_analyze = 0;
  long       n_ins_since_vacuum = 0;
  long       vacuum_count = 0;
  long       autovacuum_count = 0;
  long       analyze_count = 0;
  long       autoanalyze_count = 0;

  char       *ts = NULL;
  char       *r_seq_scan = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_seq_tup_read = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_idx_scan = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_idx_tup_fetch = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_n_tup_ins = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_n_tup_upd = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_n_tup_del = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_n_tup_hot_upd = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_n_tup_newpage_upd = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_n_live_tup = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_n_dead_tup = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_n_mod_since_analyze = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_n_ins_since_vacuum = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_vacuum_count = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_autovacuum_count = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_analyze_count = (char *)malloc(sizeof(char) * (6 + 1));
  char       *r_autoanalyze_count = (char *)malloc(sizeof(char) * (6 + 1));

  /*
   * With a filter, we assume we'll get only one row.
   * Without, we sum all the fields to get one row.
   */
  if (opts->filter == NULL)
  {
    snprintf(sql, sizeof(sql),
      "SELECT %ssum(seq_scan), sum(seq_tup_read), sum(idx_scan), sum(idx_tup_fetch), sum(n_tup_ins), "
      "sum(n_tup_upd), sum(n_tup_del)"
      "%s"
      "%s"
      "%s"
      "%s"
      "%s"
      " FROM pg_stat_all_tables "
      "WHERE schemaname <> 'information_schema' ",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      backend_minimum_version(16, 0) ? ", sum(n_tup_newpage_upd)" : "",
      backend_minimum_version(8, 3) ? ", sum(n_tup_hot_upd), sum(n_live_tup), sum(n_dead_tup)" : "",
      backend_minimum_version(9, 4) ? ", sum(n_mod_since_analyze)" : "",
      backend_minimum_version(13, 0) ? ", sum(n_ins_since_vacuum)" : "",
      backend_minimum_version(9, 1) ? ", sum(vacuum_count), sum(autovacuum_count), sum(analyze_count), sum(autoanalyze_count)" : "");

    res = PQexec(conn, sql);
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %ssum(seq_scan), sum(seq_tup_read), sum(idx_scan), sum(idx_tup_fetch), sum(n_tup_ins), "
      "sum(n_tup_upd), sum(n_tup_del)"
      "%s"
      "%s"
      "%s"
      "%s"
      "%s"
      " FROM pg_stat_all_tables "
      "WHERE schemaname <> 'information_schema' "
      "  AND relname = $1",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      backend_minimum_version(16, 0) ? ", sum(n_tup_newpage_upd)" : "",
      backend_minimum_version(8, 3) ? ", sum(n_tup_hot_upd), sum(n_live_tup), sum(n_dead_tup)" : "",
      backend_minimum_version(9, 4) ? ", sum(n_mod_since_analyze)" : "",
      backend_minimum_version(13, 0) ? ", sum(n_ins_since_vacuum)" : "",
      backend_minimum_version(9, 1) ? ", sum(vacuum_count), sum(autovacuum_count), sum(analyze_count), sum(autoanalyze_count)" : "");

    paramValues[0] = pg_strdup(opts->filter);

    res = PQexecParams(conn,
                       sql,
                       1,       /* one param */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       NULL,    /* don't need param lengths since text */
                       NULL,    /* default to all text params */
                       0);      /* ask for text results */
  }

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    seq_scan = atol(PQgetvalue(res, row, column++));
    seq_tup_read = atol(PQgetvalue(res, row, column++));
    idx_scan = atol(PQgetvalue(res, row, column++));
    idx_tup_fetch = atol(PQgetvalue(res, row, column++));
    n_tup_ins = atol(PQgetvalue(res, row, column++));
    n_tup_upd = atol(PQgetvalue(res, row, column++));
    n_tup_del = atol(PQgetvalue(res, row, column++));
    if (backend_minimum_version(16, 0))
    {
      n_tup_newpage_upd = atol(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(8, 3))
    {
      n_tup_hot_upd = atol(PQgetvalue(res, row, column++));
      n_live_tup = atol(PQgetvalue(res, row, column++));
      n_dead_tup = atol(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(9, 4))
    {
      n_mod_since_analyze = atol(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(13, 0))
    {
      n_ins_since_vacuum = atol(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(9, 1))
    {
      vacuum_count = atol(PQgetvalue(res, row, column++));
      autovacuum_count = atol(PQgetvalue(res, row, column++));
      analyze_count = atol(PQgetvalue(res, row, column++));
      autoanalyze_count = atol(PQgetvalue(res, row, column++));
    }

    /* printing the diff... note that the first line will be the current value, rather than the diff */
    format(r_seq_scan, seq_scan - previous_pgstattable->seq_scan, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_seq_tup_read, seq_tup_read - previous_pgstattable->seq_tup_read, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_idx_scan, idx_scan - previous_pgstattable->idx_scan, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_idx_tup_fetch, idx_tup_fetch - previous_pgstattable->idx_tup_fetch, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_n_tup_ins, n_tup_ins - previous_pgstattable->n_tup_ins, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_n_tup_upd, n_tup_upd - previous_pgstattable->n_tup_upd, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_n_tup_del, n_tup_del - previous_pgstattable->n_tup_del, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_n_tup_hot_upd, n_tup_hot_upd - previous_pgstattable->n_tup_hot_upd, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_n_tup_newpage_upd, n_tup_newpage_upd - previous_pgstattable->n_tup_newpage_upd, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_n_live_tup, n_live_tup - previous_pgstattable->n_live_tup, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_n_dead_tup, n_dead_tup - previous_pgstattable->n_dead_tup, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_n_mod_since_analyze, n_mod_since_analyze - previous_pgstattable->n_mod_since_analyze, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_n_ins_since_vacuum, n_ins_since_vacuum - previous_pgstattable->n_ins_since_vacuum, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_vacuum_count, vacuum_count - previous_pgstattable->vacuum_count, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_autovacuum_count, autovacuum_count - previous_pgstattable->autovacuum_count, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_analyze_count, analyze_count - previous_pgstattable->analyze_count, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_autoanalyze_count, autoanalyze_count - previous_pgstattable->autoanalyze_count, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);

    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s  %s   %s  %s   %s %s %s",
      r_seq_scan,
      r_seq_tup_read,
      r_idx_scan,
      r_idx_tup_fetch,
      r_n_tup_ins,
      r_n_tup_upd,
      r_n_tup_del
      );
    if (backend_minimum_version(8, 3))
    {
      (void)printf(" %s",
        r_n_tup_hot_upd
        );
    }
    if (backend_minimum_version(16, 0))
    {
      (void)printf("     %s",
        r_n_tup_newpage_upd
        );
    }
    if (backend_minimum_version(8, 3))
    {
      (void)printf(" %s %s",
        r_n_live_tup,
        r_n_dead_tup
        );
    }
    if (backend_minimum_version(9, 4))
    {
      (void)printf("  %s",
        r_n_mod_since_analyze
        );
    }
    if (backend_minimum_version(13, 0))
    {
      (void)printf("  %s",
        r_n_ins_since_vacuum
        );
    }
    if (backend_minimum_version(9, 1))
    {
      (void)printf("   %s     %s  %s      %s",
        r_vacuum_count,
        r_autovacuum_count,
        r_analyze_count,
        r_autoanalyze_count
        );
    }
    (void)printf("\n");

    /* setting the new old value */
    previous_pgstattable->seq_scan = seq_scan;
    previous_pgstattable->seq_tup_read = seq_tup_read;
    previous_pgstattable->idx_scan = idx_scan;
    previous_pgstattable->idx_tup_fetch = idx_tup_fetch;
    previous_pgstattable->n_tup_ins = n_tup_ins;
    previous_pgstattable->n_tup_upd = n_tup_upd;
    previous_pgstattable->n_tup_del = n_tup_del;
    previous_pgstattable->n_tup_hot_upd = n_tup_hot_upd;
    previous_pgstattable->n_tup_newpage_upd = n_tup_newpage_upd;
    previous_pgstattable->n_live_tup = n_live_tup;
    previous_pgstattable->n_dead_tup = n_dead_tup;
    previous_pgstattable->n_mod_since_analyze = n_mod_since_analyze;
    previous_pgstattable->n_ins_since_vacuum = n_ins_since_vacuum;
    previous_pgstattable->vacuum_count = vacuum_count;
    previous_pgstattable->autovacuum_count = autovacuum_count;
    previous_pgstattable->analyze_count = analyze_count;
    previous_pgstattable->autoanalyze_count = autoanalyze_count;
  }

  /* cleanup */
  free(r_seq_scan);
  free(r_seq_tup_read);
  free(r_idx_scan);
  free(r_idx_tup_fetch);
  free(r_n_tup_ins);
  free(r_n_tup_upd);
  free(r_n_tup_del);
  free(r_n_tup_hot_upd);
  free(r_n_tup_newpage_upd);
  free(r_n_live_tup);
  free(r_n_dead_tup);
  free(r_n_mod_since_analyze);
  free(r_n_ins_since_vacuum);
  free(r_vacuum_count);
  free(r_autovacuum_count);
  free(r_analyze_count);
  free(r_autoanalyze_count);
  PQclear(res);
}

/*
 * Dump all table IO stats.
 */
void
print_pgstattableio()
{
  char       sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  const char *paramValues[1];
  int        nrows;
  int        row, column;

  long       heap_blks_read = 0;
  long       heap_blks_hit = 0;
  long       idx_blks_read = 0;
  long       idx_blks_hit = 0;
  long       toast_blks_read = 0;
  long       toast_blks_hit = 0;
  long       tidx_blks_read = 0;
  long       tidx_blks_hit = 0;

  char       *ts = NULL;
  char       *r_heap_blks_read = (char *)malloc(sizeof(char) * (8 + 1));
  char       *r_heap_blks_hit = (char *)malloc(sizeof(char) * (8 + 1));
  char       *r_idx_blks_read = (char *)malloc(sizeof(char) * (8 + 1));
  char       *r_idx_blks_hit = (char *)malloc(sizeof(char) * (8 + 1));
  char       *r_toast_blks_read = (char *)malloc(sizeof(char) * (8 + 1));
  char       *r_toast_blks_hit = (char *)malloc(sizeof(char) * (8 + 1));
  char       *r_tidx_blks_read = (char *)malloc(sizeof(char) * (8 + 1));
  char       *r_tidx_blks_hit = (char *)malloc(sizeof(char) * (8 + 1));

  /*
   * With a filter, we assume we'll get only one row.
   * Without, we sum all the fields to get one row.
   */
  if (opts->filter == NULL)
  {
    snprintf(sql, sizeof(sql),
      "SELECT %ssum(heap_blks_read), sum(heap_blks_hit), sum(idx_blks_read), sum(idx_blks_hit), "
      "sum(toast_blks_read), sum(toast_blks_hit), sum(tidx_blks_read), sum(tidx_blks_hit) "
      "FROM pg_statio_all_tables "
      "WHERE schemaname <> 'information_schema' ",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");

    res = PQexec(conn, sql);
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %sheap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit, "
      "toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit "
      "FROM pg_statio_all_tables "
      "WHERE schemaname <> 'information_schema' "
      "  AND relname = $1",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");

    paramValues[0] = pg_strdup(opts->filter);

    res = PQexecParams(conn,
                       sql,
                       1,       /* one param */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       NULL,    /* don't need param lengths since text */
                       NULL,    /* default to all text params */
                       0);      /* ask for text results */
    }

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    heap_blks_read = atol(PQgetvalue(res, row, column++));
    heap_blks_hit = atol(PQgetvalue(res, row, column++));
    idx_blks_read = atol(PQgetvalue(res, row, column++));
    idx_blks_hit = atol(PQgetvalue(res, row, column++));
    toast_blks_read = atol(PQgetvalue(res, row, column++));
    toast_blks_hit = atol(PQgetvalue(res, row, column++));
    tidx_blks_read = atol(PQgetvalue(res, row, column++));
    tidx_blks_hit = atol(PQgetvalue(res, row, column++));

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    format(r_heap_blks_read, heap_blks_read - previous_pgstattableio->heap_blks_read, 8,  opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_heap_blks_hit, heap_blks_hit - previous_pgstattableio->heap_blks_hit, 8,  opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_idx_blks_read, idx_blks_read - previous_pgstattableio->idx_blks_read, 8,  opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_idx_blks_hit, idx_blks_hit - previous_pgstattableio->idx_blks_hit, 8,  opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_toast_blks_read, toast_blks_read - previous_pgstattableio->toast_blks_read, 8,  opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_toast_blks_hit, toast_blks_hit - previous_pgstattableio->toast_blks_hit, 8,  opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_tidx_blks_read, tidx_blks_read - previous_pgstattableio->tidx_blks_read, 8,  opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_tidx_blks_hit, tidx_blks_hit - previous_pgstattableio->tidx_blks_hit, 8,  opts->human_readable ? ALL_UNIT : NO_UNIT);

    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s  %s   %s  %s   %s  %s   %s  %s\n",
      r_heap_blks_read,
      r_heap_blks_hit,
      r_idx_blks_read,
      r_idx_blks_hit,
      r_toast_blks_read,
      r_toast_blks_hit,
      r_tidx_blks_read,
      r_tidx_blks_hit
      );

    /* setting the new old value */
    previous_pgstattableio->heap_blks_read = heap_blks_read;
    previous_pgstattableio->heap_blks_hit = heap_blks_hit;
    previous_pgstattableio->idx_blks_read = idx_blks_read;
    previous_pgstattableio->idx_blks_hit = idx_blks_hit;
    previous_pgstattableio->toast_blks_read = toast_blks_read;
    previous_pgstattableio->toast_blks_hit = toast_blks_hit;
    previous_pgstattableio->tidx_blks_read = tidx_blks_read;
    previous_pgstattableio->tidx_blks_hit = tidx_blks_hit;
  }

  /* cleanup */
  free(r_heap_blks_read);
  free(r_heap_blks_hit);
  free(r_idx_blks_read);
  free(r_idx_blks_hit);
  free(r_toast_blks_read);
  free(r_toast_blks_hit);
  free(r_tidx_blks_read);
  free(r_tidx_blks_hit);
  PQclear(res);
}

/*
 * Dump all index stats.
 */
void
print_pgstatindex()
{
  char       sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  const char *paramValues[1];
  int        nrows;
  int        row, column;

  long       idx_scan = 0;
  long       idx_tup_read = 0;
  long       idx_tup_fetch = 0;

  char       *ts = NULL;
  char       *r_idx_scan = (char *)malloc(sizeof(char) * (8 + 1));
  char       *r_idx_tup_read = (char *)malloc(sizeof(char) * (8 + 1));
  char       *r_idx_tup_fetch = (char *)malloc(sizeof(char) * (8 + 1));

  /*
   * With a filter, we assume we'll get only one row.
   * Without, we sum all the fields to get one row.
   */
  if (opts->filter == NULL)
  {
    snprintf(sql, sizeof(sql),
      "SELECT %ssum(idx_scan), sum(idx_tup_read), sum(idx_tup_fetch) "
      " FROM pg_stat_all_indexes "
      "WHERE schemaname <> 'information_schema' ",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");

    res = PQexec(conn, sql);
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %sidx_scan, idx_tup_read, idx_tup_fetch "
      "FROM pg_stat_all_indexes "
      "WHERE schemaname <> 'information_schema' "
      "  AND indexrelname = $1",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");

    paramValues[0] = pg_strdup(opts->filter);

    res = PQexecParams(conn,
                       sql,
                       1,       /* one param */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       NULL,    /* don't need param lengths since text */
                       NULL,    /* default to all text params */
                       0);      /* ask for text results */
  }

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    idx_scan = atol(PQgetvalue(res, row, column++));
    idx_tup_read = atof(PQgetvalue(res, row, column++));
    idx_tup_fetch = atof(PQgetvalue(res, row, column++));

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    format(r_idx_scan, idx_scan - previous_pgstatindex->idx_scan, 8, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_idx_tup_read, idx_tup_read - previous_pgstatindex->idx_tup_read, 8, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_idx_tup_fetch, idx_tup_fetch - previous_pgstatindex->idx_tup_fetch, 8, opts->human_readable ? ALL_UNIT : NO_UNIT);
    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s   %s %s\n",
      r_idx_scan,
      r_idx_tup_read,
      r_idx_tup_fetch
      );

    /* setting the new old value */
    previous_pgstatindex->idx_scan = idx_scan;
    previous_pgstatindex->idx_tup_read = idx_tup_read;
    previous_pgstatindex->idx_tup_fetch = idx_tup_fetch;
  }

  /* cleanup */
  free(r_idx_scan);
  free(r_idx_tup_read);
  free(r_idx_tup_fetch);
  PQclear(res);
}

/*
 * Dump all function stats.
 */
void
print_pgstatfunction()
{
  char       sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  const char *paramValues[1];
  int        nrows;
  int        row, column;

  long       calls = 0;
  float      total_time = 0;
  float      self_time = 0;

  char       *ts = NULL;
  char       *r_calls = (char *)malloc(sizeof(char) * (9 + 1));
  char       *r_total_time = (char *)malloc(sizeof(char) * (10 + 1));
  char       *r_self_time = (char *)malloc(sizeof(char) * (10 + 1));

  /*
   * With a filter, we assume we'll get only one row.
   * Without, we sum all the fields to get one row.
   */
  if (opts->filter == NULL)
  {
    snprintf(sql, sizeof(sql),
      "SELECT %ssum(calls), sum(total_time), sum(self_time) "
      "FROM pg_stat_user_functions "
      "WHERE schemaname <> 'information_schema' ",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");

    res = PQexec(conn, sql);
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %scalls, total_time, self_time "
      "FROM pg_stat_user_functions "
      "WHERE schemaname <> 'information_schema' "
      "  AND funcname = $1",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");

    paramValues[0] = pg_strdup(opts->filter);

    res = PQexecParams(conn,
                       sql,
                       1,       /* one param */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       NULL,    /* don't need param lengths since text */
                       NULL,    /* default to all text params */
                       0);      /* ask for text results */
  }

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    calls = atol(PQgetvalue(res, row, column++));
    total_time = atof(PQgetvalue(res, row, column++));
    self_time = atof(PQgetvalue(res, row, column++));

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    format(r_calls, calls - previous_pgstatfunction->calls, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format_time(r_total_time, total_time - previous_pgstatfunction->total_time, 10);
    format_time(r_self_time, self_time - previous_pgstatfunction->self_time, 10);

    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s   %s  %s\n",
      r_calls,
      r_total_time,
      r_self_time
      );

    /* setting the new old value */
    previous_pgstatfunction->calls = calls;
    previous_pgstatfunction->total_time = total_time;
    previous_pgstatfunction->self_time = self_time;
  }

  /* cleanup */
  free(r_calls);
  free(r_total_time);
  free(r_self_time);
  PQclear(res);
}

/*
 * Dump all statement stats.
 */
void
print_pgstatstatement()
{
  char       sql[PGSTAT_DEFAULT_STRING_SIZE];
  const char *paramValues[1];
  PGresult   *res;
  int        nrows;
  int        row, column;

  long       plans = 0;
  float      total_plan_time = 0;
  long       calls = 0;
  float      total_exec_time = 0;
  long       rows = 0;
  long       shared_blks_hit = 0;
  long       shared_blks_read = 0;
  long       shared_blks_dirtied = 0;
  long       shared_blks_written = 0;
  long       local_blks_hit = 0;
  long       local_blks_read = 0;
  long       local_blks_dirtied = 0;
  long       local_blks_written = 0;
  long       temp_blks_read = 0;
  long       temp_blks_written = 0;
  float      shared_blk_read_time = 0;
  float      shared_blk_write_time = 0;
  float      local_blk_read_time = 0;
  float      local_blk_write_time = 0;
  float      temp_blk_read_time = 0;
  float      temp_blk_write_time = 0;
  long       wal_records = 0;
  long       wal_fpi = 0;
  long       wal_bytes = 0;

  char     *ts = NULL;
  char     *r1 = (char *)malloc(sizeof(char) * (20 + 1));
  char     *r2 = (char *)malloc(sizeof(char) * (20 + 1));
  char     *r3 = (char *)malloc(sizeof(char) * (20 + 1));
  char     *r4 = (char *)malloc(sizeof(char) * (20 + 1));
  char     *r5 = (char *)malloc(sizeof(char) * (20 + 1));
  char     *r6 = (char *)malloc(sizeof(char) * (20 + 1));

  if (opts->filter == NULL)
  {
    snprintf(sql, sizeof(sql),
      "SELECT %s%ssum(calls), sum(%s), sum(rows),"
      " sum(shared_blks_hit), sum(shared_blks_read), sum(shared_blks_dirtied), sum(shared_blks_written),"
      " sum(local_blks_hit), sum(local_blks_read), sum(local_blks_dirtied), sum(local_blks_written),"
      " sum(temp_blks_read), sum(temp_blks_written)"
      "%s%s%s"
      "%s"
      " FROM %s.pg_stat_statements ",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      backend_minimum_version(13, 0) ? "sum(plans), sum(total_plan_time), " : "",
      backend_minimum_version(13, 0) ? "total_exec_time" : "total_time",
      backend_minimum_version(17, 0) ? ", sum(shared_blk_read_time), sum(shared_blk_write_time)" : ", sum(blk_read_time), sum(blk_write_time)",
      backend_minimum_version(17, 0) ? ", sum(local_blk_read_time), sum(local_blk_write_time)" : "",
      backend_minimum_version(16, 0) ? ", sum(temp_blk_read_time), sum(temp_blk_write_time)" : "",
      backend_minimum_version(13, 0) ? ", sum(wal_records), sum(wal_fpi), sum(wal_bytes)" : "",
      opts->namespace);

    res = PQexec(conn, sql);
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %s%scalls, %s, rows,"
      " shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,"
      " local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written,"
      " temp_blks_read, temp_blks_written,"
      "%s%s%s"
      "%s"
      " FROM %s.pg_stat_statements "
      "WHERE queryid=$1",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      backend_minimum_version(13, 0) ? "plans, total_plan_time, " : "",
      backend_minimum_version(13, 0) ? "total_exec_time" : "total_time",
      backend_minimum_version(17, 0) ? ", shared_blk_read_time, shared_blk_write_time" : ", blk_read_time, blk_write_time",
      backend_minimum_version(17, 0) ? ", local_blk_read_time, local_blk_write_time" : "",
      backend_minimum_version(16, 0) ? ", temp_blk_read_time, temp_blk_write_time" : "",
      backend_minimum_version(13, 0) ? ", wal_records, wal_fpi, wal_bytes" : "",
      opts->namespace);

    paramValues[0] = pg_strdup(opts->filter);

    res = PQexecParams(conn,
                       sql,
                       1,       /* one param */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       NULL,    /* don't need param lengths since text */
                       NULL,    /* default to all text params */
                       0);      /* ask for text results */
  }

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    if (backend_minimum_version(13, 0))
    {
      plans = atol(PQgetvalue(res, row, column++));
      total_plan_time = atof(PQgetvalue(res, row, column++));
    }
    calls = atol(PQgetvalue(res, row, column++));
    total_exec_time = atof(PQgetvalue(res, row, column++));
    rows = atol(PQgetvalue(res, row, column++));
    shared_blks_hit = atol(PQgetvalue(res, row, column++));
    shared_blks_read = atol(PQgetvalue(res, row, column++));
    shared_blks_dirtied = atol(PQgetvalue(res, row, column++));
    shared_blks_written = atol(PQgetvalue(res, row, column++));
    local_blks_hit = atol(PQgetvalue(res, row, column++));
    local_blks_read = atol(PQgetvalue(res, row, column++));
    local_blks_dirtied = atol(PQgetvalue(res, row, column++));
    local_blks_written = atol(PQgetvalue(res, row, column++));
    temp_blks_read = atol(PQgetvalue(res, row, column++));
    temp_blks_written = atol(PQgetvalue(res, row, column++));
    shared_blk_read_time = atof(PQgetvalue(res, row, column++));
    shared_blk_write_time = atof(PQgetvalue(res, row, column++));
    if (backend_minimum_version(17, 0))
    {
      local_blk_read_time = atof(PQgetvalue(res, row, column++));
      local_blk_write_time = atof(PQgetvalue(res, row, column++));
    }
    if (backend_minimum_version(16, 0))
    {
      temp_blk_read_time = atof(PQgetvalue(res, row, column++));
      temp_blk_write_time = atof(PQgetvalue(res, row, column++));
    }

    if (backend_minimum_version(13, 0))
    {
      wal_records = atol(PQgetvalue(res, row, column++));
      wal_fpi = atol(PQgetvalue(res, row, column++));
      wal_bytes = atol(PQgetvalue(res, row, column++));
    }

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    if ((opts->substat == NULL || strstr(opts->substat, "plan") != NULL) && backend_minimum_version(13, 0))
    {
      format(r1, plans - previous_pgstatstatement->plans, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format_time(r2, total_plan_time - previous_pgstatstatement->total_plan_time, 9);
      (void)printf(" %s %s", r1, r2);
    }
    if (opts->substat == NULL || strstr(opts->substat, "exec") != NULL)
    {
      format(r1, calls - previous_pgstatstatement->calls, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format_time(r2, total_exec_time - previous_pgstatstatement->total_exec_time, 9);
      format(r3, rows - previous_pgstatstatement->rows, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      (void)printf("   %s %s %s", r1, r2, r3);
    }
    if (opts->substat == NULL || strstr(opts->substat, "shared") != NULL)
    {
      format(r1, shared_blks_hit - previous_pgstatstatement->shared_blks_hit, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r2, shared_blks_read - previous_pgstatstatement->shared_blks_read, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r3, shared_blks_dirtied - previous_pgstatstatement->shared_blks_dirtied, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r4, shared_blks_written - previous_pgstatstatement->shared_blks_written, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      (void)printf("   %s %s %s  %s", r1, r2, r3, r4);
    }
    if (opts->substat == NULL || strstr(opts->substat, "local") != NULL)
    {
      format(r1, local_blks_hit - previous_pgstatstatement->local_blks_hit, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r2, local_blks_read - previous_pgstatstatement->local_blks_read, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r3, local_blks_dirtied - previous_pgstatstatement->local_blks_dirtied, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r4, local_blks_written - previous_pgstatstatement->local_blks_written, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      (void)printf("   %s %s %s  %s", r1, r2, r3, r4);
    }
    if (opts->substat == NULL || strstr(opts->substat, "temp") != NULL)
    {
      format(r1, temp_blks_read - previous_pgstatstatement->temp_blks_read, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r2, temp_blks_written - previous_pgstatstatement->temp_blks_written, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      (void)printf("   %s  %s", r1, r2);
    }
    if (opts->substat == NULL || strstr(opts->substat, "time") != NULL)
    {
      if (backend_minimum_version(17, 0))
      {
        format_time(r1, shared_blk_read_time - previous_pgstatstatement->shared_blk_read_time, 9);
        format_time(r2, shared_blk_write_time - previous_pgstatstatement->shared_blk_write_time, 9);
        format_time(r3, local_blk_read_time - previous_pgstatstatement->local_blk_read_time, 9);
        format_time(r4, local_blk_write_time - previous_pgstatstatement->local_blk_write_time, 9);
        format_time(r5, temp_blk_read_time - previous_pgstatstatement->temp_blk_read_time, 9);
        format_time(r6, temp_blk_write_time - previous_pgstatstatement->temp_blk_write_time, 9);
        (void)printf("   %s    %s %s   %s %s   %s", r1, r2, r3, r4, r5, r6);
      }
      else if (backend_minimum_version(16, 0))
      {
        format_time(r1, shared_blk_read_time - previous_pgstatstatement->shared_blk_read_time, 9);
        format_time(r2, shared_blk_write_time - previous_pgstatstatement->shared_blk_write_time, 9);
        format_time(r3, temp_blk_read_time - previous_pgstatstatement->temp_blk_read_time, 9);
        format_time(r4, temp_blk_write_time - previous_pgstatstatement->temp_blk_write_time, 9);
        (void)printf("   %s %s %s %s", r1, r2, r3, r4);
      }
      else if (backend_minimum_version(13, 0))
      {
        format_time(r1, shared_blk_read_time - previous_pgstatstatement->shared_blk_read_time, 9);
        format_time(r2, shared_blk_write_time - previous_pgstatstatement->shared_blk_write_time, 9);
        (void)printf("   %s %s", r1, r2);
      }
    }
    if ((opts->substat == NULL || strstr(opts->substat, "wal") != NULL) && backend_minimum_version(13, 0))
    {
      format(r1, wal_records - previous_pgstatstatement->wal_records, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r2, wal_fpi - previous_pgstatstatement->wal_fpi, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r3, wal_bytes - previous_pgstatstatement->wal_bytes, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
      (void)printf("      %s %s %s", r1, r2, r3);
    }
    (void)printf("\n");

    /* setting the new old value */
    previous_pgstatstatement->plans = plans;
    previous_pgstatstatement->total_plan_time = total_plan_time;
    previous_pgstatstatement->calls = calls;
    previous_pgstatstatement->total_exec_time = total_exec_time;
    previous_pgstatstatement->rows = rows;
    previous_pgstatstatement->shared_blks_hit = shared_blks_hit;
    previous_pgstatstatement->shared_blks_read = shared_blks_read;
    previous_pgstatstatement->shared_blks_dirtied = shared_blks_dirtied;
    previous_pgstatstatement->shared_blks_written = shared_blks_written;
    previous_pgstatstatement->local_blks_hit = local_blks_hit;
    previous_pgstatstatement->local_blks_read = local_blks_read;
    previous_pgstatstatement->local_blks_dirtied = local_blks_dirtied;
    previous_pgstatstatement->local_blks_written = local_blks_written;
    previous_pgstatstatement->temp_blks_read = temp_blks_read;
    previous_pgstatstatement->temp_blks_written = temp_blks_written;
    previous_pgstatstatement->shared_blk_read_time = shared_blk_read_time;
    previous_pgstatstatement->shared_blk_write_time = shared_blk_write_time;
    previous_pgstatstatement->local_blk_read_time = local_blk_read_time;
    previous_pgstatstatement->local_blk_write_time = local_blk_write_time;
    previous_pgstatstatement->temp_blk_read_time = temp_blk_read_time;
    previous_pgstatstatement->temp_blk_write_time = temp_blk_write_time;
    previous_pgstatstatement->wal_records = wal_records;
    previous_pgstatstatement->wal_fpi = wal_fpi;
    previous_pgstatstatement->wal_bytes = wal_bytes;
  };

  /* cleanup */
  free(r1);
  free(r2);
  free(r3);
  free(r4);
  free(r5);
  free(r6);
  PQclear(res);
}

/*
 * Dump all SLRU stats.
 */
void
print_pgstatslru()
{
  char       sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  const char *paramValues[1];
  int        nrows;
  int        row, column;

  long       blks_zeroed = 0;
  long       blks_hit = 0;
  long       blks_read = 0;
  long       blks_written = 0;
  long       blks_exists = 0;
  long       flushes = 0;
  long       truncates = 0;
  char       *stats_reset;
  bool       has_been_reset;

  char       *ts = NULL;
  char       *r_blks_zeroed = (char *)malloc(sizeof(char) * (9 + 1));
  char       *r_blks_hit = (char *)malloc(sizeof(char) * (9 + 1));
  char       *r_blks_read = (char *)malloc(sizeof(char) * (9 + 1));
  char       *r_blks_written = (char *)malloc(sizeof(char) * (9 + 1));
  char       *r_blks_exists = (char *)malloc(sizeof(char) * (9 + 1));
  char       *r_flushes = (char *)malloc(sizeof(char) * (9 + 1));
  char       *r_truncates = (char *)malloc(sizeof(char) * (9 + 1));

  /*
   * With a filter, we assume we'll get only one row.
   * Without, we sum all the fields to get one row.
   */
  if (opts->filter == NULL)
  {
    snprintf(sql, sizeof(sql),
      "SELECT %ssum(blks_zeroed), sum(blks_hit), sum(blks_read), sum(blks_written), "
      "sum(blks_exists), sum(flushes), sum(truncates), "
      "max(stats_reset), max(stats_reset)>'%s' "
      "FROM pg_stat_slru ",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      previous_pgstatslru->stats_reset);

    res = PQexec(conn, sql);
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %ssum(blks_zeroed), sum(blks_hit), sum(blks_read), sum(blks_written), "
      "sum(blks_exists), sum(flushes), sum(truncates), "
      "stats_reset, stats_reset>'%s' "
      "FROM pg_stat_slru "
      "WHERE name = $1",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      previous_pgstatslru->stats_reset);

    paramValues[0] = pg_strdup(opts->filter);

    res = PQexecParams(conn,
                       sql,
                       1,       /* one param */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       NULL,    /* don't need param lengths since text */
                       NULL,    /* default to all text params */
                       0);      /* ask for text results */
  }

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    blks_zeroed = atol(PQgetvalue(res, row, column++));
    blks_hit = atol(PQgetvalue(res, row, column++));
    blks_read = atol(PQgetvalue(res, row, column++));
    blks_written = atol(PQgetvalue(res, row, column++));
    blks_exists = atol(PQgetvalue(res, row, column++));
    flushes = atol(PQgetvalue(res, row, column++));
    truncates = atol(PQgetvalue(res, row, column++));

    stats_reset = PQgetvalue(res, row, column++);
    has_been_reset = strcmp(PQgetvalue(res, row, column++), "f") && strcmp(previous_pgstatslru->stats_reset, PGSTAT_OLDEST_STAT_RESET);

    if (has_been_reset)
    {
      (void)printf("pg_stat_slru has been reset!\n");
    }

    /* printing the diff... note that the first line will be the current value, rather than the diff */
    format(r_blks_zeroed, blks_zeroed - previous_pgstatslru->blks_zeroed, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_blks_hit, blks_hit - previous_pgstatslru->blks_hit, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_blks_read, blks_read - previous_pgstatslru->blks_read, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_blks_written, blks_written - previous_pgstatslru->blks_written, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_blks_exists, blks_exists - previous_pgstatslru->blks_exists, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_flushes, flushes - previous_pgstatslru->flushes, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_truncates, truncates - previous_pgstatslru->truncates, 9, opts->human_readable ? ALL_UNIT : NO_UNIT);

    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s %s %s %s %s %s %s\n",
      r_blks_zeroed,
      r_blks_hit,
      r_blks_read,
      r_blks_written,
      r_blks_exists,
      r_flushes,
      r_truncates
      );

    /* setting the new old value */
    previous_pgstatslru->blks_zeroed = blks_zeroed;
    previous_pgstatslru->blks_hit = blks_hit;
    previous_pgstatslru->blks_read = blks_read;
    previous_pgstatslru->blks_written = blks_written;
    previous_pgstatslru->blks_exists = blks_exists;
    previous_pgstatslru->flushes = flushes;
    previous_pgstatslru->truncates = truncates;
    previous_pgstatslru->stats_reset = stats_reset;
  }

  /* cleanup */
  free(r_blks_zeroed);
  free(r_blks_hit);
  free(r_blks_read);
  free(r_blks_written);
  free(r_blks_exists);
  free(r_flushes);
  free(r_truncates);
  PQclear(res);
}

/*
 * Dump all wal stats.
 */
void
print_pgstatwal()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  long     wal_records;
  long     wal_fpi;
  long     wal_bytes;
  long     wal_buffers_full;
  long     wal_write;
  long     wal_sync;
  float    wal_write_time;
  float    wal_sync_time;
  char     *stats_reset;
  bool     has_been_reset;

  char     *ts = NULL;
  char     *r_wal_records = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_wal_fpi = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_wal_bytes = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_wal_buffers_full = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_wal_write = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_wal_sync = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_wal_write_time = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_wal_sync_time = (char *)malloc(sizeof(char) * (10 + 1));

  /* grab the stats (this is the only stats on one line) */
  snprintf(sql, sizeof(sql),
    "SELECT %swal_records, wal_fpi, wal_bytes, wal_buffers_full%s,"
    "stats_reset, stats_reset>'%s' "
    "FROM pg_stat_wal ",
    opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
    backend_minimum_version(18, 0) ? "" : ", wal_write, wal_sync, wal_write_time, wal_sync_time",
    previous_pgstatwal->stats_reset);

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    wal_records = atol(PQgetvalue(res, row, column++));
    wal_fpi = atol(PQgetvalue(res, row, column++));
    wal_bytes = atol(PQgetvalue(res, row, column++));
    wal_buffers_full = atol(PQgetvalue(res, row, column++));
    if (!backend_minimum_version(18, 0))
    {
      wal_write = atol(PQgetvalue(res, row, column++));
      wal_sync = atol(PQgetvalue(res, row, column++));
      wal_write_time = atof(PQgetvalue(res, row, column++));
      wal_sync_time = atof(PQgetvalue(res, row, column++));
    }
    stats_reset = PQgetvalue(res, row, column++);
    has_been_reset = strcmp(PQgetvalue(res, row, column++), "f") && strcmp(previous_pgstatwal->stats_reset, PGSTAT_OLDEST_STAT_RESET);

    if (has_been_reset)
    {
      (void)printf("pg_stat_wal has been reset!\n");
    }

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    format(r_wal_records, wal_records - previous_pgstatwal->wal_records, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_wal_fpi, wal_fpi - previous_pgstatwal->wal_fpi, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_wal_bytes, wal_bytes - previous_pgstatwal->wal_bytes, 10, opts->human_readable ? SIZE_UNIT : NO_UNIT);
    format(r_wal_buffers_full, wal_buffers_full - previous_pgstatwal->wal_buffers_full, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    if (!backend_minimum_version(18, 0))
    {
      format(r_wal_write, wal_write - previous_pgstatwal->wal_write, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format(r_wal_sync, wal_sync - previous_pgstatwal->wal_sync, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
      format_time(r_wal_write_time, wal_write_time - previous_pgstatwal->wal_write_time, 10);
      format_time(r_wal_sync_time, wal_sync_time - previous_pgstatwal->wal_sync_time, 10);
    }

    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s %s %s   %s",
      r_wal_records,
      r_wal_fpi,
      r_wal_bytes,
      r_wal_buffers_full
      );
    if (!backend_minimum_version(18, 0))
    {
      (void)printf(" %s %s %s %s",
        r_wal_write,
        r_wal_sync,
        r_wal_write_time,
        r_wal_sync_time
        );
    }
    (void)printf("\n");

    /* setting the new old value */
    previous_pgstatwal->wal_records = wal_records;
    previous_pgstatwal->wal_fpi = wal_fpi;
    previous_pgstatwal->wal_bytes = wal_bytes;
    previous_pgstatwal->wal_buffers_full = wal_buffers_full;
    if (!backend_minimum_version(18, 0))
    {
      previous_pgstatwal->wal_write = wal_write;
      previous_pgstatwal->wal_sync = wal_sync;
      previous_pgstatwal->wal_write_time = wal_write_time;
      previous_pgstatwal->wal_sync_time = wal_sync_time;
    }
    previous_pgstatwal->stats_reset = stats_reset;
  }

  /* cleanup */
  free(r_wal_records);
  free(r_wal_fpi);
  free(r_wal_bytes);
  free(r_wal_buffers_full);
  free(r_wal_write);
  free(r_wal_sync);
  free(r_wal_write_time);
  free(r_wal_sync_time);
  PQclear(res);
}

/*
 * Dump all io stats.
 */
void
print_pgstatio()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  long     reads;
  long     read_bytes;
  float    read_time;
  long     writes;
  long     write_bytes;
  float    write_time;
  long     writebacks;
  float    writeback_time;
  long     extends;
  float    extend_time;
  long     hits;
  long     evictions;
  long     reuses;
  long     fsyncs;
  float    fsync_time;
  char     *stats_reset;
  bool     has_been_reset;

  char     *ts = NULL;
  char     *r_reads = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_read_bytes = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_read_time = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_writes = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_write_bytes = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_write_time = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_writebacks = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_writeback_time = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_extends = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_extend_time = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_hits = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_evictions = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_reuses = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_fsyncs = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_fsync_time = (char *)malloc(sizeof(char) * (10 + 1));

  /* grab the stats (this is the only stats on one line) */
  snprintf(sql, sizeof(sql),
    "SELECT %ssum(reads), %s, sum(read_time), sum(writes), %s, sum(write_time), "
    "sum(writebacks), sum(writeback_time), sum(extends), sum(extend_time), "
    "sum(hits), sum(evictions), sum(reuses), sum(fsyncs), sum(fsync_time), "
    "max(stats_reset), bool_and(stats_reset>'%s') "
    "FROM pg_stat_io ",
    opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
    backend_minimum_version(18, 0) ? "sum(read_bytes)" : "sum(reads*op_bytes)",
    backend_minimum_version(18, 0) ? "sum(write_bytes)" : "sum(writes*op_bytes)",
    previous_pgstatio->stats_reset);

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    reads = atol(PQgetvalue(res, row, column++));
    read_bytes = atol(PQgetvalue(res, row, column++));
    read_time = atof(PQgetvalue(res, row, column++));
    writes = atol(PQgetvalue(res, row, column++));
    write_bytes = atol(PQgetvalue(res, row, column++));
    write_time = atof(PQgetvalue(res, row, column++));
    writebacks = atol(PQgetvalue(res, row, column++));
    writeback_time = atof(PQgetvalue(res, row, column++));
    extends = atol(PQgetvalue(res, row, column++));
    extend_time = atof(PQgetvalue(res, row, column++));
    hits = atol(PQgetvalue(res, row, column++));
    evictions = atol(PQgetvalue(res, row, column++));
    reuses = atol(PQgetvalue(res, row, column++));
    fsyncs = atol(PQgetvalue(res, row, column++));
    fsync_time = atof(PQgetvalue(res, row, column++));

    stats_reset = PQgetvalue(res, row, column++);
    has_been_reset = strcmp(PQgetvalue(res, row, column++), "f") && strcmp(previous_pgstatio->stats_reset, PGSTAT_OLDEST_STAT_RESET);

    if (has_been_reset)
    {
      (void)printf("pg_stat_io has been reset!\n");
    }

    /* printing the diff...
     * note that the first line will be the current value, rather than the diff */
    format(r_reads, reads - previous_pgstatio->reads, 7, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_read_bytes, read_bytes - previous_pgstatio->read_bytes, 7, opts->human_readable ? SIZE_UNIT : NO_UNIT);
    format_time(r_read_time, read_time - previous_pgstatio->read_time, 10);
    format(r_writes, writes - previous_pgstatio->writes, 7, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_write_bytes, write_bytes - previous_pgstatio->write_bytes, 7, opts->human_readable ? SIZE_UNIT : NO_UNIT);
    format_time(r_write_time, write_time - previous_pgstatio->write_time, 10);
    format(r_writebacks, writebacks - previous_pgstatio->writebacks, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format_time(r_writeback_time, writeback_time - previous_pgstatio->writeback_time, 10);
    format(r_extends, extends - previous_pgstatio->extends, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format_time(r_extend_time, extend_time - previous_pgstatio->extend_time, 10);
    format(r_hits, hits - previous_pgstatio->hits, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_evictions, evictions - previous_pgstatio->evictions, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_reuses, reuses - previous_pgstatio->reuses, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_fsyncs, fsyncs - previous_pgstatio->fsyncs, 6, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format_time(r_fsync_time, fsync_time - previous_pgstatio->fsync_time, 10);

    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf("%s     %s %s %s     %s %s %s     %s  %s  %s %s    %s    %s %s %s\n",
      r_reads,
      r_read_bytes,
      r_read_time,
      r_writes,
      r_write_bytes,
      r_write_time,
      r_writebacks,
      r_writeback_time,
      r_extends,
      r_extend_time,
      r_hits,
      r_evictions,
      r_reuses,
      r_fsyncs,
      r_fsync_time
      );

    /* setting the new old value */
    previous_pgstatio->reads = reads;
    previous_pgstatio->read_bytes = read_bytes;
    previous_pgstatio->read_time = read_time;
    previous_pgstatio->writes = writes;
    previous_pgstatio->write_bytes = write_bytes;
    previous_pgstatio->write_time = write_time;
    previous_pgstatio->writebacks = writebacks;
    previous_pgstatio->writeback_time = writeback_time;
    previous_pgstatio->extends = extends;
    previous_pgstatio->extend_time = extend_time;
    previous_pgstatio->hits = hits;
    previous_pgstatio->evictions = evictions;
    previous_pgstatio->reuses = reuses;
    previous_pgstatio->fsyncs = fsyncs;
    previous_pgstatio->fsync_time = fsync_time;
    previous_pgstatio->stats_reset = stats_reset;
  }

  /* cleanup */
  free(r_reads);
  free(r_read_bytes);
  free(r_read_time);
  free(r_writes);
  free(r_write_bytes);
  free(r_write_time);
  free(r_writebacks);
  free(r_writeback_time);
  free(r_extends);
  free(r_extend_time);
  free(r_hits);
  free(r_evictions);
  free(r_reuses);
  free(r_fsyncs);
  free(r_fsync_time);
  PQclear(res);
}

/*
 * Dump base backup progress.
 */
void
print_pgstatprogressbasebackup()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row;

  snprintf(sql, sizeof(sql),
    "SELECT pid,"
    "     phase,"
    "       pg_size_pretty(backup_streamed),"
    "       pg_size_pretty(backup_total),"
    "       CASE WHEN backup_total>0"
    "       THEN trunc(backup_streamed::numeric*100/backup_total,2)::text"
    "       ELSE 'N/A' END,"
    "       CASE WHEN tablespaces_total>0"
    "       THEN trunc(tablespaces_streamed::numeric*100/tablespaces_total,2)::text"
    "       ELSE 'N/A' END,"
    "       (now()-query_start)::time(0) "
    "FROM pg_stat_progress_basebackup "
    "JOIN pg_stat_activity USING (pid) "
    "ORDER BY pid");

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  for (row = 0; row < nrows; row++)
  {
    /* printing the value... */
    (void)printf(" %-10s  %-28s %-10s  %-10s %6s %6s %s\n",
      PQgetvalue(res, row, 0),
      PQgetvalue(res, row, 1),
      PQgetvalue(res, row, 2),
      PQgetvalue(res, row, 3),
      PQgetvalue(res, row, 4),
      PQgetvalue(res, row, 5),
      PQgetvalue(res, row, 6)
      );
  };

  /* cleanup */
  PQclear(res);
}

/*
 * Dump analyze progress.
 */
void
print_pgstatprogressanalyze()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row;

  snprintf(sql, sizeof(sql),
    "SELECT s.datname, relname,"
    "       pg_size_pretty(pg_table_size(relid)),"
    "     phase,"
    "     CASE WHEN sample_blks_total>0"
    "            THEN trunc(sample_blks_scanned::numeric*100/sample_blks_total,2)::text"
    "            ELSE 'N/A' END,"
    "     CASE WHEN ext_stats_total>0"
    "            THEN trunc(ext_stats_computed::numeric*100/ext_stats_total,2)::text"
    "            ELSE 'N/A' END,"
    "       CASE WHEN child_tables_total>0"
    "          THEN trunc(child_tables_done::numeric*100/child_tables_total,2)::text"
    "            ELSE 'N/A' END,"
    "       (now()-query_start)::time(0) "
    "FROM pg_stat_progress_analyze s "
    "JOIN pg_stat_activity USING (pid) "
    "LEFT JOIN pg_class c ON c.oid=s.relid "
    "ORDER BY pid");

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  for (row = 0; row < nrows; row++)
  {
    /* printing the value... */
    (void)printf(" %-16s %-20s %10s   %-24s    %6s       %6s      %6s %s\n",
      PQgetvalue(res, row, 0),
      PQgetvalue(res, row, 1),
      PQgetvalue(res, row, 2),
      PQgetvalue(res, row, 3),
      PQgetvalue(res, row, 4),
      PQgetvalue(res, row, 5),
      PQgetvalue(res, row, 6),
      PQgetvalue(res, row, 7)
      );
  };

  /* cleanup */
  PQclear(res);
}

/*
 * Dump cluster progress.
 */
void
print_pgstatprogresscluster()
{
  char    sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  int      nrows;
  int      row;

  snprintf(sql, sizeof(sql),
    "SELECT s.datname, t.relname, i.relname,"
    "     phase, heap_tuples_scanned, heap_tuples_written,"
    "     CASE WHEN heap_blks_total=0 THEN 'N/A' ELSE trunc(heap_blks_scanned::numeric*100/heap_blks_total,2)::text END,"
    "     index_rebuild_count,"
    "       (now()-query_start)::time(0) "
    "FROM pg_stat_progress_cluster s "
    "JOIN pg_stat_activity USING (pid) "
    "LEFT JOIN pg_class t ON t.oid=s.relid "
    "LEFT JOIN pg_class i ON i.oid=s.cluster_index_relid "
    "ORDER BY pid");

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    /* printing the value... */
    (void)printf(" %-16s %-20s  %-20s   %-46s    %12ld   %12ld    %5s     %10ld %s\n",
      PQgetvalue(res, row, 0),
      PQgetvalue(res, row, 1),
      PQgetvalue(res, row, 2),
      PQgetvalue(res, row, 3),
      atol(PQgetvalue(res, row, 4)),
      atol(PQgetvalue(res, row, 5)),
      PQgetvalue(res, row, 6),
      atol(PQgetvalue(res, row, 7)),
      PQgetvalue(res, row, 8)
      );
  };

  /* cleanup */
  PQclear(res);
}

/*
 * Dump copy progress.
 */
void
print_pgstatprogresscopy()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row;

  snprintf(sql, sizeof(sql),
    "SELECT pc.datname, t.relname,"
    "     command, type,"
    "     bytes_processed, bytes_total, tuples_processed, tuples_excluded,"
    "       (now()-query_start)::time(0) "
    "FROM pg_stat_progress_copy pc "
    "JOIN pg_stat_activity USING (pid) "
    "LEFT JOIN pg_class t ON t.oid=pc.relid "
    "ORDER BY pid");

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    /* printing the value... */
    (void)printf(" %-16s %-20s      %-23s  %-20s  %10ld  %10ld   %10ld  %10ld         %s\n",
      PQgetvalue(res, row, 0),
      PQgetvalue(res, row, 1),
      PQgetvalue(res, row, 2),
      PQgetvalue(res, row, 3),
      atol(PQgetvalue(res, row, 4)),
      atol(PQgetvalue(res, row, 5)),
      atol(PQgetvalue(res, row, 6)),
      atol(PQgetvalue(res, row, 7)),
      PQgetvalue(res, row, 8)
      );
  };

  /* cleanup */
  PQclear(res);
}

/*
 * Dump index creation progress.
 */
void
print_pgstatprogresscreateindex()
{
  char    sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  int      nrows;
  int      row;

  snprintf(sql, sizeof(sql),
    "SELECT s.datname, t.relname, i.relname,"
    "     phase,"
    "     CASE WHEN lockers_total=0 THEN 'N/A' ELSE trunc(lockers_done::numeric*100/lockers_total,2)::text END,"
    "     CASE WHEN blocks_total=0 THEN 'N/A' ELSE trunc(blocks_done::numeric*100/blocks_total,2)::text END,"
    "     CASE WHEN tuples_total=0 THEN 'N/A' ELSE trunc(tuples_done::numeric*100/tuples_total,2)::text END,"
    "     CASE WHEN partitions_total=0 THEN 'N/A' ELSE trunc(partitions_done::numeric*100/partitions_total,2)::text END, "
    "       (now()-query_start)::time(0) "
    "FROM pg_stat_progress_create_index s "
    "JOIN pg_stat_activity USING (pid) "
    "LEFT JOIN pg_class t ON t.oid=s.relid "
    "LEFT JOIN pg_class i ON i.oid=s.index_relid "
    "ORDER BY pid");
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    /* printing the value... */
    (void)printf(" %-16s %-20s  %-20s   %-46s    %5s    %5s   %5s        %5s           %s\n",
      PQgetvalue(res, row, 0),
      PQgetvalue(res, row, 1),
      PQgetvalue(res, row, 2),
      PQgetvalue(res, row, 3),
      PQgetvalue(res, row, 4),
      PQgetvalue(res, row, 5),
      PQgetvalue(res, row, 6),
      PQgetvalue(res, row, 7),
      PQgetvalue(res, row, 8)
      );
  };

  /* cleanup */
  PQclear(res);
}

/*
 * Dump vacuum progress.
 */
void
print_pgstatprogressvacuum()
{
  char    sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  int      nrows;
  int      row;

  snprintf(sql, sizeof(sql),
    "SELECT s.datname, c.relname,"
    "  pg_size_pretty(pg_table_size(s.relid)),"
    "  s.phase,"
    "  CASE WHEN s.heap_blks_total=0 THEN 'N/A' ELSE trunc(s.heap_blks_scanned::numeric*100/s.heap_blks_total,2)::text END,"
    "  CASE WHEN s.heap_blks_total=0 THEN 'N/A' ELSE trunc(s.heap_blks_vacuumed::numeric*100/s.heap_blks_total,2)::text END,"
    "  s.index_vacuum_count,"
    "  CASE WHEN s.%s=0 THEN 'N/A' ELSE trunc(s.%s::numeric*100/s.%s,2)::text END,"
    "  (now()-a.query_start)::time(0) "
    "FROM pg_stat_progress_vacuum s "
    "JOIN pg_stat_activity a ON s.pid=a.pid "
    "LEFT JOIN pg_class c ON c.oid=s.relid "
    "ORDER BY s.pid",
    backend_minimum_version(17, 0) ? "max_dead_tuple_bytes" : "max_dead_tuples",
    backend_minimum_version(17, 0) ? "dead_tuple_bytes"     : "num_dead_tuples",
    backend_minimum_version(17, 0) ? "max_dead_tuple_bytes" : "max_dead_tuples");

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    /* printing the value... */
    (void)printf(" %-16s %-20s %10s   %-24s    %5s    %5s   %5ld        %5s %s\n",
      PQgetvalue(res, row, 0),
      PQgetvalue(res, row, 1),
      PQgetvalue(res, row, 2),
      PQgetvalue(res, row, 3),
      PQgetvalue(res, row, 4),
      PQgetvalue(res, row, 5),
      atol(PQgetvalue(res, row, 6)),
      PQgetvalue(res, row, 7),
      PQgetvalue(res, row, 8)
      );
  };

  /* cleanup */
  PQclear(res);
}

/*
 * Dump all buffercache stats.
 */
void
print_buffercache()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  long     usedblocks = 0;
  long     usedblocks_pct = 0;
  long     dirtyblocks = 0;
  long     dirtyblocks_pct = 0;

  char     *ts = NULL;
  char     *r_usedblocks = (char *)malloc(sizeof(char) * (5 + 1));
  char     *r_usedblocks_pct = (char *)malloc(sizeof(char) * (5 + 1));
  char     *r_dirtyblocks = (char *)malloc(sizeof(char) * (5 + 1));
  char     *r_dirtyblocks_pct = (char *)malloc(sizeof(char) * (5 + 1));

  snprintf(sql, sizeof(sql),
    "SELECT %scount(*) FILTER (WHERE relfilenode IS NOT NULL), "
    "100. * count(*) FILTER (WHERE relfilenode IS NOT NULL) / count(*), "
    "count(*) FILTER (WHERE isdirty), "
    "100. * count(*) FILTER (WHERE isdirty) / count(*) "
    "FROM %s.pg_buffercache ",
    opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
    opts->namespace);

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    column = 0;

    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    usedblocks = atol(PQgetvalue(res, row, column++));
    usedblocks_pct = atol(PQgetvalue(res, row, column++));
    dirtyblocks = atol(PQgetvalue(res, row, column++));
    dirtyblocks_pct = atol(PQgetvalue(res, row, column++));

    /* printing the actual values for once */
    format(r_usedblocks, usedblocks, 7, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_usedblocks_pct, usedblocks_pct, 5, NO_UNIT);
    format(r_dirtyblocks, dirtyblocks, 7, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_dirtyblocks_pct, dirtyblocks_pct, 5, NO_UNIT);

    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s    %s   %s    %s\n",
      r_usedblocks, r_usedblocks_pct, r_dirtyblocks, r_dirtyblocks_pct);
  }

  /* cleanup */
  free(r_usedblocks);
  free(r_usedblocks_pct);
  free(r_dirtyblocks);
  free(r_dirtyblocks_pct);
  PQclear(res);
}

/*
 * Dump all xlog writes stats.
 */
void
print_xlogstats()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  long     column = 0;

  char     *xlogfilename;
  char     *currentlocation;
  long     locationdiff;

  char     *ts = NULL;
  char     *r_locationdiff = (char *)malloc(sizeof(char) * (12 + 1));

  if (backend_minimum_version(10, 0))
  {
    snprintf(sql, sizeof(sql),
      "SELECT %s"
      "  pg_walfile_name(pg_current_wal_lsn()), "
      "  pg_current_wal_lsn(), "
      "  pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %s"
      "  pg_xlogfile_name(pg_current_xlog_location()), "
      "  pg_current_xlog_location(), "
      "  pg_xlog_location_diff(pg_current_xlog_location(), '0/0')",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");
  }

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  if (opts->addtimestamp)
  {
    ts = PQgetvalue(res, 0, column++);
  }
  xlogfilename = pg_strdup(PQgetvalue(res, 0, column++));
  currentlocation = pg_strdup(PQgetvalue(res, 0, column++));
  locationdiff = atol(PQgetvalue(res, 0, column++));

  /* printing the actual values for once */
  format(r_locationdiff, locationdiff - previous_xlogstats->locationdiff, 12, opts->human_readable ? SIZE_UNIT : NO_UNIT);
  if (opts->addtimestamp && ts != NULL)
  {
    (void)printf(" %s  ", ts);
  }
  (void)printf(" %s   %s     %s\n", xlogfilename, currentlocation, r_locationdiff);

  /* setting the new old value */
  previous_xlogstats->location = pg_strdup(currentlocation);
  previous_xlogstats->locationdiff = locationdiff;

  /* cleanup */
  free(r_locationdiff);
  PQclear(res);
}

/*
 * Dump dead live tuples stats.
 */
void
print_deadlivestats()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  long     column = 0;

  long     live;
  long     dead;

  char     *ts = NULL;
  char     *r_live = (char *)malloc(sizeof(char) * (10 + 1));
  char     *r_dead = (char *)malloc(sizeof(char) * (10 + 1));

  snprintf(sql, sizeof(sql),
    "SELECT %ssum(n_live_tup), sum(n_dead_tup) FROM pg_stat_all_tables",
    opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  if (opts->addtimestamp)
  {
    ts = PQgetvalue(res, 0, column++);
  }
  live = atol(PQgetvalue(res, 0, column++));
  dead = atol(PQgetvalue(res, 0, column++));

  /* printing the actual values for once */
  format(r_live, live, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
  format(r_dead, dead, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
  if (opts->addtimestamp && ts != NULL)
  {
    (void)printf(" %s  ", ts);
  }
  (void)printf(" %s  %s     %.2f\n",
    r_live,
    r_dead,
    dead+live == 0 ? 0 : 100.*dead/((dead+live)));

  /* setting the new old value */
  previous_deadlivestats->live = live;
  previous_deadlivestats->dead = dead;

  /* cleanup */
  free(r_live);
  free(r_dead);
  PQclear(res);
}

/*
 * Dump all repslots informations
 */
void
print_repslotsstats()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  long     column = 0;

  char     *xlogfilename;
  char     *currentlocation;
  long     locationdiff;

  char     *ts = NULL;
  char     *r_locationdiff = (char *)malloc(sizeof(char) * (12 + 1));

  snprintf(sql, sizeof(sql),
    "SELECT %s"
    "  pg_walfile_name(restart_lsn), "
    "  restart_lsn, "
    "  pg_wal_lsn_diff(restart_lsn, '0/0')"
    "FROM pg_replication_slots "
    "WHERE slot_name = '%s'",
    opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
    opts->filter);

  res = PQexec(conn, sql);

  if (!res || PQntuples(res) == 0)
  {
    PQclear(res);
    PQfinish(conn);
    pg_log_error("No results, meaning no replication slot");
    exit(EXIT_FAILURE);
  }

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  if (opts->addtimestamp)
  {
    ts = PQgetvalue(res, 0, column++);
  }
  xlogfilename = pg_strdup(PQgetvalue(res, 0, column++));
  currentlocation = pg_strdup(PQgetvalue(res, 0, column++));
  locationdiff = atol(PQgetvalue(res, 0, column++));

  /* printing the actual values for once */
  format(r_locationdiff, locationdiff - previous_xlogstats->locationdiff, 12, opts->human_readable ? SIZE_UNIT : NO_UNIT);
  if (opts->addtimestamp && ts != NULL)
  {
    (void)printf(" %s  ", ts);
  }
  (void)printf(" %s   %s     %s\n", xlogfilename, currentlocation, r_locationdiff);

  /* setting the new old value */
  previous_repslots->restartlsn = pg_strdup(currentlocation);
  previous_repslots->restartlsndiff = locationdiff;

  /* cleanup */
  free(r_locationdiff);
  PQclear(res);
}

/*
 * Dump all temporary files stats.
 */
void
print_tempfilestats()
{
  char      sql[2*PGSTAT_DEFAULT_STRING_SIZE];
  PGresult  *res;
  long      size = 0;
  long      count = 0;
  int       nrows;
  int       row, column;

  char      *ts = NULL;
  char      *r_size = (char *)malloc(sizeof(char) * (10 + 1));
  char      *r_count = (char *)malloc(sizeof(char) * (10 + 1));

  if (backend_minimum_version(9, 3))
  {
    snprintf(sql, sizeof(sql),
      "SELECT %sunnest(regexp_matches(agg.tmpfile, 'pgsql_tmp([0-9]*)')) AS pid, "
      "  SUM((pg_stat_file(agg.dir||'/'||agg.tmpfile)).size), "
      "  count(*) "
      "FROM "
      "  (SELECT ls.oid, ls.spcname, "
      "     ls.dir||'/'||ls.sub AS dir, CASE gs.i WHEN 1 THEN '' ELSE pglsdir END AS tmpfile "
      "   FROM "
      "     (SELECT sr.oid, sr.spcname, "
      "             'pg_tblspc/'||sr.oid||'/'||sr.spc_root AS dir, "
      "             pg_ls_dir('pg_tblspc/'||sr.oid||'/'||sr.spc_root) AS sub "
      "      FROM (SELECT spc.oid, spc.spcname, "
      "                   pg_ls_dir('pg_tblspc/'||spc.oid) AS spc_root, "
      "             trim(trailing E'\n ' FROM pg_read_file('PG_VERSION')) as v "
      "            FROM (SELECT oid, spcname FROM pg_tablespace WHERE spcname !~ '^pg_') AS spc "
      "           ) sr "
      "      WHERE sr.spc_root ~ ('^PG_'||sr.v) "
      "      UNION ALL "
      "      SELECT 0, 'pg_default', "
      "             'base' AS dir, "
      "             'pgsql_tmp' AS sub "
      "    FROM pg_ls_dir('base') AS l "
      "    WHERE l='pgsql_tmp' "
      "     ) AS ls, "
      "     (SELECT generate_series(1,2) AS i) AS gs, "
      "     LATERAL pg_ls_dir(dir||'/'||ls.sub) pglsdir "
      "   WHERE ls.sub = 'pgsql_tmp') agg "
      "GROUP BY 1%s",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      opts->addtimestamp ? ",2" : "");
  }
  else
  {
    snprintf(sql, sizeof(sql),
      "SELECT %sunnest(regexp_matches(agg.tmpfile, 'pgsql_tmp([0-9]*)')) AS pid, "
      "  SUM((pg_stat_file(agg.dir||'/'||agg.tmpfile)).size), "
      "  count(*) "
      "FROM "
      "  (SELECT ls.oid, ls.spcname, "
      "     ls.dir||'/'||ls.sub AS dir, CASE gs.i WHEN 1 THEN '' ELSE pg_ls_dir(dir||'/'||ls.sub) END AS tmpfile "
      "   FROM "
      "     (SELECT sr.oid, sr.spcname, "
      "             'pg_tblspc/'||sr.oid||'/'||sr.spc_root AS dir, "
      "             pg_ls_dir('pg_tblspc/'||sr.oid||'/'||sr.spc_root) AS sub "
      "      FROM (SELECT spc.oid, spc.spcname, "
      "                   pg_ls_dir('pg_tblspc/'||spc.oid) AS spc_root, "
      "             trim(trailing E'\n ' FROM pg_read_file('PG_VERSION')) as v "
      "            FROM (SELECT oid, spcname FROM pg_tablespace WHERE spcname !~ '^pg_') AS spc "
      "           ) sr "
      "      WHERE sr.spc_root ~ ('^PG_'||sr.v) "
      "      UNION ALL "
      "      SELECT 0, 'pg_default', "
      "             'base' AS dir, "
      "             'pgsql_tmp' AS sub "
      "    FROM pg_ls_dir('base') AS l "
      "    WHERE l='pgsql_tmp' "
      "     ) AS ls, "
      "     (SELECT generate_series(1,2) AS i) AS gs "
      "   WHERE ls.sub = 'pgsql_tmp') agg "
      "GROUP BY 1%s",
      opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "",
      opts->addtimestamp ? ",2" : "");
  }

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  for (row = 0; row < nrows; row++)
  {
    column = 1;

    /* getting new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    size += atol(PQgetvalue(res, row, column++));
    count += atol(PQgetvalue(res, row, column++));
  }

  /* printing the diff... */
  format(r_size, size, 10, opts->human_readable ? SIZE_UNIT : NO_UNIT);
  format(r_count, count, 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
  if (opts->addtimestamp)
  {
    if (ts == NULL)
    {
      PQclear(res);
      snprintf(sql, sizeof(sql), "SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS')");
      res = PQexec(conn, sql);
      /* check and deal with errors */
      if (!res || PQresultStatus(res) > 2)
      {
        pg_log_warning("query failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        pg_log_error("query was: %s", sql);
        exit(EXIT_FAILURE);
      }
      ts = PQgetvalue(res, 0, 0);
    }

    (void)printf(" %s  ", ts);
  }
  (void)printf(" %s    %s\n", r_size, r_count);

  /* cleanup */
  free(r_size);
  free(r_count);
  PQclear(res);
}

/*
 * Dump all wait event stats.
 */
void
print_pgstatwaitevent()
{
  char     sql[2*PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  char *ts = NULL;
  char *r_lwlock = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_lock = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_bufferpin = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_activity = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_client = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_extension = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_ipc = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_timeout = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_io = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_running = (char *)malloc(sizeof(char) * (10 + 1));
  char *r_all = (char *)malloc(sizeof(char) * (10 + 1));

  snprintf(sql, sizeof(sql),
    "SELECT %s"
    "  count(*) FILTER (WHERE wait_event_type='LWLock') AS LWLock, "
    "  count(*) FILTER (WHERE wait_event_type='Lock') AS Lock, "
    "  count(*) FILTER (WHERE wait_event_type='BufferPin') AS BufferPin, "
    "  count(*) FILTER (WHERE wait_event_type='Activity') AS Activity, "
    "  count(*) FILTER (WHERE wait_event_type='Client') AS Client, "
    "  count(*) FILTER (WHERE wait_event_type='Extension') AS Extension, "
    "  count(*) FILTER (WHERE wait_event_type='IPC') AS IPC, "
    "  count(*) FILTER (WHERE wait_event_type='Timeout') AS Timeout, "
    "  count(*) FILTER (WHERE wait_event_type='IO') AS IO, "
    "  count(*) FILTER (WHERE wait_event_type IS NULL) AS Running, "
    "  count(*) AS All "
    "FROM pg_stat_activity;",
    opts->addtimestamp ? "to_char(now(), 'YYYY-MM-DD HH24:MI:SS')," : "");

  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  column = 0;
  for (row = 0; row < nrows; row++)
  {
    /* printing new values */
    if (opts->addtimestamp)
    {
      ts = PQgetvalue(res, row, column++);
    }
    format(r_lwlock, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_lock, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_bufferpin, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_activity, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_client, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_extension, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_ipc, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_timeout, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_io, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_running, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);
    format(r_all, atoi(PQgetvalue(res, row, column++)), 10, opts->human_readable ? ALL_UNIT : NO_UNIT);

    if (opts->addtimestamp && ts != NULL)
    {
      (void)printf(" %s  ", ts);
    }
    (void)printf(" %s   %s    %s   %s %s    %s  %s  %s %s  %s %s\n",
      r_lwlock,
      r_lock,
      r_bufferpin,
      r_activity,
      r_client,
      r_extension,
      r_ipc,
      r_timeout,
      r_io,
      r_running,
      r_all
    );
  }

  /* cleanup */
  free(r_lwlock);
  free(r_lock);
  free(r_bufferpin);
  free(r_activity);
  free(r_client);
  free(r_extension);
  free(r_ipc);
  free(r_timeout);
  free(r_io);
  free(r_running);
  free(r_all);
  PQclear(res);
}

/*
 * Dump all pgBouncer pools stats.
 */
void
print_pgbouncerpools()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  long     cl_active = 0;
  long     cl_waiting = 0;
  long     sv_active = 0;
  long     sv_idle = 0;
  long     sv_used = 0;
  long     sv_tested = 0;
  long     sv_login = 0;
  long     maxwait = 0;

  /*
   * We cannot use a filter now, we need to get all rows.
   */
  snprintf(sql, sizeof(sql), "SHOW pools");
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    /* we don't use the first two columns */
    column = 2;

    /* getting new values */
    cl_active += atol(PQgetvalue(res, row, column++));
    cl_waiting += atol(PQgetvalue(res, row, column++));
    sv_active += atol(PQgetvalue(res, row, column++));
    sv_idle += atol(PQgetvalue(res, row, column++));
    sv_used += atol(PQgetvalue(res, row, column++));
    sv_tested += atol(PQgetvalue(res, row, column++));
    sv_login += atol(PQgetvalue(res, row, column++));
    maxwait += atol(PQgetvalue(res, row, column++));
  }

  /* printing the diff...
   * note that the first line will be the current value, rather than the diff */
  (void)printf(" %6ld   %6ld    %6ld  %6ld  %6ld  %6ld  %6ld    %6ld\n",
    cl_active,
    cl_waiting,
    sv_active,
    sv_idle,
    sv_used,
    sv_tested,
    sv_login,
    maxwait
    );

  /* cleanup */
  PQclear(res);
}

/*
 * Dump all pgBouncer stats.
 */
void
print_pgbouncerstats()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;
  int      nrows;
  int      row, column;

  long     total_request = 0;
  long     total_received = 0;
  long     total_sent = 0;
  long     total_query_time = 0;

  /*
   * We cannot use a filter now, we need to get all rows.
   */
  snprintf(sql, sizeof(sql), "SHOW stats");
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the number of fields */
  nrows = PQntuples(res);

  /* for each row, dump the information */
  /* this is stupid, a simple if would do the trick, but it will help for other cases */
  for (row = 0; row < nrows; row++)
  {
    /* we don't use the first column */
    column = 1;

    /* getting new values */
    total_request += atol(PQgetvalue(res, row, column++));
    total_received += atol(PQgetvalue(res, row, column++));
    total_sent += atol(PQgetvalue(res, row, column++));
    total_query_time += atol(PQgetvalue(res, row, column++));
  }

  /* printing the diff...
   * note that the first line will be the current value, rather than the diff */
  (void)printf("  %6ld    %6ld  %6ld      %6ld\n",
    total_request - previous_pgbouncerstats->total_request,
    total_received - previous_pgbouncerstats->total_received,
    total_sent - previous_pgbouncerstats->total_sent,
    total_query_time - previous_pgbouncerstats->total_query_time
    );

  /* setting the new old value */
  previous_pgbouncerstats->total_request = total_request;
  previous_pgbouncerstats->total_received = total_received;
  previous_pgbouncerstats->total_sent = total_sent;
  previous_pgbouncerstats->total_query_time = total_query_time;

  /* cleanup */
  PQclear(res);
}

/*
 * Fetch PostgreSQL major and minor numbers
 */
void
fetch_version()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;

  /* get the cluster version */
  snprintf(sql, sizeof(sql), "SELECT version()");

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the only row, and parse it to get major and minor numbers */
  sscanf(PQgetvalue(res, 0, 0), "%*s %d.%d", &(opts->major), &(opts->minor));

  /* print version */
  if (opts->verbose)
      printf("Detected release: %d.%d\n", opts->major, opts->minor);

  /* cleanup */
  PQclear(res);
}

/*
 * Fetch setting value
 */
char
*fetch_setting(char *name)
{
  char    sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult   *res;
  char       *setting;

  /* get the cluster version */
  snprintf(sql, sizeof(sql), "SELECT setting FROM pg_settings WHERE name='%s'", name);

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  /* get the only row as the setting value */
  setting = pg_strdup(PQgetvalue(res, 0, 0));

  /* print version */
  if (opts->verbose)
    printf("%s is set to %s\n", name, setting);

  /* cleanup */
  PQclear(res);

  return setting;
}

/*
 * Fetch pg_buffercache namespace
 */
void
fetch_pgbuffercache_namespace()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;

  /* get the pg_stat_statement installation schema */
  if (backend_minimum_version(9, 1))
  {
    snprintf(sql, sizeof(sql), "SELECT nspname FROM pg_extension e "
      "JOIN pg_namespace n ON e.extnamespace=n.oid "
      "WHERE extname='pg_buffercache'");
  }
  else
  {
    snprintf(sql, sizeof(sql), "SELECT nspname FROM pg_proc p "
      "JOIN pg_namespace n ON p.pronamespace=n.oid "
      "WHERE proname='pg_buffercache'");
  }

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  if (PQntuples(res) > 0)
  {
    /* get the only row, and parse it to get major and minor numbers */
    opts->namespace = pg_strdup(PQgetvalue(res, 0, 0));

    /* print version */
    if (opts->verbose)
      printf("pg_buffercache namespace: %s\n", opts->namespace);
  }

  /* cleanup */
  PQclear(res);
}

/*
 * Fetch pg_stat_statement namespace
 */
void
fetch_pgstatstatements_namespace()
{
  char     sql[PGSTAT_DEFAULT_STRING_SIZE];
  PGresult *res;

  /* get the pg_stat_statement installation schema */
  if (backend_minimum_version(9, 1))
  {
    snprintf(sql, sizeof(sql), "SELECT nspname FROM pg_extension e "
      "JOIN pg_namespace n ON e.extnamespace=n.oid "
      "WHERE extname='pg_stat_statements'");
  }
  else
  {
    snprintf(sql, sizeof(sql), "SELECT nspname FROM pg_proc p "
      "JOIN pg_namespace n ON p.pronamespace=n.oid "
      "WHERE proname='pg_stat_statements'");
  }

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_warning("query failed: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    pg_log_error("query was: %s", sql);
    exit(EXIT_FAILURE);
  }

  if (PQntuples(res) > 0)
  {
    /* get the only row, and parse it to get major and minor numbers */
    opts->namespace = pg_strdup(PQgetvalue(res, 0, 0));

    /* print version */
    if (opts->verbose)
      printf("pg_stat_statements namespace: %s\n", opts->namespace);
  }

  /* cleanup */
  PQclear(res);
}

/*
 * Compare given major and minor numbers to the one of the connected server
 */
bool
backend_minimum_version(int major, int minor)
{
  //return 13 > major || (13 == major && 0 >= minor);
  return opts->major > major || (opts->major == major && opts->minor >= minor);
}

/*
 * Print the right header according to the stats mode
 */
void
print_header(void)
{
  char header1[PGSTAT_DEFAULT_STRING_SIZE] = "";
  char header2[PGSTAT_DEFAULT_STRING_SIZE] = "";

  if (opts->addtimestamp)
  {
    strcat(header1, "----- timestamp ----- ");
    strcat(header2, "                      ");
  }

  switch(opts->stat)
  {
    case NONE:
      /* That shouldn't happen */
        break;
    case ARCHIVER:
      strcat(header1, "---- WAL counts ----");
      strcat(header2, " archived   failed");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case BGWRITER:
      strcat(header1, "-------------- buffers -------------");
      strcat(header2, "      clean       alloc  maxwritten");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case CHECKPOINTER:
      if (backend_minimum_version(17, 0))
      {
        strcat(header1, "----- checkpoints ----- --------- restartpoints --------- ----- time ----- - buffers -");
        strcat(header2, "     timed   requested       timed  requested       done    write    sync    written");
      }
      else if (backend_minimum_version(9, 2))
      {
        strcat(header1, "----- checkpoints ----- ----- time ----- - buffers -");
        strcat(header2, "     timed   requested    write    sync    written");
      }
      else
      {
        strcat(header1, "----- checkpoints ----- - buffers -");
        strcat(header2, "     timed   requested    written");
      }
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case CONNECTION:
      strcat(header1, " - total - active - lockwaiting - idle in transaction -  idle -");
      (void)printf("%s\n", header1);
      break;
    case DATABASE:
      if (opts->substat == NULL || strstr(opts->substat, "backends") != NULL)
      {
        strcat(header1, "- backends -");
        strcat(header2, "            ");
      }
      if (opts->substat == NULL || strstr(opts->substat, "xacts") != NULL)
      {
        strcat(header1, " ------ xacts ------");
        strcat(header2, "    commit rollback ");
      }
      if (opts->substat == NULL || strstr(opts->substat, "blocks") != NULL)
      {
        if (backend_minimum_version(9, 0))
        {
          strcat(header1, " ----------------------- blocks ----------------------");
          strcat(header2, "        read        hit hitratio read_time write_time ");
        }
        else
        {
          strcat(header1, " --------- blocks ---------");
          strcat(header2, "    read    hit hit ratio");
        }
      }
      if ((opts->substat == NULL || strstr(opts->substat, "tuples") != NULL) && backend_minimum_version(8, 3))
      {
        strcat(header1, " -------------- tuples --------------");
        strcat(header2, "     ret    fet    ins    upd    del ");
      }
      if ((opts->substat == NULL || strstr(opts->substat, "temp") != NULL) && backend_minimum_version(9, 2))
      {
        strcat(header1, " ----- temp -----");
        strcat(header2, "   files   bytes ");
      }
      if ((opts->substat == NULL || strstr(opts->substat, "session") != NULL) && backend_minimum_version(14, 0))
      {
        strcat(header1, " ------------------------------- session -------------------------------");
        strcat(header2, "     all_time active_time    iit_time numbers abandoned   fatal  killed ");
      }
      if ((opts->substat == NULL || strstr(opts->substat, "misc") != NULL) && backend_minimum_version(8, 4))
      {
        if (backend_minimum_version(12, 0))
        {
          strcat(header1, " ------------ misc -------------");
          strcat(header2, "  conflicts deadlocks checksums");
        }
        else if (backend_minimum_version(9, 2))
        {
          strcat(header1, " ------- misc --------");
          strcat(header2, "  conflicts deadlocks");
        }
        else
        {
          strcat(header1, " --- misc ---");
          strcat(header2, "   conflicts");
        }
      }
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case TABLE:
      if (backend_minimum_version(16, 0))
      {
        strcat(header1, "-- sequential -- ----- index ---- ------------------------------- tuples ------------------------------- -------------- maintenance --------------");
        strcat(header2, "   scan  tuples     scan  tuples      ins    upd    del hotupd newpageupd   live   dead analyze ins_vac   vacuum autovacuum analyze autoanalyze");
      }
      else if (backend_minimum_version(13, 0))
      {
        strcat(header1, "-- sequential -- ----- index ----- ------------------------- tuples ------------------------- -------------- maintenance --------------");
        strcat(header2, "   scan  tuples     scan  tuples      ins    upd    del hotupd   live   dead analyze ins_vac   vacuum autovacuum analyze autoanalyze");
      }
      else if (backend_minimum_version(9, 4))
      {
        strcat(header1, "-- sequential -- ----- index ---- ------------------------- tuples ------------------ -------------- maintenance ------------");
        strcat(header2, "   scan  tuples     scan  tuples      ins    upd    del hotupd   live   dead analyze   vacuum autovacuum analyze autoanalyze");
      }
      else if (backend_minimum_version(9, 1))
      {
        strcat(header1, "-- sequential -- ----- index ---- ------------------------- tuples ---------- -------------- maintenance ------------");
        strcat(header2, "   scan  tuples     scan  tuples      ins    upd    del hotupd   live   dead   vacuum autovacuum analyze autoanalyze");
      }
      else if (backend_minimum_version(8, 3))
      {
        strcat(header1, "-- sequential -- ----- index ---- ------------------------- tuples ----------");
        strcat(header2, "   scan  tuples     scan  tuples      ins    upd    del hotupd   live   dead");
      }
      else
      {
        strcat(header1, "-- sequential -- ----- index ---- ------- tuples -------");
        strcat(header2, "   scan  tuples     scan  tuples      ins    upd    del");
      }
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case TABLEIO:
      strcat(header1, "---- heap table ---- ---- toast table --- --- heap indexes --- --- toast indexes --");
      strcat(header2, "     read       hit       read       hit       read       hit       read       hit");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case INDEX:
      strcat(header1, "-- scan -- ------ tuples -----");
      strcat(header2, "                read    fetch");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case FUNCTION:
      strcat(header1, "-- count -- --------- time ---------");
      strcat(header2, "                  total        self");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case STATEMENT:
      if ((opts->substat == NULL || strstr(opts->substat, "plan") != NULL) && backend_minimum_version(13, 0))
      {
        strcat(header1, "------ plan ------");
        strcat(header2, "  plans      time ");
      }
      if (opts->substat == NULL || strstr(opts->substat, "exec") != NULL)
      {
        strcat(header1, " --------- exec ----------");
        strcat(header2, "   calls      time   rows ");
      }
      if (opts->substat == NULL || strstr(opts->substat, "shared") != NULL)
      {
        strcat(header1, " ----------- shared -----------");
        strcat(header2, "     hit   read  dirty written ");
      }
      if (opts->substat == NULL || strstr(opts->substat, "local") != NULL)
      {
        strcat(header1, "  ----------- local -----------");
        strcat(header2, "     hit   read  dirty written ");
      }
      if (opts->substat == NULL || strstr(opts->substat, "temp") != NULL)
      {
        strcat(header1, " ----- temp -----");
        strcat(header2, "    read written ");
      }
      if (opts->substat == NULL || strstr(opts->substat, "time") != NULL)
      {
        if (backend_minimum_version(17, 0))
        {
          strcat(header1, " ------------------------------- time ------------------------------");
          strcat(header2, "   shr read  shr written  loc read loc written  tmp read tmp written  ");
        }
        else if (backend_minimum_version(16, 0))
        {
          strcat(header1, " ------------------- time --------------------");
          strcat(header2, "       read   written    tmp read tmp written ");
        }
        else if (backend_minimum_version(13, 0))
        {
          strcat(header1, " -------- time --------");
          strcat(header2, "       read   written  ");
        }
      }
      if ((opts->substat == NULL || strstr(opts->substat, "wal") != NULL) && backend_minimum_version(13, 0))
      {
        strcat(header1, " ---------- wal ----------");
        strcat(header2, "   records    fpi  bytes");
      }
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case SLRU:
      strcat(header1, "    zeroed       hit      read   written    exists   flushes truncates");
      (void)printf("%s\n", header1);
      break;
    case WAL:
      strcat(header1, "    records        FPI      bytes buffers_full");
      if (!backend_minimum_version(18, 0))
      {
        strcat(header1, "     write       sync write_time  sync_time");
      }
      (void)printf("%s\n", header1);
      break;
    case IO:
      strcat(header1, "  reads  read_bytes  read_time  writes write_bytes write_time writebacks writeback_time extends extend_time   hits evictions    reuses fsyncs fsync_time");
      (void)printf("%s\n", header1);
      break;
    case BUFFERCACHE:
      strcat(header1, "----- used ------ ------ dirty -----");
      strcat(header2, "   total  percent     total  percent");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case DEADLIVE:
      strcat(header1, "       live        dead  percent");
      (void)printf("%s\n", header1);
      break;
    case XLOG:
    case REPSLOTS:
      strcat(header1, "-------- filename -------- -- location -- ---- bytes ----");
      (void)printf("%s\n", header1);
      break;
    case TEMPFILE:
      strcat(header1, "--- size --- --- count ---");
      (void)printf("%s\n", header1);
      break;
    case WAITEVENT:
      strcat(header1, "--- LWLock ------- Lock --- BufferPin --- Activity --- Client --- Extension ------- IPC --- Timeout ------- IO --- Running ------ All ---");
      (void)printf("%s\n", header1);
      break;
    case PROGRESS_ANALYZE:
      strcat(header1, "--------------------- object --------------------- ---------- phase ---------- ---------------- stats --------------- -- time elapsed --");
      strcat(header2, " database         relation              size                                    %%sample blocks  %%ext stats  %%child tables\n");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case PROGRESS_BASEBACKUP:
      strcat(header1, "--- pid --- ---------- phase ---------- ---------------------- stats -------------------- -- time elapsed --");
      strcat(header2, "                                         Sent size - Total size - %%Sent - %%Tablespaces");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case PROGRESS_CLUSTER:
      strcat(header1, "--------------------------- object -------------------------- -------------------- phase -------------------- ------------------- stats ------------------- -- time elapsed --");
      strcat(header2, " database         table                 index                                                                  tuples scanned  tuples written  %%blocks  index rebuilt");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case PROGRESS_COPY:
      strcat(header1, "----------------- object ---------------- -------------------- phase -------------------- --------- bytes --------- ------- tuples -------- -- time elapsed --");
      strcat(header2, " database         table                     command                  type                   processed       total    processed    excluded");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case PROGRESS_CREATEINDEX:
      strcat(header1, "--------------------------- object -------------------------- -------------------- phase -------------------- ------------------- stats ------------------- -- time elapsed --");
      strcat(header2, " database         table                 index                                                                  %%lockers  %%blocks  %%tuples  %%partitions");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case PROGRESS_VACUUM:
      strcat(header1, "--------------------- object --------------------- ---------- phase ---------- ---------------- stats --------------- -- time elapsed --");
      strcat(header2, " database         relation              size                                    %%scan  %%vacuum  #index  %%dead tuple");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case PBPOOLS:
      strcat(header1, "---- client -----  ---------------- server ----------------  -- misc --");
      strcat(header2, " active  waiting    active    idle    used  tested   login    maxwait");
      (void)printf("%s\n%s\n", header1, header2);
      break;
    case PBSTATS:
      strcat(header1, "---------------- total -----------------");
      strcat(header2, " request  received  sent    query time");
      (void)printf("%s\n%s\n", header1, header2);
      break;
  }

  if (wresized != 0)
    doresize();
  if (opts->dontredisplayheader)
    hdrcnt = 0;
  else
    hdrcnt = winlines;
}

/*
 * Call the right function according to the stats mode
 */
void
print_line(void)
{
  switch(opts->stat)
  {
    case NONE:
      /* That shouldn't happen */
      break;
    case ARCHIVER:
      print_pgstatarchiver();
      break;
    case BGWRITER:
      print_pgstatbgwriter();
      break;
    case CHECKPOINTER:
      print_pgstatcheckpointer();
      break;
    case CONNECTION:
      print_pgstatconnection();
      break;
    case DATABASE:
      print_pgstatdatabase();
      break;
    case TABLE:
      print_pgstattable();
      break;
    case TABLEIO:
      print_pgstattableio();
      break;
    case INDEX:
      print_pgstatindex();
      break;
    case FUNCTION:
      print_pgstatfunction();
      break;
    case STATEMENT:
      print_pgstatstatement();
      break;
    case SLRU:
      print_pgstatslru();
      break;
    case WAL:
      print_pgstatwal();
      break;
    case IO:
      print_pgstatio();
      break;
    case BUFFERCACHE:
      print_buffercache();
      break;
    case XLOG:
      print_xlogstats();
      break;
    case DEADLIVE:
      print_deadlivestats();
      break;
    case REPSLOTS:
      print_repslotsstats();
      break;
    case PROGRESS_ANALYZE:
      print_pgstatprogressanalyze();
      break;
    case PROGRESS_BASEBACKUP:
      print_pgstatprogressbasebackup();
      break;
    case PROGRESS_CLUSTER:
      print_pgstatprogresscluster();
      break;
    case PROGRESS_COPY:
      print_pgstatprogresscopy();
      break;
    case PROGRESS_CREATEINDEX:
      print_pgstatprogresscreateindex();
      break;
    case PROGRESS_VACUUM:
      print_pgstatprogressvacuum();
      break;
    case TEMPFILE:
      print_tempfilestats();
      break;
    case WAITEVENT:
      print_pgstatwaitevent();
      break;
    case PBPOOLS:
      print_pgbouncerpools();
      break;
    case PBSTATS:
      print_pgbouncerstats();
      break;
  }
}

/*
 * Allocate and initialize the right statistics struct according to the stats mode
 */
void
allocate_struct(void)
{
  switch (opts->stat)
  {
    case NONE:
      /* That shouldn't happen */
      break;
    case ARCHIVER:
      previous_pgstatarchiver = (struct pgstatarchiver *) pg_malloc(sizeof(struct pgstatarchiver));
      previous_pgstatarchiver->archived_count = 0;
      previous_pgstatarchiver->failed_count = 0;
      previous_pgstatarchiver->stats_reset = PGSTAT_OLDEST_STAT_RESET;
      break;
    case BGWRITER:
      previous_pgstatbgwriter = (struct pgstatbgwriter *) pg_malloc(sizeof(struct pgstatbgwriter));
      previous_pgstatbgwriter->buffers_clean = 0;
      previous_pgstatbgwriter->maxwritten_clean = 0;
      previous_pgstatbgwriter->buffers_alloc = 0;
      previous_pgstatbgwriter->stats_reset = PGSTAT_OLDEST_STAT_RESET;
      break;
    case CHECKPOINTER:
      previous_pgstatcheckpointer = (struct pgstatcheckpointer *) pg_malloc(sizeof(struct pgstatcheckpointer));
      previous_pgstatcheckpointer->checkpoints_timed = 0;
      previous_pgstatcheckpointer->checkpoints_requested = 0;
      previous_pgstatcheckpointer->restartpoints_timed = 0;
      previous_pgstatcheckpointer->restartpoints_requested = 0;
      previous_pgstatcheckpointer->restartpoints_timed = 0;
      previous_pgstatcheckpointer->write_time = 0;
      previous_pgstatcheckpointer->sync_time = 0;
      previous_pgstatcheckpointer->buffers_written = 0;
      previous_pgstatcheckpointer->stats_reset = PGSTAT_OLDEST_STAT_RESET;
      break;
    case CONNECTION:
      // nothing to do
      break;
    case DATABASE:
      previous_pgstatdatabase = (struct pgstatdatabase *) pg_malloc(sizeof(struct pgstatdatabase));
      previous_pgstatdatabase->xact_commit = 0;
      previous_pgstatdatabase->xact_rollback = 0;
      previous_pgstatdatabase->blks_read = 0;
      previous_pgstatdatabase->blks_hit = 0;
      previous_pgstatdatabase->tup_returned = 0;
      previous_pgstatdatabase->tup_fetched = 0;
      previous_pgstatdatabase->tup_inserted = 0;
      previous_pgstatdatabase->tup_updated = 0;
      previous_pgstatdatabase->tup_deleted = 0;
      previous_pgstatdatabase->conflicts = 0;
      previous_pgstatdatabase->temp_files = 0;
      previous_pgstatdatabase->temp_bytes = 0;
      previous_pgstatdatabase->deadlocks = 0;
      previous_pgstatdatabase->checksum_failures = 0;
      previous_pgstatdatabase->blk_read_time = 0;
      previous_pgstatdatabase->blk_write_time = 0;
      previous_pgstatdatabase->session_time = 0;
      previous_pgstatdatabase->active_time = 0;
      previous_pgstatdatabase->idle_in_transaction_time = 0;
      previous_pgstatdatabase->sessions = 0;
      previous_pgstatdatabase->sessions_abandoned = 0;
      previous_pgstatdatabase->sessions_fatal = 0;
      previous_pgstatdatabase->sessions_killed = 0;
      previous_pgstatdatabase->stats_reset = PGSTAT_OLDEST_STAT_RESET;
      break;
    case TABLE:
      previous_pgstattable = (struct pgstattable *) pg_malloc(sizeof(struct pgstattable));
      previous_pgstattable->seq_scan = 0;
      previous_pgstattable->seq_tup_read = 0;
      previous_pgstattable->idx_scan = 0;
      previous_pgstattable->idx_tup_fetch = 0;
      previous_pgstattable->n_tup_ins = 0;
      previous_pgstattable->n_tup_upd = 0;
      previous_pgstattable->n_tup_del = 0;
      previous_pgstattable->n_tup_hot_upd = 0;
      previous_pgstattable->n_tup_newpage_upd = 0;
      previous_pgstattable->n_live_tup = 0;
      previous_pgstattable->n_dead_tup = 0;
      previous_pgstattable->n_mod_since_analyze = 0;
      previous_pgstattable->n_ins_since_vacuum = 0;
      previous_pgstattable->vacuum_count = 0;
      previous_pgstattable->autovacuum_count = 0;
      previous_pgstattable->analyze_count = 0;
      previous_pgstattable->autoanalyze_count = 0;
      break;
    case TABLEIO:
      previous_pgstattableio = (struct pgstattableio *) pg_malloc(sizeof(struct pgstattableio));
      previous_pgstattableio->heap_blks_read = 0;
      previous_pgstattableio->heap_blks_hit = 0;
      previous_pgstattableio->idx_blks_read = 0;
      previous_pgstattableio->idx_blks_hit = 0;
      previous_pgstattableio->toast_blks_read = 0;
      previous_pgstattableio->toast_blks_hit = 0;
      previous_pgstattableio->tidx_blks_read = 0;
      previous_pgstattableio->tidx_blks_hit = 0;
      break;
    case INDEX:
      previous_pgstatindex = (struct pgstatindex *) pg_malloc(sizeof(struct pgstatindex));
      previous_pgstatindex->idx_scan = 0;
      previous_pgstatindex->idx_tup_read = 0;
      previous_pgstatindex->idx_tup_fetch = 0;
      break;
    case FUNCTION:
      previous_pgstatfunction = (struct pgstatfunction *) pg_malloc(sizeof(struct pgstatfunction));
      previous_pgstatfunction->calls = 0;
      previous_pgstatfunction->total_time = 0;
      previous_pgstatfunction->self_time = 0;
      break;
    case STATEMENT:
      previous_pgstatstatement = (struct pgstatstatement *) pg_malloc(sizeof(struct pgstatstatement));
      previous_pgstatstatement->plans = 0;
      previous_pgstatstatement->total_plan_time = 0;
      previous_pgstatstatement->calls = 0;
      previous_pgstatstatement->total_exec_time = 0;
      previous_pgstatstatement->rows = 0;
      previous_pgstatstatement->shared_blks_hit = 0;
      previous_pgstatstatement->shared_blks_read = 0;
      previous_pgstatstatement->shared_blks_dirtied = 0;
      previous_pgstatstatement->shared_blks_written = 0;
      previous_pgstatstatement->local_blks_hit = 0;
      previous_pgstatstatement->local_blks_read = 0;
      previous_pgstatstatement->local_blks_dirtied = 0;
      previous_pgstatstatement->local_blks_written = 0;
      previous_pgstatstatement->temp_blks_read = 0;
      previous_pgstatstatement->temp_blks_written = 0;
      previous_pgstatstatement->shared_blk_read_time = 0;
      previous_pgstatstatement->shared_blk_write_time = 0;
      previous_pgstatstatement->wal_records = 0;
      previous_pgstatstatement->wal_fpi = 0;
      previous_pgstatstatement->wal_bytes = 0;
      break;
    case SLRU:
      previous_pgstatslru = (struct pgstatslru *) pg_malloc(sizeof(struct pgstatslru));
      previous_pgstatslru->blks_zeroed = 0;
      previous_pgstatslru->blks_hit = 0;
      previous_pgstatslru->blks_read = 0;
      previous_pgstatslru->blks_written = 0;
      previous_pgstatslru->blks_exists = 0;
      previous_pgstatslru->flushes = 0;
      previous_pgstatslru->truncates = 0;
      previous_pgstatslru->stats_reset = PGSTAT_OLDEST_STAT_RESET;
      break;
    case WAL:
      previous_pgstatwal = (struct pgstatwal *) pg_malloc(sizeof(struct pgstatwal));
      previous_pgstatwal->wal_records = 0;
      previous_pgstatwal->wal_fpi = 0;
      previous_pgstatwal->wal_bytes = 0;
      previous_pgstatwal->wal_buffers_full = 0;
      previous_pgstatwal->wal_write = 0;
      previous_pgstatwal->wal_sync = 0;
      previous_pgstatwal->wal_write_time = 0;
      previous_pgstatwal->wal_sync_time = 0;
      previous_pgstatwal->stats_reset = PGSTAT_OLDEST_STAT_RESET;
      break;
    case IO:
      previous_pgstatio = (struct pgstatio *) pg_malloc(sizeof(struct pgstatio));
      previous_pgstatio->reads = 0;
      previous_pgstatio->read_time = 0;
      previous_pgstatio->writes = 0;
      previous_pgstatio->write_time = 0;
      previous_pgstatio->writebacks = 0;
      previous_pgstatio->writeback_time = 0;
      previous_pgstatio->extends = 0;
      previous_pgstatio->extend_time = 0;
      previous_pgstatio->op_bytes = 0;
      previous_pgstatio->hits = 0;
      previous_pgstatio->evictions = 0;
      previous_pgstatio->reuses = 0;
      previous_pgstatio->fsyncs = 0;
      previous_pgstatio->fsync_time = 0;
      previous_pgstatio->stats_reset = PGSTAT_OLDEST_STAT_RESET;
      break;
    case XLOG:
      previous_xlogstats = (struct xlogstats *) pg_malloc(sizeof(struct xlogstats));
      previous_xlogstats->location = pg_strdup("0/0");
      previous_xlogstats->locationdiff = 0;
      break;
    case DEADLIVE:
      previous_deadlivestats = (struct deadlivestats *) pg_malloc(sizeof(struct deadlivestats));
      previous_deadlivestats->live = 0;
      previous_deadlivestats->dead = 0;
      break;
    case REPSLOTS:
      previous_repslots = (struct repslots *) pg_malloc(sizeof(struct repslots));
      previous_repslots->restartlsn = pg_strdup("0/0");
      previous_repslots->restartlsndiff = 0;
      break;
    case BUFFERCACHE:
    case TEMPFILE:
    case WAITEVENT:
    case PROGRESS_ANALYZE:
    case PROGRESS_BASEBACKUP:
    case PROGRESS_CLUSTER:
    case PROGRESS_COPY:
    case PROGRESS_CREATEINDEX:
    case PROGRESS_VACUUM:
    case PBPOOLS:
      // no initialization worth doing...
      break;
    case PBSTATS:
      previous_pgbouncerstats = (struct pgbouncerstats *) pg_malloc(sizeof(struct pgbouncerstats));
      previous_pgbouncerstats->total_request = 0;
      previous_pgbouncerstats->total_received = 0;
      previous_pgbouncerstats->total_sent = 0;
      previous_pgbouncerstats->total_query_time = 0;
      break;
  }
}

/*
 * Force a header to be prepended to the next output.
 */
static void
needhdr(int dummy)
{
  hdrcnt = 1;
}

/*
 * When the terminal is resized, force an update of the maximum number of rows
 * printed between each header repetition.  Then force a new header to be
 * prepended to the next output.
 */
void
needresize(int signo)
{
  wresized = 1;
}

/*
 * Update the global `winlines' count of terminal rows.
 */
void
doresize(void)
{
  int status;
  struct winsize w;

  for (;;) {
    status = ioctl(fileno(stdout), TIOCGWINSZ, &w);
    if (status == -1 && errno == EINTR)
      continue;
    else if (status == -1)
      pg_log_error("ioctl");
    if (w.ws_row > 3)
      winlines = w.ws_row - 3;
    else
      winlines = PGSTAT_DEFAULT_LINES;
    break;
  }

  /*
   * Inhibit doresize() calls until we are rescheduled by SIGWINCH.
   */
  wresized = 0;
  hdrcnt = 1;
}

/*
 * Close the PostgreSQL connection, and quit
 */
static void
quit_properly(SIGNAL_ARGS)
{
  PQfinish(conn);
  exit(EXIT_FAILURE);
}

/*
 * Main function
 */
int
main(int argc, char **argv)
{
  const char *progname;
  ConnParams cparams;

  /*
   * If the user stops the program (control-Z) and then resumes it,
   * print out the header again.
   */
  pqsignal(SIGCONT, needhdr);
  pqsignal(SIGINT, quit_properly);

  /*
   * If our standard output is a tty, then install a SIGWINCH handler
   * and set wresized so that our first iteration through the main
   * pgstat loop will peek at the terminal's current rows to find out
   * how many lines can fit in a screenful of output.
   */
  if (isatty(fileno(stdout)) != 0) {
    wresized = 1;
    (void)signal(SIGWINCH, needresize);
  } else {
    wresized = 0;
    winlines = PGSTAT_DEFAULT_LINES;
  }

  /* Initialize the logging interface */
  pg_logging_init(argv[0]);

  /* Get the program name */
  progname = get_progname(argv[0]);

  /* Allocate the options struct */
  opts = (struct options *) pg_malloc(sizeof(struct options));

  /* Parse the options */
  get_opts(argc, argv);

  /* Set the connection struct */
  cparams.pghost = opts->hostname;
  cparams.pgport = opts->port;
  cparams.pguser = opts->username;
  cparams.dbname = opts->dbname;
  cparams.prompt_password = TRI_DEFAULT;
  cparams.override_dbname = NULL;

  /* Connect to the database */
  conn = connectDatabase(&cparams, progname, false, false, false);

  /* Get PostgreSQL version
   * (if we are not connected to the pseudo pgBouncer database)
   */
  if (opts->stat != PBPOOLS && opts->stat != PBSTATS)
  {
    fetch_version();
  }

  /* Without the -s option, defaults to the bgwriter statistics */
  if (opts->stat == NONE)
  {
    opts->stat = BGWRITER;
  }

  /* Check if the release number matches the statistics */
  if ((opts->stat == CONNECTION || opts->stat == XLOG) && !backend_minimum_version(9, 2))
  {
    PQfinish(conn);
    pg_log_error("You need at least v9.2 for this statistic.");
    exit(EXIT_FAILURE);
  }

  if (opts->stat == ARCHIVER && !backend_minimum_version(9, 4))
  {
    PQfinish(conn);
    pg_log_error("You need at least v9.4 for this statistic.");
    exit(EXIT_FAILURE);
  }

  if ((opts->stat == PROGRESS_VACUUM || opts->stat == WAITEVENT) && !backend_minimum_version(9, 6))
  {
    PQfinish(conn);
    pg_log_error("You need at least v9.6 for this statistic.");
    exit(EXIT_FAILURE);
  }

  if ((opts->stat == PROGRESS_CREATEINDEX || opts->stat == PROGRESS_CLUSTER) && !backend_minimum_version(12, 0))
  {
    PQfinish(conn);
    pg_log_error("You need at least v12 for this statistic.");
    exit(EXIT_FAILURE);
  }

  if ((opts->stat == PROGRESS_ANALYZE || opts->stat == PROGRESS_BASEBACKUP|| opts->stat == SLRU)
    && !backend_minimum_version(13, 0))
  {
    PQfinish(conn);
    pg_log_error("You need at least v13 for this statistic.");
    exit(EXIT_FAILURE);
  }

  if ((opts->stat == WAL || opts->stat == PROGRESS_COPY) && !backend_minimum_version(14, 0))
  {
    PQfinish(conn);
    pg_log_error("You need at least v14 for this statistic.");
    exit(EXIT_FAILURE);
  }

  if (opts->stat == IO && !backend_minimum_version(15, 0))
  {
    PQfinish(conn);
    pg_log_error("You need at least v15 for this statistic.");
    exit(EXIT_FAILURE);
  }

  /* Check if the configuration matches the statistics */
  if (opts->stat == FUNCTION)
  {
    if (strcmp(fetch_setting("track_functions"), "none") == 0)
    {
      PQfinish(conn);
      pg_log_error("track_functions is set to \"none\".");
      exit(EXIT_FAILURE);
    }
  }

  if (opts->stat == STATEMENT)
  {
    fetch_pgstatstatements_namespace();
    if (!opts->namespace)
    {
      PQfinish(conn);
      pg_log_error("Cannot find the pg_stat_statements extension.");
      exit(EXIT_FAILURE);
    }
  }

  if (opts->stat == BUFFERCACHE)
  {
    fetch_pgbuffercache_namespace();
    if (!opts->namespace)
    {
      PQfinish(conn);
      pg_log_error("Cannot find the pg_buffercache extension.");
      exit(EXIT_FAILURE);
    }
  }

  /* Filter required for replication slots */
  if (opts->stat == REPSLOTS && !opts->filter)
  {
    PQfinish(conn);
    pg_log_error("You need to specify a replication slot with -f for this statistic.");
    exit(EXIT_FAILURE);
  }

  /* Allocate and initialize statistics struct */
  allocate_struct();

  /* Grab cluster stats info */
  for (hdrcnt = 1;;) {
    if (!--hdrcnt)
      print_header();

    print_line();

    (void)fflush(stdout);

    if (--opts->count == 0)
      break;

    (void)usleep(opts->interval * 1000000);
  }

  PQfinish(conn);
  return 0;
}
