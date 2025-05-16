/*
 * pgwaitevent, a PostgreSQL app to gather statistical informations
 * on wait events of PostgreSQL PID backend.
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Guillaume Lelarge, guillaume@lelarge.info, 2019-2025.
 *
 * pgstats/pgwaitevent.c
 */


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
#define PGWAITEVENT_VERSION "1.4.0"
#define  PGWAITEVENT_DEFAULT_LINES 20
#define  PGWAITEVENT_DEFAULT_STRING_SIZE 2048


/*
 * Structs
 */

/* these are the options structure for command line parameters */
struct options
{
  /* misc */
  bool  verbose;

  /* connection parameters */
  char  *dbname;
  char  *hostname;
  char  *port;
  char  *username;

  /* version number */
  int   major;
  int   minor;

  /* pid */
  int   pid;

  /* include leader and workers PIDs */
  bool  includeleaderworkers;

  /* frequency */
  float interval;

  /* query and trace timestamps */
  char  *query_start;
  char  *trace_start;
};


/*
 * Global variables
 */
static PGconn         *conn;
static struct options *opts;
extern char           *optarg;


/*
 * Function prototypes
 */
static void help(const char *progname);
void        get_opts(int, char **);
#ifndef FE_MEMUTILS_H
void        *pg_malloc(size_t size);
char        *pg_strdup(const char *in);
#endif
void        fetch_version(void);
bool        backend_minimum_version(int major, int minor);
void        build_env(void);
bool        active_session(void);
void        handle_current_query(void);
void        drop_env(void);
static void quit_properly(SIGNAL_ARGS);


/*
 * Print help message
 */
static void
help(const char *progname)
{
  printf("%s gathers every wait events from a specific PID, grouping them by queries.\n\n"
    "Usage:\n"
    "  %s [OPTIONS] PID\n"
    "\nGeneral options:\n"
    "  -g                     include leader and workers (parallel queries) [v13+]\n"
    "  -i                     interval (default is 1s)\n"
    "  -v                     verbose\n"
    "  -?|--help              show this help, then exit\n"
    "  -V|--version           output version information, then exit\n"
    "\nConnection options:\n"
    "  -h HOSTNAME            database server host or socket directory\n"
    "  -p PORT                database server port number\n"
    "  -U USER                connect as specified database user\n"
    "  -d DBNAME              database to connect to\n\n"
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
  opts->dbname = NULL;
  opts->hostname = NULL;
  opts->port = NULL;
  opts->username = NULL;
  opts->pid = 0;
  opts->includeleaderworkers = false;
  opts->interval = 1;

  /* we should deal quickly with help and version */
  if (argc > 1)
  {
    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
    {
      help(progname);
      exit(0);
    }
    if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
    {
      puts("pgwaitevent " PGWAITEVENT_VERSION " (compiled with PostgreSQL " PG_VERSION ")");
      exit(0);
    }
  }

  /* get options */
  while ((c = getopt(argc, argv, "h:p:U:d:i:gv")) != -1)
  {
    switch (c)
    {
        /* specify the database */
      case 'd':
        opts->dbname = pg_strdup(optarg);
        break;

        /* host to connect to */
      case 'h':
        opts->hostname = pg_strdup(optarg);
        break;

        /* parallel queries */
      case 'g':
        opts->includeleaderworkers = true;
        break;

        /* interval */
      case 'i':
        opts->interval = atof(optarg);
        break;

        /* port to connect to on remote host */
      case 'p':
        opts->port = pg_strdup(optarg);
        break;

        /* username */
      case 'U':
        opts->username = pg_strdup(optarg);
        break;

        /* get verbose */
      case 'v':
        opts->verbose = true;
        break;

      default:
        pg_log_error("Try \"%s --help\" for more information.\n", progname);
        exit(EXIT_FAILURE);
    }
  }

  /* get PID to monitor */
  if (optind < argc)
  {
    opts->pid = atoi(argv[optind]);
  }
  else
  {
    pg_log_error("PID required.\n");
    pg_log_info("Try \"%s --help\" for more information.\n", progname);
    exit(EXIT_FAILURE);
  }

  /* set dbname if unset */
  if (opts->dbname == NULL)
  {
    /*
     * We want to use dbname for possible error reports later,
     * and in case someone has set and is using PGDATABASE
     * in its environment, preserve that name for later usage
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
 * Fetch PostgreSQL major and minor numbers
 */
void
fetch_version()
{
  char     sql[PGWAITEVENT_DEFAULT_STRING_SIZE];
  PGresult *res;

  /* get the cluster version */
  snprintf(sql, sizeof(sql), "SELECT version()");

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_error("query failed: %s", PQerrorMessage(conn));
    pg_log_info("query was: %s", sql);
    PQclear(res);
    PQfinish(conn);
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
 * Compare given major and minor numbers to the one of the connected server
 */
bool
backend_minimum_version(int major, int minor)
{
  return opts->major > major || (opts->major == major && opts->minor >= minor);
}


/*
 * Close the PostgreSQL connection, and quit
 */
static void
quit_properly(SIGNAL_ARGS)
{
  drop_env();
  PQfinish(conn);
  exit(EXIT_FAILURE);
}


/*
 * Create function
 */
void
build_env()
{
  char     sql[PGWAITEVENT_DEFAULT_STRING_SIZE];
  PGresult *res;

  /* build the DDL query */
  snprintf(sql, sizeof(sql),
"CREATE TEMPORARY TABLE waitevents (we text, wet text, o integer);\n"
"ALTER TABLE waitevents ADD UNIQUE(we, wet);\n");

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_error("query failed: %s", PQerrorMessage(conn));
    pg_log_info("query was: %s", sql);
    PQclear(res);
    PQfinish(conn);
    exit(EXIT_FAILURE);
  }

  /* print verbose */
  if (opts->verbose)
    printf("Temporary table created\n");

  /* cleanup */
  PQclear(res);

  /* build the DDL query */
  snprintf(sql, sizeof(sql), "CREATE SCHEMA pgwaitevent;");

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_error("query failed: %s", PQerrorMessage(conn));
    pg_log_info("query was: %s", sql);
    PQclear(res);
    PQfinish(conn);
    exit(EXIT_FAILURE);
  }

  /* print verbose */
  if (opts->verbose)
    printf("Schema created\n");

  /* build the DDL query */
  snprintf(sql, sizeof(sql),
"CREATE OR REPLACE FUNCTION pgwaitevent.trace_wait_events_for_pid(p integer, leader boolean, s numeric default 1)\n"
"RETURNS TABLE (wait_event text, wait_event_type text, occurences integer, percent numeric(5,2))\n"
"LANGUAGE plpgsql\n"
"AS $$\n"
"DECLARE\n"
"  q text;\n"
"  r record;\n"
"BEGIN\n"
"  -- check it is a backend\n"
"  SELECT query INTO q FROM pg_stat_activity\n"
"  WHERE pid=p AND backend_type='client backend' AND state='active';\n"
"\n"
"  IF NOT FOUND THEN\n"
"    RAISE EXCEPTION 'PID %% doesn''t appear to be an active backend', p\n"
"      USING HINT = 'Check the PID and its state';\n"
"  END IF;\n"
"\n"
"  -- logging\n"
"  RAISE LOG 'Tracing PID %%, sampling at %%s', p, s;\n"
"  RAISE LOG 'Query is <%%>', q;\n"
"\n"
"  -- drop if exists, then create temp table\n"
"  TRUNCATE waitevents;\n"
"\n"
"  -- loop till the end of the query\n"
"  LOOP\n"
"    -- get wait event\n"
"    IF leader THEN\n"
"      SELECT COALESCE(psa.wait_event, '[Running]') AS wait_event,\n"
"           COALESCE(psa.wait_event_type, '')   AS wait_event_type\n"
"      INTO   r\n"
"      FROM   pg_stat_activity psa\n"
"      WHERE  pid=p OR leader_pid=p;\n"
"    ELSE\n"
"      SELECT COALESCE(psa.wait_event, '[Running]') AS wait_event,\n"
"           COALESCE(psa.wait_event_type, '')   AS wait_event_type\n"
"      INTO   r\n"
"      FROM   pg_stat_activity psa\n"
"      WHERE  pid=p;\n"
"    END IF;\n"
"\n"
"    -- loop control\n"
"    EXIT WHEN r.wait_event = 'ClientRead';\n"
"\n"
"    -- update wait events stats\n"
"    INSERT INTO waitevents VALUES (r.wait_event, r.wait_event_type, 1)\n"
"      ON CONFLICT (we,wet) DO UPDATE SET o = waitevents.o+1;\n"
"\n"
"    -- sleep a bit\n"
"    PERFORM pg_sleep(s);\n"
"  END LOOP;\n"
"\n"
"  -- return stats\n"
"  RETURN QUERY\n"
"    SELECT we, wet, o, (o*100./sum(o) over ())::numeric(5,2)\n"
"    FROM waitevents\n"
"    ORDER BY o DESC;\n"
"END\n"
"$$;\n");

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_error("query failed: %s", PQerrorMessage(conn));
    pg_log_info("query was: %s", sql);
    PQclear(res);
    PQfinish(conn);
    exit(EXIT_FAILURE);
  }

  /* print verbose */
  if (opts->verbose)
    printf("Function created\n");

  /* cleanup */
  PQclear(res);
}


/*
 * Is PID an active session?
 */
bool
active_session()
{
  char     sql[PGWAITEVENT_DEFAULT_STRING_SIZE];
  PGresult *res;
  bool     active = false;

  /* build the query */
  snprintf(sql, sizeof(sql),
    "SELECT state, query, query_start, now() FROM pg_stat_activity\n"
    "WHERE backend_type='client backend'\n"
    "AND pid=%d",
    opts->pid);

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_error("query failed: %s", PQerrorMessage(conn));
    pg_log_info("query was: %s", sql);
    PQclear(res);
    PQfinish(conn);
    exit(EXIT_FAILURE);
  }

  /* if zero row, then PID is gone */
  if (PQntuples(res) == 0)
  {
    printf("\nNo more session with PID %d, exiting...\n", opts->pid);
    drop_env();
    PQfinish(conn);
    exit(2);
  }

  /* if one row, we found the good one */
  if (PQntuples(res) == 1)
  {
    if (!strncmp(PQgetvalue(res, 0, 0), "active", 6))
    {
      active = true;
      printf("\nNew query: %s\n", PQgetvalue(res, 0, 1));
      opts->query_start = pg_strdup(PQgetvalue(res, 0, 2));
      opts->trace_start = pg_strdup(PQgetvalue(res, 0, 3));
    }
  }

  /* this also means that in case of multiple rows, we treat it as no rows */

  /* cleanup */
  PQclear(res);

  return active;
}


/*
 * Handle query
 */
void
handle_current_query()
{
  char     sql[PGWAITEVENT_DEFAULT_STRING_SIZE];
  PGresult *workers_res;
  PGresult *trace_res;
  PGresult *duration_res;
  int      nrows;
  int      row;
  int      nworkers = 0;

  if (opts->includeleaderworkers)
  {
    /* build the workers query if the user asked to include leader and workers */
    snprintf(sql, sizeof(sql), "SELECT count(*) FROM pg_stat_activity "
                               "WHERE pid=%d OR leader_pid=%d",
                               opts->pid, opts->pid);

    /* execute it */
    workers_res = PQexec(conn, sql);

    /* check and deal with errors */
    if (!workers_res || PQresultStatus(workers_res) > 2)
    {
      pg_log_error("query failed: %s", PQerrorMessage(conn));
      pg_log_info("query was: %s", sql);
      PQclear(workers_res);
      PQfinish(conn);
      exit(EXIT_FAILURE);
    }

    /* get the number of leader and workers */
    nworkers = atoi(PQgetvalue(workers_res, 0, 0));

    /* clean up */
    PQclear(workers_res);
  }

  /* build the trace query */
  snprintf(sql, sizeof(sql), "SELECT * FROM pgwaitevent.trace_wait_events_for_pid(%d, %s, %f);",
    opts->pid, opts->includeleaderworkers ? "'t'" : "'f'", opts->interval);

  /* execute it */
  trace_res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!trace_res || PQresultStatus(trace_res) > 2)
  {
    pg_log_error("query failed: %s", PQerrorMessage(conn));
    pg_log_info("query was: %s", sql);
    PQclear(trace_res);
    PQfinish(conn);
    exit(EXIT_FAILURE);
  }

  /* build the duration query */
  snprintf(sql, sizeof(sql), "SELECT now()-'%s'::timestamptz, now()-'%s'::timestamptz;",
    opts->query_start, opts->trace_start);

  /* execute it */
  duration_res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!duration_res || PQresultStatus(duration_res) > 2)
  {
    pg_log_error("query failed: %s", PQerrorMessage(conn));
    pg_log_info("query was: %s", sql);
    PQclear(duration_res);
    PQfinish(conn);
    exit(EXIT_FAILURE);
  }

  /* show durations */
  (void)printf("Query duration: %s\n", PQgetvalue(duration_res, 0, 0));
  (void)printf("Trace duration: %s\n", PQgetvalue(duration_res, 0, 1));

  /* show number of workers */
  if (opts->includeleaderworkers)
  {
    (void)printf("Number of processes: %d\n", nworkers);
  }

  /* get the number of rows */
  nrows = PQntuples(trace_res);

  /* print headers */
  (void)printf(
"┌───────────────────────────────────┬───────────┬────────────┬─────────┐\n"
"│ Wait event                        │ WE type   │ Occurences │ Percent │\n"
"├───────────────────────────────────┼───────────┼────────────┼─────────┤\n");

  /* for each row, print all columns in a row */
  for (row = 0; row < nrows; row++)
  {
    (void)printf("│ %-33s │ %-9s │ %10ld │  %6.2f │\n",
      PQgetvalue(trace_res, row, 0),
      PQgetvalue(trace_res, row, 1),
      atol(PQgetvalue(trace_res, row, 2)),
      atof(PQgetvalue(trace_res, row, 3))
    );
  }

  /* print footers */
  (void)printf(
"└───────────────────────────────────┴───────────┴────────────┴─────────┘\n");

  /* cleanup */
  PQclear(duration_res);
  PQclear(trace_res);
}


/*
 * Drop env
 */
void
drop_env()
{
  char     sql[PGWAITEVENT_DEFAULT_STRING_SIZE];
  PGresult *res;

  /* no need to drop the temp table */

  /* drop function */
  snprintf(sql, sizeof(sql),
    "DROP FUNCTION pgwaitevent.trace_wait_events_for_pid(integer, boolean, numeric)");

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_error("query failed: %s", PQerrorMessage(conn));
    pg_log_info("query was: %s", sql);
    PQclear(res);
    PQfinish(conn);
    exit(EXIT_FAILURE);
  }

  /* print verbose */
  if (opts->verbose)
    printf("Function dropped\n");

  /* drop function */
  snprintf(sql, sizeof(sql),
    "DROP SCHEMA pgwaitevent");

  /* make the call */
  res = PQexec(conn, sql);

  /* check and deal with errors */
  if (!res || PQresultStatus(res) > 2)
  {
    pg_log_error("query failed: %s", PQerrorMessage(conn));
    pg_log_info("query was: %s", sql);
    PQclear(res);
    PQfinish(conn);
    exit(EXIT_FAILURE);
  }

  /* print verbose */
  if (opts->verbose)
    printf("Schema dropped\n");

  /* cleanup */
  PQclear(res);
}


/*
 * Main function
 */
int
main(int argc, char **argv)
{
  const char  *progname;
  ConnParams   cparams;

  /*
   * If the user stops the program,
   * quit nicely.
   */
  pqsignal(SIGINT, quit_properly);

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

  /* Fetch version */
  fetch_version();

  /* Check options */
  if (opts->includeleaderworkers && !backend_minimum_version(13, 0))
  {
    pg_log_error("You need at least v13 to include workers' wait events.");
    exit(EXIT_FAILURE);
  }

  /* Create the trace_wait_events_for_pid function */
  build_env();

  /* show what we're doing */
  printf("Tracing wait events for PID %d, sampling at %.3fs, %s\n",
    opts->pid,
    opts->interval,
    opts->includeleaderworkers ? "including leader and workers" : "PID only");

  while(true)
  {
    if (active_session())
    {
      /* Handle query currently executed */
      handle_current_query();
    }

    /* wait 100ms */
    (void)usleep(100000);
  }

  /* Drop the function */
  drop_env();
  PQfinish(conn);
  return 0;
}
