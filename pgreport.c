/*
 * pgreport, a PostgreSQL app to get lots of informations from PostgreSQL
 * metadata and statistics.
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Guillaume Lelarge, guillaume@lelarge.info, 2020-2025.
 *
 * pgstats/pgreport.c
 */


/*
 * PostgreSQL headers
 */
#include "postgres_fe.h"
#include "common/logging.h"
#include "fe_utils/connect_utils.h"
#include "libpq/pqsignal.h"


/*
 * pgreport headers
 */
#include "pgreport_queries.h"


/*
 * Defines
 */
#define PGREPORT_VERSION "1.4.0"
#define PGREPORT_DEFAULT_LINES 20
#define PGREPORT_DEFAULT_STRING_SIZE 2048


/*
 * Structs
 */

/* these are the options structure for command line parameters */
struct options
{
  /* misc */
  char *script;
  bool verbose;

  /* version number */
  int  major;
  int  minor;
};


/*
 * Global variables
 */
struct options *opts;
extern char    *optarg;
const char *progname;


/*
 * Function prototypes
 */
static void help();
void        get_opts(int, char **);
#ifndef FE_MEMUTILS_H
void        *pg_malloc(size_t size);
char        *pg_strdup(const char *in);
#endif
bool        backend_minimum_version(int major, int minor);
void        execute(char *query);
void        install_extension(char *extension);
void        fetch_version(void);
void        fetch_postmaster_reloadconftime(void);
void        fetch_postmaster_starttime(void);
void        fetch_table(char *label, char *query);
void        fetch_file(char *filename);
void        fetch_kernelconfig(char *cfg);
void        exec_command(char *cmd);
static void quit_properly(SIGNAL_ARGS);


/*
 * Print help message
 */
static void
help()
{
  printf("%s gets lots of informations from PostgreSQL metadata and statistics.\n\n"
       "Usage:\n"
       "  %s [OPTIONS]\n"
       "\nGeneral options:\n"
       "  -s VERSION    generate SQL script for $VERSION release\n"
       "  -v            verbose\n"
       "  -?|--help     show this help, then exit\n"
       "  -V|--version  output version information, then exit\n"
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

  /* set the defaults */
  opts->script = NULL;
  opts->verbose = false;

  /* we should deal quickly with help and version */
  if (argc > 1)
  {
    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
    {
      help();
      exit(0);
    }
    if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
    {
      puts("pgreport " PGREPORT_VERSION " (compiled with PostgreSQL " PG_VERSION ")");
      exit(0);
    }
  }

  /* get options */
  while ((c = getopt(argc, argv, "vs:")) != -1)
  {
    switch (c)
    {
        /* get script */
      case 's':
        opts->script = pg_strdup(optarg);
        sscanf(opts->script, "%d.%d", &(opts->major), &(opts->minor));
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

  if (opts->script == NULL)
  {
    opts->script = "17";
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
 * Compare given major and minor numbers to the one of the connected server
 */
bool
backend_minimum_version(int major, int minor)
{
  return opts->major > major || (opts->major == major && opts->minor >= minor);
}


/*
 * Execute query
 */
void
execute(char *query)
{
    printf("%s;\n", query);
}

/*
 * Install extension
 */
void
install_extension(char *extension)
{
  printf("CREATE EXTENSION IF NOT EXISTS %s;\n", extension);
}


/*
 * Fetch PostgreSQL major and minor numbers
 */
void
fetch_version()
{
  printf("\\echo PostgreSQL version\n");
  printf("SELECT version();\n");
}


/*
 * Fetch PostgreSQL reload configuration time
 */
void
fetch_postmaster_reloadconftime()
{
  printf("\\echo PostgreSQL reload conf time\n");
  printf("SELECT pg_conf_load_time();\n");
}


/*
 * Fetch PostgreSQL start time
 */
void
fetch_postmaster_starttime()
{
  printf("\\echo PostgreSQL start time\n");
  printf("SELECT pg_postmaster_start_time();\n");
}


/*
 * Handle query
 */
void
fetch_table(char *label, char *query)
{
  printf("\\echo %s\n",label);
  printf("%s;\n",query);
}


/*
 * Close the PostgreSQL connection, and quit
 */
static void
quit_properly(SIGNAL_ARGS)
{
  exit(EXIT_FAILURE);
}


/*
 * Main function
 */
int
main(int argc, char **argv)
{
  char       sql[10240];

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

  printf("\\echo =================================================================================\n");
  printf("\\echo == pgreport SQL script for a %s release =========================================\n", opts->script);
  printf("\\echo =================================================================================\n");
  printf("SET application_name to 'pgreport';\n");

  /* Fetch version */
  printf("\\echo # PostgreSQL Version\n\n");
  fetch_version();
  printf("\n");

  /* Create schema, and set if as our search_path */
  execute(CREATE_SCHEMA);
  execute(SET_SEARCHPATH);
  /* Install some extensions if they are not already there */
  install_extension("pg_buffercache");
  install_extension("pg_visibility");
  /* Install some functions/views */
  execute(CREATE_GETVALUE_FUNCTION_SQL);
  execute(CREATE_BLOATTABLE_VIEW_SQL);
  strcat(sql, CREATE_BLOATINDEX_VIEW_SQL_1);
  strcat(sql, CREATE_BLOATINDEX_VIEW_SQL_2);
  execute(sql);
  if (backend_minimum_version(10,0))
  {
    execute(CREATE_ORPHANEDFILES_VIEW_SQL2);
  }
  else
  {
    execute(CREATE_ORPHANEDFILES_VIEW_SQL1);
  }

  /* Fetch postmaster start time */
  printf("\\echo # PostgreSQL Start time\n\n");
  fetch_postmaster_starttime();
  printf("\n");

  /* Fetch reload conf time */
  printf("\\echo # PostgreSQL Reload conf time\n\n");
  fetch_postmaster_reloadconftime();
  printf("\n");

  /* Fetch settings by various ways */
  printf("\\echo # PostgreSQL Configuration\n\n");
  fetch_table(SETTINGS_BY_SOURCEFILE_TITLE, SETTINGS_BY_SOURCEFILE_SQL);
  fetch_table(SETTINGS_NOTCONFIGFILE_NOTDEFAULTVALUE_TITLE,
        SETTINGS_NOTCONFIGFILE_NOTDEFAULTVALUE_SQL);
  if (backend_minimum_version(9,5))
  {
    fetch_table(PGFILESETTINGS_TITLE, PGFILESETTINGS_SQL);
  }
  if (backend_minimum_version(10,0))
  {
    fetch_table(PGHBAFILERULES_TITLE, PGHBAFILERULES_SQL);
  }
  if (backend_minimum_version(15,0))
  {
    fetch_table(PGIDENTFILEMAPPINGS_TITLE, PGIDENTFILEMAPPINGS_SQL);
  }
  fetch_table(PGSETTINGS_TITLE, PGSETTINGS_SQL);

  /* Fetch global objects */
  printf("\\echo # Global objects\n\n");
  fetch_table(CLUSTER_HITRATIO_TITLE, CLUSTER_HITRATIO_SQL);
  fetch_table(CLUSTER_BUFFERSUSAGE_TITLE, CLUSTER_BUFFERSUSAGE_SQL);
  fetch_table(CLUSTER_BUFFERSUSAGEDIRTY_TITLE, CLUSTER_BUFFERSUSAGEDIRTY_SQL);
  fetch_table(DATABASES_TITLE, DATABASES_SQL);
  fetch_table(DATABASES_IN_CACHE_TITLE, DATABASES_IN_CACHE_SQL);
  fetch_table(TABLESPACES_TITLE, TABLESPACES_SQL);
  fetch_table(ROLES_TITLE, backend_minimum_version(9,5) ? ROLES_SQL_95min : ROLES_SQL_94max);
  fetch_table(USER_PASSWORDS_TITLE, USER_PASSWORDS_SQL);
  fetch_table(DATABASEUSER_CONFIG_TITLE, DATABASEUSER_CONFIG_SQL);

  /* Fetch local objects of the current database */
  if (backend_minimum_version(9,3))
  {
    printf("SELECT current_database() AS db \\gset");
    printf("\\echo # Local objects in database :'db'\n\n");
  }
  else
  {
    printf("\\echo # Local objects in current database\n\n");
  }
  fetch_table(SCHEMAS_TITLE, SCHEMAS_SQL);
  fetch_table(NBRELS_IN_SCHEMA_TITLE, NBRELS_IN_SCHEMA_SQL);
  if (backend_minimum_version(11,0))
  {
    fetch_table(NBFUNCSPROCS_IN_SCHEMA_TITLE,
          NBFUNCSPROCS_IN_SCHEMA_SQL);
  }
  else
  {
    fetch_table(NBFUNCS_IN_SCHEMA_TITLE, NBFUNCS_IN_SCHEMA_SQL);
  }
  fetch_table(HEAPTOAST_SIZE_TITLE, HEAPTOAST_SIZE_SQL);
  fetch_table(EXTENSIONS_TITLE, EXTENSIONS_SQL);
  fetch_table(EXTENSIONSTABLE_TITLE, EXTENSIONSTABLE_SQL);
  fetch_table(KINDS_SIZE_TITLE, KINDS_SIZE_SQL);
  fetch_table(DEPENDENCIES_TITLE, DEPENDENCIES_SQL);
  fetch_table(KINDS_IN_CACHE_TITLE, KINDS_IN_CACHE_SQL);
  fetch_table(AM_SIZE_TITLE, AM_SIZE_SQL);
  fetch_table(INDEXTYPE_TITLE, INDEXTYPE_SQL);
  fetch_table(INDEXONTEXT_TITLE, INDEXONTEXT_SQL);
  fetch_table(PERCENTUSEDINDEXES_TITLE, PERCENTUSEDINDEXES_SQL);
  fetch_table(UNUSEDINDEXES_TITLE, UNUSEDINDEXES_SQL);
  fetch_table(REDUNDANTINDEXES_TITLE, REDUNDANTINDEXES_SQL);
  fetch_table(ORPHANEDFILES_TITLE, ORPHANEDFILES_SQL);
  fetch_table(NBFUNCS_TITLE, NBFUNCS_SQL);
  if (backend_minimum_version(11,0))
  {
    fetch_table(FUNCSPROCS_PER_SCHEMA_AND_KIND_TITLE,
          FUNCSPROCS_PER_SCHEMA_AND_KIND_SQL);
  }
  else
  {
    fetch_table(FUNCS_PER_SCHEMA_TITLE, FUNCS_PER_SCHEMA_SQL);
  }
  fetch_table(LOBJ_TITLE, LOBJ_SQL);
  fetch_table(LOBJ_STATS_TITLE, LOBJ_STATS_SQL);
  fetch_table(RELOPTIONS_TITLE, RELOPTIONS_SQL);
  fetch_table(NEEDVACUUM_TITLE, NEEDVACUUM_SQL);
  fetch_table(NEEDANALYZE_TITLE, NEEDANALYZE_SQL);
  fetch_table(MINAGE_TITLE, MINAGE_SQL);
  fetch_table(TOBEFROZEN_TABLES_TITLE, TOBEFROZEN_TABLES_SQL);
  fetch_table(BLOATOVERVIEW_TITLE, BLOATOVERVIEW_SQL);
  fetch_table(TOP20BLOAT_TABLES_TITLE, TOP20BLOAT_TABLES_SQL);
  fetch_table(TOP20BLOAT_INDEXES_TITLE, TOP20BLOAT_INDEXES_SQL);
  fetch_table(REPSLOTS_TITLE, REPSLOTS_SQL);
  if (backend_minimum_version(10,0))
  {
    fetch_table(PUBLICATIONS_TITLE, PUBLICATIONS_SQL);
    fetch_table(SUBSCRIPTIONS_TITLE, SUBSCRIPTIONS_SQL);
  }
  /*
  fetch_table(TOP10QUERYIDS_TITLE, TOP10QUERYIDS_SQL);
  fetch_table(TOP10QUERIES_TITLE, TOP10QUERIES_SQL);
  */

  /*
   * Uninstall all
   * Actually, it drops our schema, which should get rid of all our stuff
   */
  execute(DROP_ALL);

  pg_free(opts);

  return 0;
}
