/*
 * pgreport, a PostgreSQL app to get lots of informations from PostgreSQL
 * metadata and statistics.
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Guillaume Lelarge, guillaume@lelarge.info, 2020-2021.
 *
 * pgstats/pgreport.c
 */


/*
 * Headers
 */
#include "postgres_fe.h"
#include <err.h>
#include <math.h>
#include <sys/ioctl.h>
#include <sys/signal.h>
#include <sys/stat.h>

#include <unistd.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#include "fe_utils/print.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"

#include "pgreport_queries.h"


/*
 * Defines
 */
#define PGREPORT_VERSION "1.1.0"
#define	PGREPORT_DEFAULT_LINES 20
#define	PGREPORT_DEFAULT_STRING_SIZE 2048


/*
 * Structs
 */

/* these are the options structure for command line parameters */
struct options
{
	/* misc */
	char	*script;
	bool	verbose;

	/* connection parameters */
	char	*dbname;
	char	*hostname;
	char	*port;
	char	*username;

	/* version number */
	int		major;
	int		minor;
};


/*
 * Global variables
 */
PGconn          *conn;
struct options  *opts;
extern char     *optarg;


/*
 * Function prototypes
 */
static void help(const char *progname);
void		get_opts(int, char **);
#ifndef FE_MEMUTILS_H
void	   *pg_malloc(size_t size);
char	   *pg_strdup(const char *in);
#endif
PGconn	   *sql_conn(void);
bool		backend_minimum_version(int major, int minor);
void		execute(char *query);
void		install_extension(char *extension);
void		fetch_version(void);
void		fetch_postmaster_reloadconftime(void);
void		fetch_postmaster_starttime(void);
void		fetch_table(char *label, char *query);
void		fetch_file(char *filename);
void		fetch_kernelconfig(char *cfg);
void		exec_command(char *cmd);
static void quit_properly(SIGNAL_ARGS);


/*
 * Print help message
 */
static void
help(const char *progname)
{
	printf("%s gets lots of informations from PostgreSQL metadata and statistics.\n\n"
		   "Usage:\n"
		   "  %s [OPTIONS]\n"
		   "\nGeneral options:\n"
		   "  -s VERSION    generate SQL script for $VERSION release\n"
		   "  -v            verbose\n"
		   "  -?|--help     show this help, then exit\n"
		   "  -V|--version  output version information, then exit\n"
		   "\nConnection options:\n"
		   "  -h HOSTNAME   database server host or socket directory\n"
		   "  -p PORT       database server port number\n"
		   "  -U USER       connect as specified database user\n"
		   "  -d DBNAME     database to connect to\n\n"
		   "Report bugs to <guillaume@lelarge.info>.\n",
		   progname, progname);
}


/*
 * Parse command line options and check for some usage errors
 */
void
get_opts(int argc, char **argv)
{
	int			c;
	const char *progname;

	progname = get_progname(argv[0]);

	/* set the defaults */
	opts->script = NULL;
	opts->verbose = false;
	opts->dbname = NULL;
	opts->hostname = NULL;
	opts->port = NULL;
	opts->username = NULL;

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
			puts("pgreport " PGREPORT_VERSION " (compiled with PostgreSQL " PG_VERSION ")");
			exit(0);
		}
	}

	/* get options */
	while ((c = getopt(argc, argv, "h:p:U:d:vs:")) != -1)
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

				/* port to connect to on remote host */
			case 'p':
				opts->port = pg_strdup(optarg);
				break;

				/* get script */
			case 's':
				opts->script = pg_strdup(optarg);
	            sscanf(opts->script, "%d.%d", &(opts->major), &(opts->minor));
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
				errx(1, "Try \"%s --help\" for more information.\n", progname);
		}
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
	void	   *tmp;

	/* Avoid unportable behavior of malloc(0) */
	if (size == 0)
		size = 1;
	tmp = malloc(size);
	if (!tmp)
	{
		fprintf(stderr, "out of memory\n");
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
	char	   *tmp;

	if (!in)
	{
		fprintf(stderr, "cannot duplicate null pointer (internal error)\n");
		exit(EXIT_FAILURE);
	}
	tmp = strdup(in);
	if (!tmp)
	{
		fprintf(stderr, "out of memory\n");
		exit(EXIT_FAILURE);
	}
	return tmp;
}
#endif


/*
 * Establish the PostgreSQL connection
 */
PGconn *
sql_conn()
{
	PGconn	   *my_conn;
	char	   *password = NULL;
	bool		new_pass;
#if PG_VERSION_NUM >= 90300
    const char **keywords;
    const char **values;
#else
	int size;
	char *dns;
#endif
	char		*message;

	/*
	 * Start the connection.  Loop until we have a password if requested by
	 * backend.
	 */
	do
	{

#if PG_VERSION_NUM >= 90300
		/*
		 * We don't need to check if the database name is actually a complete
		 * connection string, PQconnectdbParams being smart enough to check
		 * this itself.
		 */
#define PARAMS_ARRAY_SIZE   8
        keywords = pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
        values = pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

        keywords[0] = "host";
        values[0] = opts->hostname,
        keywords[1] = "port";
        values[1] = opts->port;
        keywords[2] = "user";
        values[2] = opts->username;
        keywords[3] = "password";
        values[3] = password;
        keywords[4] = "dbname";
        values[4] = opts->dbname;
        keywords[5] = "fallback_application_name";
        values[5] = "pgreport";
        keywords[7] = NULL;
        values[7] = NULL;

        my_conn = PQconnectdbParams(keywords, values, true);
#else
		/* 34 is the length of the fallback application name setting */
		size = 34;
		if (opts->hostname)
			size += strlen(opts->hostname) + 6;
		if (opts->port)
			size += strlen(opts->port) + 6;
		if (opts->username)
			size += strlen(opts->username) + 6;
		if (opts->dbname)
			size += strlen(opts->dbname) + 8;
		dns = pg_malloc(size);
		/*
		 * Checking the presence of a = sign is our way to check that the
		 * database name is actually a connection string. In such a case, we
		 * keep this string as the connection string, and add other parameters
		 * if they are supplied.
		 */
		sprintf(dns, "%s", "fallback_application_name='pgreport' ");

		if (strchr(opts->dbname, '=') != NULL)
			sprintf(dns, "%s%s", dns, opts->dbname);
		else if (opts->dbname)
			sprintf(dns, "%sdbname=%s ", dns, opts->dbname);

		if (opts->hostname)
			sprintf(dns, "%shost=%s ", dns, opts->hostname);
		if (opts->port)
			sprintf(dns, "%sport=%s ", dns, opts->port);
		if (opts->username)
			sprintf(dns, "%suser=%s ", dns, opts->username);

		if (opts->verbose)
			printf("Connection string: %s\n", dns);

		my_conn = PQconnectdb(dns);
#endif

        new_pass = false;

		if (!my_conn)
		{
			errx(1, "could not connect to database %s\n", opts->dbname);
		}

#if PG_VERSION_NUM >= 80200
		if (PQstatus(my_conn) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(my_conn) &&
			password == NULL)
		{
			PQfinish(my_conn);
#if PG_VERSION_NUM < 100000
			password = simple_prompt("Password: ", 100, false);
#else
			simple_prompt("Password: ", password, 100, false);
#endif
			new_pass = true;
		}
#endif
	} while (new_pass);

	if (password)
		free(password);

	/* check to see that the backend connection was successfully made */
	if (PQstatus(my_conn) == CONNECTION_BAD)
	{
		message = PQerrorMessage(my_conn);
		PQfinish(my_conn);
		errx(1, "could not connect to database %s: %s", opts->dbname, message);
	}

	/* return the conn if good */
	return my_conn;
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
 * Execute query
 */
void
execute(char *query)
{
	PGresult   *results;

	if (opts->script)
	{
		printf("%s;\n", query);
	}
	else
	{
		/* make the call */
		results = PQexec(conn, query);

		/* check and deal with errors */
		if (!results)
		{
			warnx("pgreport: query failed: %s", PQerrorMessage(conn));
			PQclear(results);
			PQfinish(conn);
			errx(1, "pgreport: query was: %s", query);
		}

		/* cleanup */
		PQclear(results);
	}
}


/*
 * Install extension
 */
void
install_extension(char *extension)
{
	char		check_sql[PGREPORT_DEFAULT_STRING_SIZE],
				install_sql[PGREPORT_DEFAULT_STRING_SIZE];
	PGresult   *check_res, *install_res;

	if (opts->script)
	{
		printf("CREATE EXTENSION IF NOT EXISTS %s;\n", extension);
	}
	else
	{
		/* check if extension is already installed */
		snprintf(check_sql, sizeof(check_sql),
			"SELECT 1 "
			"FROM pg_available_extensions "
			"WHERE name='%s' "
			"  AND installed_version IS NOT NULL",
			extension);

		/* make the call */
		check_res = PQexec(conn, check_sql);

		/* check and deal with errors */
		if (!check_res || PQresultStatus(check_res) > 2)
		{
			warnx("pgreport: query failed: %s", PQerrorMessage(conn));
			PQclear(check_res);
			PQfinish(conn);
			errx(1, "pgreport: query was: %s", check_sql);
		}

		if (PQntuples(check_res) == 0)
		{
			/* check if extension is already installed */
			snprintf(install_sql, sizeof(install_sql),
			  	"create extension %s",
			   	extension);

			/* make the call */
			install_res = PQexec(conn, install_sql);

			/* install and deal with errors */
			if (!install_res || PQresultStatus(install_res) > 2)
			{
				warnx("pgreport: query failed: %s", PQerrorMessage(conn));
				PQclear(install_res);
				PQfinish(conn);
				errx(1, "pgreport: query was: %s", install_sql);
			}
			/* cleanup */
			PQclear(install_res);
		}

		/* cleanup */
		PQclear(check_res);
	}
}


/*
 * Fetch PostgreSQL major and minor numbers
 */
void
fetch_version()
{
	char		sql[PGREPORT_DEFAULT_STRING_SIZE];
	PGresult   *res;

	if (opts->script)
	{
		printf("\\echo PostgreSQL version\n");
		printf("SELECT version();\n");
	}
	else
	{
		/* get the cluster version */
		snprintf(sql, sizeof(sql), "SELECT version()");

		/* make the call */
		res = PQexec(conn, sql);

		/* check and deal with errors */
		if (!res || PQresultStatus(res) > 2)
		{
			warnx("pgreport: query failed: %s", PQerrorMessage(conn));
			PQclear(res);
			PQfinish(conn);
			errx(1, "pgreport: query was: %s", sql);
		}

		/* get the only row, and parse it to get major and minor numbers */
		sscanf(PQgetvalue(res, 0, 0), "%*s %d.%d", &(opts->major), &(opts->minor));

		/* print version */
		if (opts->verbose)
			printf("Detected release: %d.%d\n", opts->major, opts->minor);

		printf ("PostgreSQL version: %s\n", PQgetvalue(res, 0, 0));

		/* cleanup */
		PQclear(res);
	}
}


/*
 * Fetch PostgreSQL reload configuration time
 */
void
fetch_postmaster_reloadconftime()
{
	char		sql[PGREPORT_DEFAULT_STRING_SIZE];
	PGresult   *res;

	if (opts->script)
	{
		printf("\\echo PostgreSQL reload conf time\n");
		printf("SELECT pg_conf_load_time();\n");
	}
	else
	{
		/* get the cluster version */
		snprintf(sql, sizeof(sql), "SELECT pg_conf_load_time()");

		/* make the call */
		res = PQexec(conn, sql);

		/* check and deal with errors */
		if (!res || PQresultStatus(res) > 2)
		{
			warnx("pgreport: query failed: %s", PQerrorMessage(conn));
			PQclear(res);
			PQfinish(conn);
			errx(1, "pgreport: query was: %s", sql);
		}

		printf ("PostgreSQL reload conf time: %s\n", PQgetvalue(res, 0, 0));

		/* cleanup */
		PQclear(res);
	}
}


/*
 * Fetch PostgreSQL start time
 */
void
fetch_postmaster_starttime()
{
	char		sql[PGREPORT_DEFAULT_STRING_SIZE];
	PGresult   *res;

	if (opts->script)
	{
		printf("\\echo PostgreSQL start time\n");
		printf("SELECT pg_postmaster_start_time();\n");
	}
	else
	{
		/* get the cluster version */
		snprintf(sql, sizeof(sql), "SELECT pg_postmaster_start_time()");

		/* make the call */
		res = PQexec(conn, sql);

		/* check and deal with errors */
		if (!res || PQresultStatus(res) > 2)
		{
			warnx("pgreport: query failed: %s", PQerrorMessage(conn));
			PQclear(res);
			PQfinish(conn);
			errx(1, "pgreport: query was: %s", sql);
		}

		printf ("PostgreSQL start time: %s\n", PQgetvalue(res, 0, 0));

		/* cleanup */
		PQclear(res);
	}
}


/*
 * Handle query
 */
void
fetch_table(char *label, char *query)
{
	PGresult	*res;
	printQueryOpt myopt;

	if (opts->script)
	{
		printf("\\echo %s\n",label);
		printf("%s;\n",query);
	}
	else
	{
		myopt.nullPrint = NULL;
		myopt.title = pstrdup(label);
		myopt.translate_header = false;
		myopt.n_translate_columns = 0;
		myopt.translate_columns = NULL;
		myopt.footers = NULL;
		myopt.topt.format = PRINT_ALIGNED;
		myopt.topt.expanded = 0;
		myopt.topt.border = 2;
		myopt.topt.pager = 0;
		myopt.topt.tuples_only = false;
		myopt.topt.start_table = true;
		myopt.topt.stop_table = true;
		myopt.topt.default_footer = false;
		myopt.topt.line_style = NULL;
		//myopt.topt.fieldSep = NULL;
		//myopt.topt.recordSep = NULL;
		myopt.topt.numericLocale = false;
		myopt.topt.tableAttr = NULL;
		myopt.topt.encoding = PQenv2encoding();
		myopt.topt.env_columns = 0;
		//myopt.topt.columns = 3;
		myopt.topt.unicode_border_linestyle = UNICODE_LINESTYLE_SINGLE;
		myopt.topt.unicode_column_linestyle = UNICODE_LINESTYLE_SINGLE;
		myopt.topt.unicode_header_linestyle = UNICODE_LINESTYLE_SINGLE;

		/* execute it */
		res = PQexec(conn, query);

		/* check and deal with errors */
		if (!res || PQresultStatus(res) > 2)
		{
			warnx("pgreport: query failed: %s", PQerrorMessage(conn));
			PQclear(res);
			PQfinish(conn);
			errx(1, "pgreport: query was: %s", query);
		}

		/* print results */
		printQuery(res, &myopt, stdout, false, NULL);

		/* cleanup */
		PQclear(res);
	}
}


void
fetch_kernelconfig(char *cfg)
{
	char *filename;

	filename = pg_malloc(strlen("/proc/sys/vm/")+strlen(cfg));
	sprintf(filename, "/proc/sys/vm/%s", cfg);
	printf("%s : ", cfg);
	fetch_file(filename);
	printf("\n");
	pg_free(filename);
}


void
fetch_file(char *filename)
{
	FILE *fp;
	char ch;

	fp = fopen(filename, "r"); // read mode

	if (fp == NULL)
	{
		perror("Error while opening the file.\n");
		exit(EXIT_FAILURE);
	}

	while((ch = fgetc(fp)) != EOF)
	{
		printf("%c", ch);
	}

	fclose(fp);
}


void
exec_command(char *cmd)
{
	int filedes[2];
	pid_t pid;
	char *buffer;
	ssize_t count;

	if (pipe(filedes) == -1)
	{
		perror("pipe");
		exit(1);
	}

	pid = fork();
	if (pid == -1)
	{
		perror("fork");
		exit(1);
	}
	else if (pid == 0)
	{
		while ((dup2(filedes[1], STDOUT_FILENO) == -1) && (errno == EINTR)) {}
		close(filedes[1]);
		close(filedes[0]);
		execl(cmd, cmd, (char*)0);
		perror("execl");
		_exit(1);
	}
	close(filedes[1]);

	buffer = (char *) pg_malloc(1);

	while (1)
	{
		count = read(filedes[0], buffer, sizeof(buffer));
		if (count == -1)
		{
			if (errno == EINTR)
			{
				continue;
			}
			else
			{
				perror("read");
				exit(1);
			}
		}
		else if (count == 0)
		{
			break;
		}
		else
		{
			printf("%s", buffer);
		}
	}
	close(filedes[0]);

	pg_free(buffer);
}


/*
 * Close the PostgreSQL connection, and quit
 */
static void
quit_properly(SIGNAL_ARGS)
{
	PQfinish(conn);
	exit(1);
}


/*
 * Main function
 */
int
main(int argc, char **argv)
{
	char sql[10240];

	/*
	 * If the user stops the program,
	 * quit nicely.
	 */
	pqsignal(SIGINT, quit_properly);

	/* Allocate the options struct */
	opts = (struct options *) pg_malloc(sizeof(struct options));

	/* Parse the options */
	get_opts(argc, argv);

	if (opts->script)
	{
		printf("\\echo =================================================================================\n");
		printf("\\echo == pgreport SQL script for a %s release =========================================\n", opts->script);
		printf("\\echo =================================================================================\n");
		printf("SET application_name to 'pgreport';\n");
	}
	else
	{
		/* connect to the database */
		conn = sql_conn();
	}

	/* Create schema, and set if as our search_path */
	execute(CREATE_SCHEMA);
	execute(SET_SEARCHPATH);
	/* Install some extensions if they are not already there */
	install_extension("pg_buffercache");
	/* Install some functions/views */
	execute(CREATE_GETVALUE_FUNCTION_SQL);
	execute(CREATE_BLOATTABLE_VIEW_SQL);
	strcat(sql, CREATE_BLOATINDEX_VIEW_SQL_1);
	strcat(sql, CREATE_BLOATINDEX_VIEW_SQL_2);
	execute(sql);
	execute(CREATE_ORPHANEDFILES_VIEW_SQL);

	/* Fetch version */
	printf("%s# PostgreSQL Version\n\n", opts->script ? "\\echo " : "");
	fetch_version();
	printf("\n");

	/* Fetch postmaster start time */
	printf("%s# PostgreSQL Start time\n\n", opts->script ? "\\echo " : "");
	fetch_postmaster_starttime();
	printf("\n");

	/* Fetch reload conf time */
	printf("%s# PostgreSQL Reload conf time\n\n", opts->script ? "\\echo " : "");
	fetch_postmaster_reloadconftime();
	printf("\n");

	/* Fetch settings by various ways */
	printf("%s# PostgreSQL Configuration\n\n", opts->script ? "\\echo " : "");
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
	fetch_table(PGSETTINGS_TITLE, PGSETTINGS_SQL);

	/* Fetch global objects */
	printf("%s# Global objects\n\n", opts->script ? "\\echo " : "");
	fetch_table(CLUSTER_HITRATIO_TITLE, CLUSTER_HITRATIO_SQL);
	fetch_table(DATABASES_TITLE, DATABASES_SQL);
	fetch_table(DATABASES_IN_CACHE_TITLE, DATABASES_IN_CACHE_SQL);
	fetch_table(TABLESPACES_TITLE, TABLESPACES_SQL);
	fetch_table(ROLES_TITLE, backend_minimum_version(9,5) ? ROLES_SQL_95min : ROLES_SQL_94max);
	fetch_table(USER_PASSWORDS_TITLE, USER_PASSWORDS_SQL);
	fetch_table(DATABASEUSER_CONFIG_TITLE, DATABASEUSER_CONFIG_SQL);

	/* Fetch local objects of the current database */
	printf("%s# Local objects in database %s\n\n", opts->script ? "\\echo " : "", opts->dbname);
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
	fetch_table(KINDS_SIZE_TITLE, KINDS_SIZE_SQL);
	fetch_table(DEPENDENCIES_TITLE, DEPENDENCIES_SQL);
	fetch_table(KINDS_IN_CACHE_TITLE, KINDS_IN_CACHE_SQL);
	fetch_table(AM_SIZE_TITLE, AM_SIZE_SQL);
	fetch_table(INDEXTYPE_TITLE, INDEXTYPE_SQL);
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

	if (opts->script)
	{
		/* Drop the function */
		PQfinish(conn);
	}

	pg_free(opts);

	return 0;
}
