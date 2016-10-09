/*
 * pgstat, a PostgreSQL app to gather statistical informations
 * from a PostgreSQL database, and act like a vmstat tool.
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Guillaume Lelarge, guillaume@lelarge.info, 2014-2016.
 *
 * pgstat/pgstat.c
 */


/*
 * Headers
 */
#include "postgres_fe.h"
#include <err.h>
#include <sys/ioctl.h>
#include <sys/signal.h>
#include <sys/stat.h>

#include <unistd.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#include "libpq-fe.h"
#include "libpq/pqsignal.h"

/*
 * Defines
 */
#define PGSTAT_VERSION "1.0.0"
#define	PGSTAT_DEFAULT_LINES 20
#define	PGSTAT_DEFAULT_STRING_SIZE 1024

/*
 * Structs and enums
 */
typedef enum
{
	NONE = 0,
	ARCHIVER,
	BGWRITER,
	CONNECTION,
	DATABASE,
	TABLE,
	TABLEIO,
	INDEX,
	FUNCTION,
	STATEMENT,
	XLOG,
	TEMPFILE,
	PBPOOLS,
	PBSTATS
} stat_t;


/* these are the options structure for command line parameters */
struct options
{
	/* misc */
	bool		verbose;
	bool        dontredisplayheader;
	stat_t      stat;
	char	   *filter;
	bool        human_readable;

	/* connection parameters */
	char	   *dbname;
	char	   *hostname;
	char	   *port;
	char	   *username;

	/* version number */
	int			major;
	int			minor;

	/* pg_stat_statements namespace */
	char	   *namespace;

	/* frequency */
	int			interval;
	int			count;
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
    ? stats_reset;
	*/
};

/* pg_stat_bgwriter struct */
struct pgstatbgwriter
{
	long checkpoints_timed;
	long checkpoints_req;
	long checkpoint_write_time;
	long checkpoint_sync_time;
	long buffers_checkpoint;
	long buffers_clean;
	long maxwritten_clean;
	long buffers_backend;
	long buffers_backend_fsync;
	long buffers_alloc;
};

/* pg_stat_database struct */
struct pgstatdatabase
{
	/*
	we don't put numbackends here because it makes no sense to get a diff between the new and the old values
	long numbackends;
	*/
	long xact_commit;
	long xact_rollback;
	long blks_read;
	long blks_hit;
	long tup_returned;
	long tup_fetched;
	long tup_inserted;
	long tup_updated;
	long tup_deleted;
	long conflicts;
	long temp_files;
	long temp_bytes;
	long deadlocks;
	long blk_read_time;
	long blk_write_time;
};

/* pg_stat_all_tables struct */
struct pgstattable
{
    long seq_scan;
    long seq_tup_read;
    long idx_scan;
    long idx_tup_fetch;
    long n_tup_ins;
    long n_tup_upd;
    long n_tup_del;
    long n_tup_hot_upd;
    long n_live_tup;
    long n_dead_tup;
    long n_mod_since_analyze;
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
	long idx_tup_read;
	long idx_tup_fetch;
};

/* pg_stat_user_functions struct */
struct pgstatfunction
{
    long   calls;
    float total_time;
    float self_time;
};

/* pg_stat_statements struct */
struct pgstatstatement
{
	/*
	long userid;
	long dbid;
	long queryid;
	text query;
	*/
	long calls;
	float total_time;
	long rows;
	long shared_blks_hit;
	long shared_blks_read;
	long shared_blks_dirtied;
	long shared_blks_written;
	long local_blks_hit;
	long local_blks_read;
	long local_blks_dirtied;
	long local_blks_written;
	long temp_blks_read;
	long temp_blks_written;
	float blk_read_time;
	float blk_write_time;
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
PGconn	   		       *conn;
struct options	       *opts;
extern char            *optarg;
struct pgstatarchiver  *previous_pgstatarchiver;
struct pgstatbgwriter  *previous_pgstatbgwriter;
struct pgstatdatabase  *previous_pgstatdatabase;
struct pgstattable     *previous_pgstattable;
struct pgstattableio   *previous_pgstattableio;
struct pgstatindex     *previous_pgstatindex;
struct pgstatfunction  *previous_pgstatfunction;
struct pgstatstatement *previous_pgstatstatement;
struct xlogstats       *previous_xlogstats;
struct pgbouncerstats  *previous_pgbouncerstats;
int                     hdrcnt = 0;
volatile sig_atomic_t   wresized;
static int              winlines = PGSTAT_DEFAULT_LINES;

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
void		print_pgstatarchiver(void);
void		print_pgstatbgwriter(void);
void        print_pgstatconnection(void);
void        print_pgstatdatabase(void);
void        print_pgstattable(void);
void		print_pgstattableio(void);
void        print_pgstatindex(void);
void        print_pgstatfunction(void);
void        print_pgstatstatement(void);
void        print_xlogstats(void);
void        print_tempfilestats(void);
void        print_pgbouncerpools(void);
void        print_pgbouncerstats(void);
void		fetch_version(void);
void		fetch_pgstatstatements_namespace(void);
bool		backend_minimum_version(int major, int minor);
void        print_header(void);
void        print_line(void);
void        allocate_struct(void);
static void needhdr(int dummy);
static void	needresize(int);
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
		   "  -f FILTER      include only this object\n"
		   "  -H             display human-readable values\n"
		   "  -n             do not redisplay header\n"
		   "  -s STAT        stats to collect\n"
		   "  -v             verbose\n"
		   "  -?|--help      show this help, then exit\n"
		   "  -V|--version   output version information, then exit\n"
		   "\nConnection options:\n"
		   "  -h HOSTNAME    database server host or socket directory\n"
		   "  -p PORT        database server port number\n"
		   "  -U USER        connect as specified database user\n"
		   "  -d DBNAME      database to connect to\n"
		   "\nThe default stat is pg_stat_bgwriter, but you can change it with the -s command line option,\n"
		   "and one of its value (STAT):\n"
		   "  * archiver     for pg_stat_archiver\n"
		   "  * bgwriter     for pg_stat_bgwriter\n"
		   "  * connection   (only for > 9.1)\n"
		   "  * database     for pg_stat_database\n"
		   "  * table        for pg_stat_all_tables\n"
		   "  * tableio      for pg_statio_all_tables\n"
		   "  * index        for pg_stat_all_indexes\n"
		   "  * function     for pg_stat_user_function\n"
		   "  * statement    for pg_stat_statements (needs the extension)\n"
		   "  * xlog         for xlog writes (only for > 9.2)\n"
		   "  * tempfile     for temporary file usage\n"
		   "  * pbpools      for pgBouncer pools statistics\n"
		   "  * pbstats      for pgBouncer statistics\n\n"
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
	opts->verbose = false;
	opts->dontredisplayheader = false;
	opts->stat = NONE;
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
	while ((c = getopt(argc, argv, "h:Hp:U:d:f:ns:v")) != -1)
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

				/* specify the stat */
			case 's':
				if (opts->stat != NONE)
				{
					errx(1, "You can only use once the -s command line switch.\n");
				}

				if (!strcmp(optarg, "archiver"))
				{
					opts->stat = ARCHIVER;
				}
				if (!strcmp(optarg, "bgwriter"))
				{
					opts->stat = BGWRITER;
				}
				if (!strcmp(optarg, "connection"))
				{
					opts->stat = CONNECTION;
				}
				if (!strcmp(optarg, "database"))
				{
					opts->stat = DATABASE;
				}
				if (!strcmp(optarg, "table"))
				{
					opts->stat = TABLE;
				}
				if (!strcmp(optarg, "tableio"))
				{
					opts->stat = TABLEIO;
				}
				if (!strcmp(optarg, "index"))
				{
					opts->stat = INDEX;
				}
				if (!strcmp(optarg, "function"))
				{
					opts->stat = FUNCTION;
				}
				if (!strcmp(optarg, "statement"))
				{
					opts->stat = STATEMENT;
				}
				if (!strcmp(optarg, "xlog"))
				{
					opts->stat = XLOG;
				}
				if (!strcmp(optarg, "tempfile"))
				{
					opts->stat = TEMPFILE;
				}
				if (!strcmp(optarg, "pbpools"))
				{
					opts->stat = PBPOOLS;
				}
				if (!strcmp(optarg, "pbstats"))
				{
					opts->stat = PBSTATS;
				}
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
				errx(1, "Try \"%s --help\" for more information.\n", progname);
		}
	}

	if (optind < argc)
	{
		opts->interval = atoi(argv[optind]);
		if (opts->interval == 0)
		{
			errx(1, "Invalid delay.\nTry \"%s --help\" for more information.\n", progname);
		}
		optind++;
	}

	if (optind < argc)
	{
		opts->count = atoi(argv[optind]);
		if (opts -> count == 0)
		{
			errx(1, "Invalid count.\nTry \"%s --help\" for more information.\n", progname);
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
	void       *tmp;

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
	char       *tmp;

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
        values[5] = "pgstat";
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
		sprintf(dns, "%s", "fallback_application_name='pgstat' ");

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
			password = simple_prompt("Password: ", 100, false);
			new_pass = true;
		}
#endif
	} while (new_pass);

	if (password)
		free(password);

	/* check to see that the backend connection was successfully made */
	if (PQstatus(my_conn) == CONNECTION_BAD)
	{
		errx(1, "could not connect to database %s: %s", opts->dbname, PQerrorMessage(my_conn));
		PQfinish(my_conn);
	}

	/* return the conn if good */
	return my_conn;
}

/*
 * Dump all archiver stats.
 */
void
print_pgstatarchiver()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
	int			nrows;
	int			row, column;

	long archived_count;
	long failed_count;

	/* grab the stats (this is the only stats on one line) */
	snprintf(sql, sizeof(sql),
			 "SELECT archived_count, failed_count "
             "FROM pg_stat_archiver ");

	/* make the call */
	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
		archived_count = atol(PQgetvalue(res, row, column++));
		failed_count = atol(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf("   %6ld   %6ld\n",
		    archived_count - previous_pgstatarchiver->archived_count,
		    failed_count - previous_pgstatarchiver->failed_count
		    );

		/* setting the new old value */
		previous_pgstatarchiver->archived_count = archived_count;
	    previous_pgstatarchiver->failed_count = failed_count;
	}

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all bgwriter stats.
 */
void
print_pgstatbgwriter()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
	int			nrows;
	int			row, column;

	long checkpoints_timed = 0;
	long checkpoints_req = 0;
	long checkpoint_write_time = 0;
	long checkpoint_sync_time = 0;
	long buffers_checkpoint = 0;
	long buffers_clean = 0;
	long maxwritten_clean = 0;
	long buffers_backend = 0;
	long buffers_backend_fsync = 0;
	long buffers_alloc = 0;

	/* grab the stats (this is the only stats on one line) */
	snprintf(sql, sizeof(sql),
			 "SELECT checkpoints_timed, checkpoints_req, %sbuffers_checkpoint, buffers_clean, "
			 "maxwritten_clean, buffers_backend, %sbuffers_alloc "
             "FROM pg_stat_bgwriter ",
		backend_minimum_version(9, 2) ? "checkpoint_write_time, checkpoint_sync_time, " : "",
		backend_minimum_version(9, 1) ? "buffers_backend_fsync, " : "");

	/* make the call */
	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
		checkpoints_timed = atol(PQgetvalue(res, row, column++));
		checkpoints_req = atol(PQgetvalue(res, row, column++));
		if (backend_minimum_version(9, 2))
		{
			checkpoint_write_time = atol(PQgetvalue(res, row, column++));
			checkpoint_sync_time = atol(PQgetvalue(res, row, column++));
		}
		buffers_checkpoint = atol(PQgetvalue(res, row, column++));
		buffers_clean = atol(PQgetvalue(res, row, column++));
		maxwritten_clean = atol(PQgetvalue(res, row, column++));
		buffers_backend = atol(PQgetvalue(res, row, column++));
		if (backend_minimum_version(9, 1))
		{
			buffers_backend_fsync = atol(PQgetvalue(res, row, column++));
		}
		buffers_alloc = atol(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf(" %6ld    %6ld     %6ld    %6ld       %6ld %6ld  %6ld %6ld         %4ld            %2ld\n",
		    checkpoints_timed - previous_pgstatbgwriter->checkpoints_timed,
		    checkpoints_req - previous_pgstatbgwriter->checkpoints_req,
		    checkpoint_write_time - previous_pgstatbgwriter->checkpoint_write_time,
		    checkpoint_sync_time - previous_pgstatbgwriter->checkpoint_sync_time,
		    buffers_checkpoint - previous_pgstatbgwriter->buffers_checkpoint,
		    buffers_clean - previous_pgstatbgwriter->buffers_clean,
		    buffers_backend - previous_pgstatbgwriter->buffers_backend,
		    buffers_alloc - previous_pgstatbgwriter->buffers_alloc,
		    maxwritten_clean - previous_pgstatbgwriter->maxwritten_clean,
		    buffers_backend_fsync - previous_pgstatbgwriter->buffers_backend_fsync
		    );

		/* setting the new old value */
		previous_pgstatbgwriter->checkpoints_timed = checkpoints_timed;
	    previous_pgstatbgwriter->checkpoints_req = checkpoints_req;
	    previous_pgstatbgwriter->checkpoint_write_time = checkpoint_write_time;
	    previous_pgstatbgwriter->checkpoint_sync_time = checkpoint_sync_time;
	    previous_pgstatbgwriter->buffers_checkpoint = buffers_checkpoint;
	    previous_pgstatbgwriter->buffers_clean = buffers_clean;
	    previous_pgstatbgwriter->maxwritten_clean = maxwritten_clean;
	    previous_pgstatbgwriter->buffers_backend = buffers_backend;
	    previous_pgstatbgwriter->buffers_backend_fsync = buffers_backend_fsync;
	    previous_pgstatbgwriter->buffers_alloc = buffers_alloc;
	}

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all connection stats.
 */
void
print_pgstatconnection()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
	int			nrows;
	int			row, column;

	long total = 0;
	long active = 0;
	long lockwaiting = 0;
	long idleintransaction = 0;
	long idle = 0;

	if (backend_minimum_version(9, 6))
	{
		snprintf(sql, sizeof(sql),
				 "SELECT count(*) AS total, "
    	         "  sum(CASE WHEN state='active' and wait_event IS NULL THEN 1 ELSE 0 end) AS active, "
    	         "  sum(CASE WHEN wait_event IS NOT NULL THEN 1 ELSE 0 end) AS lockwaiting, "
    	         "  sum(CASE WHEN state='idle in transaction' THEN 1 ELSE 0 end) AS idleintransaction, "
    	         "  sum(CASE WHEN state='idle' THEN 1 ELSE 0 end) AS idle "
    	         "FROM pg_stat_activity");
	}
	else
	{
		snprintf(sql, sizeof(sql),
				 "SELECT count(*) AS total, "
    	         "  sum(CASE WHEN state='active' and not waiting THEN 1 ELSE 0 end) AS active, "
    	         "  sum(CASE WHEN waiting THEN 1 ELSE 0 end) AS lockwaiting, "
    	         "  sum(CASE WHEN state='idle in transaction' THEN 1 ELSE 0 end) AS idleintransaction, "
    	         "  sum(CASE WHEN state='idle' THEN 1 ELSE 0 end) AS idle "
    	         "FROM pg_stat_activity");
	}

	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		total = atol(PQgetvalue(res, row, column++));
		active = atol(PQgetvalue(res, row, column++));
		lockwaiting = atol(PQgetvalue(res, row, column++));
		idleintransaction = atol(PQgetvalue(res, row, column++));
		idle = atol(PQgetvalue(res, row, column++));

		/* printing the actual values for once */
		(void)printf("    %4ld     %4ld          %4ld                  %4ld   %4ld  \n",
		    total, active, lockwaiting, idleintransaction, idle);
	}

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all bgwriter stats.
 */
void
print_pgstatdatabase()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

	long numbackends = 0;
	long xact_commit = 0;
	long xact_rollback = 0;
	long blks_read = 0;
	long blks_hit = 0;
	long tup_returned = 0;
	long tup_fetched = 0;
	long tup_inserted = 0;
	long tup_updated = 0;
	long tup_deleted = 0;
	long conflicts = 0;
	long temp_files = 0;
	long temp_bytes = 0;
	long deadlocks = 0;
	long blk_read_time = 0;
	long blk_write_time = 0;


	/*
	 * With a filter, we assume we'll get only one row.
	 * Without, we sum all the fields to get one row.
	 */
	if (opts->filter == NULL)
	{
		snprintf(sql, sizeof(sql),
				 "SELECT sum(numbackends), sum(xact_commit), sum(xact_rollback), sum(blks_read), sum(blks_hit)"
	             "%s%s%s "
	             "FROM pg_stat_database ",
			backend_minimum_version(8, 3) ? ", sum(tup_returned), sum(tup_fetched), sum(tup_inserted), sum(tup_updated), sum(tup_deleted)" : "",
			backend_minimum_version(9, 1) ? ", sum(conflicts)" : "",
			backend_minimum_version(9, 2) ? ", sum(temp_files), sum(temp_bytes), sum(deadlocks), sum(blk_read_time), sum(blk_write_time)" : "");

		res = PQexec(conn, sql);
	}
	else
	{
		snprintf(sql, sizeof(sql),
				 "SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit"
	             "%s%s%s "
	             "FROM pg_stat_database "
	             "WHERE datname=$1",
			backend_minimum_version(8, 3) ? ", tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted" : "",
			backend_minimum_version(9, 1) ? ", conflicts" : "",
			backend_minimum_version(9, 2) ? ", temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time" : "");

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
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
		numbackends = atol(PQgetvalue(res, row, column++));
		xact_commit = atol(PQgetvalue(res, row, column++));
		xact_rollback = atol(PQgetvalue(res, row, column++));
		blks_read = atol(PQgetvalue(res, row, column++));
		blks_hit = atol(PQgetvalue(res, row, column++));
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
			blk_read_time = atol(PQgetvalue(res, row, column++));
			blk_write_time = atol(PQgetvalue(res, row, column++));
		}

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf("      %4ld      %6ld   %6ld   %6ld %6ld    %6ld     %6ld   %6ld %6ld %6ld %6ld %6ld   %6ld %9ld   %9ld %9ld\n",
		    numbackends,
		    xact_commit - previous_pgstatdatabase->xact_commit,
		    xact_rollback - previous_pgstatdatabase->xact_rollback,
		    blks_read - previous_pgstatdatabase->blks_read,
		    blks_hit - previous_pgstatdatabase->blks_hit,
		    blk_read_time - previous_pgstatdatabase->blk_read_time,
		    blk_write_time - previous_pgstatdatabase->blk_write_time,
		    tup_returned - previous_pgstatdatabase->tup_returned,
		    tup_fetched - previous_pgstatdatabase->tup_fetched,
		    tup_inserted - previous_pgstatdatabase->tup_inserted,
		    tup_updated - previous_pgstatdatabase->tup_updated,
		    tup_deleted - previous_pgstatdatabase->tup_deleted,
		    temp_files - previous_pgstatdatabase->temp_files,
		    temp_bytes - previous_pgstatdatabase->temp_bytes,
		    conflicts - previous_pgstatdatabase->conflicts,
		    deadlocks - previous_pgstatdatabase->deadlocks
		    );

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
	}

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all table stats.
 */
void
print_pgstattable()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

    long seq_scan = 0;
    long seq_tup_read = 0;
    long idx_scan = 0;
    long idx_tup_fetch = 0;
    long n_tup_ins = 0;
    long n_tup_upd = 0;
    long n_tup_del = 0;
    long n_tup_hot_upd = 0;
    long n_live_tup = 0;
    long n_dead_tup = 0;
    long n_mod_since_analyze = 0;
    long vacuum_count = 0;
    long autovacuum_count = 0;
    long analyze_count = 0;
    long autoanalyze_count = 0;

	/*
	 * With a filter, we assume we'll get only one row.
	 * Without, we sum all the fields to get one row.
	 */
	if (opts->filter == NULL)
	{
		snprintf(sql, sizeof(sql),
				 "SELECT sum(seq_scan), sum(seq_tup_read), sum(idx_scan), sum(idx_tup_fetch), sum(n_tup_ins), "
	             "sum(n_tup_upd), sum(n_tup_del)"
	             "%s"
				 "%s"
				 "%s"
	             " FROM pg_stat_all_tables "
		     "WHERE schemaname <> 'information_schema' ",
			backend_minimum_version(8, 3) ? ", sum(n_tup_hot_upd), sum(n_live_tup), sum(n_dead_tup)" : "",
	        backend_minimum_version(9, 4) ? ", sum(n_mod_since_analyze)" : "",
	        backend_minimum_version(9, 1) ? ", sum(vacuum_count), sum(autovacuum_count), sum(analyze_count), sum(autoanalyze_count)" : "");

		res = PQexec(conn, sql);
	}
	else
	{
		snprintf(sql, sizeof(sql),
				 "SELECT sum(seq_scan), sum(seq_tup_read), sum(idx_scan), sum(idx_tup_fetch), sum(n_tup_ins), "
	             "sum(n_tup_upd), sum(n_tup_del)"
	             "%s"
				 "%s"
				 "%s"
	             " FROM pg_stat_all_tables "
		     "WHERE schemaname <> 'information_schema' "
		     "  AND relname = $1",
			backend_minimum_version(8, 3) ? ", sum(n_tup_hot_upd), sum(n_live_tup), sum(n_dead_tup)" : "",
	        backend_minimum_version(9, 4) ? ", sum(n_mod_since_analyze)" : "",
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
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
        seq_scan = atol(PQgetvalue(res, row, column++));
        seq_tup_read = atol(PQgetvalue(res, row, column++));
        idx_scan = atol(PQgetvalue(res, row, column++));
        idx_tup_fetch = atol(PQgetvalue(res, row, column++));
        n_tup_ins = atol(PQgetvalue(res, row, column++));
        n_tup_upd = atol(PQgetvalue(res, row, column++));
        n_tup_del = atol(PQgetvalue(res, row, column++));
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
		if (backend_minimum_version(9, 1))
		{
	        vacuum_count = atol(PQgetvalue(res, row, column++));
	        autovacuum_count = atol(PQgetvalue(res, row, column++));
	        analyze_count = atol(PQgetvalue(res, row, column++));
	        autoanalyze_count = atol(PQgetvalue(res, row, column++));
		}

		/* printing the diff... note that the first line will be the current value, rather than the diff */
		(void)printf(" %6ld  %6ld   %6ld  %6ld      %6ld %6ld %6ld %6ld %6ld %6ld %6ld  %6ld     %6ld  %6ld      %6ld\n",
		    seq_scan - previous_pgstattable->seq_scan,
		    seq_tup_read - previous_pgstattable->seq_tup_read,
		    idx_scan - previous_pgstattable->idx_scan,
		    idx_tup_fetch - previous_pgstattable->idx_tup_fetch,
		    n_tup_ins - previous_pgstattable->n_tup_ins,
		    n_tup_upd - previous_pgstattable->n_tup_upd,
		    n_tup_del - previous_pgstattable->n_tup_del,
		    n_tup_hot_upd - previous_pgstattable->n_tup_hot_upd,
		    n_live_tup - previous_pgstattable->n_live_tup,
		    n_dead_tup - previous_pgstattable->n_dead_tup,
		    n_mod_since_analyze - previous_pgstattable->n_mod_since_analyze,
		    vacuum_count - previous_pgstattable->vacuum_count,
		    autovacuum_count - previous_pgstattable->autovacuum_count,
		    analyze_count - previous_pgstattable->analyze_count,
		    autoanalyze_count - previous_pgstattable->autoanalyze_count
		    );

		/* setting the new old value */
		previous_pgstattable->seq_scan = seq_scan;
		previous_pgstattable->seq_tup_read = seq_tup_read;
		previous_pgstattable->idx_scan = idx_scan;
		previous_pgstattable->idx_tup_fetch = idx_tup_fetch;
		previous_pgstattable->n_tup_ins = n_tup_ins;
		previous_pgstattable->n_tup_upd = n_tup_upd;
		previous_pgstattable->n_tup_del = n_tup_del;
		previous_pgstattable->n_tup_hot_upd = n_tup_hot_upd;
		previous_pgstattable->n_live_tup = n_live_tup;
		previous_pgstattable->n_dead_tup = n_dead_tup;
		previous_pgstattable->n_mod_since_analyze = n_mod_since_analyze;
		previous_pgstattable->vacuum_count = vacuum_count;
		previous_pgstattable->autovacuum_count = autovacuum_count;
		previous_pgstattable->analyze_count = analyze_count;
		previous_pgstattable->autoanalyze_count = autoanalyze_count;
	}

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all table IO stats.
 */
void
print_pgstattableio()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

	long heap_blks_read = 0;
	long heap_blks_hit = 0;
	long idx_blks_read = 0;
	long idx_blks_hit = 0;
	long toast_blks_read = 0;
	long toast_blks_hit = 0;
	long tidx_blks_read = 0;
	long tidx_blks_hit = 0;

	/*
	 * With a filter, we assume we'll get only one row.
	 * Without, we sum all the fields to get one row.
	 */
	if (opts->filter == NULL)
	{
		snprintf(sql, sizeof(sql),
				 "SELECT sum(heap_blks_read), sum(heap_blks_hit), sum(idx_blks_read), sum(idx_blks_hit), "
	             "sum(toast_blks_read), sum(toast_blks_hit), sum(tidx_blks_read), sum(tidx_blks_hit) "
	             "FROM pg_statio_all_tables "
			     "WHERE schemaname <> 'information_schema' ");

		res = PQexec(conn, sql);
	}
	else
	{
		snprintf(sql, sizeof(sql),
				 "SELECT heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit, "
	             "toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit "
	             "FROM pg_statio_all_tables "
			     "WHERE schemaname <> 'information_schema' "
			     "  AND relname = $1");

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
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
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
		(void)printf(" %6ld    %6ld    %7ld   %7ld    %7ld    %7ld     %9ld %9ld\n",
		    heap_blks_read - previous_pgstattableio->heap_blks_read,
		    heap_blks_hit - previous_pgstattableio->heap_blks_hit,
		    idx_blks_read - previous_pgstattableio->idx_blks_read,
		    idx_blks_hit - previous_pgstattableio->idx_blks_hit,
		    toast_blks_read - previous_pgstattableio->toast_blks_read,
		    toast_blks_hit - previous_pgstattableio->toast_blks_hit,
		    tidx_blks_read - previous_pgstattableio->tidx_blks_read,
		    tidx_blks_hit - previous_pgstattableio->tidx_blks_hit
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
	PQclear(res);
}

/*
 * Dump all index stats.
 */
void
print_pgstatindex()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

	long idx_scan = 0;
	long idx_tup_read = 0;
	long idx_tup_fetch = 0;

	/*
	 * With a filter, we assume we'll get only one row.
	 * Without, we sum all the fields to get one row.
	 */
	if (opts->filter == NULL)
	{
		snprintf(sql, sizeof(sql),
				 "SELECT sum(idx_scan), sum(idx_tup_read), sum(idx_tup_fetch) "
	             " FROM pg_stat_all_indexes "
		     "WHERE schemaname <> 'information_schema' ");

		res = PQexec(conn, sql);
	}
	else
	{
		snprintf(sql, sizeof(sql),
				 "SELECT idx_scan, idx_tup_read, idx_tup_fetch "
	             " FROM pg_stat_all_indexes "
		     "WHERE schemaname <> 'information_schema' "
		     "  AND indexrelname = $1");

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
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
        idx_scan = atol(PQgetvalue(res, row, column++));
        idx_tup_read = atof(PQgetvalue(res, row, column++));
        idx_tup_fetch = atof(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf(" %8ld   %7ld %7ld\n",
		    idx_scan - previous_pgstatindex->idx_scan,
		    idx_tup_read - previous_pgstatindex->idx_tup_read,
		    idx_tup_fetch - previous_pgstatindex->idx_tup_fetch
		    );

		/* setting the new old value */
		previous_pgstatindex->idx_scan = idx_scan;
		previous_pgstatindex->idx_tup_read = idx_tup_read;
		previous_pgstatindex->idx_tup_fetch = idx_tup_fetch;
	}

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all function stats.
 */
void
print_pgstatfunction()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

    long calls = 0;
    float total_time = 0;
    float self_time = 0;

	/*
	 * With a filter, we assume we'll get only one row.
	 * Without, we sum all the fields to get one row.
	 */
	if (opts->filter == NULL)
	{
		snprintf(sql, sizeof(sql),
				 "SELECT sum(calls), sum(total_time), sum(self_time) "
	             " FROM pg_stat_user_functions "
		     "WHERE schemaname <> 'information_schema' ");

		res = PQexec(conn, sql);
	}
	else
	{
		snprintf(sql, sizeof(sql),
				 "SELECT calls, total_time, self_time "
	             " FROM pg_stat_user_functions "
		     "WHERE schemaname <> 'information_schema' "
		     "  AND funcname = $1");

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
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
        calls = atol(PQgetvalue(res, row, column++));
        total_time = atof(PQgetvalue(res, row, column++));
        self_time = atof(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf(" %9ld   %5f    %5f\n",
		    calls - previous_pgstatfunction->calls,
		    total_time - previous_pgstatfunction->total_time,
		    self_time - previous_pgstatfunction->self_time
		    );

		/* setting the new old value */
		previous_pgstatfunction->calls = calls;
		previous_pgstatfunction->total_time = total_time;
		previous_pgstatfunction->self_time = self_time;
	}

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all statement stats.
 */
void
print_pgstatstatement()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
	int			nrows;
	int			row, column;

	long calls = 0;
	float total_time = 0;
	long rows = 0;
	long shared_blks_hit = 0;
	long shared_blks_read = 0;
	long shared_blks_dirtied = 0;
	long shared_blks_written = 0;
	long local_blks_hit = 0;
	long local_blks_read = 0;
	long local_blks_dirtied = 0;
	long local_blks_written = 0;
	long temp_blks_read = 0;
	long temp_blks_written = 0;
	float blk_read_time = 0;
	float blk_write_time = 0;

	snprintf(sql, sizeof(sql),
			 "SELECT sum(calls), sum(total_time), sum(rows),"
	         " sum(shared_blks_hit), sum(shared_blks_read), sum(shared_blks_dirtied), sum(shared_blks_written),"
	         " sum(local_blks_hit), sum(local_blks_read), sum(local_blks_dirtied), sum(local_blks_written),"
	         " sum(temp_blks_read), sum(temp_blks_written),"
	         " sum(blk_read_time), sum(blk_write_time)"
             " FROM %s.pg_stat_statements ", opts->namespace);
	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
		calls = atol(PQgetvalue(res, row, column++));
		total_time = atof(PQgetvalue(res, row, column++));
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
		blk_read_time = atof(PQgetvalue(res, row, column++));
		blk_write_time = atof(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf(" %6ld   %6.2f %6ld   %6ld %6ld %6ld  %6ld   %6ld %6ld %6ld  %6ld  %6ld  %6ld      %6.2f    %6.2f\n",
			calls - previous_pgstatstatement->calls,
			total_time - previous_pgstatstatement->total_time,
			rows - previous_pgstatstatement->rows,
			shared_blks_hit - previous_pgstatstatement->shared_blks_hit,
			shared_blks_read - previous_pgstatstatement->shared_blks_read,
			shared_blks_dirtied - previous_pgstatstatement->shared_blks_dirtied,
			shared_blks_written - previous_pgstatstatement->shared_blks_written,
			local_blks_hit - previous_pgstatstatement->local_blks_hit,
			local_blks_read - previous_pgstatstatement->local_blks_read,
			local_blks_dirtied - previous_pgstatstatement->local_blks_dirtied,
			local_blks_written - previous_pgstatstatement->local_blks_written,
			temp_blks_read - previous_pgstatstatement->temp_blks_read,
			temp_blks_written - previous_pgstatstatement->temp_blks_written,
			blk_read_time - previous_pgstatstatement->blk_read_time,
			blk_write_time - previous_pgstatstatement->blk_write_time
		    );

		/* setting the new old value */
		previous_pgstatstatement->calls = calls;
		previous_pgstatstatement->total_time = total_time;
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
		previous_pgstatstatement->blk_read_time = blk_read_time;
		previous_pgstatstatement->blk_write_time = blk_write_time;
	};

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all xlog writes stats.
 */
void
print_xlogstats()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;

	char *xlogfilename;
	char *currentlocation;
	char *prettylocation;
	long locationdiff;
	char h_locationdiff[PGSTAT_DEFAULT_STRING_SIZE];

	snprintf(sql, sizeof(sql),
			 "SELECT "
			 "  pg_xlogfile_name(pg_current_xlog_location()), "
			 "  pg_current_xlog_location(), "
			 "  pg_xlog_location_diff(pg_current_xlog_location(), '0/0'), "
			 "  pg_size_pretty(pg_xlog_location_diff(pg_current_xlog_location(), '%s'))",
			 previous_xlogstats->location);

	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	xlogfilename = pg_strdup(PQgetvalue(res, 0, 0));
	currentlocation = pg_strdup(PQgetvalue(res, 0, 1));
	locationdiff = atol(PQgetvalue(res, 0, 2));
	prettylocation = pg_strdup(PQgetvalue(res, 0, 3));

	/* get the human-readable diff if asked */
	if (opts->human_readable)
	{
		snprintf(h_locationdiff, sizeof(h_locationdiff), "%s", prettylocation);
	}
	else
	{
		snprintf(h_locationdiff, sizeof(h_locationdiff), "%12ld", locationdiff - previous_xlogstats->locationdiff);
	}

	/* printing the actual values for once */
	(void)printf(" %s   %s      %s\n", xlogfilename, currentlocation, h_locationdiff);

	/* setting the new old value */
	previous_xlogstats->location = pg_strdup(currentlocation);
	previous_xlogstats->locationdiff = locationdiff;

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all temporary files stats.
 */
void
print_tempfilestats()
{
	char		sql[2*PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
	long        size = 0;
	long        count = 0;
	int			nrows;
	int			row, column;

	snprintf(sql, sizeof(sql),
             "SELECT unnest(regexp_matches(agg.tmpfile, 'pgsql_tmp([0-9]*)')) AS pid, "
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
			 "				     trim(trailing E'\n ' FROM pg_read_file('PG_VERSION')) as v "
			 "            FROM (SELECT oid, spcname FROM pg_tablespace WHERE spcname !~ '^pg_') AS spc "
			 "           ) sr "
			 "      WHERE sr.spc_root ~ ('^PG_'||sr.v) "
			 "      UNION ALL "
			 "	    SELECT 0, 'pg_default', "
			 "             'base' AS dir, "
			 "             'pgsql_tmp' AS sub "
			 "		FROM pg_ls_dir('base') AS l "
			 "		WHERE l='pgsql_tmp' "
			 "     ) AS ls, "
			 "     (SELECT generate_series(1,2) AS i) AS gs "
			 "   WHERE ls.sub = 'pgsql_tmp') agg "
			 "GROUP BY 1");

	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	for (row = 0; row < nrows; row++)
	{
		column = 1;

		/* getting new values */
		size += atol(PQgetvalue(res, row, column++));
		count += atol(PQgetvalue(res, row, column++));
	}

	/* printing the diff... */
	(void)printf(" %9ld     %9ld\n", size, count);

	/* cleanup */
	PQclear(res);
}

/*
 * Dump all pgBouncer pools stats.
 */
void
print_pgbouncerpools()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
	int			nrows;
	int			row, column;

    long cl_active = 0;
    long cl_waiting = 0;
    long sv_active = 0;
    long sv_idle = 0;
    long sv_used = 0;
    long sv_tested = 0;
    long sv_login = 0;
    long maxwait = 0;

	/*
	 * We cannot use a filter now, we need to get all rows.
	 */
	snprintf(sql, sizeof(sql), "SHOW pools");
	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
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
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
	int			nrows;
	int			row, column;

    long total_request = 0;
    long total_received = 0;
    long total_sent = 0;
    long total_query_time = 0;

	/*
	 * We cannot use a filter now, we need to get all rows.
	 */
	snprintf(sql, sizeof(sql), "SHOW stats");
	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
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
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;

	/* get the cluster version */
	snprintf(sql, sizeof(sql), "SELECT version()");

	/* make the call */
	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
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
 * Fetch pg_stat_statement namespace
 */
void
fetch_pgstatstatements_namespace()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;

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
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		errx(1, "pgstats: query was: %s", sql);
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
	return opts->major > major || (opts->major == major && opts->minor >= minor);
}

/*
 * Print the right header according to the stats mode
 */
void
print_header(void)
{
	switch(opts->stat)
	{
		case NONE:
			/* That shouldn't happen */
		    break;
		case ARCHIVER:
			(void)printf("---- WAL counts ----\n");
			(void)printf(" archived   failed \n");
			break;
		case BGWRITER:
			(void)printf("------------ checkpoints ------------- ------------- buffers ------------- ---------- misc ----------\n");
			(void)printf("  timed requested write_time sync_time   checkpoint  clean backend  alloc   maxwritten backend_fsync\n");
			break;
		case CONNECTION:
			(void)printf(" - total - active - lockwaiting - idle in transaction - idle -\n");
			break;
		case DATABASE:
			(void)printf("- backends - ------ xacts ------ -------------- blocks -------------- -------------- tuples -------------- ------ temp ------ ------- misc --------\n");
			(void)printf("                commit rollback     read    hit read_time write_time      ret    fet    ins    upd    del    files     bytes   conflicts deadlocks\n");
			break;
		case TABLE:
			(void)printf("-- sequential -- ------ index ------ ----------------- tuples -------------------------- -------------- maintenance --------------\n");
			(void)printf("   scan  tuples     scan  tuples         ins    upd    del hotupd   live   dead analyze   vacuum autovacuum analyze autoanalyze\n");
			break;
		case TABLEIO:
			(void)printf("--- heap table ---  --- toast table ---  --- heap indexes ---  --- toast indexes ---\n");
			(void)printf("   read       hit       read       hit       read        hit          read       hit \n");
			break;
		case INDEX:
			(void)printf("-- scan -- ----- tuples -----\n");
			(void)printf("               read   fetch\n");
			break;
		case FUNCTION:
			(void)printf("-- count -- ------ time ------\n");
			(void)printf("             total     self\n");
			break;
		case STATEMENT:
			(void)printf("--------- misc ---------- ----------- shared ----------- ----------- local ----------- ----- temp ----- -------- time --------\n");
			(void)printf("  calls      time   rows      hit   read  dirty written      hit   read  dirty written    read written        read   written\n");
			break;
		case XLOG:
			(void)printf("-------- filename -------- -- location -- ---- bytes ----\n");
			break;
		case TEMPFILE:
			(void)printf("--- size --- --- count ---\n");
			break;
		case PBPOOLS:
			(void)printf("---- client -----  ---------------- server ----------------  -- misc --\n");
			(void)printf(" active  waiting    active    idle    used  tested   login    maxwait\n");
			break;
		case PBSTATS:
			(void)printf("---------------- total -----------------\n");
			(void)printf(" request  received  sent    query time\n");
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
		case XLOG:
			print_xlogstats();
			break;
		case TEMPFILE:
			print_tempfilestats();
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
		    break;
		case BGWRITER:
			previous_pgstatbgwriter = (struct pgstatbgwriter *) pg_malloc(sizeof(struct pgstatbgwriter));
			previous_pgstatbgwriter->checkpoints_timed = 0;
		    previous_pgstatbgwriter->checkpoints_req = 0;
		    previous_pgstatbgwriter->checkpoint_write_time = 0;
		    previous_pgstatbgwriter->checkpoint_sync_time = 0;
		    previous_pgstatbgwriter->buffers_checkpoint = 0;
		    previous_pgstatbgwriter->buffers_clean = 0;
		    previous_pgstatbgwriter->maxwritten_clean = 0;
		    previous_pgstatbgwriter->buffers_backend = 0;
		    previous_pgstatbgwriter->buffers_backend_fsync = 0;
		    previous_pgstatbgwriter->buffers_alloc = 0;
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
			previous_pgstatdatabase->blk_read_time = 0;
			previous_pgstatdatabase->blk_write_time = 0;
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
		    previous_pgstattable->n_live_tup = 0;
		    previous_pgstattable->n_dead_tup = 0;
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
			previous_pgstatstatement->calls = 0;
			previous_pgstatstatement->total_time = 0;
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
			previous_pgstatstatement->blk_read_time = 0;
			previous_pgstatstatement->blk_write_time = 0;
			break;
		case XLOG:
			previous_xlogstats = (struct xlogstats *) pg_malloc(sizeof(struct xlogstats));
			previous_xlogstats->location = pg_strdup("0/0");
			previous_xlogstats->locationdiff = 0;
			break;
		case TEMPFILE:
			// no initialization worth doing...
			break;
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
			errx(1, "ioctl");
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
	exit(1);
}

/*
 * Main function
 */
int
main(int argc, char **argv)
{
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

	/* Allocate the options struct */
	opts = (struct options *) pg_malloc(sizeof(struct options));

	/* Parse the options */
	get_opts(argc, argv);

	/* Connect to the database */
	conn = sql_conn();

	/* Get PostgreSQL version
	 * (if we are not connected to the pseudo pgBouncer database
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
	if (opts->stat == ARCHIVER && !backend_minimum_version(9, 4))
	{
		errx(1, "You need at least 9.4 for this statistic.");
	}

	if ((opts->stat == CONNECTION || opts->stat == XLOG) && !backend_minimum_version(9, 2))
	{
		errx(1, "You need at least 9.2 for this statistic.");
	}

	if (opts->stat == STATEMENT)
	{
		fetch_pgstatstatements_namespace();
		if (!opts->namespace)
		{
			errx(1, "Cannot find the pg_stat_statements extension.");
		}
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
