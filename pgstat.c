/*
 * pgstat, a PostgreSQL app to gather statistical informations
 * from a PostgreSQL database, and act like a vmstat tool.
 *
 * Guillaume Lelarge, guillaume@lelarge.info, 2015.
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

/*
 * Structs and enums
 */
typedef enum 
{
	ARCHIVER = 1,
	BGWRITER,
	CONNECTION,
	DATABASE,
	TABLE,
	TABLEIO,
	INDEX,
	FUNCTION,
	PBPOOLS
} stat_t;

/* these are the options structure for command line parameters */
struct options
{
	/* misc */
	bool		verbose;
	bool        dontredisplayheader;
	stat_t      stat;
	char	   *filter;

	/* connection parameters */
	char	   *dbname;
	char	   *hostname;
	char	   *port;
	char	   *username;

	/* version number */
	int			major;
	int			minor;

	/* frequency */
	int			interval;
	int			count;
};

/* pg_stat_bgwriter struct */
struct pgstatarchiver
{
	int archived_count;
    /*
	we don't put these columns here because it makes no sense to get a diff between the new and the old values
	? last_archived_wal;
    ? last_archived_time;
	*/
	int failed_count;
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
	int checkpoints_timed;
	int checkpoints_req;
	int checkpoint_write_time;
	int checkpoint_sync_time;
	int buffers_checkpoint;
	int buffers_clean;
	int maxwritten_clean;
	int buffers_backend;
	int buffers_backend_fsync;
	int buffers_alloc;
};

/* pg_stat_database struct */
struct pgstatdatabase
{
	/*
	we don't put numbackends here because it makes no sense to get a diff between the new and the old values
	int numbackends;
	*/
	int xact_commit;
	int xact_rollback;
	int blks_read;
	int blks_hit;
	int tup_returned;
	int tup_fetched;
	int tup_inserted;
	int tup_updated;
	int tup_deleted;
	int conflicts;
	int temp_files;
	int temp_bytes;
	int deadlocks;
	int blk_read_time;
	int blk_write_time;
};

/* pg_stat_all_tables struct */
struct pgstattable
{
    int seq_scan;
    int seq_tup_read;
    int idx_scan;
    int idx_tup_fetch;
    int n_tup_ins;
    int n_tup_upd;
    int n_tup_del;
    int n_tup_hot_upd;
    int n_live_tup;
    int n_dead_tup;
    int n_mod_since_analyze;
    /*
	we don't put the timestamps here because it makes no sense to get a diff between the new and the old values
	? last_vacuum;
    ? last_autovacuum;
    ? last_analyze;
    ? last_autoanalyze;
	*/
	int vacuum_count;
    int autovacuum_count;
    int analyze_count;
    int autoanalyze_count;
};

/* pg_statio_all_tables struct */
struct pgstattableio
{
	int heap_blks_read;
	int heap_blks_hit;
	int idx_blks_read;
	int idx_blks_hit;
	int toast_blks_read;
	int toast_blks_hit;
	int tidx_blks_read;
	int tidx_blks_hit;
};

/* pg_stat_all_indexes struct */
struct pgstatindex
{
	int idx_scan;
	int idx_tup_read;
	int idx_tup_fetch;
};

/* pg_stat_user_functions struct */
struct pgstatfunction
{
    int   calls;
    float total_time;
    float self_time;
};

/* pgBouncer pools stats struct */
struct pgbouncerpools
{
    int   cl_active;
    int   cl_waiting;
    int   sv_active;
    int   sv_idle;
    int   sv_used;
    int   sv_tested;
    int   sv_login;
    int   maxwait;
};

/*
 * Defines
 */
#define	PGSTAT_DEFAULT_LINES 20

/*
 * Global variables
 */
PGconn	   		      *conn;
struct options	      *opts;
extern char           *optarg;
struct pgstatarchiver *previous_pgstatarchiver;
struct pgstatbgwriter *previous_pgstatbgwriter;
struct pgstatdatabase *previous_pgstatdatabase;
struct pgstattable    *previous_pgstattable;
struct pgstattableio  *previous_pgstattableio;
struct pgstatindex    *previous_pgstatindex;
struct pgstatfunction *previous_pgstatfunction;
struct pgbouncerpools *previous_pgbouncerpools;
int                    hdrcnt = 0;
volatile sig_atomic_t  wresized;
static int             winlines = PGSTAT_DEFAULT_LINES;

/*
 * Function prototypes
 */
static void help(const char *progname);
void		get_opts(int, char **);
void	   *myalloc(size_t size);
char	   *mystrdup(const char *str);
PGconn	   *sql_conn(void);
void		print_pgstatarchiver(void);
void		print_pgstatbgwriter(void);
void        print_pgstatconnection(void);
void        print_pgstatdatabase(void);
void        print_pgstattable(void);
void		print_pgstattableio(void);
void        print_pgstatindex(void);
void        print_pgstatfunction(void);
void        print_pgbouncerpools(void);
void		fetch_version(void);
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
		   "  -f FILTER    include only this object\n"
		   "  -n           do not redisplay header\n"
		   "  -s STAT      stats to collect\n"
		   "  -v           verbose\n"
		   "  --help       show this help, then exit\n"
		   "  --version    output version information, then exit\n"
		   "\nConnection options:\n"
		   "  -h HOSTNAME  database server host or socket directory\n"
		   "  -p PORT      database server port number\n"
		   "  -U USER      connect as specified database user\n"
		   "  -d DBNAME    database to connect to\n"
		   "\nThe default stat is pg_stat_bgwriter, but you can change it with the -s command line option,\n"
		   "and one of its value (STAT):\n"
		   "  * archiver   for pg_stat_archiver\n"
		   "  * bgwriter   for pg_stat_bgwriter\n"
		   "  * connection (only for > 9.1)\n"
		   "  * database   for pg_stat_database\n"
		   "  * table      for pg_stat_all_tables\n"
		   "  * tableio    for pg_statio_all_tables\n"
		   "  * index      for pg_stat_all_indexes\n"
		   "  * function   for pg_stat_user_function\n"
		   "  * pbpools    for pgBouncer pools statistics\n\n"
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
	opts->stat = BGWRITER;
	opts->filter = NULL;
	opts->dbname = NULL;
	opts->hostname = NULL;
	opts->port = NULL;
	opts->username = NULL;
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
			puts("pgstats (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	/* get opts */
	while ((c = getopt(argc, argv, "h:p:U:d:f:n:s:v")) != -1)
	{
		switch (c)
		{
				/* specify the database */
			case 'd':
				opts->dbname = mystrdup(optarg);
				break;

				/* specify the filter */
			case 'f':
				opts->filter = mystrdup(optarg);
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
				if (!strcmp(optarg, "pbpools"))
				{
					opts->stat = PBPOOLS;
				}
				break;

				/* host to connect to */
			case 'h':
				opts->hostname = mystrdup(optarg);
				break;

				/* port to connect to on remote host */
			case 'p':
				opts->port = mystrdup(optarg);
				break;

				/* username */
			case 'U':
				opts->username = mystrdup(optarg);
				break;

			default:
				err(1, "Try \"%s --help\" for more information.\n", progname);
		}
	}
	if (optind < argc)
	{
		opts->interval = atoi(argv[optind]);
		optind++;
	}
	if (optind < argc)
	{
		opts->count = atoi(argv[optind]);
	}

	if (opts->dbname == NULL)
	{
		opts->dbname = "postgres";
	}
}

/*
 * Handle memory allocation
 * and error out if none is available
 */
void *
myalloc(size_t size)
{
	void	   *ptr = malloc(size);

	if (!ptr)
	{
		err(1, "out of memory");
	}
	return ptr;
}

/*
 * Duplicate a string
 * and error out if there's no memory left
 */
char *
mystrdup(const char *str)
{
	char	   *result = strdup(str);

	if (!result)
	{
		err(1, "out of memory");
	}
	return result;
}

/*
 * Establish the PostgreSQL connection
 */
PGconn *
sql_conn()
{
	PGconn	   *my_conn;
	char	   *password = NULL;
	bool		new_pass;

	/*
	 * Start the connection.  Loop until we have a password if requested by
	 * backend.
	 */
	do
	{
		new_pass = false;
		my_conn = PQsetdbLogin(opts->hostname,
							opts->port,
							NULL,		/* options */
							NULL,		/* tty */
							opts->dbname,
							opts->username,
							password);
		if (!my_conn)
		{
			errx(1, "could not connect to database %s\n", opts->dbname);
		}

#if PG_VERSION_NUM >= 80200
		if (PQstatus(my_conn) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(conn) &&
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
		PQfinish(my_conn);
		errx(1, "could not connect to database %s: %s", opts->dbname, PQerrorMessage(my_conn));
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
	char		sql[1024];
	PGresult   *res;
	int			nrows;
	int			row, column;

	int archived_count;
	int failed_count;

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
		err(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
		archived_count = atoi(PQgetvalue(res, row, column++));
		failed_count = atoi(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf("   %6d   %6d\n",
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
	char		sql[1024];
	PGresult   *res;
	int			nrows;
	int			row, column;

	int checkpoints_timed = 0;
	int checkpoints_req = 0;
	int checkpoint_write_time = 0;
	int checkpoint_sync_time = 0;
	int buffers_checkpoint = 0;
	int buffers_clean = 0;
	int maxwritten_clean = 0;
	int buffers_backend = 0;
	int buffers_backend_fsync = 0;
	int buffers_alloc = 0;

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
		err(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
		checkpoints_timed = atoi(PQgetvalue(res, row, column++));
		checkpoints_req = atoi(PQgetvalue(res, row, column++));
		if (backend_minimum_version(9, 2))
		{
			checkpoint_write_time = atoi(PQgetvalue(res, row, column++));
			checkpoint_sync_time = atoi(PQgetvalue(res, row, column++));
		}
		buffers_checkpoint = atoi(PQgetvalue(res, row, column++));
		buffers_clean = atoi(PQgetvalue(res, row, column++));
		maxwritten_clean = atoi(PQgetvalue(res, row, column++));
		buffers_backend = atoi(PQgetvalue(res, row, column++));
		if (backend_minimum_version(9, 1))
		{
			buffers_backend_fsync = atoi(PQgetvalue(res, row, column++));
		}
		buffers_alloc = atoi(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf(" %6d    %6d     %6d    %6d       %6d %6d  %6d %6d         %4d            %2d\n",
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
	char		sql[1024];
	PGresult   *res;
	int			nrows;
	int			row, column;

	int total = 0;
	int active = 0;
	int lockwaiting = 0;
	int idleintransaction = 0;
	int idle = 0;

	snprintf(sql, sizeof(sql),
			 "SELECT count(*) AS total, "
             "  sum(CASE WHEN state='active' and not waiting THEN 1 ELSE 0 end) AS active, "
             "  sum(CASE WHEN waiting THEN 1 ELSE 0 end) AS lockwaiting, "
             "  sum(CASE WHEN state='idle in transaction' THEN 1 ELSE 0 end) AS idleintransaction, "
             "  sum(CASE WHEN state='idle' THEN 1 ELSE 0 end) AS idle "
             "FROM pg_stat_activity");

	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		warnx("pgstats: query failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		err(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		total = atoi(PQgetvalue(res, row, column++));
		active = atoi(PQgetvalue(res, row, column++));
		lockwaiting = atoi(PQgetvalue(res, row, column++));
		idleintransaction = atoi(PQgetvalue(res, row, column++));
		idle = atoi(PQgetvalue(res, row, column++));

		/* printing the actual values for once */
		(void)printf("    %4d     %4d          %4d                  %4d   %4d  \n",
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
	char		sql[1024];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

	int numbackends = 0;
	int xact_commit = 0;
	int xact_rollback = 0;
	int blks_read = 0;
	int blks_hit = 0;
	int tup_returned = 0;
	int tup_fetched = 0;
	int tup_inserted = 0;
	int tup_updated = 0;
	int tup_deleted = 0;
	int conflicts = 0;
	int temp_files = 0;
	int temp_bytes = 0;
	int deadlocks = 0;
	int blk_read_time = 0;
	int blk_write_time = 0;


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

		paramValues[0] = mystrdup(opts->filter);

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
		err(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
		numbackends = atoi(PQgetvalue(res, row, column++));
		xact_commit = atoi(PQgetvalue(res, row, column++));
		xact_rollback = atoi(PQgetvalue(res, row, column++));
		blks_read = atoi(PQgetvalue(res, row, column++));
		blks_hit = atoi(PQgetvalue(res, row, column++));
		if (backend_minimum_version(8, 3))
		{
			tup_returned = atoi(PQgetvalue(res, row, column++));
			tup_fetched = atoi(PQgetvalue(res, row, column++));
			tup_inserted = atoi(PQgetvalue(res, row, column++));
			tup_updated = atoi(PQgetvalue(res, row, column++));
			tup_deleted = atoi(PQgetvalue(res, row, column++));
		}
		if (backend_minimum_version(9, 1))
		{
			conflicts = atoi(PQgetvalue(res, row, column++));
		}
		if (backend_minimum_version(9, 2))
		{
			temp_files = atoi(PQgetvalue(res, row, column++));
			temp_bytes = atoi(PQgetvalue(res, row, column++));
			deadlocks = atoi(PQgetvalue(res, row, column++));
			blk_read_time = atoi(PQgetvalue(res, row, column++));
			blk_write_time = atoi(PQgetvalue(res, row, column++));
		}

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf("      %4d      %6d   %6d   %6d %6d    %6d     %6d   %6d %6d %6d %6d %6d   %6d %9d   %9d %9d\n",
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
	char		sql[1024];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

    int seq_scan = 0;
    int seq_tup_read = 0;
    int idx_scan = 0;
    int idx_tup_fetch = 0;
    int n_tup_ins = 0;
    int n_tup_upd = 0;
    int n_tup_del = 0;
    int n_tup_hot_upd = 0;
    int n_live_tup = 0;
    int n_dead_tup = 0;
    int n_mod_since_analyze = 0;
    int vacuum_count = 0;
    int autovacuum_count = 0;
    int analyze_count = 0;
    int autoanalyze_count = 0;

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

		paramValues[0] = mystrdup(opts->filter);

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
		err(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
        seq_scan = atoi(PQgetvalue(res, row, column++));
        seq_tup_read = atoi(PQgetvalue(res, row, column++));
        idx_scan = atoi(PQgetvalue(res, row, column++));
        idx_tup_fetch = atoi(PQgetvalue(res, row, column++));
        n_tup_ins = atoi(PQgetvalue(res, row, column++));
        n_tup_upd = atoi(PQgetvalue(res, row, column++));
        n_tup_del = atoi(PQgetvalue(res, row, column++));
		if (backend_minimum_version(8, 3))
		{
	        n_tup_hot_upd = atoi(PQgetvalue(res, row, column++));
	        n_live_tup = atoi(PQgetvalue(res, row, column++));
	        n_dead_tup = atoi(PQgetvalue(res, row, column++));
    	}
		if (backend_minimum_version(9, 4))
		{
	        n_mod_since_analyze = atoi(PQgetvalue(res, row, column++));
    	}
		if (backend_minimum_version(9, 1))
		{
	        vacuum_count = atoi(PQgetvalue(res, row, column++));
	        autovacuum_count = atoi(PQgetvalue(res, row, column++));
	        analyze_count = atoi(PQgetvalue(res, row, column++));
	        autoanalyze_count = atoi(PQgetvalue(res, row, column++));
		}

		/* printing the diff... note that the first line will be the current value, rather than the diff */
		(void)printf(" %6d  %6d   %6d  %6d      %6d %6d %6d %6d %6d %6d %6d  %6d     %6d  %6d      %6d\n",
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
	char		sql[1024];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

	int heap_blks_read = 0;
	int heap_blks_hit = 0;
	int idx_blks_read = 0;
	int idx_blks_hit = 0;
	int toast_blks_read = 0;
	int toast_blks_hit = 0;
	int tidx_blks_read = 0;
	int tidx_blks_hit = 0;

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

		paramValues[0] = mystrdup(opts->filter);

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
		err(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
		heap_blks_read = atoi(PQgetvalue(res, row, column++));
		heap_blks_hit = atoi(PQgetvalue(res, row, column++));
		idx_blks_read = atoi(PQgetvalue(res, row, column++));
		idx_blks_hit = atoi(PQgetvalue(res, row, column++));
		toast_blks_read = atoi(PQgetvalue(res, row, column++));
		toast_blks_hit = atoi(PQgetvalue(res, row, column++));
		tidx_blks_read = atoi(PQgetvalue(res, row, column++));
		tidx_blks_hit = atoi(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf(" %6d    %6d    %7d   %7d    %7d    %7d     %9d %9d\n",
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
	char		sql[1024];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

	int idx_scan = 0;
	int idx_tup_read = 0;
	int idx_tup_fetch = 0;

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

		paramValues[0] = mystrdup(opts->filter);

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
		err(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
        idx_scan = atoi(PQgetvalue(res, row, column++));
        idx_tup_read = atof(PQgetvalue(res, row, column++));
        idx_tup_fetch = atof(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf(" %8d   %7d %7d\n",
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
	char		sql[1024];
	PGresult   *res;
    const char *paramValues[1];
	int			nrows;
	int			row, column;

    int calls = 0;
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

		paramValues[0] = mystrdup(opts->filter);

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
		err(1, "pgstats: query was: %s", sql);
	}

	/* get the number of fields */
	nrows = PQntuples(res);

	/* for each row, dump the information */
	/* this is stupid, a simple if would do the trick, but it will help for other cases */
	for (row = 0; row < nrows; row++)
	{
		column = 0;

		/* getting new values */
        calls = atoi(PQgetvalue(res, row, column++));
        total_time = atof(PQgetvalue(res, row, column++));
        self_time = atof(PQgetvalue(res, row, column++));

		/* printing the diff...
		 * note that the first line will be the current value, rather than the diff */
		(void)printf(" %9d   %5f    %5f\n",
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
 * Dump all pgBouncer pools stats.
 */
void
print_pgbouncerpools()
{
	char		sql[1024];
	PGresult   *res;
	int			nrows;
	int			row, column;

    int cl_active = 0;
    int cl_waiting = 0;
    int sv_active = 0;
    int sv_idle = 0;
    int sv_used = 0;
    int sv_tested = 0;
    int sv_login = 0;
    int maxwait = 0;

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
		err(1, "pgstats: query was: %s", sql);
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
		cl_active += atoi(PQgetvalue(res, row, column++));
		cl_waiting += atoi(PQgetvalue(res, row, column++));
		sv_active += atoi(PQgetvalue(res, row, column++));
		sv_idle += atoi(PQgetvalue(res, row, column++));
		sv_used += atoi(PQgetvalue(res, row, column++));
		sv_tested += atoi(PQgetvalue(res, row, column++));
		sv_login += atoi(PQgetvalue(res, row, column++));
		maxwait += atoi(PQgetvalue(res, row, column++));
	}

	/* printing the diff...
	 * note that the first line will be the current value, rather than the diff */
	(void)printf(" %6d   %6d    %6d  %6d  %6d  %6d  %6d    %6d\n",
		cl_active - previous_pgbouncerpools->cl_active,
		cl_waiting - previous_pgbouncerpools->cl_waiting,
		sv_active - previous_pgbouncerpools->sv_active,
		sv_idle - previous_pgbouncerpools->sv_idle,
		sv_used - previous_pgbouncerpools->sv_used,
		sv_tested - previous_pgbouncerpools->sv_tested,
		sv_login - previous_pgbouncerpools->sv_login,
		maxwait - previous_pgbouncerpools->maxwait
	    );

	/* setting the new old value */
	previous_pgbouncerpools->cl_active = cl_active;
	previous_pgbouncerpools->cl_waiting = cl_waiting;
	previous_pgbouncerpools->sv_active = sv_active;
	previous_pgbouncerpools->sv_idle = sv_idle;
	previous_pgbouncerpools->sv_used = sv_used;
	previous_pgbouncerpools->sv_tested = sv_tested;
	previous_pgbouncerpools->sv_login = sv_login;
	previous_pgbouncerpools->maxwait = maxwait;

	/* cleanup */
	PQclear(res);
}

/*
 * Fetch PostgreSQL major and minor numbers
 */
void
fetch_version()
{
	char		sql[1024];
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
		err(1, "pgstats: query was: %s", sql);
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
 * Print the right header according to the stats mode
 */
void
print_header(void)
{
	switch(opts->stat)
	{
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
		case PBPOOLS:
			(void)printf("---- client -----  ---------------- server ----------------  -- misc --\n");
			(void)printf(" active  waiting    active    idle    used  tested   login    maxwait\n");
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
		case PBPOOLS:
			print_pgbouncerpools();
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
		case ARCHIVER:
			previous_pgstatarchiver = (struct pgstatarchiver *) myalloc(sizeof(struct pgstatarchiver));
			previous_pgstatarchiver->archived_count = 0;
		    previous_pgstatarchiver->failed_count = 0;
		    break;
		case BGWRITER:
			previous_pgstatbgwriter = (struct pgstatbgwriter *) myalloc(sizeof(struct pgstatbgwriter));
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
			previous_pgstatdatabase = (struct pgstatdatabase *) myalloc(sizeof(struct pgstatdatabase));
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
			previous_pgstattable = (struct pgstattable *) myalloc(sizeof(struct pgstattable));
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
			previous_pgstattableio = (struct pgstattableio *) myalloc(sizeof(struct pgstattableio));
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
			previous_pgstatindex = (struct pgstatindex *) myalloc(sizeof(struct pgstatindex));
			previous_pgstatindex->idx_scan = 0;
			previous_pgstatindex->idx_tup_read = 0;
			previous_pgstatindex->idx_tup_fetch = 0;
			break;
		case FUNCTION:
			previous_pgstatfunction = (struct pgstatfunction *) myalloc(sizeof(struct pgstatfunction));
		    previous_pgstatfunction->calls = 0;
		    previous_pgstatfunction->total_time = 0;
		    previous_pgstatfunction->self_time = 0;
			break;
		case PBPOOLS:
			previous_pgbouncerpools = (struct pgbouncerpools *) myalloc(sizeof(struct pgbouncerpools));
    		previous_pgbouncerpools->cl_active = 0;
    		previous_pgbouncerpools->cl_waiting = 0;
    		previous_pgbouncerpools->sv_active = 0;
    		previous_pgbouncerpools->sv_idle = 0;
    		previous_pgbouncerpools->sv_used = 0;
    		previous_pgbouncerpools->sv_tested = 0;
    		previous_pgbouncerpools->sv_login = 0;
    		previous_pgbouncerpools->maxwait = 0;
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
			err(1, "ioctl");
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
#if PG_VERSION_NUM >= 90200
	pqsignal(SIGCONT, needhdr);
	pqsignal(SIGINT, quit_properly);
#else
	signal(SIGCONT, needhdr);
	signal(SIGINT, quit_properly);
#endif

	/*
	 * If our standard output is a tty, then install a SIGWINCH handler
	 * and set wresized so that our first iteration through the main
	 * vmstat loop will peek at the terminal's current rows to find out
	 * how many lines can fit in a screenful of output.
	 */
	if (isatty(fileno(stdout)) != 0) {
		wresized = 1;
		(void)signal(SIGWINCH, needresize);
	} else {
		wresized = 0;
		winlines = PGSTAT_DEFAULT_LINES;
	}

	opts = (struct options *) myalloc(sizeof(struct options));

	/* parse the opts */
	get_opts(argc, argv);

	/* connect to the database */
	conn = sql_conn();

	/* get version */
	if (opts->stat != PBPOOLS)
	{
		fetch_version();
	}

	if (opts->stat == ARCHIVER && !backend_minimum_version(9, 4))
	{
		errx(1, "You need at least 9.4 for this statistic.");
	}

	if (opts->stat == CONNECTION && !backend_minimum_version(9, 2))
	{
		errx(1, "You need at least 9.2 for this statistic.");
	}

    /* allocate and initialize statistics struct */
    allocate_struct();

	/* grap cluster stats info */
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
