/*
 * pgcsvstat, a PostgreSQL app to gather statistical informations
 * from a PostgreSQL database.
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Guillaume Lelarge, guillaume@lelarge.info, 2011-2021.
 *
 * pgstats/pgcsvstat.c
 */


/*
 * Headers
 */
#include "postgres_fe.h"
#include "common/string.h"

#include <err.h>
#include <sys/stat.h>

#include <unistd.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

extern char *optarg;

#include "libpq-fe.h"

/*
 * Defines
 */
#define PGCSVSTAT_VERSION "1.2.0"

/* these are the opts structures for command line params */
struct options
{
	bool		quiet;
	bool		nodb;
	char	   *directory;

	char	   *dbname;
	char	   *hostname;
	char	   *port;
	char	   *username;

	int			major;
	int			minor;
};

/* global variables */
struct options	*opts;
PGconn	   		*conn;

/* function prototypes */
static void help(const char *progname);
void		get_opts(int, char **);
void	   *myalloc(size_t size);
char	   *mystrdup(const char *str);
PGconn	   *sql_conn(void);
int			sql_exec(const char *sql, const char *filename, bool quiet);
void        sql_exec_dump_pgstatactivity(void);
void        sql_exec_dump_pgstatarchiver(void);
void		sql_exec_dump_pgstatbgwriter(void);
void		sql_exec_dump_pgstatdatabase(void);
void        sql_exec_dump_pgstatdatabaseconflicts(void);
void        sql_exec_dump_pgstatreplication(void);
void        sql_exec_dump_pgstatreplicationslots(void);
void        sql_exec_dump_pgstatslru(void);
void        sql_exec_dump_pgstatsubscription(void);
void        sql_exec_dump_pgstatwal(void);
void		sql_exec_dump_pgstatalltables(void);
void		sql_exec_dump_pgstatallindexes(void);
void		sql_exec_dump_pgstatioalltables(void);
void		sql_exec_dump_pgstatioallindexes(void);
void		sql_exec_dump_pgstatioallsequences(void);
void		sql_exec_dump_pgstatuserfunctions(void);
void		sql_exec_dump_pgclass_size(void);
void        sql_exec_dump_pgstatstatements(void);
void		sql_exec_dump_xlog_stat(void);
void		fetch_version(void);
bool		check_superuser(void);
bool		backend_minimum_version(int major, int minor);
bool        backend_has_pgstatstatements(void);


/* function to parse command line options and check for some usage errors. */
void
get_opts(int argc, char **argv)
{
	int			c;
	const char *progname;

	progname = get_progname(argv[0]);

	/* set the defaults */
	opts->quiet = false;
	opts->nodb = false;
	opts->directory = NULL;
	opts->dbname = NULL;
	opts->hostname = NULL;
	opts->port = NULL;
	opts->username = NULL;

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			help(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pgcsvstats " PGCSVSTAT_VERSION " (compiled with PostgreSQL " PG_VERSION ")");
			exit(0);
		}
	}

	/* get opts */
	while ((c = getopt(argc, argv, "h:p:U:d:D:q")) != -1)
	{
		switch (c)
		{
				/* specify the database */
			case 'd':
				opts->dbname = mystrdup(optarg);
				break;

				/* specify the directory */
			case 'D':
				opts->directory = mystrdup(optarg);
				break;

				/* don't show headers */
			case 'q':
				opts->quiet = true;
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
				fprintf(stderr, "Try \"%s --help\" for more information.\n", progname);
				exit(1);
		}
	}
}


static void
help(const char *progname)
{
	printf("%s gathers statistics from a PostgreSQL database.\n\n"
		   "Usage:\n"
		   "  %s [OPTIONS]...\n"
		   "\nGeneral options:\n"
		   "  -d DBNAME    database to connect to\n"
		   "  -D DIRECTORY directory for stats files (defaults to current)\n"
		   "  -q           quiet\n"
		   "  --help       show this help, then exit\n"
		   "  --version    output version information, then exit\n"
		   "\nConnection options:\n"
		   "  -h HOSTNAME  database server host or socket directory\n"
		   "  -p PORT      database server port number\n"
		   "  -U USER      connect as specified database user\n"
		   "\nThe default action is to create CSV files for each report.\n\n"
		   "Report bugs to <guillaume@lelarge.info>.\n",
		   progname, progname);
}

void *
myalloc(size_t size)
{
	void	   *ptr = malloc(size);

	if (!ptr)
	{
		fprintf(stderr, "out of memory");
		exit(1);
	}
	return ptr;
}

char *
mystrdup(const char *str)
{
	char	   *result = strdup(str);

	if (!result)
	{
		fprintf(stderr, "out of memory");
		exit(1);
	}
	return result;
}

/* establish connection with database. */
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
        values[5] = "pgcsvstat";
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
		sprintf(dns, "%s", "fallback_application_name='pgcsvstat' ");

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
			!password)
		{
			PQfinish(my_conn);
#if PG_VERSION_NUM < 100000
			password = simple_prompt("Password: ", 100, false);
#elif PG_VERSION_NUM < 140000
			simple_prompt("Password: ", password, 100, false);
#else
			password = simple_prompt("Password: ", false);
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
		errx(1, "could not connect to database %s: %s", opts->dbname, message);
		PQfinish(my_conn);
	}

	/* return the conn if good */
	return my_conn;
}

/*
 * Actual code to make call to the database and print the output data.
 */
int
sql_exec(const char *query, const char* filename, bool quiet)
{
	PGresult   *res;
	FILE	   *fdcsv;
	struct stat st;

	int			nfields;
	int			nrows;
	int			i,
				j;
	int			size;

	/* open the csv file */
	fdcsv = fopen(filename, "a");
    if (!fdcsv)
    {
        fprintf(stderr, "pgcsvstat: fopen failed: %d\n", errno);
        fprintf(stderr, "pgcsvstat: filename was: %s\n", filename);
        
        PQfinish(conn);
        exit(-1);
    }

	/* get size of file */
	stat(filename, &st);
	size = st.st_size;

	/* make the call */
	res = PQexec(conn, query);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		fprintf(stderr, "pgcsvstat: query failed: %s\n", PQerrorMessage(conn));
		fprintf(stderr, "pgcsvstat: query was: %s\n", query);

		PQclear(res);
		PQfinish(conn);
		exit(-1);
	}

	/* get the number of fields */
	nrows = PQntuples(res);
	nfields = PQnfields(res);

	/* print a header */
	if (!quiet && size == 0)
	{
		for (j = 0; j < nfields; j++)
		{
			fprintf(fdcsv, "%s", PQfname(res, j));
            if (j < nfields - 1)
			    fprintf(fdcsv, ";");
		}
		fprintf(fdcsv, "\n");
	}

	/* for each row, dump the information */
	for (i = 0; i < nrows; i++)
	{
		for (j = 0; j < nfields; j++)
        {
			fprintf(fdcsv, "%s", PQgetvalue(res, i, j));
            if (j < nfields - 1)
			    fprintf(fdcsv, ";");
		}
		fprintf(fdcsv, "\n");
	}

	/* cleanup */
	PQclear(res);

	/* close the csv file */
	fclose(fdcsv);

	return 0;
}

/*
 * Dump all activities.
 */
void
sql_exec_dump_pgstatactivity()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), datid, datname, %s, %s"
             "usesysid, usename, %s%s%s%s%s"
			 "date_trunc('seconds', query_start) AS query_start, "
             "%s%s%s%s%s%s%s state "
             "FROM pg_stat_activity "
			 "ORDER BY %s",
		backend_minimum_version(9, 2) ? "pid" : "procpid",
		backend_minimum_version(13, 0) ? "leader_pid, " : "",
		backend_minimum_version(9, 0) ? "application_name, " : "",
		backend_minimum_version(8, 1) ? "client_addr, " : "",
		backend_minimum_version(9, 1) ? "client_hostname, " : "",
		backend_minimum_version(8, 1) ? "client_port, date_trunc('seconds', backend_start) AS backend_start, " : "",
        backend_minimum_version(8, 3) ? "date_trunc('seconds', xact_start) AS xact_start, " : "",
        backend_minimum_version(9, 2) ? "state_change, " : "",
        backend_minimum_version(9, 6) ? "wait_event_type, wait_event, " : backend_minimum_version(8, 2) ? "waiting, " : "",
        backend_minimum_version(9, 4) ? "backend_xid, " : "",
        backend_minimum_version(9, 4) ? "backend_xmin, " : "",
        backend_minimum_version(14, 0) ? "query_id, " : "",
        backend_minimum_version(9, 2) ? "query, " : "current_query,",
		backend_minimum_version(10, 0) ? "backend_type, " : "",
		backend_minimum_version(9, 2) ? "pid" : "procpid");   // the last one is for the ORDER BY
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_activity.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all bgwriter stats.
 */
void
sql_exec_dump_pgstatbgwriter()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), checkpoints_timed, "
             "checkpoints_req, %sbuffers_checkpoint, buffers_clean, "
			 "maxwritten_clean, buffers_backend, %sbuffers_alloc%s "
             "FROM pg_stat_bgwriter ",
		backend_minimum_version(9, 2) ? "checkpoint_write_time, checkpoint_sync_time, " : "",
		backend_minimum_version(9, 1) ? "buffers_backend_fsync, " : "",
		backend_minimum_version(9, 1) ? ", date_trunc('seconds', stats_reset) AS stats_reset " : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_bgwriter.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all archiver stats.
 */
void
sql_exec_dump_pgstatarchiver()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), archived_count, "
             "last_archived_wal, date_trunc('seconds', last_archived_time) AS last_archived_time, "
			 "failed_count, "
             "last_failed_wal, date_trunc('seconds', last_failed_time) AS last_failed_time, "
		     "date_trunc('seconds', stats_reset) AS stats_reset "
             "FROM pg_stat_archiver ");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_archiver.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all databases stats.
 * to be fixed wrt v14
 */
void
sql_exec_dump_pgstatdatabase()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), datid, datname, "
             "numbackends, xact_commit, xact_rollback, blks_read, blks_hit"
             "%s%s%s%s%s "
             "FROM pg_stat_database "
             "ORDER BY datname",
		backend_minimum_version(8, 3) ? ", tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted" : "",
		backend_minimum_version(9, 1) ? ", conflicts, date_trunc('seconds', stats_reset) AS stats_reset" : "",
		backend_minimum_version(9, 2) ? ", temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time" : "",
		backend_minimum_version(12, 0) ? ", checksum_failures, checksum_last_failure" : "",
		backend_minimum_version(14, 0) ? ", session_time, active_time, idle_in_transaction_time, sessions, sessions_abandoned, sessions_fatal, sessions_killed" : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_database.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all databases conflicts stats.
 */
void
sql_exec_dump_pgstatdatabaseconflicts()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), * "
             "FROM pg_stat_database_conflicts "
             "ORDER BY datname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_database_conflicts.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all replication stats.
 */
void
sql_exec_dump_pgstatreplication()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), %s, usesysid, usename, "
             "application_name, client_addr, client_hostname, client_port, "
             "date_trunc('seconds', backend_start) AS backend_start, %sstate, "
             "%s AS master_location, %s%s"
             "sync_priority, "
             "sync_state%s "
             "FROM pg_stat_replication "
             "ORDER BY application_name",
		backend_minimum_version(9, 2) ? "pid" : "procpid",
		backend_minimum_version(9, 4) ? "backend_xmin, " : "",
		backend_minimum_version(10, 0) ? "pg_current_wal_lsn()" : "pg_current_xlog_location()",
		backend_minimum_version(10, 0) ? "sent_lsn, write_lsn, flush_lsn, replay_lsn, " :
		     "sent_location, write_location, flush_location, replay_location, ",
		backend_minimum_version(10, 0) ? "write_lag, flush_lag, replay_lag, " : "",
		backend_minimum_version(12, 0) ? ", reply_time" : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_replication.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all replication slots stats.
 */
void
sql_exec_dump_pgstatreplicationslots()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), slot_name, "
			 "spill_txns, spill_count, spill_bytes, "
			 "stream_txns, stream_count, stream_bytes, "
			 "total_txns, total_bytes, "
			 "date_trunc('seconds', stats_reset) AS stats_reset "
             "FROM pg_stat_replication_slots "
             "ORDER BY slot_name");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_replication_slots.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all SLRU stats.
 */
void
sql_exec_dump_pgstatslru()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), name, "
			 "blks_zeroed, blks_hit, blks_read, blks_written, blks_exists, "
			 "flushes, truncates, "
			 "date_trunc('seconds', stats_reset) AS stats_reset "
             "FROM pg_stat_slru "
             "ORDER BY name");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_slru.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all subscriptions stats.
 */
void
sql_exec_dump_pgstatsubscription()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), subid, subname, "
			 "pid, relid, received_lsn, "
			 "date_trunc('seconds', last_msg_send_time) AS last_msg_send_time, "
			 "date_trunc('seconds', last_msg_receipt_time) AS last_msg_receipt_time, "
			 "latest_end_lsn, date_trunc('seconds', latest_end_time) AS latest_end_time "
             "FROM pg_stat_subscription "
             "ORDER BY subid");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_subscription.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all WAL stats.
 */
void
sql_exec_dump_pgstatwal()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), "
			 "wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write, "
			 "wal_sync, wal_write_time, wal_sync_time, "
			 "date_trunc('seconds', stats_reset) AS stats_reset "
             "FROM pg_stat_wal");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_wal.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all tables stats.
 */
void
sql_exec_dump_pgstatalltables()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), relid, schemaname, relname, "
             "seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, "
             "n_tup_upd, n_tup_del"
             "%s"
			 "%s"
			 "%s"
			 "%s"
			 "%s"
             " FROM pg_stat_all_tables "
	     "WHERE schemaname <> 'information_schema' "
	     "ORDER BY schemaname, relname",
		backend_minimum_version(8, 3) ? ", n_tup_hot_upd, n_live_tup, n_dead_tup" : "",
        backend_minimum_version(9, 4) ? ", n_mod_since_analyze" : "",
        backend_minimum_version(13, 0) ? ", n_ins_since_vacuum" : "",
        backend_minimum_version(8, 2) ? ", date_trunc('seconds', last_vacuum) AS last_vacuum, date_trunc('seconds', last_autovacuum) AS last_autovacuum, date_trunc('seconds',last_analyze) AS last_analyze, date_trunc('seconds',last_autoanalyze) AS last_autoanalyze" : "",
        backend_minimum_version(9, 1) ? ", vacuum_count, autovacuum_count, analyze_count, autoanalyze_count" : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_all_tables.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all indexes stats.
 */
void
sql_exec_dump_pgstatallindexes()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_stat_all_indexes "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, relname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_all_indexes.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all tables IO stats.
 */
void
sql_exec_dump_pgstatioalltables()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_statio_all_tables "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, relname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_statio_all_tables.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all indexes IO stats.
 */
void
sql_exec_dump_pgstatioallindexes()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_statio_all_indexes "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, relname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_statio_all_indexes.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all sequences IO stats.
 */
void
sql_exec_dump_pgstatioallsequences()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_statio_all_sequences "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, relname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_statio_all_sequences.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all functions stats.
 */
void
sql_exec_dump_pgstatuserfunctions()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_stat_user_functions "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, funcname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_user_functions.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all size class stats.
 */
void
sql_exec_dump_pgclass_size()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), n.nspname, c.relname, c.relkind, c.reltuples, c.relpages%s "
			 "FROM pg_class c, pg_namespace n "
			 "WHERE n.oid=c.relnamespace AND n.nspname <> 'information_schema' "
			 "ORDER BY n.nspname, c.relname",
		backend_minimum_version(8, 1) ? ", pg_relation_size(c.oid)" : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_class_size.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all statements stats.
 * to be fixed wrt v14
 */
void
sql_exec_dump_pgstatstatements()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
			 "SELECT date_trunc('seconds', now()), r.rolname, d.datname, "
             "%sregexp_replace(query, E'\n', ' ', 'g') as query, %scalls, %s, rows, "
             "shared_blks_hit, shared_blks_read, shared_blks_written, "
             "local_blks_hit, local_blks_read, local_blks_written, "
             "temp_blks_read, temp_blks_written%s "
			 "FROM pg_stat_statements q, pg_database d, pg_roles r "
			 "WHERE q.userid=r.oid and q.dbid=d.oid "
			 "ORDER BY r.rolname, d.datname",
		backend_minimum_version(14, 0) ? "toplevel, queryid, " : "",
		backend_minimum_version(13, 0) ? "plans, total_plan_time, min_plan_time, max_plan_time, mean_plan_time, stddev_plan_time, " : "",
		backend_minimum_version(13, 0) ? "total_exec_time, min_exec_time, max_exec_time, mean_exec_time, stddev_exec_time" : "total_time",
		backend_minimum_version(14, 0) ? ", wal_records, wal_fpi, wal_bytes" : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_statements.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}

/*
 * Dump all xlog stats.
 */
void
sql_exec_dump_xlog_stat()
{
	char		query[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query),
		backend_minimum_version(10, 0)
		?
			 "SELECT date_trunc('seconds', now()), pg_walfile_name(pg_current_wal_lsn())=pg_ls_dir AS current, pg_ls_dir AS filename, "
			 "(SELECT modification FROM pg_stat_file('pg_wal/'||pg_ls_dir)) AS modification_timestamp "
			 "FROM pg_ls_dir('pg_wal') "
			 "WHERE pg_ls_dir ~ E'^[0-9A-F]{24}' "
			 "ORDER BY pg_ls_dir"
		:
			 "SELECT date_trunc('seconds', now()), pg_xlogfile_name(pg_current_xlog_location())=pg_ls_dir AS current, pg_ls_dir AS filename, "
			 "(SELECT modification FROM pg_stat_file('pg_xlog/'||pg_ls_dir)) AS modification_timestamp "
			 "FROM pg_ls_dir('pg_xlog') "
			 "WHERE pg_ls_dir ~ E'^[0-9A-F]{24}' "
			 "ORDER BY pg_ls_dir");
	snprintf(filename, sizeof(filename),
			 "%s/pg_xlog_stat.csv", opts->directory);

	sql_exec(query, filename, opts->quiet);
}


/*
 * Fetch PostgreSQL major and minor numbers
 */
void
fetch_version()
{
	char		query[1024];
	PGresult   *res;

	/* get the oid and database name from the system pg_database table */
	snprintf(query, sizeof(query), "SELECT version()");

	/* make the call */
	res = PQexec(conn, query);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		fprintf(stderr, "pgcsvstat: query failed: %s\n", PQerrorMessage(conn));
		fprintf(stderr, "pgcsvstat: query was: %s\n", query);

		PQclear(res);
		PQfinish(conn);
		exit(-1);
	}

	/* get the only row, and parse it to get major and minor numbers */
	sscanf(PQgetvalue(res, 0, 0), "%*s %d.%d", &(opts->major), &(opts->minor));

	/* print version */
	if (!opts->quiet)
	    printf("Detected release: %d.%d\n", opts->major, opts->minor);

	/* cleanup */
	PQclear(res);
}


/*
 * Check if user has the superuser attribute
 */
bool
check_superuser()
{
	PGresult   *res;
	char		sql[1024];
    bool        is_superuser = false;

	/* get the oid and database name from the system pg_database table */
	snprintf(sql, sizeof(sql),
			 "SELECT rolsuper FROM pg_roles WHERE rolname=current_user ");

	/* make the call */
	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		fprintf(stderr, "pgcsvstat: query failed: %s\n", PQerrorMessage(conn));
		fprintf(stderr, "pgcsvstat: query was: %s\n", sql);

		PQclear(res);
		PQfinish(conn);
		exit(-1);
	}

	/* get the information */
    is_superuser = strncmp(PQgetvalue(res, 0, 0), "t", 1) == 0;

	/* cleanup */
	PQclear(res);

	return is_superuser;
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
 * Check if backend has the pg_stat_statements view
 */
bool
backend_has_pgstatstatements()
{
	PGresult   *res;
	char		sql[1024];
    bool        has_pgstatstatements = false;

	/* get the oid and database name from the system pg_database table */
	snprintf(sql, sizeof(sql),
			 "SELECT 1 "
			 "FROM pg_proc p, pg_namespace n "
			 "WHERE p.proname='pg_stat_statements' "
             "  AND p.pronamespace=n.oid");

	/* make the call */
	res = PQexec(conn, sql);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		fprintf(stderr, "pgcsvstat: query failed: %s\n", PQerrorMessage(conn));
		fprintf(stderr, "pgcsvstat: query was: %s\n", sql);

		PQclear(res);
		PQfinish(conn);
		exit(-1);
	}

	/* get the information */
    has_pgstatstatements = PQntuples(res)>0;

	/* cleanup */
	PQclear(res);

	return has_pgstatstatements;
}


int
main(int argc, char **argv)
{
    bool is_superuser = false;

	opts = (struct options *) myalloc(sizeof(struct options));

	/* parse the opts */
	get_opts(argc, argv);

	if (opts->dbname == NULL)
	{
		opts->dbname = "postgres";
		opts->nodb = true;
	}

	if (opts->directory == NULL)
	{
		opts->directory = "./";
	}

	/* connect to the database */
	conn = sql_conn();

	/* get version */
    fetch_version();

    /* check superuser attribute */
    is_superuser = check_superuser();

	/* grab cluster stats info */
	sql_exec_dump_pgstatactivity();
	if (backend_minimum_version(9, 4))
		sql_exec_dump_pgstatarchiver();
	if (backend_minimum_version(8, 3))
		sql_exec_dump_pgstatbgwriter();
	sql_exec_dump_pgstatdatabase();
	if (backend_minimum_version(9, 1))
    {
		sql_exec_dump_pgstatdatabaseconflicts();
		sql_exec_dump_pgstatreplication();
    }
	if (backend_minimum_version(14, 0))
		sql_exec_dump_pgstatreplicationslots();
	if (backend_minimum_version(13, 0))
		sql_exec_dump_pgstatslru();
	if (backend_minimum_version(10, 0))
		sql_exec_dump_pgstatsubscription();
	if (backend_minimum_version(14, 0))
		sql_exec_dump_pgstatwal();
    /* TODO pg_stat_wal_receiver */

	/* grab database stats info */
	sql_exec_dump_pgstatalltables();
	sql_exec_dump_pgstatallindexes();
	sql_exec_dump_pgstatioalltables();
	sql_exec_dump_pgstatioallindexes();
	sql_exec_dump_pgstatioallsequences();
    if (backend_minimum_version(8, 4))
		sql_exec_dump_pgstatuserfunctions();

    /* grab progress stats info */
    /* TODO */

	/* grab other informations */
	sql_exec_dump_pgclass_size();
    if (backend_has_pgstatstatements())
	    sql_exec_dump_pgstatstatements();
    if (backend_minimum_version(8, 2) && is_superuser)
		sql_exec_dump_xlog_stat();

	PQfinish(conn);
	return 0;
}
