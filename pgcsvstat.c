/*
 * pgstats, a PostgreSQL app to gather statistical informations
 * from a PostgreSQL database.
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Guillaume Lelarge, guillaume@lelarge.info, 2011-2016.
 *
 * pgstats/pgcsvstat.c
 */

#include "postgres_fe.h"
#include <sys/stat.h>

#include <unistd.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

extern char *optarg;

#include "libpq-fe.h"

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
			puts("pgstats (PostgreSQL) " PG_VERSION);
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
			fprintf(stderr, "%s: could not connect to database %s\n",
					"pgstats", opts->dbname);
			exit(1);
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
		fprintf(stderr, "%s: could not connect to database %s: %s",
				"pgstats", opts->dbname, PQerrorMessage(my_conn));
		PQfinish(my_conn);
		exit(1);
	}

	/* return the conn if good */
	return my_conn;
}

/*
 * Actual code to make call to the database and print the output data.
 */
int
sql_exec(const char *todo, const char* filename, bool quiet)
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
        fprintf(stderr, "pgstats: fopen failed: %d\n", errno);
        fprintf(stderr, "pgstats: filename was: %s\n", filename);
        
        PQfinish(conn);
        exit(-1);
    }

	/* get size of file */
	stat(filename, &st);
	size = st.st_size;

	/* make the call */
	res = PQexec(conn, todo);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		fprintf(stderr, "pgstats: query failed: %s\n", PQerrorMessage(conn));
		fprintf(stderr, "pgstats: query was: %s\n", todo);

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
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), datid, datname, %s, "
             "usesysid, usename, %s%s%s%s%s"
			 "date_trunc('seconds', query_start) AS query_start, "
             "%s%s%s%s%s "
             "FROM pg_stat_activity "
			 "ORDER BY %s",
		backend_minimum_version(9, 2) ? "pid" : "procpid",
		backend_minimum_version(9, 0) ? "application_name, " : "",
		backend_minimum_version(8, 1) ? "client_addr, " : "",
		backend_minimum_version(9, 1) ? "client_hostname, " : "",
		backend_minimum_version(8, 1) ? "client_port, date_trunc('seconds', backend_start) AS backend_start, " : "",
        backend_minimum_version(8, 3) ? "date_trunc('seconds', xact_start) AS xact_start, " : "",
        backend_minimum_version(9, 2) ? "state_change, " : "",
        backend_minimum_version(8, 2) ? "waiting, " : "",
        backend_minimum_version(9, 4) ? "backend_xid, " : "",
        backend_minimum_version(9, 4) ? "backend_xmin, " : "",
        backend_minimum_version(9, 2) ? "query" : "current_query",
		backend_minimum_version(9, 2) ? "pid" : "procpid");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_activity.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all bgwriter stats.
 */
void
sql_exec_dump_pgstatbgwriter()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), checkpoints_timed, "
             "checkpoints_req, %sbuffers_checkpoint, buffers_clean, "
			 "maxwritten_clean, buffers_backend, %sbuffers_alloc%s "
             "FROM pg_stat_bgwriter ",
		backend_minimum_version(9, 2) ? "checkpoint_write_time, checkpoint_sync_time, " : "",
		backend_minimum_version(9, 1) ? "buffers_backend_fsync, " : "",
		backend_minimum_version(9, 1) ? ", date_trunc('seconds', stats_reset) AS stats_reset " : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_bgwriter.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all archiver stats.
 */
void
sql_exec_dump_pgstatarchiver()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), archived_count, "
             "last_archived_wal, date_trunc('seconds', last_archived_time) AS last_archived_time, "
			 "failed_count, "
             "last_failed_wal, date_trunc('seconds', last_failed_time) AS last_failed_time, "
		     "date_trunc('seconds', stats_reset) AS stats_reset "
             "FROM pg_stat_archiver ");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_archiver.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all databases stats.
 */
void
sql_exec_dump_pgstatdatabase()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), datid, datname, "
             "numbackends, xact_commit, xact_rollback, blks_read, blks_hit"
             "%s%s%s "
             "FROM pg_stat_database "
             "ORDER BY datname",
		backend_minimum_version(8, 3) ? ", tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted" : "",
		backend_minimum_version(9, 1) ? ", conflicts, date_trunc('seconds', stats_reset) AS stats_reset" : "",
		backend_minimum_version(9, 2) ? ", temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time" : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_database.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all databases conflicts stats.
 */
void
sql_exec_dump_pgstatdatabaseconflicts()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), * "
             "FROM pg_stat_database_conflicts "
             "ORDER BY datname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_database_conflicts.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all replication stats.
 */
void
sql_exec_dump_pgstatreplication()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), %s, usesysid, usename, "
             "application_name, client_addr, client_hostname, client_port, "
             "date_trunc('seconds', backend_start) AS backend_start, %sstate, "
             "pg_current_xlog_location() AS master_location, "
             "sent_location, write_location, flush_location, replay_location, "
             "sync_priority, "
             "sync_state "
             "FROM pg_stat_replication "
             "ORDER BY application_name",
		backend_minimum_version(9, 2) ? "pid" : "procpid",
		backend_minimum_version(9, 4) ? "backend_xmin, " : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_replication.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all tables stats.
 */
void
sql_exec_dump_pgstatalltables()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), relid, schemaname, relname, "
             "seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, "
             "n_tup_upd, n_tup_del"
             "%s"
			 "%s"
			 "%s"
			 "%s"
             " FROM pg_stat_all_tables "
	     "WHERE schemaname <> 'information_schema' "
	     "ORDER BY schemaname, relname",
		backend_minimum_version(8, 3) ? ", n_tup_hot_upd, n_live_tup, n_dead_tup" : "",
        backend_minimum_version(9, 4) ? ", n_mod_since_analyze" : "",
        backend_minimum_version(8, 2) ? ", date_trunc('seconds', last_vacuum) AS last_vacuum, date_trunc('seconds', last_autovacuum) AS last_autovacuum, date_trunc('seconds',last_analyze) AS last_analyze, date_trunc('seconds',last_autoanalyze) AS last_autoanalyze" : "",
        backend_minimum_version(9, 1) ? ", vacuum_count, autovacuum_count, analyze_count, autoanalyze_count" : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_all_tables.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all indexes stats.
 */
void
sql_exec_dump_pgstatallindexes()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_stat_all_indexes "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, relname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_all_indexes.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all tables IO stats.
 */
void
sql_exec_dump_pgstatioalltables()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_statio_all_tables "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, relname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_statio_all_tables.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all indexes IO stats.
 */
void
sql_exec_dump_pgstatioallindexes()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_statio_all_indexes "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, relname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_statio_all_indexes.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all sequences IO stats.
 */
void
sql_exec_dump_pgstatioallsequences()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_statio_all_sequences "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, relname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_statio_all_sequences.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all functions stats.
 */
void
sql_exec_dump_pgstatuserfunctions()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), * "
			 "FROM pg_stat_user_functions "
	     		 "WHERE schemaname <> 'information_schema' "
			 "ORDER BY schemaname, funcname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_user_functions.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all size class stats.
 */
void
sql_exec_dump_pgclass_size()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), n.nspname, c.relname, c.relkind, c.reltuples, c.relpages%s "
			 "FROM pg_class c, pg_namespace n "
			 "WHERE n.oid=c.relnamespace AND n.nspname <> 'information_schema' "
			 "ORDER BY n.nspname, c.relname",
		backend_minimum_version(8, 1) ? ", pg_relation_size(c.oid)" : "");
	snprintf(filename, sizeof(filename),
			 "%s/pg_class_size.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all statements stats.
 */
void
sql_exec_dump_pgstatstatements()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), r.rolname, d.datname, "
             "regexp_replace(query, E'\n', ' ', 'g') as query, calls, total_time, rows, "
             "shared_blks_hit, shared_blks_read, shared_blks_written, "
             "local_blks_hit, local_blks_read, local_blks_written, "
             "temp_blks_read, temp_blks_written "
			 "FROM pg_stat_statements q, pg_database d, pg_roles r "
			 "WHERE q.userid=r.oid and q.dbid=d.oid "
			 "ORDER BY r.rolname, d.datname");
	snprintf(filename, sizeof(filename),
			 "%s/pg_stat_statements.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}

/*
 * Dump all xlog stats.
 */
void
sql_exec_dump_xlog_stat()
{
	char		todo[1024];
	char		filename[1024];

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo),
			 "SELECT date_trunc('seconds', now()), pg_xlogfile_name(pg_current_xlog_location())=pg_ls_dir AS current, pg_ls_dir AS filename, "
			 "(SELECT modification FROM pg_stat_file('pg_xlog/'||pg_ls_dir)) AS modification_timestamp "
			 "FROM pg_ls_dir('pg_xlog') "
			 "WHERE pg_ls_dir ~ E'^[0-9A-F]{24}' "
			 "ORDER BY pg_ls_dir");
	snprintf(filename, sizeof(filename),
			 "%s/pg_xlog_stat.csv", opts->directory);

	sql_exec(todo, filename, opts->quiet);
}


/*
 * Fetch PostgreSQL major and minor numbers
 */
void
fetch_version()
{
	char		todo[1024];
	PGresult   *res;

	/* get the oid and database name from the system pg_database table */
	snprintf(todo, sizeof(todo), "SELECT version()");

	/* make the call */
	res = PQexec(conn, todo);

	/* check and deal with errors */
	if (!res || PQresultStatus(res) > 2)
	{
		fprintf(stderr, "pgstats: query failed: %s\n", PQerrorMessage(conn));
		fprintf(stderr, "pgstats: query was: %s\n", todo);

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
		fprintf(stderr, "pgstats: query failed: %s\n", PQerrorMessage(conn));
		fprintf(stderr, "pgstats: query was: %s\n", sql);

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
		fprintf(stderr, "pgstats: query failed: %s\n", PQerrorMessage(conn));
		fprintf(stderr, "pgstats: query was: %s\n", sql);

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

	/* grabe cluster stats info */
	sql_exec_dump_pgstatactivity();
	if (backend_minimum_version(8, 3))
		sql_exec_dump_pgstatbgwriter();
	sql_exec_dump_pgstatdatabase();
	if (backend_minimum_version(9, 1))
    {
		sql_exec_dump_pgstatdatabaseconflicts();
		sql_exec_dump_pgstatreplication();
    }
	if (backend_minimum_version(9, 4))
		sql_exec_dump_pgstatarchiver();

	/* grabe database stats info */
	sql_exec_dump_pgstatalltables();
	sql_exec_dump_pgstatallindexes();
	sql_exec_dump_pgstatioalltables();
	sql_exec_dump_pgstatioallindexes();
	sql_exec_dump_pgstatioallsequences();
    if (backend_minimum_version(8, 4))
		sql_exec_dump_pgstatuserfunctions();

	/* grabe other informations */
	sql_exec_dump_pgclass_size();
    if (backend_has_pgstatstatements())
	    sql_exec_dump_pgstatstatements();
    if (backend_minimum_version(8, 2) && is_superuser)
		sql_exec_dump_xlog_stat();

	PQfinish(conn);
	return 0;
}
