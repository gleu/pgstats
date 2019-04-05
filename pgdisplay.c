/*
 * pgdisplay, a PostgreSQL app to display a table
 * in an informative way.
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Guillaume Lelarge, guillaume@lelarge.info, 2015-2019.
 *
 * pgstat/pgdisplay.c
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
#define PGDISPLAY_VERSION "0.0.1"
#define	PGSTAT_DEFAULT_STRING_SIZE 1024

#define couleur(param) printf("\033[48;2;255;%d;%dm",param,param)
#define nocouleur() printf("\033[0m")

/* these are the options structure for command line parameters */
struct options
{
	/* misc */
	bool		verbose;
	char       *table;
	int         groups;
	int			blocksize;

	/* connection parameters */
	char	   *dbname;
	char	   *hostname;
	char	   *port;
	char	   *username;

	/* version number */
	int			major;
	int			minor;
};

/*
 * Global variables
 */
PGconn	   		       *conn;
struct options	       *opts;
extern char            *optarg;

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
void        display_fsm(char *table);
void		fetch_version(void);
void		fetch_blocksize(void);
bool		backend_minimum_version(int major, int minor);
void        allocate_struct(void);
static void quit_properly(SIGNAL_ARGS);

/*
 * Print help message
 */
static void
help(const char *progname)
{
	printf("%s displays table in an informative way.\n\n"
		   "Usage:\n"
		   "  %s [OPTIONS] [delay [count]]\n"
		   "\nGeneral options:\n"
		   "  -G GROUPS      # of groups of blocks\n"
		   "  -t TABLE       table to display\n"
		   "  -v             verbose\n"
		   "  -?|--help      show this help, then exit\n"
		   "  -V|--version   output version information, then exit\n"
		   "\nConnection options:\n"
		   "  -h HOSTNAME    database server host or socket directory\n"
		   "  -p PORT        database server port number\n"
		   "  -U USER        connect as specified database user\n"
		   "  -d DBNAME      database to connect to\n"
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
	opts->groups = 20;
	opts->blocksize = 0;
	opts->table = NULL;
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
			puts("pgdisplay " PGDISPLAY_VERSION " (compiled with PostgreSQL " PG_VERSION ")");
			exit(0);
		}
	}

	/* get opts */
	while ((c = getopt(argc, argv, "h:p:U:d:t:G:v")) != -1)
	{
		switch (c)
		{
				/* specify the # of groups */
			case 'G':
				opts->groups = atoi(optarg);
				break;

				/* specify the table */
			case 't':
				opts->table = pg_strdup(optarg);
				break;

				/* don't show headers */
			case 'v':
				opts->verbose = true;
				break;

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

				/* username */
			case 'U':
				opts->username = pg_strdup(optarg);
				break;

			default:
				errx(1, "Try \"%s --help\" for more information.\n", progname);
		}
	}

	if (opts->table == NULL)
	{
		fprintf(stderr, "missing table name\n");
		exit(EXIT_FAILURE);
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
display_fsm(char *table)
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;
	int			nrows;
	int			row;
	int         color;
	int			totalspace, freespace;
	int			groupby, blocksize;
	int			n;

	blocksize = 8192;

	/* grab the stats (this is the only stats on one line) */
	/*
	snprintf(sql, sizeof(sql),
			 "with fsm as (select blkno/443 as blockrange, sum(avail) as available, 8192*443 as total from pg_freespace('%s') group by 1)"
			 "select blockrange, available, total, 100*available/total as ratio, 180*available/total as color from fsm order by 1",
			 table);
	*/
	snprintf(sql, sizeof(sql),
			 "select avail from pg_freespace('%s') order by blkno",
			 table);

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

	/* initialize some vars */
	totalspace = nrows*blocksize;
	if (nrows <= opts->groups)
		groupby = 1;
	else
		groupby = nrows/opts->groups;
	freespace = 0;
	n = 0;

	printf("Nombre de blocs: %d\n", nrows);
	printf("Taille de la table: %d\n", totalspace);
	printf("... groupe de %d\n", groupby);
	printf("\n\n");

	/* for each row, dump the information */
	for (row = 0; row < nrows; row++)
	{
		/* getting new values */
		freespace += atol(PQgetvalue(res, row, 0));

		if (++n >= groupby)
		{
			//printf("Espace libre[%d] : %d (sur %d)\n", n, freespace, groupby*blocksize);
			/* printing the diff...
			 * note that the first line will be the current value, rather than the diff */
			color = 180*freespace/(8192*groupby);
			if (color<0)
				color = 0;
			couleur(color);
			printf(" ");
			nocouleur();

			freespace = 0;
			n = 0;
		}
	}

	printf("\n\n");
	/* cleanup */
	PQclear(res);
}

/*
 * Fetch block size.
 */
void
fetch_blocksize()
{
	char		sql[PGSTAT_DEFAULT_STRING_SIZE];
	PGresult   *res;

	/* get the cluster version */
	snprintf(sql, sizeof(sql), "SELECT current_setting('block_size')");

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
	opts->blocksize = atoi(PQgetvalue(res, 0, 0));

	/* print version */
	if (opts->verbose)
	    printf("Detected block size: %d\n", opts->blocksize);

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
	pqsignal(SIGINT, quit_properly);

	/* Allocate the options struct */
	opts = (struct options *) pg_malloc(sizeof(struct options));

	/* Parse the options */
	get_opts(argc, argv);

	/* Connect to the database */
	conn = sql_conn();

	// check last vacuum timestamp
	// fetch blocks count

	fetch_blocksize();

    display_fsm(opts->table);

	PQfinish(conn);
	return 0;
}
