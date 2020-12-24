#define SETTINGS_BY_SOURCEFILE_TITLE "Settings by source file"
#define SETTINGS_BY_SOURCEFILE_SQL "SELECT source, sourcefile, count(*) AS nb FROM pg_settings GROUP BY 1, 2"

#define SETTINGS_NOTCONFIGFILE_NOTDEFAULTVALUE_TITLE "Non default value and not config file settings"
#define SETTINGS_NOTCONFIGFILE_NOTDEFAULTVALUE_SQL "SELECT source, name, setting, unit FROM pg_settings WHERE source NOT IN ('configuration file', 'default') ORDER BY source, name"

#define DATABASES_TITLE "Databases"
#define DATABASES_SQL "SELECT d.datname as \"Name\", pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\", pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\", d.datcollate as \"Collate\", d.datctype as \"Ctype\", pg_catalog.array_to_string(d.datacl, E'\n') AS \"Access privileges\", CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT') THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname)) ELSE 'No Access' END as \"Size\", t.spcname as \"Tablespace\", pg_catalog.shobj_description(d.oid, 'pg_database') as \"Description\" FROM pg_catalog.pg_database d JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid ORDER BY 1"

#define DATABASES_IN_CACHE_TITLE "Databases in cache"
#define DATABASES_IN_CACHE_SQL "SELECT CASE WHEN datname IS NULL THEN '<vide>' ELSE datname END AS datname, pg_size_pretty(count(*)*8192) FROM pg_buffercache bc LEFT JOIN pg_database d ON d.oid=bc.reldatabase GROUP BY 1 ORDER BY count(*) DESC"

#define TABLESPACES_TITLE "Tablespaces"
#define TABLESPACES_SQL "SELECT spcname AS \"Name\", pg_catalog.pg_get_userbyid(spcowner) AS \"Owner\", pg_catalog.pg_tablespace_location(oid) AS \"Location\", pg_size_pretty(pg_tablespace_size(oid)) AS \"Size\", pg_catalog.array_to_string(spcacl, E'\n') AS \"Access privileges\", spcoptions AS \"Options\", pg_catalog.shobj_description(oid, 'pg_tablespace') AS \"Description\" FROM pg_catalog.pg_tablespace ORDER BY 1"

#define ROLES_TITLE "Roles"
#define ROLES_SQL "SELECT r.rolname, r.rolsuper, r.rolinherit, r.rolcreaterole, r.rolcreatedb, r.rolcanlogin, r.rolconnlimit, r.rolvaliduntil, ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) as memberof, r.rolreplication, r.rolbypassrls FROM pg_catalog.pg_roles r WHERE r.rolname !~ '^pg_' ORDER BY 1"

#define USER_PASSWORDS_TITLE "User passwords"
#define USER_PASSWORDS_SQL "SELECT usename, valuntil, CASE WHEN passwd IS NULL THEN '<NULL>' else passwd END AS passwd FROM pg_catalog.pg_shadow ORDER BY 1"

#define DATABASEUSER_CONFIG_TITLE "Databases and users specific configuration"
#define DATABASEUSER_CONFIG_SQL "select datname, rolname, setconfig from pg_db_role_setting drs left join pg_database d on d.oid=drs.setdatabase left join pg_roles r on r.oid=drs.setrole"

#define SCHEMAS_TITLE "Schemas"
#define SCHEMAS_SQL "SELECT n.nspname AS \"Name\", pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\" FROM pg_catalog.pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY 1"

#define NBRELS_IN_SCHEMA_TITLE "Relations per kinds and schemas"
#define NBRELS_IN_SCHEMA_SQL "select nspname, rolname, count(*) filter (where relkind='r') as tables, count(*) filter (where relkind='t') as toasts, count(*) filter (where relkind='i') as index, count(*) filter (where relkind='s') as sequences from pg_namespace n join pg_roles r on r.oid=n.nspowner left join pg_class c on n.oid=c.relnamespace group by nspname, rolname order by 1, 2"

#define NBFUNCS_IN_SCHEMA_TITLE "Functions per schema"
#define NBFUNCS_IN_SCHEMA_SQL "select nspname, rolname, count(*) filter (where p.oid is not null) as functions from pg_namespace n join pg_roles r on r.oid=n.nspowner left join pg_proc p on n.oid=p.pronamespace group by nspname, rolname order by 1, 2"

#define NBFUNCSPROCS_IN_SCHEMA_TITLE "Routines per schema"
#define NBFUNCSPROCS_IN_SCHEMA_SQL "select nspname, rolname, count(*) filter (where prokind='f') as functions, count(*) filter (where prokind='p') as procedures from pg_namespace n join pg_roles r on r.oid=n.nspowner left join pg_proc p on n.oid=p.pronamespace group by nspname, rolname order by 1, 2"

#define HEAPTOAST_SIZE_TITLE "HEAP and TOAST sizes per schema"
#define HEAPTOAST_SIZE_SQL "select nspname, relname, pg_relation_size(c.oid) as heap_size, pg_relation_size(reltoastrelid) as toast_size from pg_namespace n join pg_class c on n.oid=c.relnamespace where pg_relation_size(reltoastrelid)>0 order by nspname, relname"

#define EXTENSIONS_TITLE "Extensions"
#define EXTENSIONS_SQL "SELECT e.extname AS \"Name\", e.extversion AS \"Version\", n.nspname AS \"Schema\", c.description AS \"Description\" FROM pg_catalog.pg_extension e LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace LEFT JOIN pg_catalog.pg_description c ON c.objoid = e.oid AND c.classoid = 'pg_catalog.pg_extension'::pg_catalog.regclass ORDER BY 1"

#define KINDS_SIZE_TITLE "Number and size per relations' kinds"
#define KINDS_SIZE_SQL "select relkind, count(*), pg_size_pretty(sum(pg_table_size(oid))) from pg_class group by 1"

#define DEPENDENCIES_TITLE "Dependencies"
#define DEPENDENCIES_SQL "with etypes as ( select classid::regclass, objid, deptype, e.extname from pg_depend join pg_extension e on refclassid = 'pg_extension'::regclass and refobjid = e.oid where classid = 'pg_type'::regclass ) select etypes.extname, etypes.objid::regtype as type, n.nspname as schema, c.relname as table, attname as column from pg_depend join etypes on etypes.classid = pg_depend.refclassid and etypes.objid = pg_depend.refobjid join pg_class c on c.oid = pg_depend.objid join pg_namespace n on n.oid = c.relnamespace join pg_attribute attr on attr.attrelid = pg_depend.objid and attr.attnum = pg_depend.objsubid where pg_depend.classid = 'pg_class'::regclass"

#define KINDS_IN_CACHE_TITLE "Relation kinds in cache"
#define KINDS_IN_CACHE_SQL "select relkind, pg_size_pretty(count(*)*8192) from pg_buffercache bc left join pg_class c on c.relfilenode=bc.relfilenode group by 1 order by count(*) desc"

#define AM_SIZE_TITLE "Access Methods"
#define AM_SIZE_SQL "select amname, count(*), pg_size_pretty(sum(pg_table_size(c.oid))) from pg_class c join pg_am a on a.oid=c.relam group by 1"

#define INDEXTYPE_TITLE "Index by types"
#define INDEXTYPE_SQL "SELECT count(*) FILTER (WHERE not indisunique AND not indisprimary) as standard, count(*) FILTER (WHERE indisunique AND not indisprimary) as unique, count(*) FILTER (WHERE indisprimary) as primary, count(*) FILTER (WHERE indisexclusion) as exclusion, count(*) FILTER (WHERE indisclustered) as clustered, count(*) FILTER (WHERE indisvalid) as valid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid"

#define NBFUNCS_TITLE "User routines"
#define NBFUNCS_SQL "select count(*) from pg_proc where pronamespace=2200 or pronamespace>16383"

#define FUNCSPROCS_PER_SCHEMA_AND_KIND_TITLE "Routines per schema and kind"
#define FUNCSPROCS_PER_SCHEMA_AND_KIND_SQL "select n.nspname, l.lanname, p.prokind, count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace join pg_language l on l.oid=p.prolang where pronamespace=2200 or pronamespace>16383 group by 1, 2, 3 order by 1, 2, 3"

#define FUNCS_PER_SCHEMA_TITLE "Functions per schema and language"
#define FUNCS_PER_SCHEMA_SQL "select n.nspname, l.lanname, count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace join pg_language l on l.oid=p.prolang where pronamespace=2200 or pronamespace>16383 group by 1, 2 order by 1, 2"

#define LOBJ_TITLE "Large Objects"
#define LOBJ_SQL "select count(*) from pg_largeobject"

#define LOBJ_STATS_TITLE "Large Objects Size"
#define LOBJ_STATS_SQL "select reltuples, relpages from pg_class where relname='pg_largeobject'"

#define RELOPTIONS_TITLE "Relation Options"
#define RELOPTIONS_SQL "select nspname, relkind, relname, reloptions from pg_class c join pg_namespace n on n.oid=c.relnamespace where reloptions is not null order by 1, 3, 2"

#define TOBEFROZEN_TABLES_TITLE "Tables to be frozen"
#define TOBEFROZEN_TABLES_SQL "select count(*) from pg_class where relkind='r' and age(relfrozenxid)>current_setting('autovacuum_freeze_max_age')::integer"

#define PGFILESETTINGS_TITLE "pg_file_settings"
#define PGFILESETTINGS_SQL "select * from pg_file_settings "

#define PGHBAFILERULES_TITLE "pg_hba_file_rules"
#define PGHBAFILERULES_SQL "select * from pg_hba_file_rules"

#define PUBLICATIONS_TITLE "Publications"
#define PUBLICATIONS_SQL "select * from pg_publication"

#define REPSLOTS_TITLE "Replication slots"
#define REPSLOTS_SQL "select * from pg_replication_slots"

#define SUBSCRIPTIONS_TITLE "Subscriptions"
#define SUBSCRIPTIONS_SQL "select * from pg_subscription"

#define PGSETTINGS_TITLE "pg_settings"
#define PGSETTINGS_SQL "select * from pg_settings"

#define TOP10QUERYIDS_SQL "select queryid, calls, total_time, mean_time from pg_stat_statements order by total_time desc limit 10"

#define TOP10QUERIES_SQL "select queryid, query from pg_stat_statements order by total_time desc limit 10"

#define REDUNDANTINDEXES_TITLE "Redundant indexes"
#define REDUNDANTINDEXES_SQL "SELECT pg_size_pretty(SUM(pg_relation_size(idx))::BIGINT) AS SIZE, string_agg(idx::text, ', ') AS indexes FROM ( SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'||COALESCE(indexprs::text,'')||E'\n' || COALESCE(indpred::text,'')) AS KEY FROM pg_index) sub GROUP BY KEY HAVING COUNT(*)>1 ORDER BY SUM(pg_relation_size(idx)) DESC"

#define MINAGE_TITLE "Min age"
#define MINAGE_SQL "SELECT label, age FROM ( select 'Process #'||pid AS label, age(backend_xid) AS age from pg_stat_activity UNION select 'Process #'||pid, age(backend_xmin) from pg_stat_activity UNION select 'Prepared transaction '||gid, age(transaction) from pg_prepared_xacts UNION select 'Replication slot '||slot_name, age(xmin) from pg_replication_slots UNION select 'Replication slot '||slot_name, age(catalog_xmin) from pg_replication_slots) tmp WHERE age IS NOT NULL ORDER BY age DESC;"

#define NEEDVACUUM_TITLE "Tables needing autoVACUUMs"
#define NEEDVACUUM_SQL "SELECT st.schemaname || '.' || st.relname tablename, st.n_dead_tup dead_tup, get_value('autovacuum_vacuum_threshold', c.reloptions, c.relkind) + get_value('autovacuum_vacuum_scale_factor', c.reloptions, c.relkind) * c.reltuples max_dead_tup, st.last_autovacuum FROM pg_stat_all_tables st, pg_class c WHERE c.oid = st.relid AND c.relkind IN ('r','m','t') AND st.n_dead_tup>0"

#define NEEDANALYZE_TITLE "Tables needing autoANALYZEs"
#define NEEDANALYZE_SQL "SELECT st.schemaname || '.' || st.relname tablename, st.n_mod_since_analyze mod_tup, get_value('autovacuum_analyze_threshold', c.reloptions, c.relkind) + get_value('autovacuum_analyze_scale_factor', c.reloptions, c.relkind) * c.reltuples max_mod_tup, st.last_autoanalyze FROM pg_stat_all_tables st, pg_class c WHERE c.oid = st.relid AND c.relkind IN ('r','m') AND st.n_mod_since_analyze>0"

#define GETVALUE_FUNCTION_SQL "CREATE FUNCTION get_value(param text, reloptions text[], relkind \"char\") RETURNS float AS $$ SELECT coalesce((SELECT option_value FROM   pg_options_to_table(reloptions) WHERE  option_name = CASE WHEN relkind = 't' THEN 'toast.' ELSE '' END || param), current_setting(param))::float; $$ LANGUAGE sql"
