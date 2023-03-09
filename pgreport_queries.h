#define SETTINGS_BY_SOURCEFILE_TITLE "Settings by source file"
#define SETTINGS_BY_SOURCEFILE_SQL "SELECT source, sourcefile, count(*) AS nb FROM pg_settings GROUP BY 1, 2"

#define SETTINGS_NOTCONFIGFILE_NOTDEFAULTVALUE_TITLE "Non default value and not config file settings"
#define SETTINGS_NOTCONFIGFILE_NOTDEFAULTVALUE_SQL "SELECT source, name, setting, unit FROM pg_settings WHERE source NOT IN ('configuration file', 'default') ORDER BY source, name"

#define CLUSTER_HITRATIO_TITLE "Hit ratio"
#define CLUSTER_HITRATIO_SQL "SELECT 'index hit rate' AS name, 100.*sum(idx_blks_hit) / nullif(sum(idx_blks_hit + idx_blks_read),0) AS ratio FROM pg_statio_user_indexes UNION ALL SELECT 'table hit rate' AS name, 100.*sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read),0) AS ratio FROM pg_statio_user_tables"

#define CLUSTER_BUFFERSUSAGE_TITLE "Buffers Usage"
#define CLUSTER_BUFFERSUSAGE_SQL "SELECT usagecount, count(*) FROM pg_buffercache GROUP BY 1 ORDER BY 1"

#define CLUSTER_BUFFERSUSAGEDIRTY_TITLE "Buffers Usage with dirty"
#define CLUSTER_BUFFERSUSAGEDIRTY_SQL "SELECT usagecount, isdirty, count(*) FROM pg_buffercache GROUP BY 1,2 ORDER BY 1,2"

#define DATABASES_TITLE "Databases"
#define DATABASES_SQL "SELECT d.datname as \"Name\", pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\", pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\", d.datcollate as \"Collate\", d.datctype as \"Ctype\", pg_catalog.array_to_string(d.datacl, E'\n') AS \"Access privileges\", CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT') THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname)) ELSE 'No Access' END as \"Size\", t.spcname as \"Tablespace\", pg_catalog.shobj_description(d.oid, 'pg_database') as \"Description\" FROM pg_catalog.pg_database d JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid ORDER BY 1"

#define DATABASES_IN_CACHE_TITLE "Databases in cache"
#define DATABASES_IN_CACHE_SQL "SELECT CASE WHEN datname IS NULL THEN '<vide>' ELSE datname END AS datname, pg_size_pretty(count(*)*8192) FROM pg_buffercache bc LEFT JOIN pg_database d ON d.oid=bc.reldatabase GROUP BY 1 ORDER BY count(*) DESC"

#define TABLESPACES_TITLE "Tablespaces"
#define TABLESPACES_SQL "SELECT spcname AS \"Name\", pg_catalog.pg_get_userbyid(spcowner) AS \"Owner\", pg_catalog.pg_tablespace_location(oid) AS \"Location\", pg_size_pretty(pg_tablespace_size(oid)) AS \"Size\", pg_catalog.array_to_string(spcacl, E'\n') AS \"Access privileges\", spcoptions AS \"Options\", pg_catalog.shobj_description(oid, 'pg_tablespace') AS \"Description\" FROM pg_catalog.pg_tablespace ORDER BY 1"

#define ROLES_TITLE "Roles"
#define ROLES_SQL_94max "SELECT r.rolname, r.rolsuper, r.rolinherit, r.rolcreaterole, r.rolcreatedb, r.rolcanlogin, r.rolconnlimit, r.rolvaliduntil, ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) as memberof, r.rolreplication FROM pg_catalog.pg_roles r WHERE r.rolname !~ '^pg_' ORDER BY 1"
#define ROLES_SQL_95min "SELECT r.rolname, r.rolsuper, r.rolinherit, r.rolcreaterole, r.rolcreatedb, r.rolcanlogin, r.rolconnlimit, r.rolvaliduntil, ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) as memberof, r.rolreplication, r.rolbypassrls FROM pg_catalog.pg_roles r WHERE r.rolname !~ '^pg_' ORDER BY 1"

#define USER_PASSWORDS_TITLE "User passwords"
#define USER_PASSWORDS_SQL "SELECT usename, valuntil, CASE WHEN passwd IS NULL THEN '<NULL>' else passwd END AS passwd FROM pg_catalog.pg_shadow ORDER BY 1"

#define DATABASEUSER_CONFIG_TITLE "Databases and users specific configuration"
#define DATABASEUSER_CONFIG_SQL "select datname, rolname, setconfig from pg_db_role_setting drs left join pg_database d on d.oid=drs.setdatabase left join pg_roles r on r.oid=drs.setrole"

#define SCHEMAS_TITLE "Schemas"
#define SCHEMAS_SQL "SELECT n.nspname AS \"Name\", pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\" FROM pg_catalog.pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY 1"

#define NBRELS_IN_SCHEMA_TITLE "Relations per kinds and schemas"
#define NBRELS_IN_SCHEMA_SQL "select nspname, rolname, count(*) filter (where relkind='r') as tables, count(*) filter (where relkind='t') as toasts, count(*) filter (where relkind='i') as index, count(*) filter (where relkind='S') as sequences from pg_namespace n join pg_roles r on r.oid=n.nspowner left join pg_class c on n.oid=c.relnamespace group by nspname, rolname order by 1, 2"

#define NBFUNCS_IN_SCHEMA_TITLE "Functions per schema"
#define NBFUNCS_IN_SCHEMA_SQL "select nspname, rolname, count(*) filter (where p.oid is not null) as functions from pg_namespace n join pg_roles r on r.oid=n.nspowner left join pg_proc p on n.oid=p.pronamespace group by nspname, rolname order by 1, 2"

#define NBFUNCSPROCS_IN_SCHEMA_TITLE "Routines per schema"
#define NBFUNCSPROCS_IN_SCHEMA_SQL "select nspname, rolname, count(*) filter (where prokind='f') as functions, count(*) filter (where prokind='p') as procedures from pg_namespace n join pg_roles r on r.oid=n.nspowner left join pg_proc p on n.oid=p.pronamespace group by nspname, rolname order by 1, 2"

#define HEAPTOAST_SIZE_TITLE "HEAP and TOAST sizes per schema"
#define HEAPTOAST_SIZE_SQL "select nspname, relname, pg_relation_size(c.oid) as heap_size, pg_relation_size(reltoastrelid) as toast_size from pg_namespace n join pg_class c on n.oid=c.relnamespace where pg_relation_size(reltoastrelid)>0 order by nspname, relname"

#define EXTENSIONS_TITLE "Extensions"
#define EXTENSIONS_SQL "SELECT e.extname AS \"Name\", e.extversion AS \"Version\", n.nspname AS \"Schema\", c.description AS \"Description\" FROM pg_catalog.pg_extension e LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace LEFT JOIN pg_catalog.pg_description c ON c.objoid = e.oid AND c.classoid = 'pg_catalog.pg_extension'::pg_catalog.regclass ORDER BY 1"

#define EXTENSIONSTABLE_TITLE "Extensions Tables (dumpable or not?)"
#define EXTENSIONSTABLE_SQL "WITH tables_dumped AS (SELECT e.extname, n.nspname||'.'||c.relname AS relation_name1 FROM pg_extension e, LATERAL unnest(extconfig) AS toid JOIN pg_class c on c.oid=toid JOIN pg_namespace n on n.oid=c.relnamespace) SELECT e.extname AS extension_name, relation_name2, tables_dumped.extname IS NOT NULL AS to_be_dumped FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e ON e.oid=d.refobjid, LATERAL pg_catalog.pg_describe_object(d.classid, d.objid, 0), LATERAL substr(pg_describe_object, case when pg_describe_object like 'table %' then length('table ') else length('sequence ') end + 1) AS relation_name2 LEFT JOIN tables_dumped ON tables_dumped.relation_name1=relation_name2 WHERE d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND d.deptype = 'e' AND (pg_describe_object like 'table %' OR pg_describe_object like 'sequence %') ORDER BY 1,2,3"

#define KINDS_SIZE_TITLE "Number and size per relations kinds"
#define KINDS_SIZE_SQL "SELECT nspname, relkind, count(*), pg_size_pretty(sum(pg_table_size(c.oid))) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace GROUP BY 1,2 ORDER BY 1,2"

#define DEPENDENCIES_TITLE "Dependencies"
#define DEPENDENCIES_SQL "with etypes as ( select classid::regclass, objid, deptype, e.extname from pg_depend join pg_extension e on refclassid = 'pg_extension'::regclass and refobjid = e.oid where classid = 'pg_type'::regclass ) select etypes.extname, etypes.objid::regtype as type, n.nspname as schema, c.relname as table, attname as column from pg_depend join etypes on etypes.classid = pg_depend.refclassid and etypes.objid = pg_depend.refobjid join pg_class c on c.oid = pg_depend.objid join pg_namespace n on n.oid = c.relnamespace join pg_attribute attr on attr.attrelid = pg_depend.objid and attr.attnum = pg_depend.objsubid where pg_depend.classid = 'pg_class'::regclass"

#define KINDS_IN_CACHE_TITLE "Relation kinds in cache"
#define KINDS_IN_CACHE_SQL "select relkind, pg_size_pretty(count(*)*8192) from pg_buffercache bc left join pg_class c on c.relfilenode=bc.relfilenode group by 1 order by count(*) desc"

#define AM_SIZE_TITLE "Access Methods"
#define AM_SIZE_SQL "select nspname, amname, count(*), pg_size_pretty(sum(pg_table_size(c.oid))) from pg_class c join pg_am a on a.oid=c.relam join pg_namespace n on n.oid=c.relnamespace group by 1, 2 order by 1,2"

#define INDEXTYPE_TITLE "Index by types"
#define INDEXTYPE_SQL "SELECT nspname, count(*) FILTER (WHERE not indisunique AND not indisprimary) as standard, count(*) FILTER (WHERE indisunique AND not indisprimary) as unique, count(*) FILTER (WHERE indisprimary) as primary, count(*) FILTER (WHERE indisexclusion) as exclusion, count(*) FILTER (WHERE indisclustered) as clustered, count(*) FILTER (WHERE indisvalid) as valid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace GROUP BY 1;"

#define INDEXONTEXT_TITLE "Index and opclass"
#define INDEXONTEXT_SQL "WITH colind AS (SELECT i.indrelid AS oid, i.indrelid::regclass AS tbl, c.relname AS idx, unnest(i.indkey::int4[]) AS num, unnest(i.indclass::int4[]) AS class FROM pg_class c JOIN pg_am a ON a.oid = c.relam JOIN pg_index i ON i.indexrelid = c.oid WHERE c.relkind = 'i' AND c.relname NOT LIKE 'pg%' AND a.amname = 'btree') SELECT colind.tbl AS \"Table\", colind.idx AS \"Index\", a.attname AS \"Column\", t.typname AS \"Type\", oc.opcname AS \"Operator class\", oc.opcdefault AS \"Default?\" FROM colind JOIN pg_attribute a ON a.attrelid = colind.oid AND a.attnum = colind.num JOIN pg_type t ON t.oid = a.atttypid JOIN pg_opclass oc ON oc.oid = colind.class ORDER BY colind.tbl, colind.idx, colind.num"

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

#define PGIDENTFILEMAPPINGS_TITLE "pg_ident_file_mappings"
#define PGIDENTFILEMAPPINGS_SQL "select * from pg_ident_file_mappings"

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

#define PERCENTUSEDINDEXES_TITLE "Percentage usage of indexes"
#define PERCENTUSEDINDEXES_SQL "SELECT relname, CASE idx_scan WHEN 0 THEN 'Insufficient data' ELSE (100 * idx_scan / (seq_scan + idx_scan))::text END percent_of_times_index_used, n_live_tup rows_in_table FROM pg_stat_user_tables ORDER BY n_live_tup DESC"

#define UNUSEDINDEXES_TITLE "Unused indexes"
#define UNUSEDINDEXES_SQL "select schemaname, count(*) from pg_stat_user_indexes s join pg_index i using (indexrelid) where idx_scan=0 and (not indisunique AND not indisprimary) group by 1;"

#define REDUNDANTINDEXES_TITLE "Redundant indexes"
#define REDUNDANTINDEXES_SQL "SELECT pg_size_pretty(SUM(pg_relation_size(idx))::BIGINT) AS SIZE, string_agg(idx::text, ', ') AS indexes FROM ( SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'||COALESCE(indexprs::text,'')||E'\n' || COALESCE(indpred::text,'')) AS KEY FROM pg_index) sub GROUP BY KEY HAVING COUNT(*)>1 ORDER BY SUM(pg_relation_size(idx)) DESC"

#define MINAGE_TITLE "Min age"
#define MINAGE_SQL "SELECT label, age FROM ( select 'Process #'||pid AS label, age(backend_xid) AS age from pg_stat_activity UNION select 'Process #'||pid, age(backend_xmin) from pg_stat_activity UNION select 'Prepared transaction '||gid, age(transaction) from pg_prepared_xacts UNION select 'Replication slot '||slot_name, age(xmin) from pg_replication_slots UNION select 'Replication slot '||slot_name, age(catalog_xmin) from pg_replication_slots) tmp UNION select 'Secondary '||client_addr, age(backend_xmin) FROM pg_stat_replication WHERE backend_xmin IS NOT NULL ORDER BY age DESC;"

#define NEEDVACUUM_TITLE "Tables needing autoVACUUMs"
#define NEEDVACUUM_SQL "SELECT st.schemaname || '.' || st.relname tablename, st.n_dead_tup dead_tup, round((get_value('autovacuum_vacuum_threshold', c.reloptions, c.relkind) + get_value('autovacuum_vacuum_scale_factor', c.reloptions, c.relkind  ) * c.reltuples)::numeric,2) max_dead_tup, st.last_autovacuum, count(*) FILTER (WHERE NOT all_visible) AS tobevacuumed_blocks, count(*) AS total_blocks FROM pg_stat_all_tables st, pg_class c, LATERAL pg_visibility_map(st.relid) WHERE c.oid = st.relid AND c.relkind IN ('r','m','t') AND st.n_dead_tup>0 GROUP BY 1,2,3,4"

#define NEEDANALYZE_TITLE "Tables needing autoANALYZEs"
#define NEEDANALYZE_SQL "SELECT st.schemaname || '.' || st.relname tablename, st.n_mod_since_analyze mod_tup, get_value('autovacuum_analyze_threshold', c.reloptions, c.relkind) + get_value('autovacuum_analyze_scale_factor', c.reloptions, c.relkind) * c.reltuples max_mod_tup, st.last_autoanalyze FROM pg_stat_all_tables st, pg_class c WHERE c.oid = st.relid AND c.relkind IN ('r','m') AND st.n_mod_since_analyze>0"

#define CREATE_GETVALUE_FUNCTION_SQL "CREATE FUNCTION get_value(param text, reloptions text[], relkind \"char\") RETURNS float AS $$ SELECT coalesce((SELECT option_value FROM   pg_options_to_table(reloptions) WHERE  option_name = CASE WHEN relkind = 't' THEN 'toast.' ELSE '' END || param), current_setting(param))::float; $$ LANGUAGE sql"

#define CREATE_BLOATTABLE_VIEW_SQL "CREATE TEMPORARY VIEW bloat_table AS SELECT schemaname, tblname, bs*tblpages AS real_size, (tblpages-est_tblpages)*bs AS extra_size, CASE WHEN tblpages - est_tblpages > 0 THEN 100 * (tblpages - est_tblpages)/tblpages::float ELSE 0 END AS extra_ratio, fillfactor, CASE WHEN tblpages - est_tblpages_ff > 0 THEN (tblpages-est_tblpages_ff)*bs ELSE 0 END AS bloat_size, CASE WHEN tblpages - est_tblpages_ff > 0 THEN 100 * (tblpages - est_tblpages_ff)/tblpages::float ELSE 0 END AS bloat_ratio, is_na FROM ( SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages, ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff, tblpages, fillfactor, bs, tblid, schemaname, tblname, heappages, toastpages, is_na FROM ( SELECT ( 4 + tpl_hdr_size + tpl_data_size + (2*ma) - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END - CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages, toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, fillfactor, is_na FROM ( SELECT tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples, tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages, coalesce(toast.reltuples, 0) AS toasttuples, coalesce(substring( array_to_string(tbl.reloptions, ' ') FROM 'fillfactor=([0-9]+)')::smallint, 100) AS fillfactor, current_setting('block_size')::numeric AS bs, CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma, 24 AS page_hdr, 23 + CASE WHEN MAX(coalesce(s.null_frac,0)) > 0 THEN ( 7 + count(s.attname) ) / 8 ELSE 0::int END + CASE WHEN bool_or(att.attname = 'oid' and att.attnum < 0) THEN 4 ELSE 0 END AS tpl_hdr_size, sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 0) ) AS tpl_data_size, bool_or(att.atttypid = 'pg_catalog.name'::regtype) OR sum(CASE WHEN att.attnum > 0 THEN 1 ELSE 0 END) <> count(s.attname) AS is_na FROM pg_attribute AS att JOIN pg_class AS tbl ON att.attrelid = tbl.oid JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid WHERE NOT att.attisdropped AND tbl.relkind in ('r','m') GROUP BY 1,2,3,4,5,6,7,8,9,10 ORDER BY 2,3) AS s) AS s2) AS s3"

#define CREATE_BLOATINDEX_VIEW_SQL_1 "CREATE TEMPORARY VIEW bloat_index AS SELECT nspname AS schemaname, tblname, idxname, bs*(relpages)::bigint AS real_size, bs*(relpages-est_pages)::bigint AS extra_size, 100 * (relpages-est_pages)::float / relpages AS extra_ratio, fillfactor, CASE WHEN relpages > est_pages_ff THEN bs*(relpages-est_pages_ff) ELSE 0 END AS bloat_size, 100 * (relpages-est_pages_ff)::float / relpages AS bloat_ratio, is_na FROM ( SELECT coalesce(1 + ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0) AS est_pages, coalesce(1 + ceil(reltuples/floor((bs-pageopqdata-pagehdr)*fillfactor/(100*(4+nulldatahdrwidth)::float))), 0) AS est_pages_ff, bs, nspname, tblname, idxname, relpages, fillfactor, is_na FROM ( SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, idxoid, fillfactor, ( index_tuple_hdr_bm + maxalign - CASE WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign ELSE index_tuple_hdr_bm%maxalign END + nulldatawidth + maxalign - CASE WHEN nulldatawidth = 0 THEN 0 WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign ELSE nulldatawidth::integer%maxalign END)::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na FROM ( SELECT n.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.idxoid, i.fillfactor, current_setting('block_size')::numeric AS bs, CASE WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS maxalign, 24 AS pagehdr, 16 AS pageopqdata, CASE WHEN max(coalesce(s.null_frac,0)) = 0 THEN 2 ELSE 2 + (( 32 + 8 - 1 ) / 8) END AS index_tuple_hdr_bm, sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth, max( CASE WHEN i.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na FROM ( SELECT ct.relname AS tblname, ct.relnamespace, ic.idxname, ic.attpos, ic.indkey, ic.indkey[ic.attpos], ic.reltuples, ic.relpages, ic.tbloid, ic.idxoid, ic.fillfactor, coalesce(a1.attnum, a2.attnum) AS attnum, coalesce(a1.attname, a2.attname) AS attname, coalesce(a1.atttypid, a2.atttypid) AS atttypid, CASE WHEN a1.attnum IS NULL THEN ic.idxname ELSE ct.relname END AS attrelname FROM ( SELECT idxname, reltuples, relpages, tbloid, idxoid, fillfactor, indkey, pg_catalog.generate_series(1,indnatts) AS attpos "
#define CREATE_BLOATINDEX_VIEW_SQL_2 "FROM ( SELECT ci.relname AS idxname, ci.reltuples, ci.relpages, i.indrelid AS tbloid, i.indexrelid AS idxoid, coalesce(substring( array_to_string(ci.reloptions, ' ') from 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor, i.indnatts, pg_catalog.string_to_array(pg_catalog.textin( pg_catalog.int2vectorout(i.indkey)),' ')::int[] AS indkey FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid WHERE ci.relam=(SELECT oid FROM pg_am WHERE amname = 'btree') AND ci.relpages > 0) AS idx_data) AS ic JOIN pg_catalog.pg_class ct ON ct.oid = ic.tbloid LEFT JOIN pg_catalog.pg_attribute a1 ON ic.indkey[ic.attpos] <> 0 AND a1.attrelid = ic.tbloid AND a1.attnum = ic.indkey[ic.attpos] LEFT JOIN pg_catalog.pg_attribute a2 ON ic.indkey[ic.attpos] = 0 AND a2.attrelid = ic.idxoid AND a2.attnum = ic.attpos) i JOIN pg_catalog.pg_namespace n ON n.oid = i.relnamespace JOIN pg_catalog.pg_stats s ON s.schemaname = n.nspname AND s.tablename = i.attrelname AND s.attname = i.attname GROUP BY 1,2,3,4,5,6,7,8,9,10,11) AS rows_data_stats) AS rows_hdr_pdg_stats) AS relation_stats"

#define CREATE_ORPHANEDFILES_VIEW_SQL "CREATE TEMPORARY VIEW orphaned_files AS WITH ver AS ( select current_setting('server_version_num') pgversion, v::integer/10000||'.'||mod(v::integer,10000)/100 AS version FROM current_setting('server_version_num') v), tbl_paths AS ( SELECT  tbs.oid AS tbs_oid, spcname, 'pg_tblspc/' || tbs.oid || '/' || (SELECT dir FROM pg_ls_dir('pg_tblspc/'||tbs.oid||'/',true,false)  dir WHERE dir LIKE E'PG\\_'||ver.version||E'\\_%'   ) as tbl_path FROM pg_tablespace tbs, ver WHERE tbs.spcname NOT IN ('pg_default','pg_global')), files AS ( SELECT d.oid  AS database_oid, 0         AS tbs_oid, 'base/'||d.oid AS path, file_name AS file_name, substring(file_name from E'[0-9]+' ) AS base_name FROM pg_database d, pg_ls_dir('base/' || d.oid,true,false) AS file_name WHERE d.datname = current_database() UNION ALL SELECT  d.oid, tbp.tbs_oid, tbl_path||'/'||d.oid, file_name, (substring(file_name from E'[0-9]+' )) AS base_name FROM pg_database d, tbl_paths tbp, pg_ls_dir(tbp.tbl_path||'/'|| d.oid,true,false) AS file_name WHERE d.datname = current_database()), orphans AS ( SELECT tbs_oid, base_name, file_name, current_setting('data_directory')||'/'||path||'/'||file_name as orphaned_file, pg_filenode_relation (tbs_oid,base_name::oid) as rel_without_pgclass FROM  ver, files LEFT JOIN pg_class c ON (c.relfilenode::text=files.base_name OR (c.oid::text = files.base_name and c.relfilenode=0 and c.relname like 'pg_%')) WHERE c.oid IS null AND  lower(file_name) NOT LIKE 'pg_%') SELECT orphaned_file, pg_size_pretty((pg_stat_file(orphaned_file)).size) as file_size, (pg_stat_file(orphaned_file)).modification as modification_date, current_database() FROM orphans WHERE rel_without_pgclass IS NULL"

#define BLOATOVERVIEW_TITLE "Bloat Overview"
#define BLOATOVERVIEW_SQL "SELECT 'Tables'' bloat' AS label, pg_size_pretty(sum(bloat_size)::numeric) AS bloat_size FROM bloat_table UNION SELECT 'Indexes'' bloat', pg_size_pretty(sum(bloat_size)::numeric) FROM bloat_index"
#define TOP20BLOAT_TABLES_TITLE "Top 20 most fragmented tables (over 1MB)"
#define TOP20BLOAT_TABLES_SQL "SELECT * FROM bloat_table WHERE bloat_size>1e6 ORDER BY bloat_size DESC LIMIT 20"
#define TOP20BLOAT_INDEXES_TITLE "Top 20 most fragmented indexes (over 1MB)"
#define TOP20BLOAT_INDEXES_SQL "SELECT * FROM bloat_index WHERE bloat_size>1e6 ORDER BY bloat_size DESC LIMIT 20"

#define ORPHANEDFILES_TITLE "Orphaned files"
#define ORPHANEDFILES_SQL "SELECT * FROM orphaned_files ORDER BY file_size DESC"

#define CREATE_SCHEMA "CREATE SCHEMA pgreport"
#define SET_SEARCHPATH "SET search_path TO pgreport"
#define DROP_ALL "DROP FUNCTION get_value(text, text[], \"char\");DROP EXTENSION pg_buffercache;DROP EXTENSION pg_visibility;DROP SCHEMA pgreport"

