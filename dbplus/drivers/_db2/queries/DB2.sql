-- DB2 LUW introspection queries, loaded by dbplus.drivers.DB2 via QueryStore.
-- Each query is introduced by the usual QueryStore marker on its own line.
-- Named parameters prefixed with a colon are rewritten to '?' positional
-- placeholders by dbplus.Statement before being sent to ibm_db. Optional
-- filter parameters are guarded with explicit CAST(... AS VARCHAR) because
-- DB2 cannot infer the type of '? IS NULL'.

-- name: list_schemas
SELECT
    TRIM(SCHEMANAME)  AS name,
    TRIM(OWNER)       AS owner,
    REMARKS           AS remarks
FROM SYSCAT.SCHEMATA
ORDER BY SCHEMANAME

-- name: list_tables
SELECT
    TRIM(TABSCHEMA)   AS schema,
    TRIM(TABNAME)     AS name,
    TYPE              AS type,
    REMARKS           AS remarks
FROM SYSCAT.TABLES
WHERE (CAST(:schema AS VARCHAR(128)) IS NULL OR TABSCHEMA = :schema)
  AND (CAST(:kind   AS VARCHAR(1))   IS NULL OR TYPE      = :kind)
ORDER BY TABSCHEMA, TABNAME

-- name: describe_table_header
SELECT
    TRIM(TABSCHEMA)   AS schema,
    TRIM(TABNAME)     AS name,
    TYPE              AS type,
    REMARKS           AS remarks
FROM SYSCAT.TABLES
WHERE TABSCHEMA = :schema AND TABNAME = :table

-- name: describe_table_columns
SELECT
    TRIM(COLNAME)                              AS name,
    TYPENAME                                   AS type,
    CASE NULLS WHEN 'Y' THEN 1 ELSE 0 END      AS nullable,
    "DEFAULT"                                  AS col_default,
    LENGTH                                     AS length,
    SCALE                                      AS scale,
    COLNO                                      AS ordinal,
    REMARKS                                    AS remarks
FROM SYSCAT.COLUMNS
WHERE TABSCHEMA = :schema AND TABNAME = :table
ORDER BY COLNO

-- name: describe_table_pk
SELECT
    TRIM(k.COLNAME)  AS column_name,
    k.COLSEQ         AS ordinal
FROM SYSCAT.TABCONST tc
JOIN SYSCAT.KEYCOLUSE k
  ON tc.CONSTNAME = k.CONSTNAME
 AND tc.TABSCHEMA = k.TABSCHEMA
 AND tc.TABNAME   = k.TABNAME
WHERE tc.TABSCHEMA = :schema
  AND tc.TABNAME   = :table
  AND tc.TYPE      = 'P'
ORDER BY k.COLSEQ

-- name: list_index_columns
SELECT
    TRIM(i.INDSCHEMA)   AS index_schema,
    TRIM(i.INDNAME)     AS index_name,
    TRIM(i.TABSCHEMA)   AS table_schema,
    TRIM(i.TABNAME)     AS table_name,
    CASE i.UNIQUERULE
        WHEN 'U' THEN 1
        WHEN 'P' THEN 1
        ELSE 0
    END                 AS is_unique,
    i.INDEXTYPE         AS index_type,
    TRIM(ic.COLNAME)    AS column_name,
    ic.COLSEQ           AS ordinal,
    ic.COLORDER         AS ordering
FROM SYSCAT.INDEXES i
JOIN SYSCAT.INDEXCOLUSE ic
  ON i.INDSCHEMA = ic.INDSCHEMA
 AND i.INDNAME   = ic.INDNAME
WHERE i.TABSCHEMA = :schema
  AND i.TABNAME   = :table
ORDER BY i.INDSCHEMA, i.INDNAME, ic.COLSEQ

-- name: list_foreign_keys_header
SELECT
    TRIM(r.CONSTNAME)      AS name,
    TRIM(r.TABSCHEMA)      AS schema,
    TRIM(r.TABNAME)        AS table_name,
    TRIM(r.REFTABSCHEMA)   AS ref_schema,
    TRIM(r.REFTABNAME)     AS ref_table,
    r.DELETERULE           AS delete_rule,
    r.UPDATERULE           AS update_rule
FROM SYSCAT.REFERENCES r
WHERE r.TABSCHEMA = :schema AND r.TABNAME = :table
ORDER BY r.CONSTNAME

-- name: list_foreign_keys_columns
SELECT
    TRIM(r.CONSTNAME)    AS constraint_name,
    TRIM(k.COLNAME)      AS column_name,
    TRIM(rk.COLNAME)     AS ref_column_name,
    k.COLSEQ             AS ordinal
FROM SYSCAT.REFERENCES r
JOIN SYSCAT.KEYCOLUSE k
  ON  k.CONSTNAME = r.CONSTNAME
 AND  k.TABSCHEMA = r.TABSCHEMA
 AND  k.TABNAME   = r.TABNAME
JOIN SYSCAT.KEYCOLUSE rk
  ON  rk.CONSTNAME = r.REFKEYNAME
 AND  rk.TABSCHEMA = r.REFTABSCHEMA
 AND  rk.TABNAME   = r.REFTABNAME
 AND  rk.COLSEQ    = k.COLSEQ
WHERE r.TABSCHEMA = :schema AND r.TABNAME = :table
ORDER BY r.CONSTNAME, k.COLSEQ

-- name: get_view
SELECT
    TRIM(VIEWSCHEMA)   AS schema,
    TRIM(VIEWNAME)     AS name,
    TEXT               AS definition,
    READONLY           AS readonly
FROM SYSCAT.VIEWS
WHERE VIEWSCHEMA = :schema AND VIEWNAME = :view

-- name: list_procedures
SELECT
    TRIM(ROUTINESCHEMA)  AS schema,
    TRIM(ROUTINENAME)    AS name,
    ROUTINETYPE          AS type,
    LANGUAGE             AS language,
    REMARKS              AS remarks
FROM SYSCAT.ROUTINES
WHERE (CAST(:schema AS VARCHAR(128)) IS NULL OR ROUTINESCHEMA = :schema)
  AND ROUTINETYPE = 'P'
ORDER BY ROUTINESCHEMA, ROUTINENAME

-- name: list_triggers
SELECT
    TRIM(TRIGSCHEMA)   AS schema,
    TRIM(TRIGNAME)     AS name,
    TRIM(TABSCHEMA)    AS table_schema,
    TRIM(TABNAME)      AS table_name,
    TRIGEVENT          AS event,
    TRIGTIME           AS timing,
    TEXT               AS definition
FROM SYSCAT.TRIGGERS
WHERE (CAST(:schema AS VARCHAR(128)) IS NULL OR TRIGSCHEMA = :schema)
ORDER BY TRIGSCHEMA, TRIGNAME

-- name: get_table_stats
SELECT
    TRIM(t.TABSCHEMA)                              AS schema,
    TRIM(t.TABNAME)                                AS name,
    t.CARD                                         AS row_count,
    BIGINT(t.NPAGES) * BIGINT(ts.PAGESIZE)         AS size_bytes,
    t.STATS_TIME                                   AS last_analyzed
FROM SYSCAT.TABLES t
LEFT JOIN SYSCAT.TABLESPACES ts
  ON ts.TBSPACEID = t.TBSPACEID
WHERE t.TABSCHEMA = :schema AND t.TABNAME = :table

-- ---------------------------------------------------------------------------
-- EXPLAIN plan readers. Each query returns the rows for the most recent
-- EXPLAIN_INSTANCE only (the one we just wrote with EXPLAIN PLAN FOR ...).
-- Tables are expected to exist in the current schema; the nested-JSON
-- assembly in dbplus.drivers._db2.explain.DB2ExplainMixin.explain() groups
-- the rows by their natural composite keys.
-- ---------------------------------------------------------------------------

-- name: explain_latest_instance
SELECT * FROM EXPLAIN_INSTANCE
WHERE EXPLAIN_TIME = (SELECT MAX(EXPLAIN_TIME) FROM EXPLAIN_INSTANCE)

-- name: explain_latest_statements
SELECT * FROM EXPLAIN_STATEMENT
WHERE EXPLAIN_TIME = (SELECT MAX(EXPLAIN_TIME) FROM EXPLAIN_INSTANCE)
ORDER BY STMTNO, SECTNO

-- name: explain_latest_operators
SELECT * FROM EXPLAIN_OPERATOR
WHERE EXPLAIN_TIME = (SELECT MAX(EXPLAIN_TIME) FROM EXPLAIN_INSTANCE)
ORDER BY STMTNO, SECTNO, OPERATOR_ID

-- name: explain_latest_arguments
SELECT * FROM EXPLAIN_ARGUMENT
WHERE EXPLAIN_TIME = (SELECT MAX(EXPLAIN_TIME) FROM EXPLAIN_INSTANCE)
ORDER BY OPERATOR_ID, ARGUMENT_TYPE

-- name: explain_latest_predicates
SELECT * FROM EXPLAIN_PREDICATE
WHERE EXPLAIN_TIME = (SELECT MAX(EXPLAIN_TIME) FROM EXPLAIN_INSTANCE)
ORDER BY OPERATOR_ID, PREDICATE_ID

-- name: explain_latest_streams
SELECT * FROM EXPLAIN_STREAM
WHERE EXPLAIN_TIME = (SELECT MAX(EXPLAIN_TIME) FROM EXPLAIN_INSTANCE)
ORDER BY STREAM_ID

-- name: explain_latest_objects
SELECT * FROM EXPLAIN_OBJECT
WHERE EXPLAIN_TIME = (SELECT MAX(EXPLAIN_TIME) FROM EXPLAIN_INSTANCE)
ORDER BY OBJECT_SCHEMA, OBJECT_NAME

-- name: explain_latest_diagnostics
SELECT * FROM EXPLAIN_DIAGNOSTIC
WHERE EXPLAIN_TIME = (SELECT MAX(EXPLAIN_TIME) FROM EXPLAIN_INSTANCE)
ORDER BY DIAGNOSTIC_ID

-- name: explain_latest_diagnostic_data
SELECT * FROM EXPLAIN_DIAGNOSTIC_DATA
WHERE EXPLAIN_TIME = (SELECT MAX(EXPLAIN_TIME) FROM EXPLAIN_INSTANCE)
ORDER BY DIAGNOSTIC_ID, ORDINAL

-- ---------------------------------------------------------------------------
-- Workload / monitor queries, powering the MCP workload tools
-- (workload_snapshot, locks_current, deadlocks_recent).
-- MON_GET_* functions need SQLADM or DBADM; the caller is assumed to have
-- been granted them. All of these are read-only.
-- ---------------------------------------------------------------------------

-- name: workload_package_cache
SELECT
    CAST(STMT_TEXT AS VARCHAR(4000))  AS stmt_text,
    NUM_EXECUTIONS                    AS num_executions,
    TOTAL_CPU_TIME                    AS total_cpu_time_us,
    STMT_EXEC_TIME                    AS total_exec_time_ms,
    ROWS_READ                         AS rows_read,
    ROWS_RETURNED                     AS rows_returned,
    TOTAL_ACT_WAIT_TIME               AS total_wait_time_ms,
    LAST_METRICS_UPDATE               AS last_updated
FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2))
WHERE (CAST(:minutes AS INTEGER) IS NULL
       OR LAST_METRICS_UPDATE >= CURRENT_TIMESTAMP - (CAST(:minutes AS INTEGER)) MINUTES)
ORDER BY TOTAL_CPU_TIME DESC

-- name: locks_current
-- MON_GET_LOCKS columns used here are the portable subset documented for
-- DB2 9.7+ LUW. Note: LOCK_ESCALATION is NOT a column of MON_GET_LOCKS; it
-- lives on the LOCK_EVENT table written by a locking event monitor.
SELECT
    l.APPLICATION_HANDLE  AS application_handle,
    l.LOCK_NAME           AS lock_name,
    l.LOCK_OBJECT_TYPE    AS lock_object_type,
    l.LOCK_MODE           AS lock_mode,
    l.LOCK_STATUS         AS lock_status,
    l.LOCK_ATTRIBUTES     AS lock_attributes,
    l.TBSP_ID             AS tbsp_id,
    l.TAB_FILE_ID         AS tab_file_id,
    TRIM(t.TABSCHEMA)     AS tabschema,
    TRIM(t.TABNAME)       AS tabname
FROM TABLE(MON_GET_LOCKS(NULL, -2)) l
LEFT JOIN SYSCAT.TABLES t
  ON t.TBSPACEID = l.TBSP_ID
 AND t.TABLEID   = l.TAB_FILE_ID
ORDER BY l.APPLICATION_HANDLE, l.LOCK_NAME

-- name: find_lock_event_table
-- Locates the schema of the LOCK_EVENT table written to by an active
-- CREATE EVENT MONITOR FOR LOCKING. If multiple exist, pick the most
-- recently created one.
SELECT
    TRIM(TABSCHEMA) AS schema,
    TRIM(TABNAME)   AS name
FROM SYSCAT.TABLES
WHERE TABNAME = 'LOCK_EVENT'
  AND TYPE    = 'T'
ORDER BY CREATE_TIME DESC
FETCH FIRST 1 ROW ONLY

-- ---------------------------------------------------------------------------
-- Phase 3 additions: bulk stats, search, and query introspection
-- ---------------------------------------------------------------------------

-- name: list_table_stats
-- Returns catalog stats for every base table in a schema, ordered by
-- estimated cardinality descending (biggest tables first).
SELECT
    TRIM(t.TABSCHEMA)                          AS schema,
    TRIM(t.TABNAME)                            AS name,
    t.CARD                                     AS row_count,
    BIGINT(t.NPAGES) * BIGINT(ts.PAGESIZE)     AS size_bytes,
    t.STATS_TIME                               AS last_analyzed
FROM SYSCAT.TABLES t
LEFT JOIN SYSCAT.TABLESPACES ts
  ON ts.TBSPACEID = t.TBSPACEID
WHERE t.TABSCHEMA = :schema
  AND t.TYPE      = 'T'
ORDER BY t.CARD DESC NULLS LAST, t.TABNAME

-- name: search_tables
SELECT
    'TABLE'               AS kind,
    TRIM(TABSCHEMA)       AS schema,
    TRIM(TABNAME)         AS name,
    CAST(NULL AS VARCHAR(128)) AS parent_name,
    REMARKS               AS remarks
FROM SYSCAT.TABLES
WHERE UPPER(TABNAME) LIKE UPPER(:pattern)
  AND TYPE = 'T'
ORDER BY TABSCHEMA, TABNAME

-- name: search_views
SELECT
    'VIEW'                AS kind,
    TRIM(TABSCHEMA)       AS schema,
    TRIM(TABNAME)         AS name,
    CAST(NULL AS VARCHAR(128)) AS parent_name,
    REMARKS               AS remarks
FROM SYSCAT.TABLES
WHERE UPPER(TABNAME) LIKE UPPER(:pattern)
  AND TYPE = 'V'
ORDER BY TABSCHEMA, TABNAME

-- name: search_columns
SELECT
    'COLUMN'              AS kind,
    TRIM(TABSCHEMA)       AS schema,
    TRIM(COLNAME)         AS name,
    TRIM(TABNAME)         AS parent_name,
    REMARKS               AS remarks
FROM SYSCAT.COLUMNS
WHERE UPPER(COLNAME) LIKE UPPER(:pattern)
ORDER BY TABSCHEMA, TABNAME, COLNO

-- name: search_procedures
SELECT
    'PROCEDURE'           AS kind,
    TRIM(ROUTINESCHEMA)   AS schema,
    TRIM(ROUTINENAME)     AS name,
    CAST(NULL AS VARCHAR(128)) AS parent_name,
    REMARKS               AS remarks
FROM SYSCAT.ROUTINES
WHERE UPPER(ROUTINENAME) LIKE UPPER(:pattern)
  AND ROUTINETYPE = 'P'
ORDER BY ROUTINESCHEMA, ROUTINENAME
