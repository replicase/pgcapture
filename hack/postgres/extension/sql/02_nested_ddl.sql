LOAD 'pgcapture';
TRUNCATE pgcapture.ddl_logs;

DO $$
BEGIN
    CREATE TABLE tbl1();
    CREATE TABLE tbl2();
END;
$$ LANGUAGE plpgsql;

CREATE EXTENSION pg_stat_statements;

-- Check the results
SELECT query, unnest(tags) FROM pgcapture.ddl_logs ORDER BY id;
