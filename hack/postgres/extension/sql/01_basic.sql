-- Test the DDL SRF
SELECT * FROM pgcapture.current_query();

-- Test some basic DDL commands
CREATE TABLE ctas AS SELECT 1 as id;
CREATE TABLE ct(id integer);

CREATE SCHEMA nsp1;
CREATE TABLE nsp1.tbl(id integer, val text);
CREATE INDEX ON nsp1.tbl (val) WHERE id % 2 = 0;

-- DDL command that doesn't contain a trailing semi-column
CREATE SCHEMA nsp2\g

-- Check the results
SELECT query, unnest(tags) FROM pgcapture.ddl_logs ORDER BY id;
