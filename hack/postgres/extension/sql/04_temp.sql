LOAD 'pgcapture';

-- Create a normal table that will later be dropped
CREATE TABLE dropme();
CREATE TABLE dropmetoo();

TRUNCATE pgcapture.ddl_logs;

-- Plain TEMP table should be ignored
CREATE TEMPORARY TABLE tmp1();
ALTER TABLE tmp1 ADD COLUMN id integer;

-- Table created in the pg_temp schema should be ignored
CREATE TABLE pg_temp.tmp2();
ALTER TABLE pg_temp.tmp2 ADD COLUMN id integer;

-- Table created in the pg_temp schema using the search_path should alse be
-- ignored
SET search_path TO not_a_schema, pg_temp, public;
CREATE TABLE tmp3();
ALTER TABLE tmp3 ADD COLUMN id integer;
RESET search_path;

-- CTAS / SELECT INTO for temp tables should be ignored
CREATE TABLE pg_temp.tmp4 AS SELECT 1 AS id;

-- CREATE TEMP VIEW should be ignored
CREATE TEMP VIEW v1 AS SELECT 1 AS id;

-- Implicitly temp view creation should be ignored
CREATE VIEW v2 AS SELECT * FROM tmp3;

-- ALTER ... RENAME should ignore temp relations
ALTER TABLE tmp4 RENAME COLUMN id TO id2;
ALTER TABLE tmp4 RENAME TO tmp4b;

-- Check the results
SELECT query, unnest(tags) FROM pgcapture.ddl_logs ORDER BY id;

-- Dropping only temp tables should be ignored
DROP TABLE tmp1, tmp2;

-- But dropping a mix of temp and regular table should preserve the regular
-- table names
DROP TABLE tmp3, pg_temp.tmp3, dropme, tmp4b, dropmetoo CASCADE;

-- Check the results
SELECT query, unnest(tags) FROM pgcapture.ddl_logs ORDER BY id;
