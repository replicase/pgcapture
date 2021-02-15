-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgcapture" to load this file. \quit

CREATE TABLE pgcapture.ddl_logs (id SERIAL PRIMARY KEY, query TEXT, tags TEXT[], activity JSONB);
CREATE TABLE pgcapture.sources (id TEXT PRIMARY KEY, commit pg_lsn, commit_ts timestamptz, apply_ts timestamptz DEFAULT CURRENT_TIMESTAMP, msg_id bytea, status jsonb);

CREATE FUNCTION pgcapture.current_query()
    RETURNS TEXT AS
    'MODULE_PATHNAME', 'pgl_ddl_deploy_current_query'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pgcapture.sql_command_tags(p_sql TEXT)
    RETURNS TEXT[] AS
    'MODULE_PATHNAME', 'sql_command_tags'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pgcapture.log_ddl() RETURNS event_trigger AS $$
declare
qstr TEXT;
tags TEXT[];
acti JSONB;
begin
qstr = pgcapture.current_query();
tags = pgcapture.sql_command_tags(qstr);
select row_to_json(a.*) into acti from (select datname,usename,application_name,client_addr,backend_start,xact_start from pg_stat_activity where pid = pg_backend_pid()) a;
insert into pgcapture.ddl_logs(query, tags, activity) values (qstr,tags,acti);
end;
$$ LANGUAGE plpgsql STRICT;

CREATE EVENT TRIGGER pgcapture_ddl_command_start ON ddl_command_start WHEN tag IN (
    'CREATE TABLE AS',
    'SELECT INTO',
    'DROP TRIGGER',
    'DROP FUNCTION'
) EXECUTE PROCEDURE pgcapture.log_ddl();
CREATE EVENT TRIGGER pgcapture_ddl_command_end ON ddl_command_end WHEN TAG IN (
    'ALTER AGGREGATE',
    'ALTER COLLATION',
    'ALTER CONVERSION',
    'ALTER DOMAIN',
    'ALTER DEFAULT PRIVILEGES',
    'ALTER EXTENSION',
    'ALTER FOREIGN DATA WRAPPER',
    'ALTER FOREIGN TABLE',
    'ALTER FUNCTION',
    'ALTER LANGUAGE',
    'ALTER LARGE OBJECT',
    'ALTER MATERIALIZED VIEW',
    'ALTER OPERATOR',
    'ALTER OPERATOR CLASS',
    'ALTER OPERATOR FAMILY',
    'ALTER POLICY',
    'ALTER SCHEMA',
    'ALTER SEQUENCE',
    'ALTER SERVER',
    'ALTER TABLE',
    'ALTER TEXT SEARCH CONFIGURATION',
    'ALTER TEXT SEARCH DICTIONARY',
    'ALTER TEXT SEARCH PARSER',
    'ALTER TEXT SEARCH TEMPLATE',
    'ALTER TRIGGER',
    'ALTER TYPE',
    'ALTER USER MAPPING',
    'ALTER VIEW',
    'COMMENT',
    'CREATE ACCESS METHOD',
    'CREATE AGGREGATE',
    'CREATE CAST',
    'CREATE COLLATION',
    'CREATE CONVERSION',
    'CREATE DOMAIN',
    'CREATE EXTENSION',
    'CREATE FOREIGN DATA WRAPPER',
    'CREATE FOREIGN TABLE',
    'CREATE FUNCTION',
    'CREATE INDEX',
    'CREATE LANGUAGE',
    'CREATE MATERIALIZED VIEW',
    'CREATE OPERATOR',
    'CREATE OPERATOR CLASS',
    'CREATE OPERATOR FAMILY',
    'CREATE POLICY',
    'CREATE RULE',
    'CREATE SCHEMA',
    'CREATE SEQUENCE',
    'CREATE SERVER',
    'CREATE TABLE',
    'CREATE TEXT SEARCH CONFIGURATION',
    'CREATE TEXT SEARCH DICTIONARY',
    'CREATE TEXT SEARCH PARSER',
    'CREATE TEXT SEARCH TEMPLATE',
    'CREATE TRIGGER',
    'CREATE TYPE',
    'CREATE USER MAPPING',
    'CREATE VIEW',
    'DROP ACCESS METHOD',
    'DROP AGGREGATE',
    'DROP CAST',
    'DROP COLLATION',
    'DROP CONVERSION',
    'DROP DOMAIN',
    'DROP EXTENSION',
    'DROP FOREIGN DATA WRAPPER',
    'DROP FOREIGN TABLE',
    'DROP INDEX',
    'DROP LANGUAGE',
    'DROP MATERIALIZED VIEW',
    'DROP OPERATOR',
    'DROP OPERATOR CLASS',
    'DROP OPERATOR FAMILY',
    'DROP OWNED',
    'DROP POLICY',
    'DROP RULE',
    'DROP SCHEMA',
    'DROP SEQUENCE',
    'DROP SERVER',
    'DROP TABLE',
    'DROP TEXT SEARCH CONFIGURATION',
    'DROP TEXT SEARCH DICTIONARY',
    'DROP TEXT SEARCH PARSER',
    'DROP TEXT SEARCH TEMPLATE',
    'DROP TYPE',
    'DROP USER MAPPING',
    'DROP VIEW',
    'GRANT',
    'IMPORT FOREIGN SCHEMA',
    'REVOKE',
    'SECURITY LABEL'
) EXECUTE PROCEDURE pgcapture.log_ddl();