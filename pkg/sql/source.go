package sql

var QueryAttrTypeOID = `SELECT nspname, relname, attname, atttypid
FROM pg_catalog.pg_namespace n
JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relkind = 'r'
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 and a.attisdropped = false
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pglogical') AND n.nspname !~ '^pg_toast';`

var QueryIdentityKeys = `SELECT nspname, relname, array(select attname from pg_catalog.pg_attribute where attrelid = i.indrelid AND attnum > 0 AND attnum = ANY(i.indkey)) as keys
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indrelid AND c.relkind = 'r'
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pglogical') AND n.nspname !~ '^pg_toast'
WHERE (i.indisprimary OR i.indisunique) AND i.indisvalid AND i.indpred IS NULL ORDER BY indisprimary;`

var CreateLogicalSlot = `SELECT pg_create_logical_replication_slot($1, $2);`

var InstallExtension = `CREATE EXTENSION IF NOT EXISTS pgcapture;`
