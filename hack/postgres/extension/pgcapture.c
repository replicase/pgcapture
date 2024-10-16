#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"
#include "fmgr.h"
#include "parser/analyze.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "parser/scansup.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"

/*--- Local includes ---*/

#include "pgcapture.h"
#include "pg_import.h"


PG_MODULE_MAGIC;

/*--- Private variables ---*/

/* Cache of the current utility command query string. */
static char *pgc_current_utility_string = NULL;

/*--- Functions --- */

PGDLLEXPORT void _PG_init(void);

static void pgc_ProcessUtility_hook(UTILITY_HOOK_ARGS);
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

char *extract_query_text(PlannedStmt *pstmt, const char *query,
						 int query_location, int query_len);
Oid rv_get_rename_relid(RenameStmt *rename);
Oid rv_get_drop_relid(RangeVar *rv, char relkind, bool concurrent);

PG_FUNCTION_INFO_V1(pgl_ddl_deploy_current_query);
PG_FUNCTION_INFO_V1(sql_command_tags);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Install hooks */
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pgc_ProcessUtility_hook;
}


/*
 * ProcessUtility hook
 *
 * Cache the relevant part of the current utility query string.
 *
 * Utility commands executed by a CREATE EXTENSION command are ignored.
 */
static void
pgc_ProcessUtility_hook(UTILITY_HOOK_ARGS)
{
	char *old = pgc_current_utility_string;

	if (creating_extension)
		pgc_current_utility_string = NULL;
	else
		pgc_current_utility_string = extract_query_text(pstmt,
														queryString,
														pstmt->stmt_location,
														pstmt->stmt_len);

	if (prev_ProcessUtility)
		prev_ProcessUtility(UTILITY_HOOK_ARG_NAMES);
	else
		standard_ProcessUtility(UTILITY_HOOK_ARG_NAMES);

	pgc_current_utility_string = old;
}


/*
 * Given a possibly multi-statement source string, return a palloc'd copy of
 * the relevant part of the string, ignoring temporary relations.
 *
 * For any relation related that is not a DROP TABLE, if the target relation is
 * temporary the statement is entirely ignored and NULL is returned.
 *
 * For a DROP TABLE statement, all temporary relation names are removed for the
 * statement if any.  If the resulting command doesn't have any table then NULL
 * is also returned.
 */
char *
extract_query_text(PlannedStmt *pstmt, const char *query, int query_location,
				   int query_len)
{
	char	   *extracted;
	Node	   *stmt = pstmt->utilityStmt;

	Assert(stmt);

	switch (nodeTag(stmt))
	{
		case T_CreateStmt:
			{
				CreateStmt *create = castNode(CreateStmt, stmt);
				Oid			nspid;

				/*
				 * Get the target schema the given CREATE TABLE will use.
				 * This uses the same approach as DefineRelation to ensure a
				 * consistent schema detection.
				 */
				nspid = RangeVarGetAndCheckCreationNamespace(create->relation,
															 NoLock, NULL);

				/* Ignore the statement if it's the temp schema. */
				if (isTempNamespace(nspid))
					return NULL;

				break;
			}
		case T_AlterTableStmt:
			{
				AlterTableStmt *atstmt = castNode(AlterTableStmt, stmt);
				Oid			relid;
				LOCKMODE	lockmode;

				/*
				 * Get the underlying relation.
				 * This uses the same approach as ProcessUtilitySlow for an
				 * ALTER TABLE statement to ensure consistent relation
				 * detection.
				 */
				lockmode = AlterTableGetLockLevel(atstmt->cmds);
				relid = AlterTableLookupRelation(atstmt, lockmode);

				/* Ignore the statement if it's the temp schema. */
				if (get_rel_persistence(relid) == RELPERSISTENCE_TEMP)
					return NULL;

				break;
			}
		case T_CreateTableAsStmt:
			{
				CreateTableAsStmt *create = castNode(CreateTableAsStmt, stmt);
				Oid			nspid;

				/*
				 * Get the target schema the given CREATE TABLE AS / SELECT
				 * INTO will use.
				 * This uses the same approach as ExecCreateTableAs to ensure a
				 * consistent schema detection.
				 */
				nspid = RangeVarGetAndCheckCreationNamespace(create->into->rel,
															 NoLock, NULL);

				/* Ignore the statement if it's the temp schema. */
				if (isTempNamespace(nspid))
					return NULL;

				break;
			}
		case T_DropStmt:
			{
				DropStmt   *drop = castNode(DropStmt, stmt);
				char	   *dropkind;
				char		relkind;
				StringInfo  buf;
				ListCell   *lc;
				bool		do_check = true;

				switch (drop->removeType)
				{
					case OBJECT_INDEX:
						dropkind = "INDEX";
						relkind = RELKIND_INDEX;
						break;
					case OBJECT_TABLE:
						dropkind = "TABLE";
						relkind = RELKIND_RELATION;
						break;
					case OBJECT_SEQUENCE:
						dropkind = "SEQUENCE";
						relkind = RELKIND_SEQUENCE;
						break;
					case OBJECT_VIEW:
						dropkind = "VIEW";
						relkind = RELKIND_VIEW;
						break;
					default:
						/* We won't need to check for temp relation. */
						do_check = false;
						break;
				}

				/*
				 * If the DROP statement cannot contain temp relations, stop
				 * here and let the later code extract the query text.
				 */
				if (!do_check)
					break;

				/*
				 * The DROP might contain temp relation.  Loop over all objects
				 * in the statement and generate a new DROP statement, ignoring
				 * all references to temporary relations.
				 */
				buf = makeStringInfo();

				foreach(lc, drop->objects)
				{
					List	   *names = lfirst(lc);
					RangeVar   *rv = makeRangeVarFromNameList(names);
					Oid			relid;
					char	   *nspname;
					char	   *relname;

					relid = rv_get_drop_relid(rv, relkind, drop->concurrent);

					if (get_rel_persistence(relid) == RELPERSISTENCE_TEMP)
						continue;

					nspname = get_namespace_name(get_rel_namespace(relid));
					relname = get_rel_name(relid);

					if (buf->len == 0)
					{
						/*
						 * First relation found.  Start generating the DROP
						 * command.
						 * We generate it using the same flags, so we need to
						 * detect whether the original query specified
						 * CONCURRENTLY and / or IF EXISTS.
						 */
						appendStringInfo(buf, "DROP %s ", dropkind);

						if (drop->concurrent)
							appendStringInfo(buf, "CONCURRENTLY ");

						if (drop->missing_ok)
							appendStringInfo(buf, "IF EXISTS ");
					}
					else
					{
						/*
						 * Another relation found, separate the name with a
						 * coma.
						 */
						appendStringInfoString(buf, ", ");
					}

					if (rv->schemaname)
						appendStringInfo(buf, "%s.%s",
										 quote_identifier(nspname),
										 quote_identifier(relname));
					else
						appendStringInfo(buf, "%s", quote_identifier(relname));
				}

				/*
				 * If we generated a query, return it.  Otherwise, return NULL
				 * as it mean it was a DROP that contained only temporary
				 * relations that should be ignored.
				 */
				if (buf->len > 0)
				{
					/* We need to finalize the query string before sending, if
					 * CASCADE was specified.
					 */
					if (drop->behavior == DROP_CASCADE)
						appendStringInfo(buf, " CASCADE");

					return buf->data;
				}
				else
					return NULL;

				break;
			}
		case T_RenameStmt:
			{
				RenameStmt *rename = castNode(RenameStmt, stmt);
				Oid			relid;
				bool		do_check;

				switch (rename->renameType)
				{
					case OBJECT_ATTRIBUTE:
					case OBJECT_COLUMN:
					case OBJECT_INDEX:
					case OBJECT_TABLE:
					case OBJECT_SEQUENCE:
					case OBJECT_VIEW:
						/* We will need to check for temp relation. */
						do_check = true;
						break;
					default:
						/* We won't need to check for temp relation. */
						do_check = false;
						break;
				}

				/*
				 * If the ALTER ... RENAME statement cannot contain temp
				 * relations, break here and let the regular code extract the
				 * query text.
				 */
				if (!do_check)
					break;

				relid = rv_get_rename_relid(rename);

				/*
				 * If the relation doesn't exist and the command didn't fail
				 * (ie. it has an IF EXIST clause), ignore the command.
				 */
				if (!OidIsValid(relid))
					return NULL;

				if (get_rel_persistence(relid) == RELPERSISTENCE_TEMP)
					return NULL;

				break;
			}
		case T_ViewStmt:
			{
				ViewStmt   *view = castNode(ViewStmt, stmt);
				RawStmt    *rawstmt;
				Query	   *viewParse;
				RangeVar   *rv = view->view;

				/* Explicit CREATE TEMP VIEW? */
				if (rv->relpersistence == RELPERSISTENCE_TEMP)
					return NULL;

				/*
				 * A view referring to temporary object will implicitly be a
				 * temporary view.
				 */
				rawstmt = makeNode(RawStmt);
				rawstmt->stmt = view->query;
				rawstmt->stmt_location = query_location;
				rawstmt->stmt_len = query_len;

				viewParse = parse_analyze_fixedparams(rawstmt, query, NULL, 0,
													  NULL);

				if (isQueryUsingTempRelation(viewParse))
					return NULL;

				break;
			}
		default:
			/* Nothing special to do. */
			break;
	}

	/* First apply starting offset, unless it's -1 (unknown). */
	if (query_location >= 0)
	{
		Assert(query_location <= strlen(query));
		query += query_location;
		/* Length of 0 (or -1) means "rest of string" */
		if (query_len <= 0)
			query_len = strlen(query);
		else
			Assert(query_len <= strlen(query));
	}
	else
	{
		/* If query location is unknown, distrust query_len as well */
		query_location = 0;
		query_len = strlen(query);
	}

	/*
	 * Discard leading and trailing whitespace, too.  Use scanner_isspace()
	 * not libc's isspace(), because we want to match the lexer's behavior.
	 */
	while (query_len > 0 && scanner_isspace(query[0]))
		query++, query_location++, query_len--;
	while (query_len > 0 && scanner_isspace(query[query_len - 1]))
		query_len--;

	extracted = palloc(query_len + 1);
	strncpy(extracted, query, query_len);
	extracted[query_len] = 0;

	return extracted;
}


/* Get the Oid of a relation to be dropped.
 *
 * This performs the same steps as RenameRelation to identify the actual
 * relation that is about to be modified.
 */
Oid
rv_get_rename_relid(RenameStmt *stmt)
{
	bool		is_index_stmt = stmt->renameType == OBJECT_INDEX;
	Oid			relid;

	/*
	 * Grab an exclusive lock on the target table, index, sequence, view,
	 * materialized view, or foreign table, which we will NOT release until
	 * end of transaction.
	 *
	 * Lock level used here should match RenameRelationInternal, to avoid lock
	 * escalation.  However, because ALTER INDEX can be used with any relation
	 * type, we mustn't believe without verification.
	 */
	for (;;)
	{
		LOCKMODE	lockmode;
		char		relkind;
		bool		obj_is_index;

		lockmode = is_index_stmt ? ShareUpdateExclusiveLock : AccessExclusiveLock;

		relid = RangeVarGetRelidExtended(stmt->relation, lockmode,
										 stmt->missing_ok ? RVR_MISSING_OK : 0,
										 RangeVarCallbackForAlterRelation,
										 (void *) stmt);

		if (!OidIsValid(relid))
			return InvalidOid;

		/*
		 * We allow mismatched statement and object types (e.g., ALTER INDEX
		 * to rename a table), but we might've used the wrong lock level.  If
		 * that happens, retry with the correct lock level.  We don't bother
		 * if we already acquired AccessExclusiveLock with an index, however.
		 */
		relkind = get_rel_relkind(relid);
		obj_is_index = (relkind == RELKIND_INDEX ||
						relkind == RELKIND_PARTITIONED_INDEX);
		if (obj_is_index || is_index_stmt == obj_is_index)
			break;

		UnlockRelationOid(relid, lockmode);
		is_index_stmt = obj_is_index;
	}

	return relid;
}

/* Get the Oid of a relation to be dropped.
 *
 * This performs the same steps as RemoveRelations to identify the actual
 * relation that is about to be dropped.
 */
Oid
rv_get_drop_relid(RangeVar *rv, char relkind, bool concurrent)
{
	Oid		relOid;
	LOCKMODE	lockmode = AccessExclusiveLock;
	struct DropRelationCallbackState state;

	AcceptInvalidationMessages();

	if (concurrent)
		lockmode = ShareUpdateExclusiveLock;

	/* Look up the appropriate relation using namespace search. */
	state.expected_relkind = relkind;
	state.heap_lockmode = concurrent ?
		ShareUpdateExclusiveLock : AccessExclusiveLock;
	/* We must initialize these fields to show that no locks are held: */
	state.heapOid = InvalidOid;
	state.partParentOid = InvalidOid;

	relOid = RangeVarGetRelidExtended(rv, lockmode, RVR_MISSING_OK,
									  RangeVarCallbackForDropRelation,
									  (void *) &state);

	return relOid;
}

/*
 * Returns the cached current utility command query string if any.
 */
Datum
pgl_ddl_deploy_current_query(PG_FUNCTION_ARGS)
{
	if (pgc_current_utility_string)
		PG_RETURN_TEXT_P(cstring_to_text(pgc_current_utility_string));
	else
		PG_RETURN_NULL();
}

/*
 * Return a text array of the command tags in SQL command
 */
Datum
sql_command_tags(PG_FUNCTION_ARGS)
{
    text            *sql_t  = PG_GETARG_TEXT_P(0);
    char            *sql;
    List            *parsetree_list;
    ListCell        *parsetree_item;
    const char      *commandTag;
    ArrayBuildState *astate = NULL;

    /*
     * Get the SQL parsetree
     */
    sql = text_to_cstring(sql_t);
    parsetree_list = pg_parse_query(sql);

    /*
     * Iterate through each parsetree_item to get CommandTag
     */
    foreach(parsetree_item, parsetree_list)
    {
        Node    *parsetree = (Node *) lfirst(parsetree_item);
#if PG_VERSION_NUM >= 130000
        commandTag         = CreateCommandName(parsetree);
#else
        commandTag         = CreateCommandTag(parsetree);
#endif
        astate             = accumArrayResult(astate, CStringGetTextDatum(commandTag),
                             false, TEXTOID, CurrentMemoryContext);
    }
    if (astate == NULL)
                elog(ERROR, "Invalid sql command");
    PG_RETURN_ARRAYTYPE_P(DatumGetPointer(makeArrayResult(astate, CurrentMemoryContext)));
}
