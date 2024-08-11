#include "postgres.h"

#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "fmgr.h"
#include "parser/parser.h"
#include "parser/scansup.h"
#include "tcop/utility.h"
#include "utils/builtins.h"

/*--- Local includes ---*/

#include "pgcapture.h"

PG_MODULE_MAGIC;

/*--- Private variables ---*/

/* Cache of the current utility command query string. */
char *pgc_current_utility_string = NULL;

/*--- Functions --- */

PGDLLEXPORT void _PG_init(void);

static void pgc_ProcessUtility_hook(UTILITY_HOOK_ARGS);
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

char *extract_query_text(const char *query, int query_location, int query_len);

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
		pgc_current_utility_string = extract_query_text(queryString,
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
 * the relevant part of the string.
 *
* Adapted from upstream pg_stat_statements / CleanQuerytext.
 */
char *
extract_query_text(const char *query, int query_location, int query_len)
{
	char *extracted;

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