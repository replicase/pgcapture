// source: https://github.com/enova/pgl_ddl_deploy/blob/master/pgl_ddl_deploy.c

#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "parser/parser.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pgl_ddl_deploy_current_query);
PG_FUNCTION_INFO_V1(sql_command_tags);

/* Our own version of debug_query_string - see below */
const char *pgl_ddl_deploy_debug_query_string;

/*
 * A near-copy of the current_query postgres function which caches the value, ignoring
 * the change if the string is changed to NULL, as is being done in pglogical 2.2.2.
 * This allows multiple subsequent calls to pglogical.replicate_ddl_command without
 * losing access to current_query.
 *
 * Please revisit if pglogical changes behavior and stop setting debug_query_string to NULL.
 */
Datum
pgl_ddl_deploy_current_query(PG_FUNCTION_ARGS)
{
    /* If debug_query_string is set, we always want the same value */
    if (debug_query_string)
    {
        pgl_ddl_deploy_debug_query_string = debug_query_string;
        PG_RETURN_TEXT_P(cstring_to_text(pgl_ddl_deploy_debug_query_string));
    }
    /* If it is NULL, we want to take pgl_ddl_deploy_debug_query_string instead,
       which in most cases in this code path is used in pgl_ddl_deploy we expect
       is because pglogical has reset the string to null.  But we still want to
       return the same value in this SQL statement we are executing.
    */
    else if (pgl_ddl_deploy_debug_query_string)
    {
        PG_RETURN_TEXT_P(cstring_to_text(pgl_ddl_deploy_debug_query_string));
    }
    else
    /* If both are NULL, that is legit and we want to return NULL. */
    {
        PG_RETURN_NULL();
    }
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
    PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate, CurrentMemoryContext));
}