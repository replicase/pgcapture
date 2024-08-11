/*--------------------------------------------------------------------
 * pgcapture.h
 *
 * Compatibility macros.
 *--------------------------------------------------------------------
 */
#ifndef PGCAPTURE_H
#define PGCAPTURE_H

/* ProcessUtility_hook */
#if PG_VERSION_NUM >= 140000
#define UTILITY_HOOK_ARGS PlannedStmt *pstmt, const char *queryString, \
							bool readOnlyTree, \
							ProcessUtilityContext context, ParamListInfo params, \
							QueryEnvironment *queryEnv, \
							DestReceiver *dest, QueryCompletion *qc
#define UTILITY_HOOK_ARG_NAMES pstmt, queryString, \
							readOnlyTree, \
							context, params, \
							queryEnv, \
							dest, qc
#elif PG_VERSION_NUM >= 130000
#define UTILITY_HOOK_ARGS PlannedStmt *pstmt, \
							const char *queryString, \
							ProcessUtilityContext context, \
							ParamListInfo params, \
							QueryEnvironment *queryEnv, \
							DestReceiver *dest, QueryCompletion *qc
#define UTILITY_HOOK_ARG_NAMES pstmt, \
							queryString, \
							context, \
							params, \
							queryEnv, \
							dest, qc
#elif PG_VERSION_NUM >= 100000
#define UTILITY_HOOK_ARGS PlannedStmt *pstmt, \
							const char *queryString, \
							ProcessUtilityContext context, \
							ParamListInfo params, \
							QueryEnvironment *queryEnv, \
							DestReceiver *dest, char *completionTag
#define UTILITY_HOOK_ARG_NAMES pstmt, \
							queryString, \
							context, \
							params, \
							queryEnv, \
							dest, completionTag
#endif
/* end of ProcessUtility_hook */

#endif		/* PGCAPTURE_H */
