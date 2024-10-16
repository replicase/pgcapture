/*--------------------------------------------------------------------
 * pg_import.h
 *
 * Code imported from upstream.
 *--------------------------------------------------------------------
 */
#ifndef PG_IMPORT_H
#define PG_IMPORT_H

#include "postgres.h"

#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/partition.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/syscache.h"

/* Imported from src/backend/commands/tablecmds.c */
struct DropRelationCallbackState
{
	/* These fields are set by RemoveRelations: */
	char		expected_relkind;
	LOCKMODE	heap_lockmode;
	/* These fields are state to track which subsidiary locks are held: */
	Oid			heapOid;
	Oid			partParentOid;
	/* These fields are passed back by RangeVarCallbackForDropRelation: */
	char		actual_relkind;
	char		actual_relpersistence;
};

/* Imported from src/backend/commands/tablecmds.c */
void RangeVarCallbackForAlterRelation(const RangeVar *rv, Oid relid,
											 Oid oldrelid, void *arg);

/* Imported from src/backend/commands/tablecmds.c */
void RangeVarCallbackForDropRelation(const RangeVar *rel, Oid relOid,
											Oid oldRelOid, void *arg);

#endif		/* PG_IMPORT_H */
