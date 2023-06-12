// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "db721_reader.h"

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "../../../../src/include/foreign/foreign.h"
#include "../../../../src/include/commands/defrem.h"
#include "../../../../src/include/optimizer/optimizer.h"
}
// clang-format on

#define OPTION_NAME_FILENAME "filename"
#define OPTION_NAME_TABLENAME "tablename"

typedef struct Db721FdwOptions
{
	char *filename;
  char *tablename;
} Db721FdwOptions;

static Db721FdwOptions * Db721GetOptions(Oid foreignTableId);
static char * Db721GetOptionValue(Oid foreignTableId, const char *optionName);
static double TupleCountEstimate(RelOptInfo *baserel, const char *filename);
extern uint64 Db721TableRowCount(const char *filename);


/*
 * Db721FdwOptions holds the option values to be used when reading or writing
 * a Db721 file. To resolve these values, we first check foreign table's options,
 * and if not present, we then fall back to the default values specified above.
 */
static Db721FdwOptions *
Db721GetOptions(Oid foreignTableId)
{
  Db721FdwOptions *db721FdwOptions = nullptr;
  char *filename = nullptr;
  char *tablename = nullptr;
  
  filename = Db721GetOptionValue(foreignTableId, OPTION_NAME_FILENAME);
  tablename = Db721GetOptionValue(foreignTableId, OPTION_NAME_TABLENAME);

  db721FdwOptions = reinterpret_cast<Db721FdwOptions *>(palloc0(sizeof(Db721FdwOptions)));
  db721FdwOptions->filename = filename;
  db721FdwOptions->tablename = tablename;

  return db721FdwOptions;
}

/*
 * Db721GetOptionValue walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value. This function is unchanged from cstore_fdw.
 */
static char *
Db721GetOptionValue(Oid foreignTableId, const char *optionName)
{
	ForeignTable *foreignTable = NULL;
	ForeignServer *foreignServer = NULL;
	List *optionList = NIL;
	ListCell *optionCell = NULL;
	char *optionValue = NULL;

	foreignTable = GetForeignTable(foreignTableId);
	foreignServer = GetForeignServer(foreignTable->serverid);

	optionList = list_concat(optionList, foreignTable->options);
	optionList = list_concat(optionList, foreignServer->options);

	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionDefName = optionDef->defname;

		if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0)
		{
			optionValue = defGetString(optionDef);
			break;
		}
	}

	return optionValue;
}

/*
 * TupleCountEstimate estimates the number of base relation tuples in the given
 * file.
 */
static double
TupleCountEstimate(RelOptInfo *baserel, const char *filename)
{
  
  double tupleCountEstimate = 0.0;

  /* check if the user executed Analyze on this foreign table before */
  if (baserel->pages > 0)
  {
    ereport(ERROR, (errmsg("baserel->pages should be zero"),
            errhint("baserel->pages is: %ud", baserel->pages)));
  }
  else
  {
    tupleCountEstimate = static_cast<double>(Db721TableRowCount(filename));
  }
  
  return tupleCountEstimate;
}


extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  // TODO(721): Write me!
  Dog terrior("Terrior");
  Db721FdwOptions *db721FdwOptions = Db721GetOptions(foreigntableid);
  elog(LOG, "[db721_GetForeignRelSize] tablename: %s, filename: %s", db721FdwOptions->tablename, db721FdwOptions->filename);
  double tupleCountEstimate = TupleCountEstimate(baserel, db721FdwOptions->filename);
  elog(LOG, "[db721_GetForeignRelSize] tupleCountEstimate: %lf", tupleCountEstimate);
  double rowSelectivity = clauselist_selectivity(root, baserel->baserestrictinfo,
                                                 0, JOIN_INNER, NULL);
  elog(LOG, "[db721_GetForeignRelSize] rowSelectivity: %lf", rowSelectivity);
  double outputRowCount = clamp_row_est(tupleCountEstimate * rowSelectivity);
  baserel->rows = outputRowCount;
  elog(LOG, "[db721_GetForeignRelSize]: outputRowCount = %lf", outputRowCount);
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                    Oid foreigntableid) {
  // TODO(721): Write me!
  Dog scout("Scout");
  elog(LOG, "db721_GetForeignPaths: %s", scout.Bark().c_str());
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses,
                   Plan *outer_plan) {
  // TODO(721): Write me!
  return nullptr;
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  // TODO(721): Write me!
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  return nullptr;
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}