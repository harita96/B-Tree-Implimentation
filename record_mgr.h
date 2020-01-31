#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"

// Bookkeeping for scans
typedef struct RM_ScanHandle
{
  RM_TableData *rel;
  void *mgmtData;
} RM_ScanHandle;

// table and manager
extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager ();
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

/*Condition Defination*/
#define forloop(start,cond,do) for(int start = 0;cond;start++){do}
#define condif(a,b) if(a){b}
#define elif(a,b) else if(a){b}
#define dec(a,b) a=b
#define unequal(a,b) a>=b
#define fof(a,b) for(a){b}
#define eqfn(a,b) a==b
#define lessthan(a,b) a<b
#define notequal(a,b) a!=b
#define greaterthan(a,b) a>b
#define greaterthanequal(a,b) a>=b
#define lessequalvalue(a,b) a<=b
#define whileloop(a,b) while(a){b}
#define equalvalue(a,b,c) a=b=c
#endif // RECORD_MGR_H
