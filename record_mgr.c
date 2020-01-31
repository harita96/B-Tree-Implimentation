#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

const int MAX_NUMBER_OF_PAGES = 100;		//size of max number of pages.

const int ATTRIBUTE_SIZE = 15;	//name attribute size

//In order to use the record manager, we have to define a custom data structure.
typedef struct RecordManager
{
	int tuplesCnt;	//total number of tuples in the table.
	int firstFreePage; //The location of first free page that has empty slots in the table.
	int scannedRecordCount;		//number of traversed records.
	BM_PageHandle pgHandle; //Page handle for BUffer Manager to access the page files.
	RID rID; //Record ID
	BM_BufferPool bPool; //Buffer Pool used for Buffer Manager
	Expr *conditionValue;	//Condition Value is used to traverse the records in the table.
} RecordManager;


RecordManager *recordManager;


// function to return an available free slot
int findFreeSlot(char *data, int rSize)
{
	int j = 0;
	int totalSlots = (PAGE_SIZE / rSize);
	while (j < totalSlots){
		if (data[j * rSize] != '+')
			
			return j;
		j++;
	}
	return -1;
}


// function to begin the record manager.
extern RC initRecordManager (void *mgmtData)
{
	//Initilializing the storage manager.
	initStorageManager();
	return RC_OK;
}

// function to terminate the record manager
extern RC shutdownRecordManager ()
{
	recordManager = NULL;
	
	free(recordManager);
	return RC_OK;
}

// function to create a table with "name" as its table name and its schema is denoted by "schema".
extern RC createTable (char *name, Schema *schema)
{
	recordManager = (RecordManager*) malloc(sizeof(RecordManager));	//malloc function to allocate the memory space to the record manager

	initBufferPool(&recordManager->bPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);

	int result, l=0;
	int m=0;
	int k = 0;

	char data[PAGE_SIZE];
	char *pgHandle = data;

	
	*(int*)pgHandle = 0;
	pgHandle = pgHandle + sizeof(int);
	*(int*)pgHandle = 1;
	pgHandle = pgHandle + sizeof(int);
	*(int*)pgHandle = schema->numAttr;
	pgHandle = pgHandle + sizeof(int);
 	*(int*)pgHandle = schema->keySize;
	pgHandle = pgHandle + sizeof(int);

	
	while(k < schema->numAttr)
    	{
		//setting the attribute name
       		strncpy(pgHandle, schema->attrNames[k], ATTRIBUTE_SIZE);
	       	pgHandle = pgHandle + ATTRIBUTE_SIZE;

		//setting the attribute data type
	       	*(int*)pgHandle = (int)schema->dataTypes[k];

		//increment the pointer by sizeof(int).
	       	pgHandle = pgHandle + sizeof(int);

		//setting the attribute data type length.
	       	*(int*)pgHandle = (int) schema->typeLength[k];

		//increment the pointer by sizeof(int).
	       	pgHandle = pgHandle + sizeof(int);

		m++;
		l++;
		k++;
    	}

	SM_FileHandle fileHandle;
	//Created a page file 'name' as its table name.
	result = createPageFile(name);
	
	if(result != RC_OK)
		return result;
	
	// opening a newly created page and storing it in the result
	result = openPageFile(name, &fileHandle);
	if(result != RC_OK)
		return result;

	
	result = writeBlock(0, &fileHandle, data);
	if(result !=  RC_OK)
		return result;

	
	//closing a file after writing into it.
	result = closePageFile(&fileHandle);
	if(result != RC_OK)
		return result;

	return RC_OK;
}

// function to open the table.
extern RC openTable (RM_TableData *rel, char *name)
{
	SM_PageHandle pgHandle;

	int m =0,l=0,attCount,k = 0;

	//table meta data set to the custom record manager.
	rel->mgmtData = recordManager;
	//setting the table name.
	rel->name = name;

	

	//Buffer pool is pinned with a pagea page.
	pinPage(&recordManager->bPool, &recordManager->pgHandle, 0);

	//setting the 0th pointer to the page handle data.
	pgHandle = (char*) recordManager->pgHandle.data;

	
	//abstracting the total number of tuples from the page file.
	recordManager->tuplesCnt = *(int*)pgHandle;
	pgHandle = pgHandle + sizeof(int);

	int y = 0;
	//retrieving a free page from the page file.
	recordManager->firstFreePage = *(int*) pgHandle;
    	pgHandle = pgHandle + sizeof(int);

	//retrieving the number of attributes from the page file.
    	attCount = *(int*)pgHandle;
	pgHandle = pgHandle + sizeof(int);

	Schema *schema;
	m++;

	schema = (Schema*) malloc(sizeof(Schema));

	//setting the parameters.
	schema->numAttr = attCount;
	schema->attrNames = (char**) malloc(sizeof(char*) *attCount);
	
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *attCount);
	schema->typeLength = (int*) malloc(sizeof(int) *attCount);
	l++;

	while(k < attCount){
		schema->attrNames[k] = (char*) malloc(ATTRIBUTE_SIZE);
		k++;
	}
	int i = 0;
    k = 0;
	while(k < schema->numAttr)
    	{
		//setting the attribute name.
		strncpy(schema->attrNames[k], pgHandle, ATTRIBUTE_SIZE);
		pgHandle = pgHandle + ATTRIBUTE_SIZE;

		i++;

		//setting the attribute data type.
		schema->dataTypes[k] = *(int*) pgHandle;
		i+=2;
		pgHandle = pgHandle + sizeof(int);

		//setting the attribute data type length.
		schema->typeLength[k] = *(int*)pgHandle;
		i-=2;
		pgHandle = pgHandle + sizeof(int);
		i--;
		k++;
	}

	rel->schema = schema;

	unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//removing the page from buffer pool.
	forcePage(&recordManager->bPool, &recordManager->pgHandle);	//writing the page to disk.
	return RC_OK;
}

//function to close the table 'rel'.
extern RC closeTable (RM_TableData *rel)
{
	//storing the metadata.
	RecordManager *recordManager = rel->mgmtData;
	

	//Shutting down the buffer Pool.
	shutdownBufferPool(&recordManager->bPool);
	return RC_OK;
}

//function to delete the table.
extern RC deleteTable (char *nam)
{
	//destroying the page file.
	
	destroyPageFile(nam);
	
	return RC_OK;
}

//function to return the number of records in the table.
extern int getNumTuples (RM_TableData *rel)
{
	//accessing and returning the tuple count(tuplesCnt).
	RecordManager *recordManager = rel->mgmtData;
	
	return recordManager->tuplesCnt;
}


//function to insert a new record into the table and to update the 'record' with the ID of the latest inserted record.
extern RC insertRecord (RM_TableData *rel, Record *record)
{
	char *data,*slotPointer;

	//Retrieving the stored metadata from the table.
	RecordManager *recordManager = rel->mgmtData;

	//setting the Id for the record.
	RID *rID = &record->id;

	//setting the first free page to the current page.
	rID->page = recordManager->firstFreePage;

	pinPage(&recordManager->bPool, &recordManager->pgHandle, rID->page);

	//setting the data to the initial position of the data on the record.
	data = recordManager->pgHandle.data;

	//funciton to get a free slot.
	rID->slot = findFreeSlot(data, getRecordSize(rel->schema));
	int x = 1;
	while(rID->slot == -1)
	{
		unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//Unpinning the page if there are no free slots on the page that is already pinned.
		rID->page++; //incrementing the page.
		x += 2;
		pinPage(&recordManager->bPool, &recordManager->pgHandle, rID->page);	//adding a new page to the buffer Pool.
		data = recordManager->pgHandle.data;		//setting the data to the initial position of the data on the record.
		x -=1;
		rID->slot = findFreeSlot(data, getRecordSize(rel->schema));		//function to find the free slot.
	}

	slotPointer = data;

	markDirty(&recordManager->bPool, &recordManager->pgHandle);	//When a page is modified, mark it dirty.
	slotPointer = slotPointer + (rID->slot * getRecordSize(rel->schema));	//Calculation the Slot's initial position.
	x = 0;
	*slotPointer = '+';		//'+' is added in the end to denote that this is a new record and should be eliminated if the spare space is less.
	memcpy(++slotPointer, record->data + 1, getRecordSize(rel->schema) - 1);	//Slot pointer points to the memory location where the data is copied.
	unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//Page is removed from the buffer pool.
	x +=3;
	recordManager->tuplesCnt++;	//incrementing the tuple count.
	x -= 1;
	pinPage(&recordManager->bPool, &recordManager->pgHandle, 0);	//Pinning the page again.
	return RC_OK;
}

//function to delete a record having an "Id" in the table.
extern RC deleteRecord (RM_TableData *rel, RID id)
{
	int del =0;
	RecordManager *recordManager = rel->mgmtData;	//Retreiving the meta data stored in the table.
	pinPage(&recordManager->bPool, &recordManager->pgHandle, id.page);		//pinning the page that has the record that needs to be updated.
	int d = 0;
	recordManager->firstFreePage = id.page;		//Updating the free page.
	del++;
	d++;
	char *data = recordManager->pgHandle.data;
	data = data + (id.slot * getRecordSize(rel->schema));
	d -= 1;
	*data = '-';	//deleting the record.
	markDirty(&recordManager->bPool, &recordManager->pgHandle);	//Since the page is modified, it has been marked dirty.
	
	unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//After retrieving the record, the page is not required to be kept in the memory. Hence, Unpin the page.
	return RC_OK;
}

//function to update a "record" in the table.
extern RC updateRecord (RM_TableData *rel, Record *record)
{
	int dataValue = 0;
	char *data;

	RecordManager *recordManager = rel->mgmtData;	//retriving the meta data stored in the table.
	pinPage(&recordManager->bPool, &recordManager->pgHandle, record->id.page);	//pinning the page that contains the record we've to update.

	int r = 0;
 	RID id = record->id;//Setting the record ID.

	//Calculating the start position of the new data.
	data = recordManager->pgHandle.data;
	r += 3;
	data = data + (id.slot * getRecordSize(rel->schema));
	dataValue = dataValue+1;
	*data  = '+';	//the record is not empty.

	memcpy(++data, record->data + 1, getRecordSize(rel->schema) - 1 );	//to the already existing record, a new record's data has been copied.
	r -= 1;
	markDirty(&recordManager->bPool, &recordManager->pgHandle);	//Page is modified. Hence, marked dirty.
	
	unpinPage(&recordManager->bPool, &recordManager->pgHandle);	//Retriving the record from the memory and unpinning the page as it is no longer required.
	return RC_OK;
}

//function to retrieve a record having Record ID in the table. The result is then stored in the location.
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	int recordCount = 0;
	RecordManager *recordManager = rel->mgmtData;	//Retrieving the metadata stored.
	pinPage(&recordManager->bPool, &recordManager->pgHandle, id.page);		//pinning the page that is to be retreived.

	char *dataPointer = recordManager->pgHandle.data;


	dataPointer = dataPointer + (id.slot * getRecordSize(rel->schema));

	if(*dataPointer == '+')
	{
		record->id = id;	//Setting the record ID.
		char *data = record->data;	//record to data pointer is set.
		
		memcpy(++data, dataPointer + 1, getRecordSize(rel->schema) - 1);
		recordCount = recordCount +1;

	}
	else
	{
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;	// When no matching record is found in the table,it returns an error indicating no tuple with the given ID found.

	}

	//page gets unpinned after retrieving the record.
	unpinPage(&recordManager->bPool, &recordManager->pgHandle);
	return RC_OK;
}



//function that starts scanning all the records.
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	//condition to check whether the cond is present or not.
	if(cond != NULL)
	{
		int scanVal = 0;
		openTable(rel, "ScanTable");	//function to open the table in the memory.

		RecordManager *tableManager;
		RecordManager *scanManager;

		
		int j = 10;
	    	scanManager = (RecordManager*) malloc(sizeof(RecordManager));
	    	scan->mgmtData = scanManager;
	    	j += 12;
	    	scanManager->rID.page = 1;
	    	scanManager->rID.slot = 0;
	    	j -= 10;
		scanManager->scannedRecordCount = 0;

			j -= 12;
	    	scanManager->conditionValue = cond;
	  	tableManager = rel->mgmtData;
	  	j += 1;
		tableManager->tuplesCnt = ATTRIBUTE_SIZE;
		scan->rel = rel;
		j = 0;
		scanVal = scanVal+1;
		return RC_OK;


	}
	else{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}


}

//Scanning every record in the table and the result is stored in the location 'record'.
extern RC next (RM_ScanHandle *scan, Record *record)
{
	//Initialiing the scanned data.
	RecordManager *scanManager = scan->mgmtData;
	int c = 0;
	RecordManager *tableManager = scan->rel->mgmtData;
    	Schema *schema = scan->rel->schema;

	//Checks whether the conditionValue is present or not.
	if(scanManager->conditionValue != NULL){
		int nextValue = 2,totalSlots,scannedRecordCount,tuplesCnt;
		char *data;
		Value *result = (Value *) malloc(sizeof(Value));

		c  = 1;

		totalSlots = (PAGE_SIZE / getRecordSize(schema));	//calculating the total number of slots.

		scannedRecordCount = scanManager->scannedRecordCount;	//retrieving the scanned record count.

		c = 2;

		tuplesCnt = tableManager->tuplesCnt;	//getting the tuple count.

		// Checking whether the table's tuple count is 0 or not.
		if(tuplesCnt != 0){
			while(scannedRecordCount <= tuplesCnt)	//Iterating the loop through tuples.
			{
				//Executing the block after scanning all the tuples.
				if(scannedRecordCount > 0)
				{
					scanManager->rID.slot++;

					//Execute the block after scanning all the slots.
					if( scanManager->rID.slot >= totalSlots)
					{
						scanManager->rID.slot = 0;
						c = 4;
						scanManager->rID.page++;
						nextValue--;
					}
				}
				else
				{
					scanManager->rID.page = 1;
					c = 0;
					scanManager->rID.slot = 0;
					nextValue++;
				}

				pinPage(&tableManager->bPool, &scanManager->pgHandle, scanManager->rID.page);	//putting the page in the buffer pool.

				data = scanManager->pgHandle.data;	//retrieving the page data.

				c = 0;

				data = data + (scanManager->rID.slot * getRecordSize(schema));	//calculating the data location is calculated from rId slot and record size.

				// setting the page, rID slot and scan Manager slot.
				record->id.page = scanManager->rID.page;
				c = 1;
				record->id.slot = scanManager->rID.slot;


				char *dataPointer = record->data;	//initializing the 1st location of record to data.


				*dataPointer = '-';

				memcpy(++dataPointer, data + 1, getRecordSize(schema) - 1);

				c = 5;

				//Since a record is scanned previously, the scan count is incremented.
				scanManager->scannedRecordCount++;
				scannedRecordCount++;

				//Testing the record for a specific test expression.
				evalExpr(record, schema, scanManager->conditionValue, &result);

				nextValue++;


				if(result->v.boolV == TRUE)
				{
					//unpinning the page.
					c--;
					unpinPage(&tableManager->bPool, &scanManager->pgHandle);

					return RC_OK;
				}
			}


			unpinPage(&tableManager->bPool, &scanManager->pgHandle);

			//reseting the scan manager value.
			scanManager->rID.page = 1;
			c = 2;
			scanManager->rID.slot = 0;
			scanManager->scannedRecordCount = 0;


			return RC_RM_NO_MORE_TUPLES;
		}
		else{
			return RC_RM_NO_MORE_TUPLES;
		}
	}
	else
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}


}

//function to turn off the scan operation.
extern RC closeScan (RM_ScanHandle *scan)
{
	
	RecordManager *recordManager = scan->rel->mgmtData;
	int closePointer = 0;
	RecordManager *scanManager = scan->mgmtData;


	if(scanManager->scannedRecordCount > 0)
	{

		unpinPage(&recordManager->bPool, &scanManager->pgHandle);

		scanManager->scannedRecordCount = 0;
		int v = 0;
		scanManager->rID.page = 1;
		scanManager->rID.slot = 0;

		v++;
		closePointer++;
	}

	//meta data has been allocated some memory space that is now deallocated.
    	scan->mgmtData = NULL;
    	
    	free(scan->mgmtData);
	return RC_OK;
}



//function to return the record size of the schema.
extern int getRecordSize (Schema *schema)
{
	int recordSize = 0,size = 0, i=0; //setting the offset to zero.

	//iterating through all the attributes.
	while(i < schema->numAttr)
	{
		int j;
		if(schema->dataTypes[i] == DT_STRING){


			size = size + schema->typeLength[i];
			j = 1;
			recordSize++;
		}
		else if(schema->dataTypes[i] == DT_INT){

			size = size + sizeof(int);
			j = 2;
			recordSize++;
		}
		else if(schema->dataTypes[i] == DT_FLOAT){

			size = size + sizeof(float);
			j = 3;
			recordSize++;
		}
		else if(schema->dataTypes[i] == DT_BOOL){

			size = size + sizeof(bool);
			j = 4;
			recordSize++;
		}
		i++;
	}
	return ++size;
}

//function to create a new schema.
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	Schema *schema = (Schema *) malloc(sizeof(Schema));
	schema->attrNames = attrNames;
	int x = 0;
	schema->numAttr = numAttr;
	schema->typeLength = typeLength;
	x++;
	schema->dataTypes = dataTypes;
	schema->keyAttrs = keys;
	x = x-1;
	schema->keySize = keySize;

	return schema;
}

//function to allocate the memory space to schema.
extern RC freeSchema (Schema *schema)
{
	//deallocating the already allocated memory space occupied by 'schema'.
	free(schema);
	return RC_OK;
}



// A new record in the schema referenced by "schema"
extern RC createRecord (Record **record, Schema *schema)
{
	int recordSize = getRecordSize(schema);	//retrieving the record size.
	Record *newRecord = (Record*) malloc(sizeof(Record));	//allocating the memory space to the new record.
	
	newRecord->data = (char*) malloc(recordSize);
	int x = 0;
	newRecord->id.page = newRecord->id.slot = -1;
	char *dataPointer = newRecord->data;
	x = x+1;
	*dataPointer = '-';
	*(++dataPointer) = '\0';
	x--;
	*record = newRecord;
	return RC_OK;
}

//function for attribute offset.
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	*result = 1;
	int i=0,attrVal =1;

	//Iterating through all the attributes.
	while(i < attrNum)
	{
		int j = 0;
		if(schema->dataTypes[i] == DT_STRING){

			*result = *result + schema->typeLength[i];
			j = 1;
			attrVal++;
		}
		else if(schema->dataTypes[i] == DT_INT){

			*result = *result + sizeof(int);
			j = 1;
			attrVal++;
		}
		else if(schema->dataTypes[i] == DT_FLOAT){

			*result = *result + sizeof(float);
			j = 1;
			attrVal++;
		}
		else if(schema->dataTypes[i] == DT_BOOL){

			*result = *result + sizeof(bool);
			j = 1;
			attrVal++;
		}
		i++;
	}
	return RC_OK;
}

//function to remove he record from the memory.
extern RC freeRecord (Record *record)
{
	//freeing the space allocated to the records.
	free(record);
	
	return RC_OK;
}

//function to retrieve the attribute from the given record.
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	Value *attribute = (Value*) malloc(sizeof(Value));	//Malloc function.
	int offset = 0,getAttrVal = 0;
	attrOffset(schema, attrNum, &offset);	//retrieving the attribute's offset value.
	char *dataPointer = record->data;	//retrieving the intial position of data in the memory.
	dataPointer = dataPointer + offset;		//adding the offset to the beginning.
	// If attrNum = 1 or not.
	int s = 0;
	if(attrNum != 1){
		schema->dataTypes[attrNum] = schema->dataTypes[attrNum];
	}
	else{
		schema->dataTypes[attrNum] = 1;
	}
	//retrieving the attribute's value depending on its type.
	if(schema->dataTypes[attrNum] == DT_STRING){
		int length = schema->typeLength[attrNum];

		attribute->v.stringV = (char *) malloc(length + 1);
		s = 1;
		strncpy(attribute->v.stringV, dataPointer, length);
		attribute->v.stringV[length] = '\0';
		s--;
		attribute->dt = DT_STRING;
		getAttrVal++;
	}
	else if(schema->dataTypes[attrNum] == DT_INT){
		//retrieving the value of attribute.
		int value = 0;
		s = 2;
		memcpy(&value, dataPointer, sizeof(int));
		attribute->v.intV = value;
		s--;
		attribute->dt = DT_INT;
		getAttrVal++;
	}
	else if(schema->dataTypes[attrNum] == DT_FLOAT){
		//float type
		float value;
		s=3;
		memcpy(&value, dataPointer, sizeof(float));
		attribute->v.floatV = value;
		s--;
		attribute->dt = DT_FLOAT;
		getAttrVal++;
	}
	else if(schema->dataTypes[attrNum] == DT_BOOL){
		//booloan type attribute value.
		bool value;
		s = 3;
		memcpy(&value,dataPointer, sizeof(bool));
		attribute->v.boolV = value;
		s--;
		attribute->dt = DT_BOOL;
		getAttrVal++;
	}
	else{
		printf("\nFor the given datatype,serializer is not defined \n");
	}

	*value = attribute;
	return RC_OK;
}

//function to set The attribute value in the specified schema.
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset = 0,dataLevel = 0,dataPoint = 0;
	attrOffset(schema, attrNum, &offset);	//retrieving the offset value of attributes.
	char *dataPointer = record->data;
	dataPointer = dataPointer + offset;
	dataPoint = dataLevel = 1;
	int a = 0;
	if(schema->dataTypes[attrNum] == DT_STRING){

		int length = schema->typeLength[attrNum];
		a++;
		//attribute values are copied to the location pointed by the data pointer.
		strncpy(dataPointer, value->v.stringV, length);
		dataPoint++;
		a = a+3;
		dataPointer = dataPointer + schema->typeLength[attrNum];
	}
	else if(schema->dataTypes[attrNum] == DT_INT){
		//setting the attribute value - type integer
		*(int *) dataPointer = value->v.intV;
		a=a+2;
		dataPointer = dataPointer + sizeof(int);
		dataLevel++;
	}
	else if(schema->dataTypes[attrNum] == DT_FLOAT){
		//float type
		*(float *) dataPointer = value->v.floatV;
		a = a+3;
		dataPointer = dataPointer + sizeof(float);
		dataPoint++;
	}
	else if(schema->dataTypes[attrNum] == DT_BOOL){
		//boolean type
		*(bool *) dataPointer = value->v.boolV;
		a = a+4;
		dataPointer = dataPointer + sizeof(bool);
		dataLevel++;
	}
	else{
		printf("\nSerializer is not defined for the given datatype. \n");
	}
	return RC_OK;
}
