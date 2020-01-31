#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<math.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<string.h>
#include "storage_mgr.h"


FILE *f;

void initStorageManager (void) {
	printf("\n Storage Manager Booting up\n ");
	f = NULL; // file pointer initialized
}

// Function for constructing a page file
RC createPageFile (char *fileName) {
	f = fopen(fileName, "w+"); // Authorization to write is granted while opening the file

	// Condition to check whether the file is empty
	condition((f != NULL),{

		SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));// empty page creation in thr memory

		// writing operation in the memory.
		condition((fwrite(emptyPage, sizeof(char), PAGE_SIZE,f) >= PAGE_SIZE),
		{
			printf("write successful \n");
		})
		else
			printf("write failure \n");

		//closing a file
		fclose(f);
		// on the completion of the write function, memory is set free.
		free(emptyPage);
		return RC_OK;
	})
	else
		return RC_FILE_NOT_FOUND;
}

//function definition for opening a page file
RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
	// read permission granted while opening a file
	f = fopen(fileName, "r");
	condition(doEq(f,NULL),
	{
		return RC_FILE_NOT_FOUND;
	})
	else
	{
		//fseek(f, 0, SEEK_END);//function to set the pointer to the end position of the page file
			struct stat fi;
	    //Assigning values to file handler.
	    (*fHandle).fileName = fileName;
	    (*fHandle).curPagePos = 0;
	    //(*fHandle).totalNumPages = (ftell(f) + 1) / PAGE_SIZE;
			condition(lesser((fstat(fileno(f), &fi)),0),
				{
					return RC_ERROR;
				})

			fHandle->totalNumPages = (fi.st_size/ PAGE_SIZE);


	    fclose(f); //rewind function used to reset the pointer to its commencement.
	    return RC_OK;
	}
}

//function to terminate the page file
RC closePageFile (SM_FileHandle *fHandle) {
	condition((f != NULL),{f = NULL;})
	return RC_OK;
}

//destroying the generated page file
RC destroyPageFile (char *fileName) {
	f = fopen(fileName,"r");
	condition(doEq(f,NULL),
	{
		return RC_FILE_NOT_FOUND;
	})

	remove(fileName); //delete the file after its done
	return RC_OK;
}

/*Reading Operation */
 //function for extracting the data from the block in reading mode.
 
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int isSeek;
	f = fopen(fHandle->fileName, "r");
	condition(doEq(f,NULL),
		{return RC_FILE_NOT_FOUND;})
	condition(((pageNum < 0) || (pageNum > fHandle->totalNumPages)),
	{return RC_READ_NON_EXISTING_PAGE;})
	
	isSeek = fseek(f, (pageNum * PAGE_SIZE), SEEK_SET);
	condition(doEq(isSeek, 0),
	{
	// Reading the content
  // If else condition to check the block size
		condition((fread(memPage, sizeof(char), PAGE_SIZE, f) < PAGE_SIZE),{return RC_ERROR;})
	})
	else
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	//modifying the current page position
	fHandle->curPagePos = ftell(f);
	// Closing file.
	fclose(f);
    return RC_OK;
}

//function for extracting the position of the current block
int getBlockPos (SM_FileHandle *fHandle) {
	return fHandle->curPagePos;
}

//function from extracting the data from the first block
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	f = fopen(fHandle->fileName, "r");
	
	condition(doEq(f,NULL),{return RC_FILE_NOT_FOUND;})
	for(int i=0;i<PAGE_SIZE;i++)
	{
		char bit = fgetc(f);
		condition(feof(f),{break;})
		else
		{
			memPage[i] = bit;
		}
	}
	fHandle->curPagePos = ftell(f);
	fclose(f);
	return RC_OK;
}

//function for extracting the data from the previous block
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	int currentPageNumber,startPosition;
	char bit;
	//if condition for checking the index
	condition((fHandle->curPagePos > PAGE_SIZE),{
		f = fopen(fHandle->fileName, "r");
		condition(doEq(f,NULL),{return RC_FILE_NOT_FOUND;})
		
		currentPageNumber = (fHandle->curPagePos / PAGE_SIZE);
		startPosition = (PAGE_SIZE * (currentPageNumber - 2));

		// function to set the pointer to the previous block position.
		fseek(f, startPosition, SEEK_SET);
		
		for(int i = 0; i < PAGE_SIZE; i++) {
			bit = fgetc(f);
			memPage[i] = bit;
		}

		
		(fHandle->curPagePos) = ftell(f);
		
		fclose(f);
		return RC_OK;
	})
	else
	{
		printf("\n No previous block as this is the first block.");
		return RC_READ_NON_EXISTING_PAGE;
	}
}

//function to read the current block
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int currentPageNumber,startPosition;
	
	f = fopen(fHandle->fileName, "r");
	
	condition(doEq(f,NULL),{return RC_FILE_NOT_FOUND;})

	
	currentPageNumber = (fHandle->curPagePos / PAGE_SIZE);
	startPosition = (PAGE_SIZE * (currentPageNumber - 2));


	
	fseek(f, startPosition, SEEK_SET);
	
	for(int i = 0; i < PAGE_SIZE; i++) {
		condition((feof(f)),{break;})
		memPage[i] = fgetc(f);
	}

	//pointer set to the current page position
	fHandle->curPagePos = ftell(f);

	
	fclose(f);
	return RC_OK;
}

//Reading next block of the file
 RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
 	int currentPageNumber,startPosition;
 	
	f = (fopen(fHandle->fileName, "r"));
	
	condition((fHandle->curPagePos != PAGE_SIZE),{
	
	//incrementing the index of pointer to the succeeding block
		currentPageNumber = (fHandle->curPagePos / PAGE_SIZE);
		startPosition = (PAGE_SIZE * (currentPageNumber - 2));
		
		fseek(f, startPosition, SEEK_SET);

		
		condition(doEq(f,NULL),{return RC_FILE_NOT_FOUND;})

		
		for(int i = 0; i < PAGE_SIZE; i++) {
			condition((feof(f)),{break;})
			memPage[i] = fgetc(f);
		}

		
		fHandle->curPagePos = ftell(f);

		
		fclose(f);
		return RC_OK;
	})
	else
	{
		printf("\n This is last block. Hence, next block cant be found.");
		return RC_READ_NON_EXISTING_PAGE;
	}
}

//function to execute the last block reading operation
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	int startPosition;
	startPosition = ((fHandle->totalNumPages - 1) * PAGE_SIZE);
	
	f = (fopen(fHandle->fileName, "r"));
	
	condition(doEq(f,NULL),{return RC_FILE_NOT_FOUND;})
	
	fseek(f, startPosition, SEEK_SET);
	

	for(int i = 0; i < PAGE_SIZE; i++) {
		condition((feof(f)),{break;})
		memPage[i] = fgetc(f);
	}

	//decrementing the index of pointer to the preceeding block
	fHandle->curPagePos = ftell(f);

	
	fclose(f);
	return RC_OK;
}

/*Writing Operation */
// writing block at a given page number
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int startPosition;
	
	f = (fopen(fHandle->fileName, "r+"));

	
	condition(doEq(f,NULL),{return RC_FILE_NOT_FOUND;})
	
	condition(((pageNum < 0) || (pageNum > fHandle->totalNumPages)),{return RC_WRITE_FAILED;})
	startPosition = (pageNum * PAGE_SIZE);

	condition((pageNum != 0),{
		// data written to the first page.
		fHandle->curPagePos = startPosition;
		fclose(f);
		writeCurrentBlock(fHandle, memPage);
	})
	else
	{
		//updating the current page location to the pagenum
		fseek(f, startPosition, SEEK_SET);
		for(int i = 0; i < PAGE_SIZE; i++)
		{
			
			if(feof(f) == "TRUE")
			{
				appendEmptyBlock(fHandle);
			}
			
			fputc(memPage[i], f);
		}

		
		fHandle->curPagePos = ftell(f);
		
		fclose(f);
	}
	return RC_OK;
}

//exteding the file by adding an empty block
RC appendEmptyBlock (SM_FileHandle *fHandle) {
	int isSeek;
	
	SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
	
	isSeek = fseek(f, 0, SEEK_END);

	condition((isSeek != 0), {
		free(emptyBlock);
		return RC_WRITE_FAILED;
	})
	else
	{
		
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, f);
	}
	
	free(emptyBlock);

	
	fHandle->totalNumPages = fHandle->totalNumPages + 1;
	return RC_OK;
}

//function to write to the current block
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    
	f = (fopen(fHandle->fileName, "r+"));

	
	condition(doEq(f,NULL),{return RC_FILE_NOT_FOUND;})
	
	appendEmptyBlock(fHandle);
	fseek(f, fHandle->curPagePos, SEEK_SET);

	
	fwrite(memPage, sizeof(char), strlen(memPage), f);
	fHandle->curPagePos = ftell(f);
	fclose(f);
	return RC_OK;
}

//ensuring the capacity of the file

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	
	f = fopen(fHandle->fileName, "a");
	condition(doEq(f,NULL),{return RC_FILE_NOT_FOUND;})

	
	while(numberOfPages > fHandle->totalNumPages){
		appendEmptyBlock(fHandle);
	}
	
	fclose(f);
	return RC_OK;
}
