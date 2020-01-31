#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>
int size_buffer = 0;
int count_write = 0;
int index_rear = 0;
int count_hit = 0;
int pointer_clock = 0;
int pointer_lfu = 0;

typedef struct Page
{
	PageNumber pageNum; // An identification integer is assigned to every page
	SM_PageHandle data; // actual data has been displayed
	int total_hits;   // recent page given to LRU algorithm.
	int bit_b; // whether the content is modified by the client
	int number_ref;   // least frequent page handed to LRU algorithm.
	int count_fix; // count of clients indicated
} PFrame;

// FIFO function Implementation
extern void FIFO(BM_BufferPool *const bm, PFrame *page)
{
	int flag=0;
	int index_front;
	PFrame *pframe = (PFrame *) bm->mgmtData;
	index_front = (index_rear % size_buffer);

	// page frames in buffer pool are repeated
	while(flag < size_buffer)
	{
		condition((pframe[index_front].count_fix != 0),{

			index_front++;
			if (index_front%size_buffer == 0)
			{
				index_front = 0;
			}
			else
			{
				index_front = index_front;
			}
		})
		else
		{
			// condition to check whether the page in memory has been updated
			if(pframe[index_front].bit_b == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pframe[index_front].pageNum, &fh, pframe[index_front].data);

				// count_write incremented.
				count_write = count_write + 1;
			}
			//page frame and new page content set

			pframe[index_front].pageNum = page->pageNum;
			pframe[index_front].count_fix = page->count_fix;
			pframe[index_front].bit_b = page->bit_b;
			pframe[index_front].data = page->data;

			break;
		}
		flag++;
	}
}

// LFU function Implementation
extern void LFU(BM_BufferPool *const bm, PFrame *page)
{
	int flag = 0, temp_flag = 0, leastFreqIndex, leastFreqRef;
	PFrame *pframe =  (PFrame *) bm->mgmtData;

	leastFreqIndex = pointer_lfu;


	while(flag < size_buffer)
	{
		if(pframe[leastFreqIndex].count_fix == 0)
		{
			leastFreqRef = pframe[((leastFreqIndex + flag) % size_buffer)].number_ref;
			break;
		}
		flag++;
	}
	flag  = ((leastFreqIndex + 1) % size_buffer);


	while(temp_flag < size_buffer)
	{
		if(pframe[flag].number_ref < leastFreqRef)
		{
			leastFreqIndex = flag;
			leastFreqRef = pframe[flag].number_ref;
		}
		flag = ((flag + 1) % size_buffer);
		temp_flag = temp_flag + 1;
	}

	// condition to check whether the page in memory has been modified
	condition(doEq(pframe[leastFreqIndex].bit_b, 1),
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pframe[leastFreqIndex].pageNum, &fh, pframe[leastFreqIndex].data);

		// count_write incremented.
		count_write++;
	})

	// Page frame and new page content set
	pframe[leastFreqIndex].bit_b = page->bit_b;
	pointer_lfu = (leastFreqIndex + 1);
	pframe[leastFreqIndex].count_fix  = page->count_fix;
	pframe[leastFreqIndex].data = page->data;
	pframe[leastFreqIndex].pageNum = page->pageNum;

}

//LRU function Implementation
extern void LRU(BM_BufferPool *const bm, PFrame *page)
{
	int flag = 0, leastHitIndex, leastHitNum;
	PFrame *pframe = (PFrame *) bm->mgmtData;


	while(flag < size_buffer)
	{

		condition(doEq(pframe[flag].count_fix,0),
		{
			leastHitIndex = flag;
			leastHitNum = pframe[flag].total_hits;
			break;
		})
		flag = flag + 1;
	}

	flag = leastHitIndex + 1;

	while(flag < size_buffer)
	{
		condition((pframe[flag].total_hits < leastHitNum),
		{
			leastHitIndex = flag;
			leastHitNum = pframe[flag].total_hits;
		})
		flag = flag + 1;
	}
	// condition to check whether the page in memory has been modified and then the page is written to the disk
	condition(doEq(pframe[leastHitIndex].bit_b, 1),
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pframe[leastHitIndex].pageNum, &fh, pframe[leastHitIndex].data);

		// count_write incremented.
		count_write++;
	})

	// Page frame and new page content set

	pframe[leastHitIndex].count_fix = page->count_fix;
	pframe[leastHitIndex].pageNum = page->pageNum;
	pframe[leastHitIndex].data = page->data;
	pframe[leastHitIndex].total_hits = page->total_hits;
	pframe[leastHitIndex].bit_b = page->bit_b;

}

// CLOCK function implementation
extern void CLOCK(BM_BufferPool *const bm, PFrame *page)
{
	PFrame *pframe = (PFrame *) bm->mgmtData;
	while(TRUE)
	{
		condition(doEq(pointer_clock % size_buffer,0),
		{
			pointer_clock = 0;
		})
		else
		{
			pointer_clock = pointer_clock;
		}
		condition((pframe[pointer_clock].total_hits != 0),
		{

			pframe[pointer_clock++].total_hits = 0;
		})
		else
		{
			// condition to Check whether the page in memory has been modified and then the page is written to disk
			condition(doEq(pframe[pointer_clock].bit_b, 1),
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pframe[pointer_clock].pageNum, &fh, pframe[pointer_clock].data);

				// count_write incremented.
				count_write++;
			})


			pframe[pointer_clock].data = page->data;
			pframe[pointer_clock].total_hits = page->total_hits;
			pframe[pointer_clock].bit_b = page->bit_b;
			pframe[pointer_clock].pageNum = page->pageNum;
			pframe[pointer_clock].count_fix = page->count_fix;
			pointer_clock = pointer_clock + 1;
			break;
		}
	}
}

// Buffer pool function implementation

extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		  const int numPages, ReplacementStrategy strategy,
		  void *stratData)
{
	int flag = 0;
	bm->numPages = numPages;
	bm->pageFile = (char *)pageFileName;
	bm->strategy = strategy;

	// Reserver memory space calculation
	PFrame *page  = malloc(sizeof(PFrame) * numPages);


	size_buffer = numPages;


	// pages in buffer pool are initiated. values can either be null or 0
	while(flag < size_buffer)
	{
		page[flag].pageNum = -1;
		page[flag].count_fix = 0;
		page[flag].data = NULL;
		page[flag].number_ref = 0;
		page[flag].bit_b = 0;
		page[flag].total_hits = 0;
		flag = flag + 1;
	}

	bm->mgmtData = page;
	count_write = 0;
	pointer_clock = 0;
	pointer_lfu = 0;
	return RC_OK;

}

//function implementation for Shutdown operation.
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	int flag = 0;
	PFrame *pframe = (PFrame *)bm->mgmtData;
	// updated pages written again to disk
	forceFlushPool(bm);
	while(flag < size_buffer)
	{

		condition((pframe[flag].count_fix != 0),
		{
			return RC_PINNED_PAGES_IN_BUFFER;
		})
		flag = flag + 1;
	}
	//freeing the space
	free(pframe);
	bm->mgmtData = NULL;
	return RC_OK;
}

// function to write the modified pages to the disk
extern RC forceFlushPool(BM_BufferPool *const bm)
{
	int flag = 0;
	PFrame *pframe = (PFrame *)bm->mgmtData;

	// storing modified pages in memory
	while(flag < size_buffer)
	{
		if (pframe[flag].count_fix == 0 && pframe[flag].bit_b == 1)
		{
			SM_FileHandle fh;

			openPageFile(bm->pageFile, &fh);

			writeBlock(pframe[flag].pageNum, &fh, pframe[flag].data);

			pframe[flag].bit_b = 0;
			//count_write incremented
			count_write++;
		}
		flag = flag + 1;
	}
	return RC_OK;
}


// PAGE MANAGEMENT FUNCTIONS IMPLEMENTATION


extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int flag = 0;
	PFrame *pframe = (PFrame *)bm->mgmtData;



	while(flag < size_buffer)
	{

		condition(doEq(pframe[flag].pageNum, page->pageNum),
		{
			pframe[flag].bit_b = 1;
			return RC_OK;
		})
		flag = flag + 1;
	}
	return RC_ERROR;
}

// function to remove a page from the memory
extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int flag = 0;
	PFrame *pframe = (PFrame *)bm->mgmtData;


	while(flag < size_buffer)
	{
		// Decrease count_fix
		condition(doEq(pframe[flag].pageNum, page->pageNum),
		{
			pframe[flag].count_fix = pframe[flag].count_fix - 1;
			break;
		})
		flag = flag + 1;
	}
	return RC_OK;
}

//function to modify the contents of the pages and writing them onto disk
extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int flag = 0;
	PFrame *pframe = (PFrame *)bm->mgmtData;


	while(flag < size_buffer)
	{

		condition(doEq(pframe[flag].pageNum, page->pageNum),
		{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(pframe[flag].pageNum, &fh, pframe[flag].data);
			pframe[flag].bit_b = 0;

			// count_write incremented.
			count_write++;
		})
		flag = flag + 1;
	}
	return RC_OK;
}


extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
	    const PageNumber pageNum)
{
	int i;
	PFrame *pframe = (PFrame *)bm->mgmtData;

	if(pframe[0].pageNum==-1)
	{
		// page read from the disk and page frame is initialized
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		pframe[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, pframe[0].data);
		pframe[0].pageNum = pageNum;
		pframe[0].count_fix = pframe[0].count_fix + 1;
		index_rear = 0;
		count_hit = 0;
		pframe[0].total_hits = count_hit;
		pframe[0].number_ref = 0;
		page->pageNum= pageNum;
		page->data = pframe[0].data;

		return RC_OK;
	}
	else
	{
		bool isBufferFull = true;

		for(i = 0; i < size_buffer; i++)
		{
			condition((pframe[i].pageNum != -1),
			{
				// condition to Check whether the page is present in the memory
				condition(doEq(pframe[i].pageNum,pageNum),
				{

					pframe[i].count_fix = pframe[i].count_fix + 1;
					isBufferFull=false;
					count_hit = count_hit+1; // Increment hits

					condition(doEq(bm->strategy,RS_LRU),

						{
							pframe[i].total_hits = count_hit;
						})
					elif(doEq(bm->strategy,RS_CLOCK),

						{
							pframe[i].total_hits = 1;
						})
					elif(doEq(bm->strategy,RS_LFU),

						{
							pframe[i].number_ref = pframe[i].number_ref+1;
						})
					page->pageNum=pageNum;
					page->data=pframe[i].data;

					pointer_clock = pointer_clock+1;
					break;
				})
			}) else {
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				pframe[i].data=(SM_PageHandle) malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, pframe[i].data);
				pframe[i].pageNum=pageNum;
				pframe[i].count_fix = 1;
				pframe[i].number_ref=0;
				index_rear = index_rear + 1;
				count_hit = count_hit + 1;
				condition(doEq(bm->strategy,RS_LRU),

					{
						pframe[i].total_hits=count_hit;
					})
				elif((bm->strategy == RS_CLOCK),

					{
						pframe[i].total_hits=1;
					})

				page->pageNum=pageNum;
				page->data=pframe[i].data;
				isBufferFull=false;
				break;
			}
		}


		condition(doEq(isBufferFull,true),
		{

			PFrame *newPage = (PFrame *) malloc(sizeof(PFrame));

			// Page frame's content initializtion
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			newPage->data = (SM_PageHandle) malloc(PAGE_SIZE);
			readBlock(pageNum, &fh, newPage->data);
			newPage->pageNum = pageNum;
			newPage->bit_b=0;
			newPage->count_fix=1;
			newPage->number_ref=0;
			index_rear = index_rear + 1;
			count_hit = count_hit + 1;

			condition((bm->strategy == RS_LRU),

				{newPage->total_hits=count_hit;})
			elif(doEq(bm->strategy,RS_CLOCK),

				{newPage->total_hits=1;})

			page->pageNum=pageNum;
			page->data=newPage->data;

			if(bm->strategy == RS_FIFO){
				FIFO(bm, newPage);
			}
			else if(bm->strategy == RS_LRU){
				LRU(bm, newPage);
			}
			else if(bm->strategy == RS_LFU){
				LFU(bm, newPage);
			}
			else if(bm->strategy == RS_CLOCK){
				CLOCK(bm, newPage);
			}
			else if(bm->strategy == RS_LRU_K){
				printf("\n LRU-k algorithm is yet to be implemented");
			}
			else{
				printf("\nError Implementing the algorithm\n");
			}
		})
		return RC_OK;
	}
}


// STATISTICS Function Implementation

// An array of page numbers returened.
extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PFrame *pframe = (PFrame *) bm->mgmtData;
	PageNumber *fc = malloc(sizeof(PageNumber) * size_buffer);


	for(int i = 0;i < size_buffer;i++) {
		condition((pframe[i].pageNum != -1),
		{
			fc[i] = pframe[i].pageNum;
		})
		else
		{
			fc[i] = NO_PAGE;
		}
	}
	return fc;
}

extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	PFrame *pframe = (PFrame *)bm->mgmtData;
	bool *dFlags = malloc(sizeof(bool) * size_buffer);

	for(int i = 0; i < size_buffer; i++)
	{
		condition(doEq(pframe[i].bit_b,1),
		{
			dFlags[i] = true;
		})
		else
		{
			dFlags[i] = false;
		}
	}
	return dFlags;
}

extern int *getFixCounts (BM_BufferPool *const bm)
{
	PFrame *pframe = (PFrame *)bm->mgmtData;
	int *get_fix_counts = malloc(sizeof(int) * size_buffer);

		for(int i = 0; i < size_buffer;i++){
		condition((pframe[i].count_fix != -1),
		{
			get_fix_counts[i] =  pframe[i].count_fix;
		})
		else
		{
			get_fix_counts[i] = 0;
		}
	}
	return get_fix_counts;
}


extern int getNumReadIO (BM_BufferPool *const bm)
{
	// Adding 1 to index_rear.
	return (index_rear + 1);
}

extern int getNumWriteIO (BM_BufferPool *const bm)
{
	return count_write;
}
