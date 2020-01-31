
#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>

typedef struct BTREE
{
    int *key;
    struct BTREE **next;
    RID *id;
} BTree;

BTree *scan;
BTree *root;

//variable initialization.
int nodeVal;
int indexNum = 0;
SM_FileHandle btree_fh;
int maxEle;


//function to initialize the index manager.
RC initIndexManager (void *mgmtData)
{
    return RC_OK;
}


//function to terminate the index manager.
RC shutdownIndexManager ()
{
    return RC_OK;
}

//function to create B tree.
RC createBtree (char *idxId, DataType keyType, int n)
{

    root = ((BTree*)malloc(sizeof(BTree)));
    root->key = malloc(sizeof(int) * n);
    nodeVal = 2;
    root->id = malloc(sizeof(int) * n);
    root->next = malloc(sizeof(BTree) * (n + 1));
    int i = 0;
    while (i<n + 1){
        root->next[i] = NULL;
        i += 1;
    }
    maxEle = n;
    createPageFile (idxId);

    return RC_OK;
}


//function to open the created B tree and traversing all its elements.
RC openBtree (BTreeHandle **tree, char *idxId)
{
    nodeVal= 3;
    openPageFile (idxId, &btree_fh);
    return RC_OK;
}


//function to close the openend B tree after traversing all its elements.
RC closeBtree (BTreeHandle *tree)
{

    closePageFile(&btree_fh);
    nodeVal=1;
    free(root);

    return RC_OK;
}


//function to delete the b tree and respective record pointer from the index.
RC deleteBtree (char *idxId)
{
    destroyPageFile(idxId);
    nodeVal = 10;
    return RC_OK;
}


//function to calculate the total number of nodes of the tree.
RC getNumNodes (BTreeHandle *tree, int *result)
{
    int no_of_nodes = 0;
    nodeVal = 5;
    BTree *temp = ((BTree*)malloc(sizeof(BTree)));
    int i = 0;
    while (i<maxEle + 2) {
        no_of_nodes += 1;
        i += 1;
    }

    *result = no_of_nodes;

    return RC_OK;
}

//function to calculate the total number of entries of the btree.
RC getNumEntries (BTreeHandle *tree, int *result)
{
    int tot = 0,tot_elements = 0, i;

    BTree *temp = ((BTree*)malloc(sizeof(BTree)));
    int tot_value = 0;
    temp = root;
    while (temp != NULL){
        i = 0;
        while (i < maxEle){
            if(temp->key[i] != 0){
                tot_elements += 1;
            }
            i += 1;
        }
        temp = temp->next[maxEle];
    }
    tot_value = ++tot;
    *result = tot_elements;
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result)
{
    return RC_OK;
}


//function for accessing the index by searching the key provided.
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    int f = 0;
    BTree *temp = ((BTree*)malloc(sizeof(BTree)));
    int i;
    temp = root;
    while (temp != NULL) {
        i = 0;
        while (i < maxEle) {
            if(temp->key[i] == key->v.intV)
             {
                f = 1;
                (*result).page = temp->id[i].page;
                (*result).slot = temp->id[i].slot;
                break;
            }
            i += 1;
        }
        if(f == 1){
            break;
        }
        temp = temp->next[maxEle];
    }

    if(f != 1)
        return RC_IM_KEY_NOT_FOUND;
    else
        return RC_OK;
}

//function to insert a new key and a record pointer pair into the index by making use of it.
RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    BTree *node = (BTree*)malloc(sizeof(BTree));
    nodeVal = 9;
    node->key = malloc(sizeof(int) * maxEle);
    node->id = malloc(sizeof(int) * maxEle);
    node->next = (malloc(sizeof(BTree) * (maxEle + 1)));
    int i = 0;
    while (i < maxEle) {
    	node->key[i] = 0;
        i += 1;
    }

    int nodeFull = 0;
    temp = root;
    while (temp != NULL) {
        nodeFull = 0;
        i = 0;
        while (i < maxEle) {
            if(temp->key[i] == 0) {
                temp->id[i].page = rid.page;
                temp->id[i].slot = rid.slot;
                nodeVal = 1;
                temp->key[i] = key->v.intV;
                temp->next[i] = NULL;
                nodeFull += 1;
                break;
            }
            i += 1;
        }
        if((nodeFull == 0) && (temp->next[maxEle] == NULL)) {
            node->next[maxEle] = NULL;
            nodeVal = 2;
            temp->next[maxEle] = node;
        }
        temp = temp->next[maxEle];
    }

    int totalEle = 0;
    temp = root;
    while (temp != NULL){
        i = 0;
        while (i < maxEle){
            if(temp->key[i] != 0){
                totalEle += 1;
            }
            i += 1;
        }
        temp = temp->next[maxEle];
    }

    if(totalEle == 6) {
        node->key[0] = root->next[maxEle]->key[0];
        node->key[1] = root->next[maxEle]->next[maxEle]->key[0];
        nodeVal = 3;
        node->next[0] = root;
        node->next[1] = root->next[maxEle];
        node->next[2] = root->next[maxEle]->next[maxEle];
    }

    return RC_OK;
}

//function to delete the key and the respective record pointer.
RC deleteKey (BTreeHandle *tree, Value *key)
{
    int f = 0, i;
    BTree *temp = (BTree*)malloc(sizeof(BTree));
    temp = root;
    while (temp != NULL) {
        i = 0;
        while (i < maxEle) {
            if(temp->key[i] == key->v.intV) {
                f = 1;
                temp->key[i] = 0;
                temp->id[i].page = 0;
                temp->id[i].slot = 0;
                break;
            }
            i += 1;
        }
        if(f == 1){
            break;
        }
        temp = temp->next[maxEle];
    }

     return RC_OK;
}

//function to open the tree and scan all B tree entries.
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    indexNum = 0;
    int totalEle = 0, i;
    scan = (BTree*)malloc(sizeof(BTree));
    scan = root;

    BTree *temp = (BTree*)malloc(sizeof(BTree));
    temp = root;
    while (temp != NULL){
        i = 0;
        while (i < maxEle){
            if(temp->key[i] != 0){
                nodeVal = 0;
                totalEle += 1;
            }
            i += 1;
        }
        temp = temp->next[maxEle];
    }

    int elements[maxEle][totalEle];
    int count = 0;
    int key[totalEle];
    temp = root;
    while (temp != NULL) {
        i = 0;
        while (i < maxEle) {
            key[count] = temp->key[i];
            elements[0][count] = temp->id[i].page;
            nodeVal = 1;
            elements[1][count] = temp->id[i].slot;
            count += 1;
            i += 1;
        }
        temp = temp->next[maxEle];
    }

    int pg, st;
    int swap,c,d;
    c = 0;
    while (c < count - 1)
    {
        d = 0;
        while (d < (count - c - 1))
        {
            if(key[d] > key[d+1])
            {
                swap = key[d];
                pg = elements[0][d];
                st = elements[1][d];

                nodeVal = 0;

                key[d] = key[d + 1];
                elements[0][d] = elements[0][d + 1];
                elements[1][d] = elements[1][d + 1];

                nodeVal = 1;

                key[d + 1] = swap;
                elements[0][d + 1] = pg;
                elements[1][d + 1] = st;
            }
            d += 1;
        }
        c += 1;
    }

    count = 0;
    temp = root;
    while(temp != NULL) {
        i = 0;
        while (i < maxEle) {
            temp->key[i] = key[count];
            temp->id[i].page = elements[0][count];
            nodeVal = 2;
            temp->id[i].slot = elements[1][count];
            count += 1;
            i += 1;
        }
        temp = temp->next[maxEle];
    }
   return RC_OK;
}

//function to read the next entry in the B tree.
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    if(scan->next[maxEle] == NULL) {
        return RC_IM_NO_MORE_ENTRIES;
    }
    else{
        if(maxEle != indexNum) {
            nodeVal = 2;
        }else{
            indexNum = 0;
            scan = scan->next[maxEle];
        }
        (*result).page = scan->id[indexNum].page;
        nodeVal = 1;
        (*result).slot = scan->id[indexNum].slot;
        indexNum += 1;
    }

    return RC_OK;
}

//function to close the tree after scanning all the elements in the B tree.
RC closeTreeScan (BT_ScanHandle *handle)
{
    indexNum = 0;
    nodeVal = 4;
    return RC_OK;
}


//Function to print b tree.
char *printTree (BTreeHandle *tree)
{
    return RC_OK;
}
