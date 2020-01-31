# B-Tree-Implimentation

Code Execution Instructions:

1. Navigate to /assign4 root directory
2. Run $ make clean       //to clean up/delete all the object files (if existing)
3. Run $ make             //to run all the necessary files and create their object files.
4. Run $ ./test_expr      // To run the test_expr file
5. Run $ ./test_assign4   // To run the test_assign4_1 file

The assignment's root directory consists of following files :

btree_mgr.c
btree_mgr.h
buffer_mgr_stat.c
buffer_mgr_stat.h
buffer_mgr.c
buffer_mgr.h
dberror.h
dberror.c
dt.h
expr.c
expr.h
Makefile
README.txt
record_mgr.c
record_mgr.h
rm_serializer.c
storage_mgr.c
storage_mgr.h
tables.h
test_assign4_1.c
test_expr.c
test_helper.h
testidx


Code Documentation:

createBtree()
This function is used to create a B+ tree index.

deleteBtree()
This function is used to delete a B+ tree index.It also removes the corresponding page file.

openBtree()
This function is used to open an existing B+ tree index.

closeBtree()
This function is used to close an opened B+ tree index.
Every other newly created or modified pages of the index are flushed back to the disk later with the help of the index manager.

findKey()
This method searches the B+ Tree for the key provided in as one of the arguments.
RC_IM_KEY_NOT_FOUND error code is thrown if the key is not found

insertKey()
This function takes in a key and RID as its arguments.
returns the error code RC_IM_KEY_ALREADY_EXISTS if the key already exists.

deleteKey()
This function is used to remove a key and the corresponding record pointer (RID) from the index.
RC_IM_KEY_NOT_FOUND is thrown if key not found.

openTreeScan()
This function is used to open the tree  and scan through all entries of a tree.

nextEntry()
This function is used to read the next entry that of the tree.

closeTreeScan()
This function is used to close the tree successfully after all the elements of the B+ tree are scanned.

getNumNodes()
This function is used to get the total number of nodes in the tree.

getNumEntries()
This function calculates the total number of entries present in the tree.
Takes in the pointer object result and tree as arguments for computing.

getKeyType()
The datatype of the keys being stored in our B+ Tree are determined and returned.

printTree()

This last function has been defined to print the tree (created mainly for debugging purpose).
