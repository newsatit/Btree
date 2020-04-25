/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	bufMgr = bufMgrIn;
	headerPageNum = 1;

	// Create name of the index file
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	// Set up object attributes
	attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	scanExecuting = false;
	
	try {
		file = new BlobFile(outIndexName, false); // Try opening existing index file

		// use existing file
		std::cout << "Open exisitng Index File" << outIndexName << std::endl;

		// Read meta page
		Page *metaPage;
		bufMgr->readPage(file, headerPageNum, metaPage);

		// Set up rootPageNo and leafRoot from IndexMetaInfo
		IndexMetaInfo *meta = (IndexMetaInfo*)metaPage;
		rootPageNum = meta->rootPageNo;
		leafRoot = meta->leafRoot;

		bufMgr->unPinPage(file, headerPageNum, false); // Meta Info page no longer needed

	} catch(FileNotFoundException e) {
		// have to create new file
		std::cout << "Creating new index file" << outIndexName << std::endl;
		file = new BlobFile(outIndexName, true);

		// allocate page for meta info
		Page *metaPage;
		bufMgr->allocPage(file, headerPageNum, metaPage);
		std::cout << "metaPageNum: " << headerPageNum << std::endl;

		// allocate page for root
		leafRoot = true; // root is the only node and is a leaf.
		Page *rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);
		std::cout << "rootPageNum: " << rootPageNum << std::endl;
		// initialize root node
		LeafNodeInt *root = (LeafNodeInt*)(rootPage);
		root->numEntries = 0;
		root->rightSibPageNo = 0;
		
		// populate meta info with the root page num
		IndexMetaInfo *meta = (IndexMetaInfo*)(metaPage);
		meta->attrByteOffset = attrByteOffset;
		meta->attrType = attributeType;
		meta->rootPageNo = rootPageNum;
		meta->leafRoot = true;

		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, false);

		// scan the file with the relation data (use FileScan) and keep the entries <key, rid>		FileScan fscan(relationName, bufMgr);
		FileScan fscan(relationName, bufMgr);
		try
		{
			RecordId scanRid;
			while(1)
			{
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				void *key = (void*)(record + attrByteOffset);
				insertEntry(key, scanRid);
				// std::cout << "Inserted key: " << *((int*)key) << " rid: (" << scanRid.page_number << ", " << scanRid.slot_number << ")" << std::endl;
			}
		}
		catch(EndOfFileException e)
		{
			std::cout << "Finish inserted all to B+ Tree records" << std::endl;
		}
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// Update Index meta info to the Index meta info page
	Page *metaPage;
	IndexMetaInfo *meta;
	bufMgr->readPage(file, headerPageNum, metaPage);
	meta = (IndexMetaInfo*)(metaPage);
	meta->attrByteOffset = attrByteOffset;
	meta->attrType = attributeType;
	meta->rootPageNo = rootPageNum;
	meta->leafRoot = leafRoot;
	bufMgr->unPinPage(file, headerPageNum, true);

	// Unpin page that is currently scanning
	if (currentPageNum) {
		bufMgr->unPinPage(file, currentPageNum, false);
	}

	bufMgr->flushFile(file);
	delete file;
}

//TODO: comment
void insertLeafArrays(const RIDKeyPair<int> ridKey, int keyArray[], RecordId ridArray[], const int numEntries) 
{
	// Append at the end
	if (ridKey.key >= keyArray[numEntries - 1]) {
		keyArray[numEntries] = ridKey.key;
		ridArray[numEntries] = ridKey.rid;
	// Have to shift some elements to the right
	} else {
		int insertIdx;
		// Find index in keyArray to insert to
		for (int i = 0; i < numEntries; i++) {
			if (ridKey.key < keyArray[i]) {
				insertIdx = i;
			}
		}
		// Shift elements to the right of it
		for (int i = numEntries; i > insertIdx; i--) {
			keyArray[i] = keyArray[i-1];
			ridArray[i] = ridArray[i-1];
		}
		// Insert key, rid
		keyArray[insertIdx] = ridKey.key;
		ridArray[insertIdx] = ridKey.rid;
	}
}

// TODO: comment
void insertNonleafArrays(const PropogationInfo propInfo, const int insertIdx, 
												int keyArray[], PageId pageNoArray[], const int numEntries) 
{
	// Shift element to the right of insertIdx
	for (int i = numEntries; i > insertIdx; i--) {
		keyArray[i] = keyArray[i-1];
		pageNoArray[i+1] = pageNoArray[i];
	}

	// Insert key, rid
	keyArray[insertIdx] = propInfo.middleKey;
	pageNoArray[insertIdx] = propInfo.leftPageNo;
	pageNoArray[insertIdx+1] = propInfo.rightPageNo;	
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::insertHelper(const RIDKeyPair<int> ridKey, const PageId nodePageNo, const int nodeType, 
															PropogationInfo & propInfo, bool & splitted)
{
	Page *page;
	bufMgr->readPage(file, nodePageNo, page);

	if (nodeType) { // leaf
		LeafNodeInt *node = (LeafNodeInt*)(page);
		
		// Leaf Node is full
		splitted = true;
		if (node->numEntries == INTARRAYLEAFSIZE) {
			// Copy and insert to temporary arrays
			int tempKeyArray[ INTARRAYLEAFSIZE + 1];
			RecordId tempRidArray[ INTARRAYLEAFSIZE + 1];
			std::copy(node->keyArray, node->keyArray + node->numEntries, tempKeyArray);
			std::copy(node->ridArray, node->ridArray + node->numEntries, tempRidArray);
			insertLeafArrays(ridKey, tempKeyArray, tempRidArray, node->numEntries);

			// Distrubute entries to both nodes
			// Allocate right page. Left page will used the page allocated by the original page.
			Page *rightPage;
			propInfo.leftPageNo = nodePageNo;
			bufMgr->allocPage(file, propInfo.rightPageNo, rightPage);
			LeafNodeInt *leftNode = node;
			LeafNodeInt *rightNode = (LeafNodeInt*)(rightPage);
			leftNode->numEntries = (node->numEntries+1-1)/2;
			rightNode->numEntries = (node->numEntries+1-1) - leftNode->numEntries;
			
			//Distrubute to left page
			std::copy(tempKeyArray, tempKeyArray + leftNode->numEntries, leftNode->keyArray);
			std::copy(tempRidArray, tempRidArray + leftNode->numEntries, leftNode->ridArray);
		
			//Distrubute to right page
			std::copy(tempKeyArray + leftNode->numEntries, tempKeyArray + node->numEntries + 1, rightNode->keyArray);
			std::copy(tempRidArray + leftNode->numEntries, tempRidArray + node->numEntries + 1, rightNode->ridArray);

			// Set up sibling of both page
			rightNode->rightSibPageNo = node->rightSibPageNo;
			leftNode->rightSibPageNo = propInfo.rightPageNo;

			// Set up necessary info for propogation
			propInfo.middleKey = rightNode->keyArray[0];
			propInfo.fromLeaf = true;

			// Get rid of old page node and unpin new pages
			bufMgr->unPinPage(file, propInfo.leftPageNo, true);
			bufMgr->unPinPage(file, propInfo.rightPageNo, true);

		// Leaf Node is not full
		} else {
			splitted = false;
			insertLeafArrays(ridKey, node->keyArray, node->ridArray, node->numEntries);
			node->numEntries++;
			bufMgr->unPinPage(file, nodePageNo, true); 
		}
	} else { // Nonleaf
		// Find the next page to traverse
		NonLeafNodeInt *node = (NonLeafNodeInt*)(page);
		PageId childPageNo;
		PropogationInfo childPropInfo;
		bool childSplitted;
		int insertIdx;
		childPageNo = node->keyArray[node->numEntries - 1];
		insertIdx = node->numEntries;
		for (int i = 0; i < node->numEntries; i++) {
			if (ridKey.key < node->keyArray[i]) {
				childPageNo = node->keyArray[i];
				insertIdx = i;
				break;
			}
		}

		insertHelper(ridKey, childPageNo, node->level, childPropInfo, childSplitted); // start traversing

		// Handle split propogation
		if (childSplitted) {
			// Nonleaf node is full
			if (node->numEntries == INTARRAYNONLEAFSIZE) {
			// Copy and insert to temporary arrays
			int tempKeyArray[ INTARRAYLEAFSIZE + 1];
			PageId tempPageNoArray[ INTARRAYLEAFSIZE + 2];
			std::copy(node->keyArray, node->keyArray + node->numEntries, tempKeyArray);
			std::copy(node->pageNoArray, node->pageNoArray + node->numEntries + 1, tempPageNoArray);
			insertNonleafArrays(childPropInfo, insertIdx, tempKeyArray, tempPageNoArray, node->numEntries);

			// Distrubute entries to both nodes
			// Allocate right page. Left page will used the page allocated by the original page.
			Page *rightPage;
			propInfo.leftPageNo = nodePageNo; 
			bufMgr->allocPage(file, propInfo.rightPageNo, rightPage);
			NonLeafNodeInt *leftNode = node;
			NonLeafNodeInt *rightNode = (NonLeafNodeInt*)(rightPage);
			leftNode->numEntries = (node->numEntries+1)/2;
			rightNode->numEntries = (node->numEntries+1) - leftNode->numEntries;
			
			//Distrubute to left page
			std::copy(tempKeyArray, tempKeyArray + leftNode->numEntries, leftNode->keyArray);
			std::copy(tempPageNoArray, tempPageNoArray + leftNode->numEntries + 1, leftNode->pageNoArray);
		
			//Distrubute to right page
			std::copy(tempKeyArray + leftNode->numEntries + 1, tempKeyArray + node->numEntries + 1, rightNode->keyArray);
			std::copy(tempPageNoArray + leftNode->numEntries + 1, tempPageNoArray + node->numEntries + 2, rightNode->pageNoArray);

			//Set up the levels of both pages
			leftNode->level = childPropInfo.fromLeaf;
			rightNode->level = childPropInfo.fromLeaf;

			// Set up necessary info for propogation
			propInfo.middleKey = rightNode->keyArray[0];
			propInfo.fromLeaf = false;

			// Get rid of old page node and unpin new pages
			bufMgr->unPinPage(file, propInfo.leftPageNo, true);
			bufMgr->unPinPage(file, propInfo.rightPageNo, true);

			// Nonleaf node is not full
			} else {
				splitted = false;
				insertNonleafArrays(childPropInfo, insertIdx, node->keyArray, node->pageNoArray, node->numEntries);
				node->numEntries++;
				bufMgr->unPinPage(file, nodePageNo, true); 
			}
		// Child was not splitted
		} else {
			bufMgr->unPinPage(file, nodePageNo, false);
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	PropogationInfo propInfo;
	bool splitted;
	RIDKeyPair<int> ridKey;
	ridKey.set(rid, *((int*)(key)));

	insertHelper(ridKey, rootPageNum, leafRoot, propInfo, splitted); // Start traversing the root page.

	if (splitted) { // Have to create new root page
		Page *rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage); // Allocate new root page
		
		// Set up content of the root page.
		NonLeafNodeInt *root = (NonLeafNodeInt*)(rootPage);
		root->level = propInfo.fromLeaf;
		root->numEntries = 1;
		root->keyArray[0] = propInfo.middleKey;
		root->pageNoArray[0] = propInfo.leftPageNo;
		root->pageNoArray[1] = propInfo.rightPageNo;

		bufMgr->unPinPage(file, rootPageNum, true);
	} 
}

const void BTreeIndex::findBestChild(NonLeafNodeInt *currentNode, PageId &nextNodeNum, int lowVal)
{
	int j = 0;

	while(j <= currentNode->numEntries && currentNode->pageNoArray[j] == 0)
	{
		j++;
	}
	// TODO might need to change if the thing is GTE
	while(j < currentNode->numEntries && currentNode->keyArray[j-1] <= lowVal)
	{
		j++;
	}

  nextNodeNum = currentNode->pageNoArray[j];

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
   const Operator lowOpParm,
   const void* highValParm,
   const Operator highOpParm)
{

	if(scanExecuting){
		return;
	}
	else{

	scanExecuting = true;
	lowOp = lowOpParm;
	highOp = highOpParm;
	lowValInt = *(int*)lowValParm;
	highValInt = *(int*)highValParm;

	}

	if ((lowOpParm != GTE && lowOpParm != GT) || (highOpParm != LT && highOpParm != LTE )){
		throw new BadOpcodesException;
	}

	if (lowValInt > highValInt){
		throw new BadScanrangeException;
	}

	// get meta page and meta info
	Page* meta;
	bufMgr->readPage(file, headerPageNum, meta);
	IndexMetaInfo* metaPage = (IndexMetaInfo*)meta;

	rootPageNum = metaPage->rootPageNo;
	std::cout << rootPageNum << std::endl;

	// if the root node is the only node in the tree
	if (metaPage->leafRoot){

		bufMgr->readPage(file, rootPageNum, currentPageData);
		LeafNodeInt* currentLeafRoot = (LeafNodeInt*) currentPageData;

		// [1, 3, 5, 7, 12] >=5  nextEntry: 1
		for (int i = 0; i < INTARRAYLEAFSIZE; i++){

			if (lowOp == GTE && currentLeafRoot->keyArray[i] >= lowValInt){
				nextEntry = i;
				break;
			}

			if(lowOp == GT && currentLeafRoot->keyArray[i] > lowValInt){
				nextEntry = i;
				break;
			}
		}

	}

	// If the root node is not a leaf and not the only node in tree
	else {
		
		bufMgr->readPage(file, rootPageNum, currentPageData);
		currentPageNum = rootPageNum;
		NonLeafNodeInt* currentNode = (NonLeafNodeInt*) currentPageData;

		bool leaf = false;

		//find leaf
		while(!leaf){
			std::cout << "Find Leaf Loop, Level: " << currentNode->level << std::endl;

			currentNode = (NonLeafNodeInt*) currentPageData;

			if (currentNode->level == 1){

				leaf = true;

			}
			PageId nextId;
			findBestChild(currentNode, nextId, lowValInt);
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = nextId;
			bufMgr->readPage(file, currentPageNum, currentPageData);

		}
		//find best in leaf
		bool findBest = false;
		while(!findBest){
			//std::cout << "Find best Loop, Level: " << currentNode->level << std::endl;


			LeafNodeInt* currentNode = (LeafNodeInt*) currentPageData;

			// if the leaf page is null throw exception
			if(currentNode->ridArray[0].page_number == 0){

				bufMgr->unPinPage(file, currentPageNum, false);
      			throw NoSuchKeyFoundException();

			}
			// TODO maybe check to see if its the last one cause that could be empty
			for (int i = 0; i < leafOccupancy && currentNode->ridArray[i + 1].page_number == 0; i++){
				//different satisfactory requirements
				if(lowOp == GTE && highOp == LTE){
					if(currentNode->keyArray[i] >= lowValInt && currentNode->keyArray[i] <= highValInt){
						findBest = true;
						nextEntry = i;
						break;
					}
				}
				else if(lowOp == GT && highOp == LTE){
					if(currentNode->keyArray[i] > lowValInt && currentNode->keyArray[i] <= highValInt){
						findBest = true;
						nextEntry = i;
						break;
					}
				}
				else if(lowOp == GTE && highOp == LT){
					if(currentNode->keyArray[i] >= lowValInt && currentNode->keyArray[i] < highValInt){
						findBest = true;
						nextEntry = i;
						break;
					}
				}
				else if(lowOp == GT && highOp == LT){
					if(currentNode->keyArray[i] > lowValInt && currentNode->keyArray[i] < highValInt){
						findBest = true;
						nextEntry = i;
						break;
					}
				}	

				//keys get too high
				else if(highOp == LT && currentNode->keyArray[i] >= highValInt){
					bufMgr->unPinPage(file, currentPageNum, false);
					throw NoSuchKeyFoundException();
				}
				else if(highOp == LTE && currentNode->keyArray[i] > highValInt){
					bufMgr->unPinPage(file, currentPageNum, false);
					throw NoSuchKeyFoundException();
				}

				//no valid key in leaf so go to next		
				else if(currentNode->ridArray[i + 1].page_number == 0 || i == leafOccupancy-1){
					bufMgr->unPinPage(file, currentPageNum, false);

					// must check to see if leaf has sibling
					if(currentNode->rightSibPageNo == 0){
						throw NoSuchKeyFoundException();
					}
					else {
						currentPageNum = currentNode->rightSibPageNo;
						bufMgr->readPage(file, currentPageNum, currentPageData);
					}
				}
			}

		}
	}
}

// 		// start at root
// 		std::cout << "Num Level: " << currentNode->level << std::endl;
// 		while (currentNode->level != 1){

// 			std::cout << "Num Entry: " << currentNode->numEntries << std::endl;
// 			std::cout << "rootPageNum" << rootPageNum << std::endl;

// 			for (int i = 0; i < currentNode->numEntries; i++){

// 				if(lowValInt >= currentNode->keyArray[i]){
// 					if(lowOp == GTE && lowValInt == currentNode->keyArray[i]){
// 						nextEntry = i;
// 						break;
// 					}
// 					nextEntry = i;
// 				}
// 			}
			
// 			//unpin old page and read new page number
// 			nextId = currentNode->pageNoArray[nextEntry+1];
// 			bufMgr->unPinPage(file, currentPageNum, false);
// 			bufMgr->readPage(file, nextId, currentPageData);
// 	    	currentNode = (NonLeafNodeInt*) currentPageData;
// 			currentPageNum = nextId;
// 		}

// 		for (int i = 0; i < currentNode->numEntries; i++){

// 			if(lowValInt >= currentNode->keyArray[i]){
// 				if(lowOp == GTE && lowValInt == currentNode->keyArray[i]){
// 					nextEntry = i;
// 					break;
// 				}
// 				nextEntry = i;
// 			}
// 		}

// 		//unpin old page and read new page number
// 		nextId = currentNode->pageNoArray[nextEntry+1];
// 		bufMgr->unPinPage(file, currentPageNum, false);
// 		bufMgr->readPage(file, nextId, currentPageData);
// 		LeafNodeInt* currentNodeLeaf = (LeafNodeInt*) currentPageData;
// 		currentPageNum = nextId;

// 		bool found = false;
// 		while(!found){
// 			for (int i = 0; i < currentNode->numEntries; i++){

// 				if(lowOp == GT && lowValInt < currentNode->keyArray[i]){
// 					nextEntry = i;
// 					return;
// 				}
// 				else if(lowOp == GTE && lowValInt <= currentNode->keyArray[i]){
// 					nextEntry = i;
// 					return;
// 				}
//     		    else if((highOp == LT and currentNode->keyArray[i] >= highValInt) or (highOp == LTE and currentNode->keyArray[i] > highValInt))
// 				{
// 					bufMgr->unPinPage(file, currentPageNum, false);
// 					throw NoSuchKeyFoundException();
// 				}
// 			}
// 	        //unpin page
// 			bufMgr->unPinPage(file, currentPageNum, false);
// 			//did not find the matching one in the most right leaf
// 			if(currentNodeLeaf->rightSibPageNo == 0){

// 				throw NoSuchKeyFoundException();
			
// 			}
// 			currentPageNum = currentNodeLeaf->rightSibPageNo;
// 			bufMgr->readPage(file, currentPageNum, currentPageData);
// 		}
// 	}
// }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// - Returns (via the outRid parameter) the RecordId of the next record from the relation being scanned. 
// - It throws EndOfFileException() when the end of relation is reached.
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Ensure scan is currently executing
    if(!scanExecuting)
    {
        throw ScanNotInitializedException();
    }

    // Cast page to leaf node
    LeafNodeInt* currentNode = (LeafNodeInt*)currentPageData;
    // if the next entry exceeds a leaf's key occupancy or the page is
    if (nextEntry == currentNode->numEntries) {
        // unpin the page
        bufMgr->unPinPage(file, currentPageNum, false);
        // get the sibling
        currentPageNum = currentNode->rightSibPageNo;
        // if there isn't another node
        if(currentPageNum == 0)
        {
            throw IndexScanCompletedException();
        }
        else {
            // reset the entry
            nextEntry = 0;
            // read next page, update node
            bufMgr->readPage(file, currentPageNum, currentPageData);
            currentNode = (LeafNodeInt*)currentPageData;
        }

    }
    // get current key
    int currentKey = currentNode->keyArray[nextEntry];
    // check if key is in valid range
    if(lowOp == GT && highOp == LT && !(currentKey > lowValInt && currentKey < highValInt))
    {
        throw IndexScanCompletedException();
    }
    else if(lowOp == GT && highOp == LTE && !(currentKey > lowValInt && currentKey <= highValInt ))
    {
        throw IndexScanCompletedException();
    }
    else if(lowOp == GTE && highOp == LT && !(currentKey >= lowValInt && currentKey < highValInt))
    {
        throw IndexScanCompletedException();
    }
    else if (lowOp == GTE && highOp == LTE && !(currentKey >= lowValInt && currentKey <= highValInt ))
    {
        throw IndexScanCompletedException();
    }
    outRid = currentNode->ridArray[nextEntry];
    // set next entry
    nextEntry++;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
    // Check Exceptions
    if(!scanExecuting)
    {
        throw ScanNotInitializedException();
    }
    // unpin the page
    bufMgr->unPinPage(file, currentPageNum, false);
    // set scan to not executing
    scanExecuting = false;
}

}