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
	
	IndexMetaInfo meta;
	try {
		file = new BlobFile(outIndexName, false); // Try opening existing index file

		// use existing file
		std::cout << "Open exisitng Index File" << outIndexName << std::endl;
		// Read meta page
		Page *metaPage;
		bufMgr->readPage(file, headerPageNum, metaPage);

		// Set up rootPageNo and leafRoot from IndexMetaInfo
		std::string metaRecord = metaPage->getRecord(METARECORDID);
    meta = *reinterpret_cast<const IndexMetaInfo*>(metaRecord.data());
		
		rootPageNum = meta.rootPageNo;
		leafRoot = meta.leafRoot;

		bufMgr->unPinPage(file, headerPageNum, false);

	} catch(FileNotFoundException e) {
		// have to create new file
		std::cout << "Creating new index file" << outIndexName << std::endl;
		file = new BlobFile(outIndexName, true);

		// allocate page for meta info
		Page *metaPage;
		bufMgr->allocPage(file, headerPageNum, metaPage);
		std::cout << "metaPageNum: " << headerPageNum << std::endl;

		// allocate page for root
		Page *rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);
		leafRoot = true;
		std::cout << "rootPageNum: " << rootPageNum << std::endl;

		// populate meta info with the root page num
		meta.attrByteOffset = attrByteOffset;
		meta.attrType = attributeType;
		meta.rootPageNo = rootPageNum;
		meta.leafRoot = true;
		std::string metaRecord(reinterpret_cast<char*>(&meta),sizeof(meta));
		metaPage->insertRecord(metaRecord);
	
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
				// TODO: add the data in the index
				insertEntry(key, scanRid);
			}
		}
		catch(EndOfFileException e)
		{
			std::cout << "Finish inserted all to B+ Tree records" << std::endl;
		}

		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, false);
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	IndexMetaInfo meta;
	meta.attrByteOffset = attrByteOffset;
	meta.attrType = attributeType;
	meta.rootPageNo = rootPageNum;
	meta.leafRoot = leafRoot;

	// Update Index meta info to the Index meta info page
	Page *metaPage;
	std::string newMetaRecord(reinterpret_cast<char*>(&meta), sizeof(meta));
	bufMgr->readPage(file, headerPageNum, metaPage);
	metaPage->updateRecord(METARECORDID, newMetaRecord);
	bufMgr->unPinPage(file, headerPageNum, true);

	// Unpin page that is currently scanning
	if (currentPageNum) {
		bufMgr->unPinPage(file, currentPageNum, false);
	}

	bufMgr->flushFile(file);
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
