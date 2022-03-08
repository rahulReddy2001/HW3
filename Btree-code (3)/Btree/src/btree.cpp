/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string& relationName,
                       std::string& outIndexName, BufMgr* bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {
    this->bufMgr = bufMgrIn;
    this->attributeType = attrType;
    this->attrByteOffset = attrByteOffset;

    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    outIndexName = idxStr.str();

    bool createNew = !file->exists(outIndexName);
    file = new BlobFile(outIndexName, createNew);

    if (createNew) {
        // Get a page for the root
        PageId rootPageNo;
        NonLeafNodeInt* rootNode;
        bufMgr->allocPage(file, rootPageNo, (Page*&)rootNode);
        rootPageNum = rootPageNo;
        rootNode->keyArray[0] = INT_MAX;
        rootNode->pageNoArray[0] = INT_MAX;
        memset(rootNode, 0, INTARRAYLEAFSIZE);
        PageId headerPageNo;
        Page* headerPage;
        bufMgr->allocPage(file, headerPageNo, headerPage);
        headerPageNum = headerPageNo;
        char relation_array[relationName.length() + 1];
        strcpy(relation_array, relationName.c_str());
        struct IndexMetaInfo meta = {*relation_array, attrByteOffset, attrType,
                                     rootPageNo};
        headerPage = ((Page*)&meta);
        bufMgr->unPinPage(file, headerPageNo, true);
        bufMgr->unPinPage(file, rootPageNo, true);
    }
    FileScan* fileScan = new FileScan(relationName, bufMgrIn);
    bool done = false;
    while (!done) {
        try {
            RecordId recordId;
            fileScan->scanNext(recordId);
            const void* key = &recordId + attrByteOffset;
            this->insertEntry(key, recordId);
        } catch (EndOfFileException&) {
            done = true;
            continue;
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
    scanExecuting = false;
    bufMgr->flushFile(file);
    delete file;
}

bool BTreeIndex::isLeafFull(LeafNodeInt* leaf) {
    return leaf->ridArray[INTARRAYLEAFSIZE - 1].page_number != 0;
}

bool BTreeIndex::isLeafEmpty(LeafNodeInt* leaf) {
    return leaf->ridArray[0].page_number == 0;
}

int BTreeIndex::nonLeafPageLen(NonLeafNodeInt* nonLeaf) {
    int count = 0;
    int arraySize =
        sizeof(nonLeaf->pageNoArray) / sizeof(nonLeaf->pageNoArray[0]);
    for (int i = 0; i < arraySize; i++) {
        if (nonLeaf->pageNoArray[i] != 0) {
            count++;
        } else {
            return count;
        }
    }
    return -1;
}

int BTreeIndex::leafPageLen(LeafNodeInt* leaf) {
    int count = 0;
    int arraySize = sizeof(leaf->keyArray) / sizeof(leaf->keyArray[0]);
    for (int i = 0; i < arraySize; i++) {
        if (leaf->keyArray[i] != 0) {
            count++;
        } else {
            return count;
        }
    }
    return -1;
}

int BTreeIndex::findArrayIndex(const int *arr, int key, bool gte) {
	int retVal = -1;
	for(int i = 0; i < )
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void* key, const RecordId rid) {
    Page* root;
    bufMgr->readPage(file, rootPageNum, root);
    PageKeyPair<int>* newchildEntry = nullptr;
    bool leafNode;
    if (1 == rootPageNum) {
        leafNode = true;
    } else {
        leafNode = false;
    }
    RIDKeyPair<int> dataEntry;
    int insertKey = *((int*)key);
    dataEntry.set(rid, insertKey);
    // insert(root, rootPageNum, leafNode, dataEntry, newchildEntry);
}


void BTreeIndex::moveToNextPage(LeafNodeInt *node) {
  bufMgr->unPinPage(file, currentPageNum, false);
  currentPageNum = node->rightSibPageNo;
  bufMgr->readPage(file, currentPageNum, currentPageData);
  nextEntry = 0;
}

void BTreeIndex::setEntryIndexForScan() {
  LeafNodeInt *node = (LeafNodeInt *)currentPageData;
  int entryIndex = findArrayIndex(node->keyArray, lowValInt);
  if (entryIndex == -1)
    moveToNextPage(node);
  else
    nextEntry = entryIndex;
}


void BTreeIndex::findPageForScan(){

	NonLeafNodeInt* currentNode = (NonLeafNodeInt *) currentPageData;
    bool foundLeaf = false;
    while(!foundLeaf){      
      currentNode = (NonLeafNodeInt *) currentPageData;      
      if(currentNode->level == 1) foundLeaf = true;      
      PageId nextPageNum;      
	  int i;
      for(i = nonLeafPageLen(currentNode) - 1; i > 0 ;i--){
		  i--;
	  }
	  nextPageNum = currentNode->pageNoArray[i];
      bufMgr->unPinPage(file, currentPageNum, false);
      currentPageNum = nextPageNum;      
      bufMgr->readPage(file, currentPageNum, currentPageData);
	}
		LeafNodeInt *node = (LeafNodeInt *)currentPageData;
  int entryIndex = findArrayIndex(node->keyArray, lowValInt);
  if (entryIndex == -1)
    moveToNextPage(node);
  else
    nextEntry = entryIndex;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm, const Operator lowOpParm,
                           const void* highValParm, const Operator highOpParm) {
    if (lowOpParm != GT && lowOpParm != GTE) {
        throw BadOpcodesException();
    }
    if (highOpParm != LT && highOpParm != LTE) {
        throw BadOpcodesException();
    }
    this->lowValInt = *((int*)lowValParm);
    this->highValInt = *((int*)highValParm);
    if (this->lowValInt > this->highValInt) {
        throw BadScanrangeException();
    }
    this->scanExecuting = true;
    bufMgr->readPage(this->file, headerPageNum, currentPageData);
    currentPageNum = headerPageNum;

    RecordId next = ((LeafNodeInt*)currentPageData)->ridArray[nextEntry];
    if ((next.page_number == 0 && next.slot_number == 0) {
            throw NoSuchKeyFoundException();
            endScan();
        } else if ((LeafNodeInt*)currentPageData)
            ->keyArray[nextEntry] > highValInt) {
        throw NoSuchKeyFoundException();
        endScan();
    } else if (highOp == LT && (LeafNodeInt*)currentPageData)->keyArray[nextEntry] == highValInt)) {
            endScan();
            throw NoSuchKeyFoundException();
        }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) {}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {
    if (!scanExecuting) throw ScanNotInitializedException();
    scanExecuting = false;
    bufMgr->unPinPage(file, currentPageNum, false);
}

}  // namespace badgerdb
