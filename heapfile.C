//----------------------------------------
// Group 9 Project 5
// 1. Joe Elert - 9081168636
// 2. Derek Calamari - 9081197635
// 3. Michael Feist - 9082159105
//----------------------------------------

#include "heapfile.h"
#include "error.h"

/**
 * Creates a HeapFile with the given name.
 * 
 * @author       Joe Elert
 * @param fileName Name of the file to create.
 * @return       OK if successful,
 *               FILEEXISTS if heapfile already exists,
 *               Otherwise returns status of failing method
*/
// TODO - JOE
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate an empty header page and data page.
        status = db.createFile(fileName);
        if (status != OK) {
            return status;
        }

        status = db.openFile(fileName, file);
        if (status != OK) {
            return status;
        }

        // allocate an empty page by invoking bm->allocPage() appropriately. 
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK) {
            return status;
        }

        // Take the Page* pointer returned from allocPage() and cast it to a FileHdrPage*. 
        hdrPage = (FileHdrPage*) newPage;

        // Using this pointer initialize the values in the header page.
        strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);
        hdrPage->recCnt = 0;


        // Then make a second call to bm->allocPage(). This page will be the first data page of the file. 
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) {
            return status;
        }
        // Using the Page* pointer returned, invoke its init() method to initialize the page contents. 
        newPage->init(newPageNo);
        
        // Store the page number of the data page in firstPage and lastPage attributes of the FileHdrPage.
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;

        // Unpin page and mark as dirty.
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK) {
            return status;
        }

        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK) {
            return status;
        }

        return OK;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

/**
 * Heapfile Constructor method
 * 
 * @author       Derek Calamari
 * @param fileName Name of the file to open.
 * @param returnStatus Status of the method.
 * @return       void
*/
// TODO - DEREK
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;
    int headerPageNum;
    int firstPageNum;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        //get the page number for the headerPage
        status = filePtr->getFirstPage(headerPageNum);
        if(status != OK){
            returnStatus = status;
            return;
        }

        //read and pin header page for the file in the buffer pool
        status = bufMgr->readPage(filePtr, headerPageNum, pagePtr);
        if(status != OK){
            returnStatus = status;
            return;
        }

        //initialize private data members: headerPage, headerPageNo, hdrDirtyFlag
        //fileName.copy(headerPage->fileName,MAXNAMESIZE);
        //headerPage->fileName[MAXNAMESIZE-1] = '\0';
        headerPage = (FileHdrPage*)pagePtr;
        headerPageNo = headerPageNum;
        hdrDirtyFlag = false;
        
        // get the page number for the first page (which is nextPage from headerPage)
        firstPageNum = headerPage->firstPage;
        //Read and pin the first page of the file into the buffer pool
        status = bufMgr->readPage(filePtr, firstPageNum, pagePtr);
        if(status != OK){
            returnStatus = status;
            return;
        }

        // initializing the values of curPage, curPageNo, and curDirtyFlag appropriately
        curPage = pagePtr;
        curPageNo = firstPageNum;
        curDirtyFlag = false;

        // Set curRec to NULLRID.
        curRec = NULLRID;

        returnStatus = OK;
        return;
		
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

/**
 * Retrieve an arbitrary record from a file.
 * If record is not on the currently pinned page, the current page
 * is unpinned and the required page is read into the buffer pool
 * and pinned.  Returns a pointer to the record via the rec parameter.
 * 
 * @author       Joe Elert
 * @param rid    rid of record to retrieve
 * @param rec    reference to pointer to record
 * @return       OK if successful,
 *               Otherwise returns status of failing method
*/
// TODO - JOE
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;

    // Case 1, page and record match, we can call curPage->getRecord
    if (curPage != NULL && curPageNo == rid.pageNo) {
        status = curPage->getRecord(rid, rec);
        if (status != OK) {
            return status;
        }

        curRec = rid;
        return OK;
    } else if (curPage == NULL) { // Case 2, we haven't loaded a page in yet. Get pagNo from RID
        curPageNo = rid.pageNo;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        curDirtyFlag = false;
        status = curPage->getRecord(rid, rec);
        if (status != OK) {
            return status;
        }
        curRec = rid;
        return OK;
    } else { // Case 3, we have a page pinned but it's the wrong one
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            return status;
        }
        curPageNo = rid.pageNo;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        curDirtyFlag = false;
        status = curPage->getRecord(rid, rec);
        if (status != OK) {
            return status;
        }
        curRec = rid;
        return OK;
    }

    return status;

}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}

/**
 * Scans heap file for the next record that matches 
 * the given predicate and hands back the RID.
 * 
 * @author       Michael Feist
 * @param outRid Return parameter for RID
 * @return       OK if successful,
 *               FILEEOF if no record found,
 *               Otherwise returns status of failing method
*/
// TODO - Michael
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status status = OK;
    Record rec;
    int nextPageNo;

    // Loop over pages
    while(true) {

        status = curPage->firstRecord(curRec);
        if(status != OK) goto END;

        // Loop over records
        while(true) {

            // Check record
            status = getRecord(rec);
            if(status != OK) goto END;
            if(matchRec(rec)) {
                outRid = curRec;
                goto END;
            }

            // Move to next record
            status = curPage->nextRecord(curRec, curRec);
            if(status == ENDOFPAGE) break;
        }

        // Move to next page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if(status != OK) goto END;

        status = curPage->getNextPage(nextPageNo);
        if(status != OK) goto END;
        if(nextPageNo == -1) {
            status = FILEEOF;
            goto END;
        }
        curPageNo = nextPageNo;

        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if(status != OK) goto END;
    }
    
    END:
    markScan();
    return status;
	
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

/**
 * Insert a record into the file
 * 
 * @author       Derek Calamari
 * @param rec reference to the record to insert
 * @param outRid rid of the reference inserted
 * @return       OK if successful,
 *               INVALIDRECLEN if rec is too long,
 *               Otherwise returns status of failing method
*/
// TODO - DEREK
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (curPage == NULL) {
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            return status;
        }
    }
    // try to insert the record described by rec into curPage of file
    status = curPage->insertRecord(rec, rid);

    if (status != OK) {
        // No space to insert record on current page 

        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) {
            return status;
        }

        newPage->init(newPageNo);

        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;
        status = newPage->setNextPage(-1);
        if (status != OK) {
            return status;
        }

        status = curPage->setNextPage(newPageNo);
        if (status != OK) {
            return status;
        }

        // Unpin the currently pinned page 
    	unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (unpinstatus != OK) {
            return unpinstatus;
        }

        // insert record into new page
        newPage->insertRecord(rec, rid);
        if(status != OK){
            return status;
        }

        //update curPage to be newPage in buffer Pool
        curPage = newPage;
        curPageNo = newPageNo;
    }

    // record count incremented
    headerPage->recCnt++;

    // Set header page to dirty since we updated recCnt
    hdrDirtyFlag = true;

    // inserted record to curpage --> dirty
    curDirtyFlag = true;

    // rid of inserted record into outRid
    outRid = rid;

    //return 
    return status;
}


