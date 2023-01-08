#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
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
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
        status = db.createFile(fileName);
        if (status != OK){return status;}
        status = db.openFile(fileName, file);
        if (status != OK){return status;}
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK){return status;}
        // hdrPageNo, newPage is the new empty header page

        // Take the Page* pointer returned from allocPage() and cast it to a FileHdrPage*
        // Using this pointer initialize the values in the header page.
        hdrPage = reinterpret_cast<FileHdrPage *>(newPage);
        std::strcpy( hdrPage->fileName, fileName.c_str()); // strcpy is for c-style string
        // The strcpy() function copies the string pointed to by src, including the terminating null byte ('\0'), to the buffer pointed to by dest.
        hdrPage->recCnt += 1;
        hdrPage->pageCnt += 1;

        // Create first data page of the file
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK){return status;}

        // initialize the page contents
        newPage->init(newPageNo);
        // store the page number of the data page in firstPage and lastPage attributes
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;

        // unpin both pages and mark them as dirty
        status = bufMgr->unPinPage(file, hdrPageNo, "true");
        if (status != OK){return status;}
        status = bufMgr->unPinPage(file, newPageNo, "true");
        if (status != OK){return status;}

        // close the file
        status = db.closeFile(file);
        if (status != OK){return status;}
        return status;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // read the header page number
        if ((status = filePtr->getFirstPage(headerPageNo)) != OK) {
            db.closeFile(filePtr);
            returnStatus = status;
            return;
        }

        // read head page from buffer
        if ((status = bufMgr->readPage(filePtr, headerPageNo, pagePtr)) != OK) {
            db.closeFile(filePtr);
            returnStatus = status;
            return;
        }

        // update headerPage & curPageNo
        headerPage = (FileHdrPage *) pagePtr;
        curPageNo = headerPage->firstPage;
        hdrDirtyFlag = true;

        // read first page from buffer
        if ((status = bufMgr->readPage(filePtr, curPageNo, curPage)) != OK) {
            db.closeFile(filePtr);
            returnStatus = status;
            return;
        }
        curDirtyFlag = true;

        curRec = NULLRID;
        returnStatus = OK;
    } else {
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

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    if (curPageNo != rid.pageNo) {
        // not the current page, unpin
        if ((status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK) {
            return status;
        }

        // get currentPage with rid
        if ((status = bufMgr->readPage(filePtr, rid.pageNo, curPage)) != OK) {
            return status;
        }
        curPageNo = rid.pageNo;
    }

    // get current record from current page
    if ((status = curPage->getRecord(rid, rec)) != OK) {
        return status;
    }
    curDirtyFlag = true;
    curRec = rid;
    return OK;
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


const Status HeapFileScan::scanNext(RID& outRid)
// Returns (via the outRid parameter) the RID of the next record that satisfies the scan predicate.
// scan the file one page at a time
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    // 1. if at the end of the file
    if(curPageNo < 0)
    {
        curRec.pageNo = -1;
        curRec.slotNo = -1;
        return FILEEOF;
    }

    // 2. if curPage is NULL
    if (curPage == NULL)
    {
        // 2.1. take 1st page as the curPage
        curPageNo = headerPage->firstPage;
        // 2.1.1. if the 1st page is empty
        if(curPageNo < 0) return FILEEOF;
        // 2.1.2. read the 1st page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        // 2.1.3. reset flag & curRec
        curDirtyFlag = false;
        curRec.pageNo = -1;
        curRec.slotNo = -1;
        // 2.1.4. handle error
        if (status != OK) return status;

        // 2.2. get the 1st record from the new page
        status  = curPage->firstRecord(tmpRid);
        curRec = tmpRid;
        // 2.3. if no record in the new page
        if (status == NORECORDS)
        {
            // 2.3.1. unpin the new page
            status = bufMgr->unPinPage(filePtr, curPageNo,curDirtyFlag);
            if (status != OK) return status;
            // 2.3.2 reset curPage
            curPageNo = -1;
            curPage = NULL;
            // 2.3.3. return error
            return FILEEOF;
        }
        // 2.4. handle other errors
        if (status != NORECORDS && status != OK) return status;

        // 2.5. get the record
        status = curPage->getRecord(tmpRid, rec);
        // 2.5.1. handle error
        if (status != OK) return status;

        // 2.6 match the record with given predicate
        bool matched = this->matchRec(rec);
        // 2.6.2. if matched, return tmpRid as outRid
        if (matched)
        {
            outRid = tmpRid;
            return OK;
        }
        // 2.6.2. did not match, then visit next record
    }

    // 3. look for the next record matched
    while (true)
    {
        status  = curPage->nextRecord(curRec, nextRid);
        if (status == ENDOFPAGE)
        {
            // 3.1. find the next non-empty page
            while (status != OK)
            {
                // 3.1.1. get next page number
                status = curPage->getNextPage(nextPageNo);
                // if run out of page
                if (nextPageNo == -1) return FILEEOF;

                // 3.1.2. reset curPage
                status = bufMgr->unPinPage(filePtr,curPageNo, curDirtyFlag);
                curPage = NULL;
                curPageNo = -1;
                curDirtyFlag = false;
                if (status != OK) return status;

                // 3.1.3. move to read the next page
                curPageNo = nextPageNo;
                status = bufMgr->readPage(filePtr,curPageNo,curPage);
                if (status != OK) return status;

                // 3.1.4. try to get the first record from the new page
                status  = curPage->firstRecord(curRec);
            }
        }

        // 3.2. get next record
        else
        {
            curRec = nextRid;
        }

        // now we have a valid record RID in curRec

        // 3.3. match with the predicate
        // 3.3.1. fetch Record itself
        status = curPage->getRecord(curRec, rec);
        if (status != OK) return status;
        bool matched = matchRec(rec);
        // 3.3.2. if not matched, go to next loop
        if (!matched) continue;

        // 3.4 return RID to outRid
        outRid = curRec;
        return OK;
    }
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
    // Heapfile constructor will read the header page and the first
    // data page of the file into the buffer pool
    // if the first data page of the file is not the last data page of the file
    // unpin the current page and read the last page
    if ((curPage != NULL) && (curPageNo != headerPage->lastPage))
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) cerr << "error in unpin of data page\n";
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) cerr << "error in readPage \n";
        curDirtyFlag = false;
        }
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

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus, rollbackStatus;
    RID		rid;

    // 1. check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // 2. check if curPage is NULL
    if (curPage == NULL)
    {
        // 2.1. make the last page the current page
        curPageNo = headerPage->lastPage;
        // 2.2. read it into the buffer.
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
    }

    // 3. Call curPage->insertRecord to insert the record
    status = curPage->insertRecord(rec, rid);
    // 3.1. If successful, DO THE BOOKKEEPING.
    if (status == OK)
    {
        outRid = rid;
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
        return status;
    }

    // 4. If can't insert into the current page
    // 4.1. create a new page
    status = bufMgr->allocPage(filePtr, newPageNo, newPage);
    if (status != OK) return status;

    // 4.2. initialize the new page
    newPage->init(newPageNo);
    status = newPage->setNextPage(-1); // no next page
    if (status != OK) return status;

    // 4.3. DO THE BOOKKEEPING in header page
    headerPage->lastPage = newPageNo;
    headerPage->pageCnt++;
    hdrDirtyFlag = true;

    // 4.4. link up the new page
    status = curPage->setNextPage(newPageNo);  // set forward pointer
    if (status != OK) return status;

    // 4.4. unPin the old page
    unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, true);
    if (unpinstatus != OK) {
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;

        // unpin the new page
        rollbackStatus = bufMgr->unPinPage(filePtr, newPageNo, true);
        if (rollbackStatus != OK)
        {
            cout << "After unPinning the old page failed, rollback failed." << endl;
            return rollbackStatus;
        }
        return unpinstatus;
    }

    // 4.5. make the current page to be the newly allocated page
    curPage = newPage;
    curPageNo = newPageNo;

    // 4.6. try to insert the record
    status = curPage->insertRecord(rec, rid);
    if (status != OK)
    {
        cout << "insertRecord failed after creating a new page." << endl;
        return status;
    };

    // 4.7. DO THE BOOKKEEPING
    headerPage->recCnt++;
    curDirtyFlag = true;
    hdrDirtyFlag = true;

    // 4.8. return the rid
    outRid = rid;
    return status;

  
  
  
  
  
  
  
}


