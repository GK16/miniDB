#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

const Status BufMgr::allocBuf(int &frame)
{
    bool found = false;
    // scan twice
    bool scanedTwice = false;
    BufDesc* curDesc = &bufTable[clockHand];
    for(int ind = 0; ind < numBufs * 2; ind++){
        // 1. move clock
        advanceClock();

        // 2. valid set?
        if(!bufTable[clockHand].valid) break;

        // 3. check refBir set
        // 3.1. valid refBir
        if(bufTable[clockHand].refbit){
            bufStats.accesses++; //Total number of accesses to buffer pool
            bufTable[clockHand].refbit = false; // clear refBir
        } 
        // 3.2. invalid refBir
        else {
            BufDesc* curDesc = &bufTable[clockHand];
            // 3.2.1. the page pinned
            if (bufTable[clockHand].pinCnt == 0){
                File* file = bufTable[clockHand].file;
                int pageNo = bufTable[clockHand].pageNo;
                hashTable->remove(file, pageNo);
                found = true;
                break;
            }
            
        }

        if (ind == numBufs * 2 - 1) scanedTwice = true;
    }

    // 4. if the pool is full
    if(!found && scanedTwice) return BUFFEREXCEEDED;

    // 5. dirty bit?
    if(bufTable[clockHand].dirty){
        bufStats.diskwrites++;
        File* fi = bufTable[clockHand].file;
        try
        {
            Status res = fi->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
            if(res != OK) throw UNIXERR;
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
            return UNIXERR;
        }
        
    }

    frame = clockHand;
    return OK;

}

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    // check whether the page is already in the buffer pool
    Status status = OK;
    int frameNo;
    status = hashTable->lookup(file, PageNo, frameNo);
    // page is not in the buffer pool
    if (status != OK)
    {
        // allocate buffer frame
        status = allocBuf(frameNo);
        if (status != OK)
            return status; // exit if something goes wrong
        // read the page from disk to buffer frame
        status = file->readPage(PageNo, &bufPool[frameNo]);
        if (status != OK)
            return status;
        // insert the page to hashtable
        status = hashTable->insert(file, PageNo, frameNo);
        if (status != OK)
            return status;
        // set the frame properly
        bufTable[frameNo].Set(file, PageNo);
        // return a pointer to the frame containing the page
        page = &bufPool[frameNo];
    }
    // page is in the buffer pool
    else
    {
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        // return a pointer to the frame containing the page
        page = &bufPool[frameNo];
    }

    return OK;
}

const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty) {
    Status status;
    // lookup hashTable to position frame
    int frameNo = 0;
    if ((status = hashTable->lookup(file, PageNo, frameNo)) != OK) {
        // HASHNOTFOUND
        return status;
    }

    // decrement the pinCnt
    BufDesc *curBuf = &bufTable[frameNo];
    if (curBuf->pinCnt == 0) {
        return PAGENOTPINNED;
    }
    curBuf->pinCnt--;

    // set the dirty bit
    if (dirty) {
        curBuf->dirty = true;
    }
    return OK;
}

const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page) {
    Status status;
    // alloc an empty page, get pageNo
    if ((status = file->allocatePage(pageNo)) != OK) {
        return status;
    }

    // allocBuf to get frameNo
    int frameNo = 0;
    if ((status = allocBuf(frameNo)) != OK) {
        return status;
    }

    // insert into hashTable
    if ((status = hashTable->insert(file, pageNo, frameNo)) != OK) {
        return status;
    }

    // initialize the new page info
    bufTable[frameNo].Set(file, pageNo);

    // readPage with pageNo, and assign the value to page
    if ((status = file->readPage(pageNo, &bufPool[frameNo])) != OK) {
        return status;
    }

    page = &bufPool[frameNo];
    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
