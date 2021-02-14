/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * Sean Engelhart (sengelhart)
 * Roshan Poduval (rpoduval)
 * Xuanting Chen(xchen)
 *
 * Implements the functionality of a buffer manager. The buffer manager moves
 * pages on disk in and out of the frames of the buffer pool, which resides in memory.
 * The buffer manager uses the clock algorithm for replacing pages. This
 * algorithm approximates the least-recently used (LRU) replacement policy by
 * cyclically scanning pages and tracking which ones were recently referenced.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/hash_already_present_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	// TODO: test this method
	for (std::uint32_t i = 0; i < numBufs; ++i) {
		// flush dirty pages
		if (bufDescTable[i].valid && bufDescTable[i].dirty) {
				bufDescTable[i].file->writePage(bufPool[i]);
		}
	}
	// deallocate BufMgr resources
	delete[] bufDescTable;
	delete[] bufPool;
	delete hashTable;
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	// TODO: test this method
	// Each iteration represents an advancement of the clock hand
	for (std::uint32_t framesPinned = 0; framesPinned < numBufs; advanceClock()) {
		if (bufDescTable[clockHand].valid) {
			if (bufDescTable[clockHand].refbit) {
				bufDescTable[clockHand].refbit = false;
				continue;
			}
			if (bufDescTable[clockHand].pinCnt > 0) {
				++framesPinned;
				continue;
			}
			if (bufDescTable[clockHand].dirty) {
				// write dirty page to disk before it's replaced
				bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			}
			// remove the hash table entry of the page that is being replaced and
			// clear this frame's info
			hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			bufDescTable[clockHand].Clear();
		}
		frame = clockHand;
		advanceClock();
		return;
	}

	// If this code is reached, all frames are pinned
	throw BufferExceededException();
}

void BufMgr::readPageInBufferPool(File* file, const PageId pageNo, Page*& page, FrameId frameToReadFrom)
{
	bufDescTable[frameToReadFrom].refbit = true;
	bufDescTable[frameToReadFrom].pinCnt += 1;
	page = &bufPool[frameToReadFrom];
}

void BufMgr::readPageNotInBufferPool(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameForPageToRead;
	allocBuf(frameForPageToRead);
	bufPool[frameForPageToRead] = file->readPage(pageNo);

	hashTable->insert(file, pageNo, frameForPageToRead);
	bufDescTable[frameForPageToRead].Set(file, pageNo);
	page = &bufPool[frameForPageToRead];
}
	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	// TODO: test this method
	FrameId frameToReadFrom;
	try 
	{
		hashTable->lookup(file, pageNo, frameToReadFrom);
		// If code reaches here, the given page is in the buffer pool
		readPageInBufferPool(file, pageNo, page, frameToReadFrom);
	}
	catch (const HashNotFoundException &e)
	{
		// given page is not in the buffer pool
		readPageNotInBufferPool(file, pageNo, page);
	}
}

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	// This method will unpin the page and set dirty with the given file and pageNo
	// If file and pageNo do not exist in bufferpool, throw PageNotPinnedException
	
	FrameId frameIdNo;
	try{
		hashTable->lookup(file, pageNo, frameIdNo);
	}
	catch(const HashNotFoundException &e){
		return;
	}
	if(bufDescTable[frameIdNo].pinCnt == 0){
		throw PageNotPinnedException("PAGENOTPINNED", bufDescTable[frameIdNo].pageNo, frameIdNo);
	}
	bufDescTable[frameIdNo].pinCnt--; // decrese the pin count
	if(dirty){
		bufDescTable[frameIdNo].dirty = true; // set dirty bit
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	// This method will allocate a new empty page in the given file
	// This method will return a newly allocated page
	
	FrameId frameIdNo;
	Page alloPage = file->allocatePage(); // give a new page
	allocBuf(frameIdNo);
	PageId alloNo = alloPage.page_number();

	hashTable->insert(file, alloNo, frameIdNo);  // insert into hashtable
	bufDescTable[frameIdNo].Set(file, alloNo); 
	
	// return both the page number and a pointer to the buffer frame
	bufPool[frameIdNo] = alloPage;
	page = &bufPool[frameIdNo];
	pageNo = alloNo;
}

void BufMgr::flushFile(const File* file) 
{
	// Should scan bufTable for pages belonging to the file.
	// For each page encountered, it should:
	// a) if the page is dirty, call file->writePage() to flush the page to disk and then set the dirty bit for the page to false
	// b) remove the page from the hashtable (dirty or not)
	// c) and invoke the Clear() method of BufDesc for the page frame
	// Throws PagePinnedException if some page of the file is pinned.
	// Throws BadBufferException if an invalid page belonging to the file is encountered.
	
	for (FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file) { // if the page belongs to the file
			if (!bufDescTable[i].valid) { // if the page is invalid throw BadBufferException
				throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
			if (bufDescTable[i].pinCnt > 0) { // if the page is pinned throw PagePinnedException
				throw PagePinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			if (bufDescTable[i].dirty) { // check if page is dirty
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo); // remove page from hashtable
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	// This method deletes a particular page from file.
	// Make sure if page being deleted is allocated a frame in the buffer pool, that frame is freed and the corresponding entry in the hash table is removed.
	// Delete page from file.
	FrameId frameToClear;
	try { // if
		hashTable->lookup(file, PageNo, frameToClear); // page being deleted is allocated a frame in the buffer pool, then
		hashTable->remove(file, PageNo); // remove entry from hashtable
		bufDescTable[frameToClear].Clear(); // free page
	} catch (HashNotFoundException &e) { // else 
		// Do nothing
	}
	file->deletePage(PageNo); // delete page from file
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
