#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
	if(page_table_.find(page_id) == page_table_.end()) //cant find the page
	{
		//choose a frame page
		frame_id_t ffid; //free frame id
		if(free_list_.size() > 0){    //firstly  free list
			ffid = free_list_.back();   // or front()
			free_list_.pop_back();
		}else{   //no page in free_list_   find a victim in the replacer
			bool success = replacer_->Victim(&ffid);
			if(!success)       
			{
				printf("error in BufferPoolManger::FetchPage\n");
				return nullptr;
			}
		}

		//now we get a free frame page   
		//firstly, check dirty
		page_id_t pre_id = pages_[ffid].page_id_;
		if(pages_[ffid].is_dirty_)
		{  //for dirty frame_page, it should be written back
			disk_manager_->WritePage(pre_id, pages_[ffid].data_);   //(page_id, data)
		}

		pages_[ffid].is_dirty_ = false;
		pages_[ffid].ResetMemory();
		pages_[ffid].page_id_ = page_id;

		//updata the page table
		page_table_.erase(pre_id);
		page_table_[page_id] = ffid;

		//read the page to the frame-page
		disk_manager_->ReadPage(page_id, pages_[ffid].data_); 
		replacer_->Pin(ffid);
		pages_[ffid].pin_count_++; //unnecessary?
		return pages_ + ffid;
	}else{  //find the page in the buffer pool
		frame_id_t frame_id = page_table_[page_id];
		replacer_->Pin(frame_id); //PIn the page
		pages_[frame_id].pin_count_++; //unnecessary?
		return pages_+ frame_id;
	}
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
	frame_id_t ffid;
	
	if(free_list_.size() > 0){    //firstly  free list
		ffid = free_list_.back();   // or front()
		free_list_.pop_back();
	}else{   //no page in free_list_   find a victim in the replacer
		bool success = replacer_->Victim(&ffid);
		if(!success)      // all the frame page is pinned!! 
		{
			//printf("error in BufferPoolManger::FetchPage\n");
			return nullptr;
		}
	}
	//now we get a free frame page   
	//firstly, check dirty
	page_id = AllocatePage();
	
	page_id_t pre_id = pages_[ffid].page_id_;
	if(pages_[ffid].is_dirty_)
	{  //for dirty frame_page, it should be written back
		disk_manager_->WritePage(pre_id, pages_[ffid].data_);   //(page_id, data)
	}

	pages_[ffid].is_dirty_ = false;
	pages_[ffid].ResetMemory();
	pages_[ffid].page_id_ = page_id;

	//updata the page table
	page_table_.erase(pre_id);
	page_table_[page_id] = ffid;

	replacer_->Pin(ffid);
	pages_[ffid].pin_count_ = 1; //unnecessary?

	return pages_ + ffid;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(page_table_.find(page_id) != page_table_.end())
	{
		frame_id_t ffid = page_table_[page_id];
		if(pages_[ffid].pin_count_ > 0)
			return false;
		else{
			page_table_.erase(page_id);
			pages_[ffid].ResetMemory();
			pages_[ffid].is_dirty_ = false;
			DeallocatePage(page_id);

			//add the frame page to free_list and erase it from the lru-list/map!!
			replacer_->Pin(ffid);  //not necessary
			free_list_.emplace_back(ffid);
			return true;
		}
	}
	else{
		DeallocatePage(page_id);
		return true;
	}
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
	if(page_table_.find(page_id) == page_table_.end())	
		return false;
	else{
		frame_id_t ffid = page_table_[page_id];
		
		//set dirty and pin_count 
		pages_[ffid].is_dirty_ |= is_dirty;
		pages_[ffid].pin_count_--;
		if(pages_[ffid].pin_count_ == 0)
			replacer_->Unpin(ffid);   //no thread Pin the page,  the page should be put into LRU-list
		return true;
	}
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_table_.find(page_id) == page_table_.end())
	{ // the page is not in in the frame page table
		return false;
	}else{
		frame_id_t ffid = page_table_[page_id];
		disk_manager_->WritePage(page_id, pages_[ffid].data_);
		pages_[ffid].is_dirty_ = false;
		return true;
	}
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
