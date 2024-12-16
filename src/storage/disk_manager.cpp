#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
	DiskFileMetaPage* meta_ptr = reinterpret_cast<DiskFileMetaPage *>(meta_data_);  //cast the meta_data into MetaPage
	uint32_t *eup = meta_ptr->extent_used_page_; 
	
	uint32_t extent_id = 0;
	while(eup[extent_id] == BITMAP_SIZE){
		extent_id++;
	}
	page_id_t bitmap_p_id = extent_id * (BITMAP_SIZE + 1) + 1; // +1 Disk Meta Page
	
	char bitmap[BITMAP_SIZE];
	ReadPhysicalPage(bitmap_p_id,bitmap);
	BitmapPage<PAGE_SIZE> *Bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);

	uint32_t free_page_id;
	bool success = Bitmap->AllocatePage(free_page_id);
	if(!success)
	{
		printf("error in DiskManger::AllocatePage\n");
		exit(0);
	}

	//update  Because concurrency is not considered, metadata is modified directly
	meta_ptr->num_allocated_pages_++;
	meta_ptr->extent_used_page_[extent_id]++;

	if(extent_id >= meta_ptr->num_extents_)
		meta_ptr->num_extents_++;

	//write
	WritePhysicalPage(META_PAGE_ID, meta_data_);
	WritePhysicalPage(bitmap_p_id, bitmap);
	return free_page_id + extent_id * BITMAP_SIZE;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
	DiskFileMetaPage* meta_ptr = reinterpret_cast<DiskFileMetaPage *>(meta_data_);  //cast the meta_data into MetaPage
	page_id_t bitmap_p_id = (logical_page_id) / BITMAP_SIZE  * (BITMAP_SIZE + 1) + 1;

	char bitmap[BITMAP_SIZE];
	ReadPhysicalPage(bitmap_p_id,bitmap);
	BitmapPage<PAGE_SIZE> *Bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
	
	Bitmap->DeAllocatePage(logical_page_id % BITMAP_SIZE);

	uint32_t extent_id = logical_page_id / BITMAP_SIZE;
	//update  Because concurrency is not considered, metadata is modified directly
	meta_ptr->num_allocated_pages_--;
	meta_ptr->extent_used_page_[extent_id]--;

	if(meta_ptr->extent_used_page_[extent_id] == 0)
		meta_ptr->num_extents_--;

	//write
	WritePhysicalPage(META_PAGE_ID, meta_data_);
	WritePhysicalPage(bitmap_p_id, bitmap);
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
	//get the bitmap page(physical!!)
	page_id_t bitmap_index = (logical_page_id / BITMAP_SIZE) * (BITMAP_SIZE + 1) + 1; // +1 Disk Meta Page 

	char bitmap[PAGE_SIZE];
	ReadPhysicalPage(bitmap_index, bitmap); //read the bitmap
	BitmapPage<PAGE_SIZE> *page = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap);  //transfer to BitMapPage
	return page->IsPageFree(logical_page_id % BITMAP_SIZE);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id + logical_page_id / BITMAP_SIZE + 2;  // +2
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
