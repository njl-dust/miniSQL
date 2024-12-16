#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ >= MAX_CHARS * 8)
		return false;
	
	page_allocated_++;
	page_offset = next_free_page_;
	bytes[page_offset / 8] |= (0x80 >> (page_offset % 8));  // 1000 0000 >> x 


	//update next_free_page
	if(page_allocated_ == MAX_CHARS * 8)
		return true; //no free page

	uint32_t free_index = 0;

	while(free_index < MAX_CHARS * 8 && !IsPageFree(free_index))
		free_index++;

	next_free_page_ = free_index;
	return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
	if(IsPageFree(page_offset))
		return false;  //already free

	
	bytes[page_offset / 8] &= ~(0x80 >> (page_offset % 8));  //  bytes[page_offset / 8] ^= (0x80 >> (page_offset % 8));  
	page_allocated_--;
	next_free_page_ = page_offset;
	return true;	
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (0x80 >> bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
