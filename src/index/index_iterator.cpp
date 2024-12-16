#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) 
{
  if(INVALID_PAGE_ID != current_page_id)
		page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
	else
		page = nullptr;
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  //ASSERT(false, "Not implemented yet.");
	return {page->KeyAt(item_index), page->ValueAt(item_index)};
}

int count = 10;
IndexIterator &IndexIterator::operator++() {
	count--;
  //ASSERT(false, "Not implemented yet.");
	int num_index = page->GetSize();
	if(item_index + 1 < num_index)
	{
		item_index++;
		return *this;
	}

	//try to move to next leaf
	page_id_t now_page_id = current_page_id;
	page_id_t parent_id = page->GetParentPageId();

	while(1)
	{
		if(parent_id == INVALID_PAGE_ID)  // no next ele, set *this to end()
			break;

		auto parent_page = buffer_pool_manager->FetchPage(parent_id);
		auto parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());

		int now_index = parent_node->ValueIndex(now_page_id);
		int parent_num_index = parent_node->GetSize();

		if(now_index + 1 < parent_num_index)  //find a sibling
		{
			buffer_pool_manager->UnpinPage(current_page_id, true); //unpin 
			current_page_id = parent_node->ValueAt(now_index+1); //switch the root_page_id
			buffer_pool_manager->UnpinPage(parent_id, false);  //unpin

			while(1)
			{  //get current page
				auto cur_page = buffer_pool_manager->FetchPage(current_page_id);
				auto cur_node = reinterpret_cast<BPlusTreePage*>(cur_page->GetData());
				if(cur_node->IsLeafPage())
				{
					page = reinterpret_cast<LeafPage*>(cur_node);
					item_index = 0; //don't unpin!!  ~iterator will unpin it
					return *this;
				}

				page_id_t leftmost_page_id = reinterpret_cast<InternalPage*>(cur_node)->ValueAt(0);
				buffer_pool_manager->UnpinPage(current_page_id, false);
				current_page_id = leftmost_page_id;
			}
		}

		//parent 
		now_page_id = parent_id;
		parent_id = parent_node->GetParentPageId();
	
		buffer_pool_manager->UnpinPage(now_page_id, false);
	}

	//can not find next ele!!  set as end()
	buffer_pool_manager->UnpinPage(current_page_id, true);
	item_index = -1;
	current_page_id = INVALID_PAGE_ID;
	page = nullptr;
	return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}
