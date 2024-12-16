#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
	SetPageId(page_id);
	SetParentPageId(parent_id);
	SetKeySize(key_size);
	SetMaxSize(max_size);
	//set current size  and type
	SetSize(0);
	SetPageType(IndexPageType::INTERNAL_PAGE);
}


/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}
		
void* InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int size = GetSize();
	int left = 0, right = size, mid = size / 2;  // (left, right)
	while(1)
	{
		if(left+1 == right) // (a, a+1)
			return ValueAt(left);
		
		GenericKey *mid_key = KeyAt(mid);
		int equal = KM.CompareKeys(mid_key, key);   //compare 

		if(equal == 0)  //find it
			return ValueAt(mid);
		// (0) 1 2 3 4 (5)
		if(equal < 0)   //mid_key < key
		{
			left = mid;
			mid = (left + right) / 2;
		}else{
			right = mid;  //(left, right)
			mid = (left + right) / 2;
		}
	}
	//cant find id
	return INVALID_PAGE_ID;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
	SetValueAt(0, old_value);  //the first pair has no Key!!
	SetKeyAt(1, new_key);
	SetValueAt(1, new_value);
		
	SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
	int size = GetSize(), index = size;
	
	while(index > 0)
	{
		page_id_t page_id = ValueAt(index-1);
		if(page_id == old_value)
			break;     //break when find the old_value

		//move the pair at index-1 to index
		GenericKey *key = KeyAt(index-1);
		SetValueAt(index, page_id);
		SetKeyAt(index, key);

		index--;
	}

	//value[index-1] == old_value
	SetValueAt(index, new_value);
	SetKeyAt(index, new_key);
	SetSize(size + 1);
	return 0;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {  //left_more = 1 : left node has more element
	int size = (GetSize()) / 2;
	
	recipient->CopyNFrom(pairs_off + size * pair_size, GetSize() - size, buffer_pool_manager);
	SetSize(size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
	int cur_size = GetSize();
	for(int i = 0; i < size ; i++)
	{
		page_id_t p_id = *(reinterpret_cast<page_id_t*>(reinterpret_cast<char*>(src) + i * pair_size + val_off));
		SetValueAt(cur_size + i, p_id);
		
		GenericKey *g_key = reinterpret_cast<GenericKey*>(reinterpret_cast<char*>(src) + i * pair_size + key_off);
		SetKeyAt(cur_size + i, g_key);
		
		//change the parent_id of the son_page
		auto page = buffer_pool_manager->FetchPage(p_id);   //frame page  contains the data of a page
		page->WLatch();

		//TODO: GetData()?
		BPlusTreePage *bt_page = reinterpret_cast<BPlusTreePage*>(page->GetData());  //page
		bt_page->SetParentPageId(GetPageId());
		
		page->WUnlatch();
		buffer_pool_manager->UnpinPage(p_id, true);
	}

	IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
	int size = GetSize();
	for(int i = index+1; i < size; i++)   //if size-1 == index, just ignore the last pair
	{  //move pair i to i-1
		page_id_t page_id = ValueAt(i);
		GenericKey *key = KeyAt(i);
		
		SetValueAt(i-1, page_id);
		SetKeyAt(i-1, key);
	}

	SetSize(size-1);
	//TODO :: free the page
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  page_id_t page_id = ValueAt(0);
	Remove(0);
	return page_id;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
	//middle_key is from parent-page
	SetKeyAt(0, middle_key);
	recipient->CopyNFrom(pairs_off, GetSize(), buffer_pool_manager);
	SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
	recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
	Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
	int cur_size = GetSize();

	SetValueAt(cur_size, value);
	SetKeyAt(cur_size, key);

	//update parent_id
	auto page = buffer_pool_manager->FetchPage(value);
	page->WLatch();

	BPlusTreePage* bt_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
	bt_page->SetParentPageId(this->GetPageId());

	page->WUnlatch();
	buffer_pool_manager->UnpinPage(value, true);

	SetSize(cur_size + 1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
	recipient->CopyFirstFrom(ValueAt(GetSize()-1), buffer_pool_manager);
	recipient->SetKeyAt(0, middle_key);
	Remove(GetSize()-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
	//since the entry is added at the beginning, we don't need key
	int index = GetSize();

	while(index != 0)
	{
		//move the pair at index-1 to index
		page_id_t page_id = ValueAt(index-1);
		GenericKey *key = KeyAt(index-1);
		
		SetValueAt(index, page_id);
		SetKeyAt(index, key);

		index--;
	}

	//set the first entry
	SetValueAt(0, value);
	
	//update the son-page
	auto page = buffer_pool_manager->FetchPage(value);
	page->WLatch();

	BPlusTreePage *bt_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
	bt_page->SetParentPageId(GetPageId());

	page->WUnlatch();
	buffer_pool_manager->UnpinPage(value, true);

	IncreaseSize(1);
}
