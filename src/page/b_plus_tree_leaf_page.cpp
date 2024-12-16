#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
	SetPageId(page_id);
	SetParentPageId(parent_id);
	SetKeySize(key_size);
	SetMaxSize(max_size);

	SetPageType(IndexPageType::LEAF_PAGE);
	SetSize(0);
	SetNextPageId(INVALID_PAGE_ID);
}



/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
 	int size = GetSize();
	int left = 0, right = size, mid = size / 2;  // [left, right)
	while(1)
	{
		if(left == right) // [left, right)
			return right;  

		GenericKey *mid_key = KeyAt(mid);
		int equal = KM.CompareKeys(mid_key, key);   //compare 

		if(equal == 0)  //find it
			return mid;
		// 0 1 2 3 4 (5)
		if(equal < 0)   //mid_key < key
		{
			left = mid + 1;
			mid = (left + right) / 2;
		}else{
			right = mid;  //[left, right)
			mid = (left + right) / 2;
		}
	}

	ASSERT(0, "never reach here!\n");
	return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    //return make_pair(nullptr, RowId());
  GenericKey* key = reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
  RowId row_id(*(reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off)));  //may wrong   int64*

	return make_pair(key, row_id);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int size = GetSize(), index = size;
	
	while(index > 0)
	{
		GenericKey *key_tmp = KeyAt(index-1);

		int equal = KM.CompareKeys(key_tmp, key);
		if(equal == 0)
		{
			ASSERT(0, "equal key? not implement yet\n");
			return GetSize();     //break when find the old_value
		}

		if(equal < 0) //key_tmp < key
			break;
		//key_tmp > key
		//move the pair at index-1 to index
		RowId row_id = ValueAt(index-1);
		SetValueAt(index, row_id);
		SetKeyAt(index, key_tmp);

		index--;
	}

	SetValueAt(index, value);
	SetKeyAt(index, key);
	SetSize(size + 1);

	return size+1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
	int size = (GetSize()) / 2;
	
	recipient->CopyNFrom(pairs_off + size * pair_size, GetSize() - size);
	SetSize(size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
	int cur_size = GetSize();
	for(int i = 0; i < size ; i++)
	{
		RowId row_id = *(reinterpret_cast<RowId*>(reinterpret_cast<char*>(src) + i * pair_size + val_off));
		SetValueAt(cur_size + i, row_id);
		
		GenericKey *g_key = reinterpret_cast<GenericKey*>(reinterpret_cast<char*>(src) + i * pair_size + key_off);
		SetKeyAt(cur_size + i, g_key);
	}

	IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int size = GetSize();
	int left = 0, right = size, mid = size / 2;  // [left, right)
	while(1)
	{
		if(left == right) // [left, right)
			return false;  

		GenericKey *mid_key = KeyAt(mid);
		int equal = KM.CompareKeys(mid_key, key);   //compare 

		if(equal == 0)  //find it
			break;
		// 0 1 2 3 4 (5)
		if(equal < 0)   //mid_key < key
		{
			left = mid + 1;
			mid = (left + right) / 2;
		}else{
			right = mid;  //[left, right)
			mid = (left + right) / 2;
		}
	}

	value = ValueAt(mid);
	
	return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  RowId row_id;   // INVALID_ROWID
	if(Lookup(key, row_id, KM) == false)   //speed up the search by Lookup
		return -1;

	int size = GetSize(), index = 0;
	while(1)
	{
		GenericKey *key_tmp = KeyAt(index);

		int equal = KM.CompareKeys(key_tmp, key);
		if(equal == 0)
			break;
		
		index++;
	}
	
	while(index+1 < size)
	{
		GenericKey *key_next = KeyAt(index+1);
		RowId row_id_next = ValueAt(index+1);

		SetKeyAt(index, key_next);
		SetValueAt(index, row_id_next);

		index++;
	}

	SetSize(size-1);
	return size-1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
	int size = GetSize();
	recipient->SetNextPageId(GetNextPageId());
	recipient->CopyNFrom(pairs_off, size);
	SetSize(0);
 //TODO:  sibling????	
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
	recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
	
	for(int index = 0; index+1 < GetSize(); index++)
	{
		GenericKey *key_next = KeyAt(index+1);
		RowId row_id_next = ValueAt(index+1);

		SetKeyAt(index, key_next);
		SetValueAt(index, row_id_next);
	}
	IncreaseSize(-1);	
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
	int size = GetSize();
	
	SetValueAt(size, value);
	SetKeyAt(size, key);

	IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
	recipient->CopyFirstFrom(KeyAt(GetSize()-1), ValueAt(GetSize()-1));
	IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {	
	for(int index = GetSize(); index > 0; index--)
	{
		GenericKey *key_next = KeyAt(index-1);
		RowId row_id_next = ValueAt(index-1);

		SetKeyAt(index, key_next);
		SetValueAt(index, row_id_next);
	}

	SetKeyAt(0, key);
	SetValueAt(0, value);
	IncreaseSize(1);	
}
