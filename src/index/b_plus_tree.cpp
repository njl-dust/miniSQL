#include "index/b_plus_tree.h"
#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) 
{  //get Page 1(it contains data about all index)
	auto index_root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
	auto irp_data = reinterpret_cast<IndexRootsPage*>(index_root_page->GetData());
	
	root_page_id_ = INVALID_PAGE_ID;	
	bool success = irp_data->GetRootId(index_id, &root_page_id_);
	if(!success)
		root_page_id_ = INVALID_PAGE_ID;	

	buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

//TODO:: how to destroy a BP tree?

void BPlusTree::Destroy_Page(page_id_t page_id)
{
	auto page = buffer_pool_manager_->FetchPage(page_id);
	auto page_node = reinterpret_cast<BPlusTreePage*>(page->GetData());
	if(!(page_node->IsLeafPage()))
	{
		int size = page_node->GetSize(); 
		for(int i = 0;i < size; i++)
			Destroy_Page(reinterpret_cast<InternalPage*>(page_node)->ValueAt(i));
	}

	//delete the page 
	buffer_pool_manager_->UnpinPage(page_id, false);
	buffer_pool_manager_->DeletePage(page_id);

}

void BPlusTree::Destroy() {
	//destroy the page 
	if(INVALID_PAGE_ID != root_page_id_)
	{  
		Destroy_Page(root_page_id_);	

		//change the data in indexRootPage-----delete the index_id	
		auto index_root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
		auto irp_data = reinterpret_cast<IndexRootsPage*>(index_root_page->GetData());

		bool success = irp_data->Delete(index_id_);
		if(!success)
			cout << "fail to destroy" << endl;
	
		buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
	}
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return INVALID_PAGE_ID == root_page_id_;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
	Page*  l_page = FindLeafPage(key, root_page_id_);

	if(nullptr == l_page)
	{
		cout << "failed to find the leaf page in nBPlusTree::GetValue" << endl;
		return false;
	}


	auto leaf_page = reinterpret_cast<LeafPage*>(l_page->GetData());
	RowId row_id;
	bool success = leaf_page->Lookup(key, row_id, processor_);
	
	buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);
	
	if(success){
		result.push_back(row_id);
		return true;
	}else	
		return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
	if(IsEmpty())
	{ // the tree is empty now
		StartNewTree(key, value);
		return true;
	}else
		return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
	auto page = buffer_pool_manager_->NewPage(root_page_id_);
	if(nullptr == page)
	{
		cout << "oom in BPT::StartNewTree" << endl;
		return;
	}

	//we should insert the {index_id_,root_page_id_} into the IndexRootPage
	UpdateRootPageId(true);

	
	auto root_page = reinterpret_cast<LeafPage*>(page->GetData()); // now the root page is a leaf page
	root_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(),leaf_max_size_);
	root_page->Insert(key, value, processor_);

	buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
	auto page = FindLeafPage(key, root_page_id_); //find the leaf page that key should be inserted to
	
	auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());	
	
	RowId tmp;   //we need a tmp RowId, because Lookup will write the input_param if it find the key which conflict with "const RowId"
	if(leaf_page->Lookup(key, tmp, processor_))
	{ //the key is already in the page
		buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
		return false;
	}

	if(leaf_page->GetSize() < leaf_page->GetMaxSize())
	{
		leaf_page->Insert(key, value, processor_);
		buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
		return true;
	}else{  //split!!
		page_id_t sib_id = Split(leaf_page, transaction);
		auto page2 = buffer_pool_manager_->FetchPage(sib_id);
		auto sib_page = reinterpret_cast<LeafPage*>(page2->GetData());
	
		//link
		sib_page->SetNextPageId(leaf_page->GetNextPageId());
		leaf_page->SetNextPageId(sib_page->GetPageId());

		//insert the key
		if(processor_.CompareKeys(key, sib_page->KeyAt(0)) >= 0)
		{
			sib_page->MoveFirstToEndOf(leaf_page);
			sib_page->Insert(key, value, processor_);
		}
		else{
			leaf_page->Insert(key, value, processor_);
		}

		
		//insert into parent page
		/*
		Row tmp;
		processor_.DeserializeToKey(key, tmp, processor_.key_schema_);
		cout << tmp.GetField(0)->value_.integer_ << endl;

		processor_.DeserializeToKey(sib_page->KeyAt(0), tmp, processor_.key_schema_);
		cout << tmp.GetField(0)->value_.integer_ << endl;
		*/
		InsertIntoParent(leaf_page, sib_page->KeyAt(0), sib_page, transaction);

		buffer_pool_manager_->UnpinPage(page2->GetPageId(), true);
		buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
		return true;
	}
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */

// split will new a page (which will pin it)
page_id_t BPlusTree::Split(InternalPage *node, Transaction *transaction) {
	page_id_t page_id;
	auto page = buffer_pool_manager_->NewPage(page_id); 
	if(nullptr == page)
	{
		cout << "oom in BPT::Split" << endl;
		buffer_pool_manager_->UnpinPage(page_id, true);
		return INVALID_PAGE_ID;
	}	

		
	auto internal_page = reinterpret_cast<InternalPage*>(page->GetData());
	internal_page->Init(page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
	node->MoveHalfTo(internal_page, buffer_pool_manager_);
	
	buffer_pool_manager_->UnpinPage(page_id, true);
	return page_id;
}

page_id_t BPlusTree::Split(LeafPage *node, Transaction *transaction) {
	page_id_t page_id;
	auto page = buffer_pool_manager_->NewPage(page_id); 
	if(nullptr == page)
	{
		cout << "oom in BPT::Split" << endl;
		buffer_pool_manager_->UnpinPage(page_id, true);
		return INVALID_PAGE_ID;
	}	

	
	auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
	leaf_page->Init(page_id, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
	node->MoveHalfTo(leaf_page);
	
	buffer_pool_manager_->UnpinPage(page_id, true);
	return page_id;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
	static int count_ = 0;
	count_++;
	if (old_node->IsRootPage()) {
    auto page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) {
			cout << "oom in BPT::InsertIntoParent" << endl;
			buffer_pool_manager_->UnpinPage(root_page_id_, false);
			return;
		}

    auto root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId()); //set the root_page
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(false); //update
    
		buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {  //old page is not root
    page_id_t parent_page_id = old_node->GetParentPageId();
    auto page = buffer_pool_manager_->FetchPage(parent_page_id);  //pinned
    if (page == nullptr) {
			cout << "fail to fetch in BPT::InsertIntoParent" << endl;
			buffer_pool_manager_->UnpinPage(parent_page_id, false);
			return;
    }
    auto parent_page = reinterpret_cast<InternalPage*>(page->GetData());
    if (parent_page->GetSize() < parent_page->GetMaxSize()) {  //insert the key value into parent (key == minimal-ele of new_page,  value == new_page_id)

			parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(parent_page->GetPageId());

			buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    } else {
      //split parent_page
      page_id_t sib_id = Split(parent_page, transaction);// if max_size = 5, parent_size = 2, sibling_size = 3
			auto page2 = buffer_pool_manager_->FetchPage(sib_id);

			auto sibling_page = reinterpret_cast<InternalPage*>(page2->GetData());
			int equal = processor_.CompareKeys(key, sibling_page->KeyAt(0));
      if (equal < 0) { //insert the new node 
        new_node->SetParentPageId(parent_page->GetPageId());
        parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      } else if (equal == 0) {
				cout << "equal key??" << endl;
        new_node->SetParentPageId(sibling_page->GetPageId());
        sibling_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      } else {
        new_node->SetParentPageId(sibling_page->GetPageId());
        sibling_page->MoveFirstToEndOf(parent_page, sibling_page->KeyAt(0), buffer_pool_manager_);
				sibling_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      }

      //      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
      InsertIntoParent(parent_page, sibling_page->KeyAt(0), sibling_page);

      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
	if(IsEmpty())
		return;
	auto page = FindLeafPage(key, root_page_id_);

	auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
	int count = leaf_page->GetSize();
	//cout <<"leaf_page" <<  leaf_page->GetPageId() << endl;
	int new_count = leaf_page->RemoveAndDeleteRecord(key, processor_);
	//cout << "insert" << endl; 	
	if(new_count == -1)
	{
		cout << "can not find the key" << endl;
		return;
	}

	if(count == new_count)
	{
		buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
		cout << "failed to delete" << endl;
		return ;
	}else
	{
		//fisrtly  update the keys of parent page
		int key_index = leaf_page->KeyIndex(key, processor_);
		if(0 == key_index)   //if the deleted key is not Key0, not need to update
			UpdateParentKey(leaf_page);

		bool to_delete = false;
		if(leaf_page->GetSize() < leaf_page->GetMinSize() && !leaf_page->IsRootPage()){
			to_delete = CoalesceOrRedistribute(leaf_page, transaction);
		}
		
		buffer_pool_manager_->UnpinPage(page->GetPageId(), true);  //note  Unpin the page before delete it!!

		if(to_delete)
			buffer_pool_manager_->DeletePage(page->GetPageId());
		return;
	}
}


//added function-----update the key of parent page
template <typename N>
void BPlusTree::UpdateParentKey(N *node) {
  if (node->IsRootPage()) 
		return;
  
	auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());

  auto parent = reinterpret_cast<InternalPage*>(parent_page->GetData());
  int update_index = parent->ValueIndex(node->GetPageId());
  parent->SetKeyAt(update_index, node->KeyAt(0));
  
	if (update_index == 0) {
    UpdateParentKey(parent);
  }
  
	buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}


/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *node, Transaction *transaction) {
	if(node->IsRootPage())  //root
		return AdjustRoot(node);

	if(node->GetSize() >= node->GetMinSize())
		return false;

	auto p_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
	
	//from parent page, get the page_id of sibling
	auto parent_node = reinterpret_cast<InternalPage*>(p_page->GetData());
	int node_index = parent_node->ValueIndex(node->GetPageId());
	int sib_index;
	if(node_index == 0)
		sib_index = 1;
	else
		sib_index = node_index-1;
	page_id_t sib_page_id = parent_node->ValueAt(sib_index);

	auto sib_page = buffer_pool_manager_->FetchPage(sib_page_id);

	auto sib_node = reinterpret_cast<N*>(sib_page->GetData());
	bool to_delete = false;
	bool parent_to_delete = false;
	if((sib_node->GetSize() + node->GetSize()) <= node->GetMaxSize())
	{ //merge 
		if(0 == node_index)
		{ // node <- sib
			to_delete = false;
			parent_to_delete = Coalesce(node, sib_node, parent_node, 1, transaction);
		}
		else
		{ //sib <- node
			to_delete = true;
			parent_to_delete = Coalesce(sib_node, node, parent_node ,node_index, transaction);
		}
		buffer_pool_manager_->UnpinPage(sib_page->GetPageId(), true);
		buffer_pool_manager_->UnpinPage(p_page->GetPageId(), true);

		if(!to_delete) //delete sib_node
			buffer_pool_manager_->DeletePage(sib_page->GetPageId());
		if(parent_to_delete)
			buffer_pool_manager_->DeletePage(p_page->GetPageId());
		return to_delete;
	}
	else{
		buffer_pool_manager_->UnpinPage(p_page->GetPageId(), true);

		//we dont need parent now, so Unpin it!!
		Redistribute(sib_node, node, node_index);
		
		buffer_pool_manager_->UnpinPage(sib_page->GetPageId(), true);
		
		if(false != to_delete)
			cout << "error in CoalesceOrRedistribute" << endl;
		return to_delete;  //false
	}	
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(InternalPage *neighbor_node, InternalPage *node, InternalPage *parent, int index,
                         Transaction *transaction) {
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);  //don't forget to get middle_key!
	parent->Remove(index);  
	
	bool to_delete = false;
  if (parent->GetSize() < parent->GetMinSize()) {
		to_delete = CoalesceOrRedistribute(parent, transaction);  //coalesce of redistribute the parent page
  }
  return to_delete;
}

bool BPlusTree::Coalesce(LeafPage *neighbor_node, LeafPage *node, InternalPage *parent, int index,
                         Transaction *transaction) {
  node->MoveAllTo(neighbor_node); 
	parent->Remove(index);  
	
	bool to_delete = false;
  if (parent->GetSize() < parent->GetMinSize()) {
		to_delete = CoalesceOrRedistribute(parent, transaction);  //coalesce of redistribute the parent page
  }
  return to_delete;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  auto parent_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);

  auto parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  if (index == 0) { // node <- sib
    neighbor_node->MoveFirstToEndOf(node);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
  } else { // sib -> node
    neighbor_node->MoveLastToFrontOf(node);
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
	auto parent_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_id);

  auto parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  if (index == 0) { // node <- sib
    auto key = parent_node->KeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()));
    neighbor_node->MoveFirstToEndOf(node, key, buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
  } else { // sib -> node
    //auto key = parent_node->KeyAt(parent_node->ValueIndex(node->GetPageId()));
    auto key = neighbor_node->KeyAt(neighbor_node->GetSize()-1);
		neighbor_node->MoveLastToFrontOf(node, key, buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  //case 1  root is leaf_page
	if(old_root_node->IsLeafPage())
	{
		if(old_root_node->GetSize() == 0)
		{  //no element in the index_B tree
			root_page_id_ = INVALID_PAGE_ID;
			UpdateRootPageId(false);
			return true;
		}
		return false;
	}

	//case 2 root is internal page
  if (old_root_node->GetSize() == 1) {
    auto root_node = reinterpret_cast<InternalPage *>(old_root_node);
    //set new root
		root_page_id_ = root_node->ValueAt(0);
    UpdateRootPageId(false);
    auto new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
	return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
	page_id_t cur_page_id = root_page_id_;
	while(1)
	{
		auto cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
		auto cur_node = reinterpret_cast<BPlusTreePage*>(cur_page->GetData());

		if(cur_node->IsLeafPage())
		{
			buffer_pool_manager_->UnpinPage(cur_page_id, false);
			return IndexIterator(cur_page_id, buffer_pool_manager_); //default index = 0
		}

		page_id_t next_page_id = reinterpret_cast<InternalPage*>(cur_node)->ValueAt(0);
		buffer_pool_manager_->UnpinPage(cur_page_id, false);
		cur_page_id = next_page_id;
	}
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
	Page *page = FindLeafPage(key);
  
	LeafPage *leaf_page =  reinterpret_cast<LeafPage *>(page->GetData());
  int index = leaf_page->KeyIndex(key, processor_);
	int page_id = page->GetPageId();

	buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, -1); //INVALID_PAGE_ID, item_index:-1 , page:nullptr 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(INVALID_PAGE_ID == page_id)
		return nullptr;
	
	auto page = buffer_pool_manager_->FetchPage(page_id);  //fetch root page
	while(1)
	{
		auto internal_page = reinterpret_cast<InternalPage*>(page->GetData()); //internal page
		if(internal_page->IsLeafPage()){
			break;
		}
		
		if(leftMost)
			page_id = internal_page->ValueAt(0);
		else
			page_id = internal_page->Lookup(key, processor_);

		buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

		page = buffer_pool_manager_->FetchPage(page_id);
	}

	return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
	auto index_root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
	auto irp_data = reinterpret_cast<IndexRootsPage*>(index_root_page->GetData());
	
	if(insert_record)
		irp_data->Insert(index_id_, root_page_id_);
	else
		irp_data->Update(index_id_, root_page_id_);
	
	buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false); 
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
			//out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
			//NOTE:: if you want to see the key's value of leaf instead of the row data of key, use the following code
			//and you should make the KeyManager's key_schema_, Field's value as public
      Row tmp;
			processor_.DeserializeToKey(leaf->KeyAt(i), tmp, processor_.key_schema_);
			out << "<TD>" << tmp.GetField(0)->value_.integer_ << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
				//NOTE:: if you want to see the key's value of leaf instead of the row data of key, use the following code
				//and you should make the KeyManager's key_schema_, Field's value as public
				Row tmp;
				processor_.DeserializeToKey(inner->KeyAt(i), tmp, processor_.key_schema_);
				out <<tmp.GetField(0)->value_.integer_;

        //out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    //std::cout << std::endl;
    //std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    //std::cout << std::endl;
    //std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
