#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  //search the TableHeap find a TablePage to insert the tuple
	auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(GetFirstPageId())); //start from the root of TableHeap
	page_id_t page_id;
	bool success;

	while(1)
	{
		if(page == nullptr)
			return false;	
		
		//try to insert
		page->WLatch();
		success = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
		page->WUnlatch();
		
		if(success)  //finish
		{
			buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);

			return true;
		}
		
		buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
		//try next page
		page_id = page->GetNextPageId(); 
		
		if(page_id == INVALID_PAGE_ID) //-1
			break;
		page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
	}
	
	//get a new page to insert the tuple
	//insert the new page to page->next
	page_id_t new_page_id;
	page_id = page->GetTablePageId();
	auto new_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(new_page_id));
	page->WLatch();
	page->SetNextPageId(new_page_id);
	page->WUnlatch();
	buffer_pool_manager_->UnpinPage(page_id, true);

	new_page->WLatch();
	new_page->Init(new_page_id, page_id, log_manager_, txn);
	success = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
	new_page->WUnlatch();
	buffer_pool_manager_->UnpinPage(new_page->GetTablePageId(), true);

	return success;
}


bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }

  // Otherwise, do the UPDATE
	int state;
	bool success;
	Row old_row(rid);  //old_row will have its own MemHeap
  page->WLatch();
  success = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_, state);
  page->WUnlatch();
  //buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);

	if(!success)
	{// state 1 ; slot number invalid  state 2: tuple is deleted  state 3: not enough space----delete + insert
		buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
		if(state == 3)
		{
			bool success;
			ApplyDelete(rid, txn);  //delete the old data
			success = InsertTuple(row, txn);  //insert the new row

			if(!success)
				return false;

			return true;
		}
		return false;
	}

  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */

//TODO: if a page is empty, we should free it !!
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
	auto page  = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
	
	ASSERT(page != nullptr, "error in TableHeap::ApplyDelete");	

	page->WLatch();
	page->ApplyDelete(rid, txn, log_manager_);
	page->WUnlatch();
	
	//delete if empty
	if(page->IsEmpty())
	{
		page_id_t page_id, next_page_id, prev_page_id;

		page_id      = page->GetTablePageId();
		next_page_id = page->GetNextPageId();
		prev_page_id = page->GetPrevPageId();
		
		buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
		//try to delete the page from buffer_pool  may fail if someone has Pinned it

		if(next_page_id == INVALID_PAGE_ID && prev_page_id == INVALID_PAGE_ID)
		{ //if it is the only page in the Table Heap, remain it
			return;
		}else if(next_page_id == INVALID_PAGE_ID)
		{	
			buffer_pool_manager_->DeletePage(page_id);
			
			auto prev_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(prev_page_id));
			prev_page->WLatch();
			prev_page->SetNextPageId(INVALID_PAGE_ID);  // prev now invalid --->   prev_of_prev  prev  invalid
			prev_page->WUnlatch();
			buffer_pool_manager_->UnpinPage(prev_page_id, true);
			return;
		}else if(prev_page_id == INVALID_PAGE_ID)
		{
			buffer_pool_manager_->DeletePage(page_id);
			if(page_id == first_page_id_)   //must be?
				first_page_id_ = next_page_id;  //change the first page id
			
			auto next_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(next_page_id));
			next_page->WLatch();
			next_page->SetPrevPageId(INVALID_PAGE_ID);  // invalid now next --->   invalid  next
			next_page->WUnlatch();
			buffer_pool_manager_->UnpinPage(next_page_id, true);
		}else
		{
			auto next_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(next_page_id));
			next_page->WLatch();
			next_page->SetPrevPageId(prev_page_id);  
			next_page->WUnlatch();
			buffer_pool_manager_->UnpinPage(next_page_id, true);
			
			auto prev_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(prev_page_id));
			prev_page->WLatch();
			prev_page->SetNextPageId(next_page_id); 
			prev_page->WUnlatch();
			buffer_pool_manager_->UnpinPage(prev_page_id, true);
		}
	}
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
	//according to TablePage::GetTuple  we need get Read lock before call TablePage::GetTuple
	page_id_t page_id = row->GetRowId().GetPageId();  //get page id from row
	if(page_id == -1)
		return false;

	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  //get the page
	ASSERT(page != nullptr, "error in TableHeap::GetTuple");
	
	page->RLatch();
	bool success = page->GetTuple(row, schema_, txn, lock_manager_);
	if(!success)
	{
		page->RUnlatch();
		buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
	}else{
		page->RUnlatch();
		buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
	}
	return success;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  //return the first tuple in first page
	page_id_t page_id;
	page_id = GetFirstPageId();
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
	
	//get first tuple rid
	RowId row_id;
	page->GetFirstTupleRid(&row_id);
	bool success;

	page->RLatch();
	page->GetFirstTupleRid(&row_id);  //get the first tuple id in the first page
	page->RUnlatch();

	Row row(row_id);
	buffer_pool_manager_->UnpinPage(page_id, false);
	return TableIterator(this, row, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  //return TableIterator();
	Row row(INVALID_ROWID);
	return TableIterator(this, row, static_cast<Transaction*>(nullptr));
}
