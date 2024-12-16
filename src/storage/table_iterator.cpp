#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *th, Row row, Transaction *txn): th_(th), row_(row), txn_(txn){
	if(!(row_.GetRowId().GetPageId() == INVALID_ROWID.GetPageId() && row_.GetRowId().GetSlotNum() == INVALID_ROWID.GetSlotNum()))
	{
		th_->GetTuple(&row_, txn_);
	}
}

TableIterator::TableIterator(const TableIterator &other): th_(other.th_), row_(other.row_), txn_(other.txn_){}


TableIterator::~TableIterator() {

}


bool TableIterator::operator==(const TableIterator &itr) const {
	if(itr.th_ != th_ || itr.txn_ != txn_)
		return false;
	else if(itr.row_.GetRowId() == INVALID_ROWID && row_.GetRowId() == INVALID_ROWID)
		return true;
	else if(itr.row_.GetRowId() == INVALID_ROWID || row_.GetRowId() == INVALID_ROWID)
		return false;
	else 
		return (itr.row_.GetRowId().GetPageId() == row_.GetRowId().GetPageId()) && (itr.row_.GetRowId().GetSlotNum() == row_.GetRowId().GetSlotNum());
}


bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}
const Row &TableIterator::operator*() {
	ASSERT(*this != th_->End(), "End itr can not *"); 
	return row_;
}

Row *TableIterator::operator->() {
  return &row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
	th_ = itr.th_;
	row_ = itr.row_;
	txn_ = itr.txn_;
	return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  BufferPoolManager *buffer_pool_manager = th_->buffer_pool_manager_;

	auto page = reinterpret_cast<TablePage*>(buffer_pool_manager->FetchPage(row_.GetRowId().GetPageId()));
	page->RLatch();
	//ASSERT(page != nullptr, "error in operator++");
	if(nullptr == page)
	{
		cout << "error in operator++" << endl;
		return *this;
	}
	RowId next_row_id;
	if(!(page->GetNextTupleRid(row_.GetRowId(), &next_row_id)))  //if this row is the last element of the page
	{
		//try to get next page
		if(page->GetNextPageId() != INVALID_PAGE_ID) {
      auto next_page = static_cast<TablePage *>(buffer_pool_manager->FetchPage(page->GetNextPageId()));
      page->RUnlatch();
      buffer_pool_manager->UnpinPage(page->GetPageId(), false);
      page = next_page;
			page->RLatch(); 

			if (!(page->GetFirstTupleRid(&next_row_id)))
			{
				cout << "SHOULD NOT REACH HERE" << endl;
			}
		}else{
			//No next page ? End()
			next_row_id = INVALID_ROWID;
		}
	}	


	page->RUnlatch();
	buffer_pool_manager->UnpinPage(page->GetPageId(), false);

	//set the row_ and get new tuple data
	row_ = Row(next_row_id);

	if(!(row_.GetRowId().GetPageId() == INVALID_ROWID.GetPageId() && row_.GetRowId().GetSlotNum() == INVALID_ROWID.GetSlotNum()))
		th_->GetTuple(&row_, txn_);

	return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator clone(th_, row_, txn_);
	++(*this);
	return clone;
}

Row *TableIterator::GetRow() {
  return &row_;
}
