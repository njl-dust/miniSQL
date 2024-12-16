//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
 * TODO: Student Implement
 */

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  string table_name_ = plan_->GetTableName();                       // get table name
  exec_ctx_->GetCatalog()->GetTable(table_name_, table_info_);      // get table info
  exec_ctx_->GetCatalog()->GetTableIndexes(table_name_, indexes_);  // get indexes of the table
  child_executor_->Init();                                          // the data we need to delete is from child
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row temp_row;
  RowId temp_rid;
  // check if there is a row left as well as get the row
  if (!child_executor_->Next(&temp_row, &temp_rid)) {
    return false;
  }
  // delete the row
  if (!table_info_->GetTableHeap()->MarkDelete(temp_rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  Row key_row;
  // delete the index
  for (const auto index : indexes_) {
    // get the key
    temp_row.GetKeyFromRow(table_info_->GetSchema(), index->GetIndexKeySchema(), key_row);
    // delete the entry
    index->GetIndex()->RemoveEntry(key_row, temp_rid, exec_ctx_->GetTransaction());
  }
  return true;
}