//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"
#include "planner/expressions/constant_value_expression.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
 * TODO: Student Implement
 */
void UpdateExecutor::Init() {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), index_info_);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  /** We delete the old tuples first **/
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
  for (const auto index : index_info_) {
    // get the key
    temp_row.GetKeyFromRow(table_info_->GetSchema(), index->GetIndexKeySchema(), key_row);
    // delete the entry
    index->GetIndex()->RemoveEntry(key_row, temp_rid, exec_ctx_->GetTransaction());
  }

  /** Now insert the new data **/
  // generate new tuple
  Row new_row = GenerateUpdatedTuple(temp_row);
  // insert tuple
  if (!table_info_->GetTableHeap()->InsertTuple(new_row, exec_ctx_->GetTransaction())) {
    return false;
  }
  // insert index
  for (auto index : index_info_) {
    Row key_row;
    new_row.GetKeyFromRow(table_info_->GetSchema(), index->GetIndexKeySchema(), key_row);
    index->GetIndex()->InsertEntry(key_row, new_row.GetRowId(), exec_ctx_->GetTransaction());
  }
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  std::vector<Field> values;
  const auto &update_attrs = plan_->GetUpdateAttr();  // get the update attributes

  for (uint32_t column_id = 0; column_id < table_info_->GetSchema()->GetColumnCount(); column_id++) {
    if (update_attrs.find(column_id) == update_attrs.cend()) {  // if not found, keep the old value
      Field t = Field(*(src_row.GetField(column_id)));
      values.emplace_back(t);
    } else {                                                                  // if found, update the value
      const AbstractExpressionRef update_info_ = update_attrs.at(column_id);  // get the update info
      Field new_filed(update_info_->Evaluate(nullptr)); // evaluate the update info
      values.emplace_back(new_filed);
    }
  }
  return Row(values);
}