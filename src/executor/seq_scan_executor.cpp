//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
 * TODO: Student Implement
 */
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto table_name_ = plan_->GetTableName();                     // get table name
  exec_ctx_->GetCatalog()->GetTable(table_name_, table_info_);  // get table info
}

void SeqScanExecutor::Init() {
  table_heap_ = table_info_->GetTableHeap();                // get table heap
  iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());  // get iterator
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  while (true) {
    Field temp(kTypeInt, 1);
    if (iter_ == table_heap_->End()) {
      break;
    }
    if (plan_->GetPredicate() == nullptr) {  // No predicate
      *row = Row(*iter_.GetRow());           // copy
      *rid = row->GetRowId();
      ++iter_;
      return true;
    }

    if (temp.CompareEquals(plan_->GetPredicate()->Evaluate(iter_.GetRow()))) {  // if predicate is true
      *row = Row(*iter_.GetRow());                                              // copy
      *rid = row->GetRowId();
      ++iter_;
      return true;
    } else {
      ++iter_;
    }
  }
  return false;
}
