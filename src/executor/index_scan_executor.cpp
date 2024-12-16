#include "executor/executors/index_scan_executor.h"
#include "common/generate_name.h"
/**
 * TODO: Student Implement
 */
#include <algorithm>
#include "planner/expressions/constant_value_expression.h"

IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

bool extractKey(string &comparison, IndexInfo *index, Row &row, AbstractExpressionRef expression) {
  if (expression->GetType() == ExpressionType::ComparisonExpression) {
    auto comparisonExpression = dynamic_pointer_cast<ComparisonExpression>(expression);
    // get the comparison type
    auto columnExpression = dynamic_pointer_cast<ColumnValueExpression>(expression->GetChildAt(0));
    // get the column value
    auto constantExpression = dynamic_pointer_cast<ConstantValueExpression>(expression->GetChildAt(1));
    // get the constant value

    if (columnExpression->GetColIdx() == index->GetIndexKeySchema()->GetColumn(0)->GetTableInd()) {
      comparison = comparisonExpression->GetComparisonType();
      Field temp(constantExpression->Evaluate(nullptr));
      vector<Field> result;
      result.push_back(temp);
      row = Row(result);
      return true;
    } else {
      return false;
    }
  } else {
    if (extractKey(comparison, index, row, expression->GetChildAt(0))) {
      return true;
    } else if (extractKey(comparison, index, row, expression->GetChildAt(1))) {
      return true;
    } else {
      ASSERT(false, "index not found");
    }
  }
}

void IndexScanExecutor::Init() {
  int flag = 0;
  for (auto index : plan_->indexes_) {
    string comparison;
    Row key;
    extractKey(comparison, index, key, plan_->filter_predicate_);  // Get the first column of the index

    if (!flag) {                                                     // If it's the first index
      index->GetIndex()->ScanKey(key, result, nullptr, comparison);  // Scan the index
      flag = 1;
    } else {  // If it's not the first index, use set_intersection to find the intersection and store the result
      vector<RowId> temp;
      index->GetIndex()->ScanKey(key, temp, nullptr, comparison);
      vector<RowId> tempResult;
      std::set_intersection(result.begin(), result.end(), temp.begin(), temp.end(), std::back_inserter(tempResult));
      // Intersection
      result = tempResult;
    }
  }
  begin = result.begin();
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  // Output one element from the set
  if (result.size() == 0) {
    return false;
  }// If the set is empty, return false

  if (plan_->need_filter_) {
    // If need_filter is true, iterate through the set and check the predicate using a method similar to sequential scan
    while (begin != result.end()) {
      Row resultRow(*begin);
      table_info->GetTableHeap()->GetTuple(&resultRow, exec_ctx_->GetTransaction());
      Field temp(kTypeInt, 1);

      // If need_filter
      if (temp.CompareEquals(plan_->GetPredicate()->Evaluate(&resultRow))) {
        // Compare if the condition is satisfied
        *row = resultRow;
        *rid = *begin;
        begin++;
        return true;
      } else {
        begin++;
      }
    }
  } else {
    // If need_filter is false, iterate through the set and retrieve the rows directly
    while (begin != result.end()) {
      Row resultRow(*begin);
      table_info->GetTableHeap()->GetTuple(&resultRow, exec_ctx_->GetTransaction());
      *row = resultRow;
      *rid = *begin;
      begin++;
      return true;
    }
  }
  return false;
}
