//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"
#include "common/generate_name.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  auto table_name_ = plan_->GetTableName();  // get table name
  TableInfo *table_info_;                    // get table info
  exec_ctx_->GetCatalog()->GetTable(table_name_, table_info_);
  table_heap_ = table_info_->GetTableHeap();                // get table heap
  iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());  // get iterator
	child_executor_->Init();
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row new_row;
  RowId new_row_id;
  if (child_executor_->Next(&new_row, &new_row_id)) {
    auto table_name_ = plan_->GetTableName();  // get table name
    TableInfo *table_info_;                    // get table info
    exec_ctx_->GetCatalog()->GetTable(table_name_, table_info_);

    // check if the primary key has already existed
    if(table_info_->GetPrimaryKeys().size() != 0){
			IndexInfo *primary_index_info_;  // get index info
			std::string primary_index_name = Auto_Generate_Primary_Key(table_name_);
			exec_ctx_->GetCatalog()->GetIndex(table_name_, primary_index_name, primary_index_info_);
			vector<RowId> result;
			Row key_row;
			new_row.GetKeyFromRow(table_info_->GetSchema(), primary_index_info_->GetIndexKeySchema(), key_row);
			dberr_t res = primary_index_info_->GetIndex()->ScanKey(key_row, result, nullptr);
			if (!(res == DB_KEY_NOT_FOUND)) {
				throw std::runtime_error("The primary key already exists");
			}
		}
    // check if the unique key has already existed
		if(table_info_->GetUniqueKeys().size() != 0){	
			for (auto &unique_key_name : table_info_->GetUniqueKeys()) {
				IndexInfo *unique_index_info_;  // get index info
				string unique_index_name = Auto_Generate_Unique_Key(table_name_, unique_key_name);
				exec_ctx_->GetCatalog()->GetIndex(table_name_, unique_index_name, unique_index_info_);
				vector<RowId> result;
				Row key_row;
				new_row.GetKeyFromRow(table_info_->GetSchema(), unique_index_info_->GetIndexKeySchema(), key_row);
				dberr_t res = unique_index_info_->GetIndex()->ScanKey(key_row, result, nullptr);
				if (!(res == DB_KEY_NOT_FOUND)) {
					throw std::runtime_error("The unique key already exists");
				}
			}
		}

    /** insert the row into the table **/
	 	// if the row is empty
    if (!table_info_->GetTableHeap()->InsertTuple(new_row, exec_ctx_->GetTransaction())) {
      return false;
    }

    new_row_id = new_row.GetRowId();
    vector<IndexInfo *> index_infos_;  // get all index infos
    exec_ctx_->GetCatalog()->GetTableIndexes(table_name_, index_infos_);
    for (auto &index_info : index_infos_) {
      Row key_row;
      new_row.GetKeyFromRow(table_info_->GetSchema(), index_info->GetIndexKeySchema(), key_row);
      index_info->GetIndex()->InsertEntry(key_row, new_row.GetRowId(), exec_ctx_->GetTransaction());
    }

  } else {
    return false;
  }
}
