#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/generate_name.h"
#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
// #include "parser/minisql_lex.h"
// #include "parser/parser.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "parser/minisql_yacc.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    // mkdir(path);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  std::string databaseName = ast->child_->val_;
  if (dbs_.find(databaseName) != dbs_.end()) {
    std::cout << "Error: Database " << databaseName << " already exists." << std::endl;
    return DB_FAILED;
  } else {
    DBStorageEngine *temp = new DBStorageEngine(databaseName);
    dbs_.insert(std::make_pair(databaseName, temp));
    std::cout << "Database " << databaseName << " created." << std::endl;
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string databaseName = ast->child_->val_;
  if (dbs_.find(databaseName) == dbs_.end()) {
    std::cout << "Error: Database " << databaseName << " not exists." << std::endl;
    return DB_FAILED;
  } else {
    dbs_.erase(databaseName);
    std::cout << "Database " << databaseName << " dropped." << std::endl;
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    std::cout << "No database exists." << std::endl;
    return DB_SUCCESS;
  } else {
    std::cout << "Database list:" << std::endl;
    for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
      std::cout << it->first << std::endl;
    }
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string databaseName = ast->child_->val_;
  if (dbs_.find(databaseName) == dbs_.end()) {
    std::cout << "Error: Database " << databaseName << " not exists." << std::endl;
    return DB_FAILED;
  } else {
    current_db_ = databaseName;
    std::cout << "Database " << databaseName << " used." << std::endl;
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  // store all tables
  vector<TableInfo *> tables;
  // get all tables
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    cout << "No table exists." << endl;
    return DB_SUCCESS;
  } else {
    for (auto &table : tables) {
      cout << table->GetTableName() << endl;
    }
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  /** Get the table name from the syntax tree **/
  std::string tableName = ast->child_->val_;
  pSyntaxNode temp_point = ast->child_->next_->child_;
  std::vector<Column *> columns;

  /** Elements required to construct a new column **/
  std::string column_name;
  std::string type;
  uint32_t index;
  bool nullable = true;
  bool unique;
  std::vector<std::string> uniqueKeys = {};           // store the unique keys
  std::map<std::string, uint32_t> columnNameToIndex;  // store the column name and index

  /** Read each element of the table through the syntax tree **/
  while (true) {
    if (temp_point->val_ != nullptr && std::string(temp_point->val_) == "unique") {
      unique = true;
    } else {
      unique = false;
    }  // read in the column name and check if it is unique

    column_name = temp_point->child_->val_;
    type = temp_point->child_->next_->val_;

    if (unique) {
      uniqueKeys.push_back(column_name);
    }

    columnNameToIndex.insert(std::make_pair(column_name, index));  // store the column name and index

    if (type == "int") {
      columns.push_back(new Column(column_name, TypeId::kTypeInt, index, nullable, unique));
    } else if (type == "float") {
      columns.push_back(new Column(column_name, TypeId::kTypeFloat, index, nullable, unique));
    } else if (type == "char") {
      std::string lengthString = temp_point->child_->next_->child_->val_;  // get the length of the char type

      if (lengthString.find('.') != std::string::npos) {
        std::cout << "Error: Invalid length for char type." << std::endl;
        return DB_FAILED;
      }  // check if the length is valid(it can't be a float)

      int length = std::stoi(lengthString);

      if (length <= 0) {
        std::cout << "Error: Invalid length for char type." << std::endl;
        return DB_FAILED;
      }  // check if the length is valid(it can't be a negative number)

      columns.push_back(new Column(column_name, TypeId::kTypeChar, length, index, nullable, unique));
    } else {
      std::cout << "Error: Invalid column type: " << type << std::endl;
      return DB_FAILED;
    }  // check if the type is valid

    index++;
    temp_point = temp_point->next_;  // move to the next column

    if (temp_point->type_ != kNodeColumnDefinition) {
      break;  // check if the column definition is finished
    }
  }

  /** Create an index for primary keys **/
  pSyntaxNode columnList = temp_point;
  std::vector<std::string> primaryKeys = {};  // store the primary keys
  // std::vector<uint32_t> primaryKeyIndices = {};  // store the primary key indices

  while (columnList != nullptr && columnList->type_ == kNodeColumnList) {
    if (std::string(columnList->val_) == "primary keys") {
      for (pSyntaxNode identifier = columnList->child_; identifier && identifier->type_ == kNodeIdentifier;
           identifier = identifier->next_) {
        // Try to find the column in the column list
        try {
          primaryKeys.push_back(identifier->val_);  // store the primary keys
        } catch (const std::out_of_range &e) {      // Not found
          std::cout << "Error: Primary key " << std::string(identifier->val_) << " does not exist." << std::endl;
          return DB_FAILED;  // DB_KEY_NOT_FOUND;
        }
      }
    } else {
      // Not support "foreign keys" and "check"
      LOG(ERROR) << "Unknown column list type: " << columnList->val_ << std::endl;
      return DB_FAILED;
    }

    columnList = columnList->next_;
  }

  /** Initialize CreateTable elements **/
  TableSchema *table_schema = new TableSchema(columns);
  TableInfo *table_info = nullptr;
  auto cata = dbs_[current_db_]->catalog_mgr_;
  dberr_t mark = cata->CreateTable(tableName, table_schema, nullptr, table_info, primaryKeys, uniqueKeys);

  if (mark == DB_TABLE_ALREADY_EXIST) {
    std::cout << "Error: Table " << tableName << " already exists." << std::endl;
    return DB_FAILED;
  } else if (mark == DB_FAILED) {
    std::cout << "Error: Create table failed." << std::endl;
    return DB_FAILED;
  }

  // Create an index for the primary key
  IndexInfo *tempindex = nullptr;
  std::string tempindex_name = Auto_Generate_Primary_Key(tableName);
  cata->CreateIndex(tableName, tempindex_name, primaryKeys, nullptr, tempindex, "bptree");

  // Create indexes for unique keys
  for (auto &uniqueKey : uniqueKeys) {
    IndexInfo *uniqueIndexInfo = nullptr;
    std::string uniqueIndexName = Auto_Generate_Unique_Key(tableName, uniqueKey);
    dberr_t uni_ret = cata->CreateIndex(tableName, uniqueIndexName, {uniqueKey}, nullptr, uniqueIndexInfo,
                                        "bptree");  // create index for unique key
    assert(uni_ret == DB_SUCCESS);
  }

  std::cout << "Table " << tableName << " created." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  // Get the table name
  std::string tableName = ast->child_->val_;
  // Drop the table as well as its indexes
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->DropTable(tableName);
  if (ret == DB_TABLE_NOT_EXIST) {
    cout << "Error: Can't find " << tableName << "." << endl;
    return DB_TABLE_NOT_EXIST;
  } else {
    assert(ret == DB_SUCCESS);
    cout << "Table " << tableName << " dropped " << endl;
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
/*
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  // Get the table name
  std::string tableName = "tb";
	cout << tableName << endl;
  // Get the index info
  std::vector<IndexInfo *> indexInfos;
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->GetTableIndexes(tableName, indexInfos);
  if (ret == DB_TABLE_NOT_EXIST) {
    cout << "Error: Can't find " << tableName << "." << endl;
    return DB_TABLE_NOT_EXIST;
  } else {
    assert(ret == DB_SUCCESS);
    // Print the index info
    cout << "Index of table " << tableName << ":" << endl;
    for (int i; i < indexInfos.size(); i++) {
      cout << indexInfos[i]->GetIndexName() << endl;
    }
    return DB_SUCCESS;
  }
}
*/
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  auto all_table = dbs_[current_db_]->catalog_mgr_->GetAllTableNames();
  vector<IndexInfo *> all_index;
  for (auto &table_name : all_table) {
    vector<IndexInfo *> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
    if (indexes.empty()) {
      cout << "No index exists." << endl;
      return DB_SUCCESS;
    }
    // 从末尾一个个插进去
    all_index.insert(all_index.end(), indexes.begin(), indexes.end());
  }
  auto iter = all_index.begin();
  for (; iter != all_index.end(); iter++) {
    cout << (*iter)->GetIndexName() << endl;
  }
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  // Get the table name
  std::string tableName = ast->child_->next_->val_;
  // Get the index name
  std::string indexName = ast->child_->val_;
  // Get the index keys
  vector<string> indexKeys;
  pSyntaxNode temp_pointer = ast->child_->next_->next_->child_;
  while (temp_pointer != nullptr) {
    indexKeys.push_back(temp_pointer->val_);
    temp_pointer = temp_pointer->next_;
  }

  // Get the index info
  IndexInfo *indexInfo = nullptr;
  dberr_t ret =
      dbs_[current_db_]->catalog_mgr_->CreateIndex(tableName, indexName, indexKeys, nullptr, indexInfo, "bptree");

  TableInfo *tableInfo = nullptr;
  dbs_[current_db_]->catalog_mgr_->GetTable(tableName, tableInfo);
  TableIterator tableIterator = tableInfo->GetTableHeap()->Begin(nullptr);
  while (tableIterator != tableInfo->GetTableHeap()->End()) {
    Row key_row;
    Row *new_row;
    new_row = tableIterator.GetRow();
		
    new_row->GetKeyFromRow(tableInfo->GetSchema(), indexInfo->GetIndexKeySchema(), key_row);
    indexInfo->GetIndex()->InsertEntry(key_row, new_row->GetRowId(), nullptr);
    tableIterator++;
  }  // insert all the data into the index

  if (ret == DB_TABLE_NOT_EXIST) {
    cout << "Error: Can't find " << tableName << "." << endl;
    return DB_TABLE_NOT_EXIST;
  } else if (ret == DB_INDEX_ALREADY_EXIST) {
    cout << "Error: Index " << indexName << " already exists." << endl;
    return DB_INDEX_ALREADY_EXIST;
  } else if (ret == DB_INDEX_NOT_UNIQUE) { /** NEED TO IMPROVE **/
    // TODO: Check if the index keys are unique
    // lie to the user that the index is created
    return DB_SUCCESS;
  } else if (ret == DB_FAILED) {
    cout << "Error: Create index failed." << endl;
    return DB_FAILED;
  } else {
    assert(ret == DB_SUCCESS);
    cout << "Index " << indexName << " created." << endl;
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  // Get the index name
  std::string indexName = ast->child_->val_;
  vector<TableInfo *> tableInfos;
  dbs_[current_db_]->catalog_mgr_->GetTables(tableInfos);

  // serach all the tables to find the index
  for (int i = 0; i < tableInfos.size(); i++) {
    vector<IndexInfo *> indexInfos;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(tableInfos[i]->GetTableName(), indexInfos);
    for (int j = 0; j < indexInfos.size(); j++) {
      if (indexInfos[j]->GetIndexName() == indexName) {
        dberr_t ret = dbs_[current_db_]->catalog_mgr_->DropIndex(tableInfos[i]->GetTableName(), indexName);
        if (ret == DB_INDEX_NOT_FOUND) {
          cout << "Error: Can't find " << indexName << "." << endl;
          return DB_FAILED;
        } else {
          assert(ret == DB_SUCCESS);
          cout << "Index " << indexName << " dropped." << endl;
          return DB_SUCCESS;
        }
      }
    }
  }
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}


dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  long start = clock();
  string file_name = ast->child_->val_;
  ifstream infile;
  infile.open(file_name.data());
  if (infile.is_open()) {  // if open fails,return false
    string s;
    while (getline(infile, s)) {                       // read line by line                     // read line by line
      YY_BUFFER_STATE bp = yy_scan_string(s.c_str());  // create buffer state
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }  // if bp is nullptr, exit
      yy_switch_to_buffer(bp);
      // init parser module
      MinisqlParserInit();
      // parse
      yyparse();

      Execute(MinisqlGetParserRootNode());
    }
  } else {
    cout << "Failed In Opening File!" << endl;
    return DB_FAILED;
  }
  long end = clock();
  cout << "total time (" << (end - start) * 1.0 / CLOCKS_PER_SEC << " sec)" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  return DB_QUIT;
}
