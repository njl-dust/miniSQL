#include "catalog/catalog.h"
#include <algorithm>

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  // CatalogMeta *meta = new CatalogMeta();
  CatalogMeta *meta = ALLOC_P(heap, CatalogMeta)();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t ofs = 4 + 4 + 4 + 8 * table_meta_pages_.size() + 8 * index_meta_pages_.size();
  return ofs;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      c_heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
  if (init) {  // if the DataBase is created for the first time, init the metadata
    // catalog_meta_ = ALLOC_P(c_heap, CatalogMeta)();  //failed, CatelogMeta() is a private function
    catalog_meta_ = CatalogMeta::NewInstance(c_heap_);
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  } else {
    // read the metadata from CatalogPage
    auto meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData(),
                                                 c_heap_);  // dont forget to use the MemHeap  Read data from the page

    // load data of tables from page   (map<table_id_t, page_id_t>)
    for (auto ele : *(catalog_meta_->GetTableMetaPages())) {
      LoadTable(ele.first, ele.second);
    }

    // load data of index from page
    for (auto ele : *(catalog_meta_->GetIndexMetaPages())) {
      LoadIndex(ele.first, ele.second);
    }

    // update NextId
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();

    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
}

CatalogManager::~CatalogManager() {
  /** After you finish the code for the CatalogManager section,
  *  you can uncomment the commented code. Otherwise it will affect b+tree test
         FlushCatalogMetaPage();
   delete catalog_meta_;
   for (auto iter : tables_) {
     delete iter.second;
   }
   for (auto iter : indexes_) {
     delete iter.second;
   }
   **/
  FlushCatalogMetaPage();
  delete c_heap_;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info, const vector<string>& primaryKeys, const vector<string>& uniqueKeys) {
  // ASSERT(false, "Not Implemented yet");
  // firstly, check if the table already exist
  if (table_names_.find(table_name) != table_names_.end()) return DB_TABLE_ALREADY_EXIST;

  // get the table_id
  table_id_t table_id = next_table_id_;
  ++next_table_id_;
  // check
  if (tables_.find(table_id) != tables_.end() || index_names_.find(table_name) != index_names_.end() ||
      catalog_meta_->GetTableMetaPages()->find(table_id) != catalog_meta_->GetTableMetaPages()->end()) {
    cout << "error in CreateTable, data error in tables_/indexes_names_/catalog_meta_" << endl;
    return DB_FAILED;
  }

  // create a new table_info
  table_info = TableInfo::Create(c_heap_);

  // to init the table_info, we need meta_data and tableheap
  TableHeap *table_heap =
      TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, table_info->GetMemHeap());
  TableMetadata *table_meta =
      TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema, primaryKeys, uniqueKeys, table_info->GetMemHeap());
  // use table_info's own memheap   note that the root_page_id in table_meta is the first_page_id of the heap (I think
  // so ?)

  // init
  table_info->Init(table_meta, table_heap);

  // write the meta_data into a page
  page_id_t table_meta_page_id;
  auto table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);
  table_meta->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);

  // insert into maps  (already checked)
  tables_[table_id] = table_info;
  table_names_[table_name] = table_id;
  catalog_meta_->GetTableMetaPages()->insert({table_id, table_meta_page_id});
  // debug (you can remove this code for better performance)
  if (catalog_meta_->GetTableMetaPages()->find(table_id) == catalog_meta_->GetTableMetaPages()->end()) {
    cout << "error in CreateTable, insertion failed" << endl;
    return DB_FAILED;
  }

  // flush
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  // firstly, check if the table already exist
  vector<string> primaryKeys;
  vector<string> uniqueKeys;
  if (table_names_.find(table_name) != table_names_.end()) return DB_TABLE_ALREADY_EXIST;

  // get the table_id
  table_id_t table_id = next_table_id_;
  ++next_table_id_;
  // check
  if (tables_.find(table_id) != tables_.end() || index_names_.find(table_name) != index_names_.end() ||
      catalog_meta_->GetTableMetaPages()->find(table_id) != catalog_meta_->GetTableMetaPages()->end()) {
    cout << "error in CreateTable, data error in tables_/indexes_names_/catalog_meta_" << endl;
    return DB_FAILED;
  }

  // create a new table_info
  table_info = TableInfo::Create(c_heap_);

  // to init the table_info, we need meta_data and tableheap
  TableHeap *table_heap =
      TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, table_info->GetMemHeap());
  TableMetadata *table_meta =
      TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema, primaryKeys, uniqueKeys, table_info->GetMemHeap());
  // use table_info's own memheap   note that the root_page_id in table_meta is the first_page_id of the heap (I think
  // so ?)

  // init
  table_info->Init(table_meta, table_heap);

  // write the meta_data into a page
  page_id_t table_meta_page_id;
  auto table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);
  table_meta->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);

  // insert into maps  (already checked)
  tables_[table_id] = table_info;
  table_names_[table_name] = table_id;
  catalog_meta_->GetTableMetaPages()->insert({table_id, table_meta_page_id});
  // debug (you can remove this code for better performance)
  if (catalog_meta_->GetTableMetaPages()->find(table_id) == catalog_meta_->GetTableMetaPages()->end()) {
    cout << "error in CreateTable, insertion failed" << endl;
    return DB_FAILED;
  }

  // flush
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  // return DB_FAILED;
  auto iter_tname = table_names_.find(table_name);
  if (table_names_.end() == iter_tname) return DB_TABLE_NOT_EXIST;

  table_id_t table_id = iter_tname->second;
  auto iter_table_info = tables_.find(table_id);
  if (tables_.end() == iter_table_info) {
    cout << "error in GetTable" << endl;
    return DB_FAILED;
  }

  table_info = iter_table_info->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  auto iter_table_info = tables_.begin();
  while (tables_.end() != iter_table_info) {
    tables.push_back(iter_table_info->second);
    ++iter_table_info;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  auto iter_tname = table_names_.find(table_name);
  if (table_names_.end() == iter_tname) return DB_TABLE_NOT_EXIST;

  auto iter_tablemap = index_names_.find(table_name);  // note that, maybe tha table exists but it has no index, so we
                                                       // should check whether the iter is end()
  /*
  if((index_names_.end() != iter_tablemap) && (iter_tablemap->second.find(index_name) != iter_tablemap->second.end()))
          return DB_INDEX_ALREADY_EXIST;
  */
  if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) return DB_INDEX_ALREADY_EXIST;
  // get index_id
  index_id_t index_id = next_index_id_;
  ++next_index_id_;
  // check
  if (indexes_.find(index_id) != indexes_.end() ||
      catalog_meta_->GetIndexMetaPages()->find(index_id) != catalog_meta_->GetIndexMetaPages()->end()) {
    cout << "error in CreateIndex, data error in indexes_/catalog_meta" << endl;
    return DB_FAILED;
  }

  // create index_info
  index_info = IndexInfo::Create(c_heap_);

  // to init index_info, we need index_meta_data
  // firstly, get essential data
  table_id_t table_id = iter_tname->second;

  auto iter_table_info = tables_.find(table_id);
  if (tables_.end() == iter_table_info) {
    cout << "error in CreateIndex, data error in tables_" << endl;
    return DB_FAILED;
  }
  TableInfo *table_info = iter_table_info->second;

 /*
  vector<string> primary_keys=table_info->GetPrimaryKeys();
  vector<string> unique_keys=table_info->GetUniqueKeys();
  for(auto &key_name : index_keys){
    if(find(primary_keys.begin(),primary_keys.end(),key_name)==primary_keys.end()||
       find(unique_keys.begin(),unique_keys.end(),key_name)==unique_keys.end()){
      cout<<"error in CreateIndex, index key is not primary key or unique key"<<endl;
      return DB_INDEX_NOT_UNIQUE;
    }
  }
  */

  // to create index meta_data, we need to get key_map
  std::vector<uint32_t> key_map;
  for (auto &key_name : index_keys) {
    uint32_t key_pos;
    if (table_info->GetSchema()->GetColumnIndex(key_name, key_pos) == DB_COLUMN_NAME_NOT_EXIST)
      return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(key_pos);
  }

  // create index_meta_data
  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map, index_info->GetMemHeap());

  // init index_info
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  // we need to store the meta_data into a page
  page_id_t index_meta_page_id;
  auto index_meta_page = buffer_pool_manager_->NewPage(index_meta_page_id);
  index_meta->SerializeTo(index_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(index_meta_page_id, true);

  // updata maps  all the maps have been checked
  indexes_[index_id] = index_info;
  catalog_meta_->GetIndexMetaPages()->insert({index_id, index_meta_page_id});
  if (index_names_.end() == iter_tablemap) {  // init the tablemap
    std::unordered_map<std::string, index_id_t> tablemap;
    tablemap[index_name] = index_id;
    index_names_[table_name] = tablemap;
  } else
    iter_tablemap->second[index_name] = index_id;

  // flush
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  auto iter_tnames = table_names_.find(table_name);
  if (table_names_.end() == iter_tnames) return DB_TABLE_NOT_EXIST;

  auto iter_tablemap = index_names_.find(table_name);
  if (index_names_.end() == iter_tablemap)  // this table has no index
    return DB_INDEX_NOT_FOUND;

  auto iter_name_index = iter_tablemap->second.find(index_name);
  if (iter_tablemap->second.end() == iter_name_index) return DB_INDEX_NOT_FOUND;  // no such index

  index_id_t index_id = iter_name_index->second;
  auto iter_index_info = indexes_.find(index_id);
  if (indexes_.end() == iter_index_info)  // index not in indexes
  {
    cout << "Error in GetIndex" << endl;
    return DB_FAILED;
  }

  index_info = iter_index_info->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  auto iter_tnames = table_names_.find(table_name);
  if (table_names_.end() == iter_tnames) return DB_TABLE_NOT_EXIST;

  auto iter_tablemap = index_names_.find(table_name);  // map of index_name --- index_id
  if (index_names_.end() != iter_tablemap) {
    auto iter_name_index = iter_tablemap->second.begin();
    while (iter_tablemap->second.end() != iter_name_index) {
      index_id_t index_id = iter_name_index->second;
      auto iter_index_info = indexes_.find(index_id);  // iter {index_id, index_info*}
      if (indexes_.end() == iter_index_info) {
        cout << "Error in GetTableIndexes" << endl;
        return DB_FAILED;
      }

      indexes.push_back(iter_index_info->second);
      ++iter_name_index;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  auto iter_tnames = table_names_.find(table_name);
  if (table_names_.end() == iter_tnames)  // table not exist
    return DB_TABLE_NOT_EXIST;

  // get data
  table_id_t table_id = iter_tnames->second;
  TableInfo *table_info = tables_[table_id];
  page_id_t table_page_id = catalog_meta_->GetTableMetaPages()->at(table_id);

  // delete data
  catalog_meta_->GetTableMetaPages()->erase(catalog_meta_->GetTableMetaPages()->find(table_id));
  table_names_.erase(iter_tnames);
  tables_.erase(tables_.find(table_id));
  // index_names_.erase(index_names_.find(table_name));  //don't forget this!!!
  // TODO:: is there any better method to delete the related index?
  auto iter_tablemap = index_names_.find(table_name);
  if (index_names_.end() != iter_tablemap) {  // Drop all the indexes related to this table
    auto iter_name_index = iter_tablemap->second.begin();
    int count = iter_tablemap->second.size();  // debug	  not necessary
		cout << count << endl;
    while (iter_tablemap->second.end() != iter_name_index) {
      string index_name = iter_name_index->first;
      ++iter_name_index;  // ++iter before drop it --------- because in DropIndex, it will delete the corresponding data
                          // !!!   see test.cpp
      DropIndex(table_name, index_name);
      count--;
    }

    if (count != 0) {
			cout << "failed in Drop table!!!  table should not exist in index_names_" << endl;
      return DB_FAILED;
    }
  }

  // deallocate pages
  // 2 step  deallocate all the pages in table-heap    deallocate the page with table-meta-data
  table_info->GetTableHeap()->DeleteTable();  // it will deallocate all the page in TableHeap
  // don't delete table_info table_meta...... they will be deleted when c_heap is destoyed

  bool success = buffer_pool_manager_->DeletePage(table_page_id);
  if (!success) {
    cout << "fail to deallocate page" << endl;
    return DB_FAILED;
  }

  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  auto iter_tname = table_names_.find(table_name);
  if (table_names_.end() == iter_tname)  // table not exist
    return DB_TABLE_NOT_EXIST;

  auto iter_tablemap = index_names_.find(table_name);
  if (index_names_.end() == iter_tablemap)  // table not exist in index_names_ ----  this table has no index
    return DB_INDEX_NOT_FOUND;

  auto iter_iname_index = iter_tablemap->second.find(index_name);
  if (iter_tablemap->second.end() == iter_iname_index)  // index is not found
    return DB_INDEX_NOT_FOUND;

  // index exist
  // get essential data
  index_id_t index_id = iter_iname_index->second;
  IndexInfo *index_info = indexes_[index_id];
  page_id_t index_page_id = catalog_meta_->GetIndexMetaPages()->at(index_id);

  // delete data
  catalog_meta_->GetIndexMetaPages()->erase(
      catalog_meta_->GetIndexMetaPages()->find(index_id));  // catolog_meta: index_id---page_id
  indexes_.erase(indexes_.find(index_id));                  // indexes_  index_id --- index_info
  iter_tablemap->second.erase(iter_iname_index);            // index_name ---- index_id
  if (iter_tablemap->second.size() ==
      0) {  // the only index of this page will be deleted, so we should also delete the map(of this page)
    index_names_.erase(iter_tablemap);
  }

  // deallocate pages
  // 2 step:  deallocate the page containing data of index-tree    deallocate the page containing meta-data of index
  // delete data of the indextree (b plus tree)
  index_info->GetIndex()->Destroy();  // don't free the index_info, it will be freed when c_heap_ is destroyed
  // the destroy process: BPlusTreeIndex->Destroy()  ---->	 container.Destroy() (the container is BPlusTree) ---->
  // BPlusTree.Destroy()

  // dealloc meta data page
  bool success = buffer_pool_manager_->DeletePage(index_page_id);  // dealloc the page(with data of this index)
  if (!success) {
    std::cout << "failed to deallocate the page with index_meta_data!" << std::endl;
    return DB_FAILED;
  }

  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);

  // Write to page
  catalog_meta_->SerializeTo(page->GetData());

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  auto page = buffer_pool_manager_->FetchPage(page_id);
  // alloc a new table_info, and init with the info from page
  TableInfo *table_info = TableInfo::Create(c_heap_);

  // to init the table_info, we need table_meta_data and table_heap
  // metadata
  TableMetadata *t_meta_data = nullptr;
  TableMetadata::DeserializeFrom(
      page->GetData(), t_meta_data,
      table_info
          ->GetMemHeap());  // why use table_info's own MemHeap-----it's more Mem-Friendly when we delete the Table
  if (table_id != t_meta_data->GetTableId()) {
    cout << "Erorr ID!" << endl;
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_FAILED;
  }
  table_names_[t_meta_data->GetTableName()] = table_id;  // table name->table_id

  // table heap
  TableHeap *table_heap =
      TableHeap::Create(buffer_pool_manager_, t_meta_data->GetFirstPageId(), t_meta_data->GetSchema(), log_manager_,
                        lock_manager_, table_info->GetMemHeap());

  // init table info
  table_info->Init(t_meta_data, table_heap);
  tables_[table_id] = table_info;

  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  auto page = buffer_pool_manager_->FetchPage(page_id);
  IndexInfo *index_info = IndexInfo::Create(c_heap_);

  // get index meta_data from page
  IndexMetadata *i_meta_data = nullptr;
  IndexMetadata::DeserializeFrom(page->GetData(), i_meta_data, index_info->GetMemHeap());
  if (index_id != i_meta_data->GetIndexId()) {
    cout << "Erorr ID!" << endl;
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_FAILED;
  }

  // now we need to insert the index into map
  std::string index_name = i_meta_data->GetIndexName();
  table_id_t table_id = i_meta_data->GetTableId();

  // fisrt, check if the table exist
  if (tables_.find(table_id) == tables_.end())  // not exist
  {
    cout << "table not exist" << endl;
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_TABLE_NOT_EXIST;
  }

  TableInfo *table_info = tables_[table_id];
  std::string table_name = table_info->GetTableName();

  // secondly  insert into map
  if (index_names_.find(table_name) == index_names_.end()) {  // init the map
    std::unordered_map<std::string, index_id_t> name_index_map;
    name_index_map[index_name] = index_id;
    index_names_[table_name] = name_index_map;
  } else {
    index_names_[table_name][index_name] = index_id;
  }

  // init index_info  and insert it into map
  index_info->Init(i_meta_data, table_info, buffer_pool_manager_);
  indexes_[index_id] = index_info;

  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if (tables_.find(table_id) == tables_.end()) return DB_TABLE_NOT_EXIST;

  table_info = tables_[table_id];
  return DB_SUCCESS;
}
