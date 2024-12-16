#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "common/macros.h"
#include "common/rowid.h"
#include "index/b_plus_tree_index.h"
#include "index/generic_key.h"
#include "record/schema.h"
#include "common/heap.h"

class IndexMetadata {
  friend class IndexInfo;

 public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                               const std::vector<uint32_t> &key_map, MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

 private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                         const std::vector<uint32_t> &key_map);

 private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_; /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
 public:
  static IndexInfo *Create(MemHeap *heap) 
	{ 
		//return new IndexInfo(); 
		return ALLOC_P(heap, IndexInfo)();
	}

  ~IndexInfo() {
		delete i_heap_;
  }

/**
 * TODO: Student Implement
 */
  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    // Step2: mapping index key to key schema
    // Step3: call CreateIndex to create the index
    // ASSERT(false, "Not Implemented yet.");
		meta_data_ = meta_data;
		table_info_ = table_info;
		
		//shallow copy the cols into key_schema_  e.g.  key_map_ ={3,1}  table_schema = {name,sex,year,weight},  key_schema_ = {weight, sex}
    key_schema_ = Schema::ShallowCopySchema(table_info_->GetSchema(), meta_data_->GetKeyMapping(), i_heap_);
    
		this->index_ = CreateIndex(buffer_pool_manager);  //auto-free when i_heap_ is destoyed
  }

  inline Index *GetIndex() { return index_; }
	
	inline TableInfo* GetTableInfo() {return table_info_;}
  
	//add
	inline MemHeap *GetMemHeap() { return i_heap_; }

  std::string GetIndexName() { return meta_data_->GetIndexName(); }

  IndexSchema *GetIndexKeySchema() { return key_schema_; }

 private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, key_schema_{nullptr}, i_heap_(new SimpleMemHeap()) {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager, const string &index_type = "bptree");

 private:
  IndexMetadata *meta_data_;
  Index *index_;
  IndexSchema *key_schema_;
	//add
	TableInfo *table_info_;
	MemHeap *i_heap_; 
};

#endif  // MINISQL_INDEXES_H
