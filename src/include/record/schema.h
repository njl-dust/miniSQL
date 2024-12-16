#include <cstdint>
#include <iostream>
#include <vector>

#include "common/dberr.h"
#include "common/macros.h"
#include "glog/logging.h"
#include "record/column.h"
#include "common/heap.h"

#ifndef MINISQL_SCHEMA_H
#define MINISQL_SCHEMA_H
using namespace std;
class Schema {
 public:
  explicit Schema(const std::vector<Column *> columns, bool is_manage_ = true)
      : columns_(std::move(columns)), is_manage_(is_manage_) {}  //note:std move!!!!

  ~Schema() {
    if (is_manage_) {
      for (auto column : columns_) {
        delete column;
      }
    }
  }

  inline const std::vector<Column *> &GetColumns() const { return columns_; }

  inline const Column *GetColumn(const uint32_t column_index) const { return columns_[column_index]; }

  dberr_t GetColumnIndex(const std::string &col_name, uint32_t &index) const {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
      if (columns_[i]->GetName() == col_name) {
        index = i;
        return DB_SUCCESS;
      }
    }
    return DB_COLUMN_NAME_NOT_EXIST;
  }

  inline uint32_t GetColumnCount() const { return static_cast<uint32_t>(columns_.size()); }

  /**
   * Shallow copy schema, only used in index
   *
   * @param: attrs Column index map from index to tuple
   * eg: Tuple(A, B, C, D)  Index(D, A) ==> attrs(3, 0)
   */
  static Schema *ShallowCopySchema(const Schema *table_schema, const std::vector<uint32_t> &attrs, MemHeap *heap) {
    std::vector<Column *> cols;
    cols.reserve(attrs.size());   //here the clos is just for temporarily store the cols_ptrs ??
    for (const auto i : attrs) {
      cols.emplace_back(table_schema->columns_[i]);
    }

    //return new Schema(cols, false);
		return ALLOC_P(heap, Schema)(cols, false);
	}

  /**
   * Deep copy schema
   */
  static Schema *DeepCopySchema(const Schema *from, MemHeap * heap) {
    std::vector<Column *> cols;
    /*
		for (uint32_t i = 0; i < from->GetColumnCount(); i++) {
      cols.push_back(new Column(from->GetColumn(i)));
    }
    return new Schema(cols, true);
		*/

		for(uint32_t i = 0; i < from->GetColumnCount(); i++)
		{
			cols.push_back(ALLOC_P(heap, Column)(from->GetColumn(i)));
		}
		return ALLOC_P(heap, Schema)(cols, true);
	}

  /**
   * Only used in table
   */
  uint32_t SerializeTo(char *buf) const;

  /**
   * Only used in table
   */
  uint32_t GetSerializedSize() const;

  /**
   * Only used in table
   */
  static uint32_t DeserializeFrom(char *buf, Schema *&schema, MemHeap * heap);

 private:
  static constexpr uint32_t SCHEMA_MAGIC_NUM = 200715;
  std::vector<Column *> columns_;
  bool is_manage_ = false; /** if false, don't need to delete pointer to column */
};

using IndexSchema = Schema;
using TableSchema = Schema;
#endif
