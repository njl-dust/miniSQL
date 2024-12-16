#ifndef MINISQL_ROW_H
#define MINISQL_ROW_H

#include <memory>
#include <vector>

#include "common/macros.h"
#include "common/rowid.h"
#include "record/field.h"
#include "record/schema.h"
#include "common/heap.h"
using namespace std;
/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 *
 *
 */
class Row {
 public:
  /**
   * Row used for insert
   * Field integrity should check by upper level
   */
  Row(std::vector<Field> &fields): col_heap_(new SimpleMemHeap) {
    // deep copy   but for char*, maybe shallow 
    for (auto &field : fields) {
      //fields_.push_back(new Field(field));   
			//version 2 : allocate from mem heap
			Field* f = ALLOC_P(col_heap_, Field)(field);
			fields_.push_back(f);
    }
  }

  void destroy() {
		/*
    if (!fields_.empty()) {
      for (auto field : fields_) {
        delete field;
      }
      fields_.clear();
    }
		*/
		delete col_heap_; //when delete mem_heap, all the memory allocate by this mem-heap will be free!!
  }

  ~Row() { destroy(); };

  /**
   * Row used for deserialize
   */
  //Row() = default;
	Row(): rid_(INVALID_ROWID), col_heap_(new SimpleMemHeap)
	{}

  /**
   * Row used for deserialize and update
   */
	//NOTE: initialize the heap!!!
  Row(RowId rid) : rid_(rid), col_heap_(new SimpleMemHeap) {}

  /**
   * Row copy function, deep copy
   */
  Row(const Row &other):col_heap_(new SimpleMemHeap) {  //note : every Row must have its own mem-heap!!
    /*
		destroy();
    rid_ = other.rid_;
    for (auto &field : other.fields_) {
      fields_.push_back(new Field(*field));
    }
		*/
		rid_ = other.rid_;
		if(!fields_.empty())
		{ //clear
			for(auto & ele : fields_)
				col_heap_->Free(ele); 
			fields_.clear();
		}

		for(auto& ele : other.fields_)
		{
			Field* f = ALLOC_P(col_heap_, Field)(*ele);
			fields_.push_back(f);
		}
  }

  /**
   * Assign operator, deep copy
   */
  Row &operator=(const Row &other) {
    /*
		destroy(); 
		rid_ = other.rid_;
    for (auto &field : other.fields_) {
      fields_.push_back(new Field(*field));
    }
		*/
		rid_ = other.rid_;
		if(!fields_.empty())
		{ //clear
			for(auto & ele : fields_)
				col_heap_->Free(ele); 
			fields_.clear();
		}

		for(auto& ele : other.fields_)
		{
			Field* f = ALLOC_P(col_heap_, Field)(*ele);
			fields_.push_back(f);
		}

    return *this;
  }

  /**
   * Note: Make sure that bytes write to buf is equal to GetSerializedSize()
   */
  uint32_t SerializeTo(char *buf, Schema *schema) const;

  uint32_t DeserializeFrom(char *buf, Schema *schema);

  /**
   * For empty row, return 0
   * For non-empty row with null fields, eg: |null|null|null|, return header size only
   * @return
   */
  uint32_t GetSerializedSize(Schema *schema) const;

  void GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row);

  inline const RowId GetRowId() const { return rid_; }

  inline void SetRowId(RowId rid) { rid_ = rid; }

  inline std::vector<Field *> &GetFields() { return fields_; }

  inline Field *GetField(uint32_t idx) const {
    ASSERT(idx < fields_.size(), "Failed to access field");
    return fields_[idx];
  }

  inline size_t GetFieldCount() const { return fields_.size(); }

 private:
  RowId rid_{};
  std::vector<Field *> fields_; /** Make sure that all field ptr are destructed*/
	MemHeap * col_heap_{nullptr};
};

#endif  // MINISQL_ROW_H
