#ifndef MINISQL_RID_H
#define MINISQL_RID_H

#include <cstdint>

#include "common/config.h"

/**
 * | page_id(32bit) | slot_num(32bit) |
 */
class RowId {
 public:
  RowId() = default;

  RowId(page_id_t page_id, uint32_t slot_num) : page_id_(page_id), slot_num_(slot_num) {}

  RowId(const RowId &rid) : page_id_(rid.page_id_), slot_num_(rid.slot_num_) {}

  explicit RowId(int64_t rid) : page_id_(static_cast<page_id_t>(rid >> 32)), slot_num_(static_cast<uint32_t>(rid)) {}

  inline int64_t Get() const { return (static_cast<int64_t>(page_id_)) << 32 | slot_num_; }

  inline page_id_t GetPageId() const { return page_id_; }

  inline uint32_t GetSlotNum() const { return slot_num_; }

  inline void Set(page_id_t page_id, uint32_t slot_num) {
    page_id_ = page_id;
    slot_num_ = slot_num;
  }

  bool operator==(const RowId &other) const { return page_id_ == other.page_id_ && slot_num_ == other.slot_num_; }
  RowId &operator=(const RowId &other) {
    page_id_ = other.page_id_;
    slot_num_ = other.slot_num_;
    return *this;
  }
  bool operator<=(const RowId &other) const {
    if (page_id_ < other.page_id_)
      return true;
    else if (page_id_ == other.page_id_)
      return slot_num_ <= other.slot_num_;
    else
      return false;
  }
  bool operator<(const RowId &other) const {
    if (page_id_ < other.page_id_)
      return true;
    else if (page_id_ == other.page_id_)
      return slot_num_ < other.slot_num_;
    else
      return false;
  }

 private:
  page_id_t page_id_{INVALID_PAGE_ID};
  uint32_t slot_num_{0};  // logical offset of the record in page, starts from 0. eg:0, 1, 2...
};

static const RowId INVALID_ROWID = RowId(INVALID_PAGE_ID, 0);

#endif  // MINISQL_RID_H