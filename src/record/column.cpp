#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
	// magic  null  unique table_ind_ typeid  len_  name-len  name  
	uint32_t offset = 0;
	MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
	buf += 4;
	offset += 4;
	
	MACH_WRITE_TO(bool, buf, nullable_);
	buf += 1;
	offset += 1;

	MACH_WRITE_TO(bool, buf, unique_);
	buf += 1;
	offset += 1;

	MACH_WRITE_UINT32(buf, table_ind_);
	buf += 4;
	offset += 4;

	MACH_WRITE_TO(TypeId, buf, type_);
	buf += sizeof(TypeId);
	offset += sizeof(TypeId);

	MACH_WRITE_UINT32(buf, len_);
	buf += 4;
	offset += 4;

	MACH_WRITE_UINT32(buf, name_.length());
	buf += 4;
	offset += 4;

	MACH_WRITE_STRING(buf, name_);
	buf += name_.length();
	offset += name_.length();

  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
	uint32_t ss = 0;
	ss += 4 + 1 + 1 + 4 + sizeof(TypeId) + 4 + 4 + name_.length();
  return ss;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap* heap) {
  // replace with your code here
	// magic  null  unique table_ind_ typeid  len_  name-len  name  
	uint32_t offset = 0;
	uint32_t magic_num = MACH_READ_UINT32(buf);
	
	ASSERT(MACH_READ_UINT32(buf) == 210928, "Error in column magic num!");
	buf += 4;
	offset += 4;

	bool nullable = MACH_READ_FROM(bool, buf);
	buf += 1;
	offset += 1;

	bool unique = MACH_READ_FROM(bool, buf);
	buf += 1;
	offset += 1;

	uint32_t table_ind = MACH_READ_UINT32(buf);
	buf += 4;
	offset += 4;

	TypeId type = MACH_READ_FROM(TypeId, buf);
	buf += sizeof(TypeId);
	offset += sizeof(TypeId);	

	uint32_t len = MACH_READ_UINT32(buf);
	buf += 4;
	offset += 4;

	uint32_t name_len = MACH_READ_UINT32(buf);
	buf += 4;
	offset += 4;

	offset += name_len;
	char cname[name_len+1];
	for(int i = 0;i < name_len; i++)
	{
		cname[i] = MACH_READ_FROM(char, buf);
	}
	cname[name_len] = '\0';

	string name = cname;

	//put the column into MemHeap
	if(type == kTypeChar)
	{
		column = ALLOC_P(heap, Column)(name, type, len, table_ind, nullable, unique);  //need the len of data
	}
	else{
		column = ALLOC_P(heap, Column)(name, type, table_ind, nullable, unique);
	}
  return offset;
}
