#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
	uint32_t offset = 0;
	MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
	buf += 4;
	offset += 4;

	MACH_WRITE_UINT32(buf, (uint32_t)columns_.size());
	buf += 4;
	offset += 4;

	for(int i = 0;i < columns_.size(); i++)
	{
		columns_[i]->SerializeTo(buf);
		uint32_t cl = columns_[i]->GetSerializedSize();
		buf += cl;
		offset += cl;
	}
	
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
	uint32_t offset = 4 + 4;
	
	for(int i = 0;i < columns_.size(); i++)
	{
		offset += columns_[i]->GetSerializedSize();
	}

  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
	uint32_t offset = 0;
	uint32_t magic_num = MACH_READ_UINT32(buf);
	ASSERT(magic_num == SCHEMA_MAGIC_NUM, "error in schema magic num!");
	buf += 4;
	offset += 4;

	uint32_t cs = MACH_READ_UINT32(buf);
	buf += 4;
	offset += 4;

	vector<Column*> cols;
	for(uint32_t i = 0;i < cs; i++)
	{
		Column *col;
		Column::DeserializeFrom(buf, col, heap); // get the colunm from buf
		buf += col->GetSerializedSize();
		offset += col->GetSerializedSize();

		cols.push_back(col);
	}
	schema = ALLOC_P(heap, Schema)(cols);  //alloc the mem from memheap  the constructor of Schema will move the mem from cols to columns_
  return offset;
}
