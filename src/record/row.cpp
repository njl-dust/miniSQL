#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
	// schema useless??

	uint32_t fs = fields_.size(), offset = 0;
	if(fs == 0)
		return 0;
	
	MACH_WRITE_UINT32(buf, fs);  //write field num into buf
	buf += 4;
	offset += 4;

	char bits = 0;
	uint32_t map[fs];
	for(int i = 0;i < fs; i++)
	{
		if(!fields_[i]->IsNull())
		{
			bits |= 0x80 >> (i%8);
			map[i] = 1;
		}
		else
			map[i] = 0;

		if(i%8 == 7)   //write into buf per Byte
		{
			MACH_WRITE_TO(char, buf, bits);  //write bits into buf
			bits = 0;
			buf += 1;
			offset += 1;
		}
	}
	
	if((fs % 8) != 0)
	{
		MACH_WRITE_TO(char, buf, bits);  //write bits into buf
		bits = 0;
		buf += 1;
		offset += 1;
	}

	for(int i = 0;i < fs;i++)
	{
		if(map[i])
		{
			uint32_t offset_ = fields_[i]->SerializeTo(buf);
			buf += offset_;
			offset += offset_;
		}
	}

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  //ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
	
	uint32_t offset = 0;
	uint32_t fs = MACH_READ_UINT32(buf);
	buf += 4;
	offset += 4;

	uint32_t map[fs];
	char bits = 0;
	for(int i = 0;i < fs;i++)
	{
		if(i%8 == 0)  //read 8 bits 
		{
			bits = MACH_READ_FROM(char, buf);
			buf += 1;
			offset += 1;
		}
		map[i] = bits & (0x80 >> (i%8));
	}


	for(int i = 0;i < fs;i++)
	{
		TypeId type = schema->GetColumn(i)->GetType();
		
		Field *field;    //TODO: for char type, how to detect manage ?
		
		if(map[i])
		{
			uint32_t offset_;
			
			if (type == TypeId::kTypeInt) {
			//field = new Field(TypeId::kTypeInt, 0);
				offset_ = TypeInt().DeserializeFrom(buf, &field, false, col_heap_);
			}else if (type == TypeId::kTypeChar) {
				offset_ = TypeChar().DeserializeFrom(buf, &field, false, col_heap_);
			}else if (type == TypeId::kTypeFloat) {

				offset_ = TypeFloat().DeserializeFrom(buf, &field, false, col_heap_);
			}
			
			//offset_ = Type::GetInstance(type)->DeserializeFrom(buf, &field, false, col_heap_);
			buf += offset_;
			offset += offset_;
		}else{	
			uint32_t offset_;
			/*
			if (type == TypeId::kTypeInt) {
			//field = new Field(TypeId::kTypeInt, 0);
				offset_ = TypeInt::DeserializeFrom(buf, &field, true, col_heap_);
			}else if (type == TypeId::kTypeChar) {
				offset_ = TypeChar::DeserializeFrom(buf, &field, true, col_heap_);
			}else if (type == TypeId::kTypeFloat) {
				offset_ = TypeFloat::DeserializeFrom(buf, &field, true, col_heap_);
			}
			*/

			offset_ = Type::GetInstance(type)->DeserializeFrom(buf, &field, true, col_heap_);
			buf += offset_;
			offset += offset_;
		}
		
		fields_.push_back(field);
	}
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
	if(fields_.size() == 0)
		return 0;
	
	uint32_t ss = 0;
	ss += 4;  //field num
	ss += (uint32_t)ceil((double)fields_.size() / 8);  // bit/8

	for(int i = 0;i < fields_.size();i++)
	{
		if(!fields_[i]->IsNull())
		{
			ss += fields_[i]->GetSerializedSize();
		}
	}
  return ss;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
