#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
    char *p = buf;
    uint32_t ofs = GetSerializedSize();
    ASSERT(ofs <= PAGE_SIZE, "Failed to serialize table info.");
    // magic num
    MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);
    buf += 4;
    // table id
    MACH_WRITE_TO(table_id_t, buf, table_id_);
    buf += 4;
    // table name
    MACH_WRITE_UINT32(buf, table_name_.length());
    buf += 4;
    MACH_WRITE_STRING(buf, table_name_);
    buf += table_name_.length();
    // table heap root page id
    MACH_WRITE_TO(page_id_t, buf, root_page_id_);
    buf += 4;
    // table schema
    buf += schema_->SerializeTo(buf);

    
    // primary keys size
    int size = primaryKeys_.size();
    MACH_WRITE_UINT32(buf, size);
    buf += 4;

    // primary keys
    for(int i = 0;i < primaryKeys_.size();i++)
    {
        MACH_WRITE_STRING(buf, primaryKeys_[i]);
        buf += primaryKeys_[i].length();
    }

    // unique keys size
    size = uniqueKeys_.size();
    MACH_WRITE_UINT32(buf, size);
    buf += 4;

    // unique keys
    for(int i = 0;i < uniqueKeys_.size();i++)
    {
        MACH_WRITE_STRING(buf, uniqueKeys_[i]);
        buf += uniqueKeys_[i].length();
    }
    ASSERT(buf - p == ofs, "Unexpected serialize size.");
    return ofs;
}


/**
 * TODO: Student Implement
 */
uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t ofs = 4 + 4 + 4 + table_name_.length() + 4 
  + schema_->GetSerializedSize() + 8;
    int size = primaryKeys_.size();
    for(int i = 0;i < size; i++)
        ofs += primaryKeys_[i].length();
    
    size = uniqueKeys_.size();
    for(int i = 0;i < size; i++)
        ofs += uniqueKeys_[i].length();
        
    return ofs;
}

uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap* heap) {
		if (table_meta != nullptr) {
        LOG(WARNING) << "Pointer object table info is not null in table info deserialize." << std::endl;
    }
    char *p = buf;
    // magic num
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == TABLE_METADATA_MAGIC_NUM, "Failed to deserialize table info.");
    // table id
    table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    // table name
    uint32_t len = MACH_READ_UINT32(buf);
    buf += 4;
    std::string table_name(buf, len);
    buf += len;
    // table heap root page id
    page_id_t root_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    // table schema
    TableSchema *schema = nullptr;
    buf += TableSchema::DeserializeFrom(buf, schema, heap);
    // primary keys size
    int p_size = MACH_READ_UINT32(buf);
    // primary keys
    vector<string> primaryKeys;
    for(int i = 0;i < p_size;i++)
    {
        string s = MACH_READ_FROM(string, buf);
        buf += s.length();
        primaryKeys.push_back(s);
    }
    // unique keys
    int u_size = MACH_READ_UINT32(buf);
    // unique keys
    vector<string> uniqueKeys;
    for(int i = 0;i < u_size;i++)
    {
        string s = MACH_READ_FROM(string, buf);
        buf += s.length();
        uniqueKeys.push_back(s);
    }
    // allocate space for table metadata
	table_meta = ALLOC_P(heap, TableMetadata)(table_id, table_name, root_page_id,schema, primaryKeys, uniqueKeys);
	return buf - p;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  //return new TableMetadata(table_id, table_name, root_page_id, schema);
	return ALLOC_P(heap, TableMetadata)(table_id, table_name, root_page_id, schema);
}


TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema, vector<string> primaryKeys, vector<string> uniqueKeys, MemHeap *heap) {
  // allocate space for table metadata
  //return new TableMetadata(table_id, table_name, root_page_id, schema);
	return ALLOC_P(heap, TableMetadata)(table_id, table_name, root_page_id, schema, primaryKeys, uniqueKeys);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema, vector<string> primaryKeys, vector<string> uniqueKeys)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema), primaryKeys_(primaryKeys), uniqueKeys_(uniqueKeys) {}
