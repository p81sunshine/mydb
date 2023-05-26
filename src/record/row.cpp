#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  //get bitmap len
  uint32_t len = fields_.size()/8+1;
  char* bitmap = new char[len];
  memset(bitmap, 0, len);
  uint32_t offset = 0;
  //write into a uint32
  MACH_WRITE_UINT32(buf,fields_.size());
  offset = offset + 4;
  //get bitmap
  uint32_t which_byte,which_bit;
  for(uint32_t i = 0;i<fields_.size();i++){
    if(fields_[i]->IsNull()){
      which_byte = i/8;
      which_bit = i%8;
      bitmap[which_byte] |= 1<<which_bit;
    }
  }
  //write the bitmap into the buf
  for(uint32_t i;i<len;i++){
    MACH_WRITE_TO(char, buf + i + offset, bitmap[i]);
  }
  offset += len;
  //write the field into the buf
  for(auto &field : fields_){
    if(field!=nullptr){
      offset += field->SerializeTo(buf+offset);
    }
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  if(buf==NULL) return 0;
  uint32_t offset = 0;
  //read field_.size()
  uint32_t num = MACH_READ_FROM(uint32_t, buf+offset);
  uint32_t len = num/8 + 1;
  offset = offset + 4;
  char *bitmap = new char[len];
  //read bitmap
  for (uint32_t i=0;i<len;i++){
    bitmap[i] = *(buf + i + offset);
  }
  offset += len;
  uint32_t which_byte,which_bit;
  for(uint32_t i = 0; i < num; i++){
    Field* temp = nullptr;                             // 3 - fields
    which_byte = i/8;
    which_bit = i%8;
    bool check = (bitmap[which_byte]&(1<<which_bit))!=0;
    offset += Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(),
                                  &temp, check);  
    fields_.push_back(temp);
  } 

  return offset;
}


uint32_t Row::GetSerializedSize(Schema *schema) const {
  if(this->fields_.size()==0){
    return 0;
  }

  uint32_t ofs = 0;
  ofs += 4;                                          // 1 - field_num

  uint32_t bitmap_len = fields_.size()/8 + 1;
  ofs += bitmap_len;                                 // 2 - null_bitmap

  for (auto &field : fields_){
    if( !(field==nullptr) ){
      ofs += field->GetSerializedSize();             // 3 - fields
    }
  }
  
  return ofs;
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
