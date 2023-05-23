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

uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset=0;
  MACH_WRITE_UINT32(buf+offset, COLUMN_MAGIC_NUM);
  offset+=4;
  MACH_WRITE_UINT32(buf+offset, this->name_.length()); 
  offset+=4;
  MACH_WRITE_STRING(buf+offset, this->name_);  
  offset+=name_.length(); 
  MACH_WRITE_TO(TypeId, (buf+offset), (this->type_));  
  offset+=4;
  MACH_WRITE_UINT32(buf+offset, this->len_);
  offset+=4;
  MACH_WRITE_UINT32(buf+offset, this->table_ind_);
  offset+=4;
  MACH_WRITE_UINT32(buf+offset, this->nullable_); 
  offset+=4;
  MACH_WRITE_UINT32(buf+offset, this->unique_); 
  offset+=4;
  return 0;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  return sizeof(uint32_t) *7 + name_.size();
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t offset=0;
  if(buf==NULL) return 0;
  uint32_t Magic_Number=MACH_READ_FROM(uint32_t, buf+offset);//1-Read the Magic_Number
  if(Magic_Number!=210928) return 0;
  offset+=4;
  uint32_t length=MACH_READ_FROM(uint32_t, buf+offset);  //2-Read the length of the name_
  offset+=4;
  std::string  column_name;
  for(uint32_t i=0;i<length;i++)    //3-Read the Name from the buf
  {
    column_name.push_back(buf[i+offset]);
  }
  offset+=length;
  TypeId type=MACH_READ_FROM(TypeId, buf+offset);  //4-Read the type
  offset+=4;
  uint32_t len_=MACH_READ_FROM(uint32_t,buf+offset); //5-Read the len_
  offset+=4;
  uint32_t col_ind=MACH_READ_FROM(uint32_t,buf+offset);  //6-Read the col_ind
  offset+=4;
  bool nullable=MACH_READ_FROM(uint32_t,buf+offset); //7-Read the nullable
  offset+=4;
  bool unique=MACH_READ_FROM(uint32_t,buf+offset); //8-Read the unique
  offset+=4;
  if (type == kTypeInt || type == kTypeFloat) {
    column = new Column(column_name, type, col_ind, nullable, unique);
  } else if (type == kTypeChar) {
    column = new Column(column_name, type, len_, col_ind, nullable, unique);
  }
  return offset;
}
