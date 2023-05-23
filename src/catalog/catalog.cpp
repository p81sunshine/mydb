#include "catalog/catalog.h"
#include <algorithm>
#include <set>
#include <iostream>


void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  //按照class中的定义一个一个累加即可
  uint32_t offset = 0;
  offset += 4;
  offset += 4;
  offset += 4;
  offset += table_meta_pages_.size()*8;
  offset += index_meta_pages_.size()*8;
  return offset;
}

 CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  //如果是新创建的情况，直接new一下
  if(init==true){
    catalog_meta_=CatalogMeta::NewInstance();
  }
  else{
    //从缓存池中读取meta页
    Page *page=buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    //create a new space
    catalog_meta_=CatalogMeta::NewInstance();
    //反序列化获取数据
    this->catalog_meta_=CatalogMeta::DeserializeFrom(page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
    //获取迭代器的首位，以便之后的逐步更新
    auto iter=catalog_meta_->table_meta_pages_.begin();//更新table
    while(iter!=catalog_meta_->table_meta_pages_.end()){
      //将内容一个一个更新
      LoadTable(iter->first,iter->second);
      iter++;
    }
    iter=this->catalog_meta_->index_meta_pages_.begin();//更新index
    while(iter!=this->catalog_meta_->index_meta_pages_.end()){
      LoadIndex(iter->first,iter->second);
      iter++;
    }
  }     
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  // for (auto iter : tables_) {
  //   delete iter.second;
  // }
  // for (auto iter : indexes_) {
  //   delete iter.second;
  // }
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  //查看表名是否已经存在
  if(table_names_.count(table_name)>0){
    return DB_TABLE_ALREADY_EXIST;
  }            
  
  //需要创建一个TableInfo，而TableInfo里面有两个部分，TableHeap和TableMetadata
  //所以需要先把TableHeap和TableMetadata的初始化做好，就可以创建新的TableInfo
  TableHeap *th=TableHeap::Create(this->buffer_pool_manager_,schema,txn,this->log_manager_,this->lock_manager_);
  //获取下一个表的位置id
  table_id_t tableID=this->catalog_meta_->GetNextTableId();
  TableMetadata *tm=TableMetadata::Create(tableID,table_name,th->GetFirstPageId(),schema);
  table_info=TableInfo::Create();
  //利用th和tm对TableInfo进行初始化
  table_info->Init(tm,th);
  page_id_t pageID;
  Page *page=buffer_pool_manager_->NewPage(pageID);  
  //更新表单
  catalog_meta_->table_meta_pages_[tableID]=pageID;
  table_names_[table_name]=tableID;
  tables_[tableID]=table_info;
  //序列化写入
  tm->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(pageID, true);                         
  return DB_SUCCESS;
}




/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  //首先查看有没有这个表
  if(this->table_names_.count(table_name)==0){
    return DB_TABLE_NOT_EXIST;
  }
  //通过名字获取id
  table_id_t table_id = table_names_[table_name];
  //通过id获取表
  table_info=tables_[table_id];  
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  auto iter=table_names_.begin();
  while(iter!=table_names_.end()){
    tables.push_back(tables_.at(iter->second));
    iter++;
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  if(table_names_.find(table_name)==table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  if(index_names_.find(table_name)!=index_names_.end()){
    return DB_INDEX_ALREADY_EXIST;
  }
  index_info = IndexInfo::Create();
  //找到下一个index的id
  index_id_t index_id = catalog_meta_->GetNextIndexId();
  //根据名字找到对应的表
  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info = tables_[table_id];
  std::vector<uint32_t> key_map;
  uint32_t key_index;
  //把index_keys作为参数，找到每个index对应的column
  for(uint32_t i=0; i<index_keys.size(); i++){
    if(table_info->GetSchema()->GetColumnIndex(index_keys[i], key_index) == DB_COLUMN_NAME_NOT_EXIST){
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    else{
      key_map.push_back(key_index);
    }
  }
  IndexMetadata *im = IndexMetadata::Create(index_id, index_name, table_id, key_map);
  index_info->Init(im, table_info, buffer_pool_manager_);
  //更新index_names_
  unordered_map<std::string, index_id_t> temp;
  temp[index_name] = index_id;
  index_names_[table_name] = temp;
  indexes_[index_id] = index_info;
  //create a new page to store the index
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if(im->GetSerializedSize() > PAGE_SIZE){
    return DB_FAILED;
  }
  im->SerializeTo(page->GetData());
  //update catalog_mega
  catalog_meta_->index_meta_pages_[index_id] = page_id;
  buffer_pool_manager_->UnpinPage(page_id, true);
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}


// /**
//  * TODO: Student Implement
//  */
// dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
//                                     const std::vector<std::string> &index_keys, Transaction *txn,
//                                     IndexInfo *&index_info, const string &index_type) {
//   if(table_names_.count(table_name)==0){
//     return DB_TABLE_NOT_EXIST;
//   }   
//   index_id_t nextID=this->catalog_meta_->GetNextIndexId();
//   //获取table
//   TableInfo *tf=tables_.at(table_names_.at(table_name)); 
//   page_id_t pageID;
//   Page *page=buffer_pool_manager_->NewPage(pageID);  
//   vector<uint32_t> record;
//   uint32_t index_id;
//   vector<Column *> col=tf->GetSchema()->GetColumns();
//   //一个一个读取
//   for(uint32_t i=0;i<index_keys.size();i++){     
//     if(tf->GetSchema()->GetColumnIndex(index_keys[i],index_id)!=DB_COLUMN_NAME_NOT_EXIST){
//       record.push_back(index_id);
//     }
//     else{
//       return DB_COLUMN_NAME_NOT_EXIST;
//     }
//   }
//   //check primary key
//   vector<uint32_t> primary = tf->GetPrimaryKeyIndexs();
//   sort(record.begin(), record.end());
//   sort(primary.begin(), primary.end());
//   bool isPrimaryKey = false;
//   if((record == primary)){
//     isPrimaryKey = true;
//   }
//   bool is_set_unique = false;   //只要建索引的集合里有一个unique即可
//   if (!isPrimaryKey){
//     for (auto &i : record) {
//       if(col[i]->IsUnique()==true){    //unique为true是需要唯一
//           is_set_unique = true;
//           break;
//       }
//     }
//   }   
//   IndexMetadata *mdata = IndexMetadata::Create(this->catalog_meta_->GetNextIndexId(),index_name,this->table_names_.at(table_name),record);
//   index_info = IndexInfo::Create();
//   index_info->Init(mdata,tf,buffer_pool_manager_);
//   //insert one by one
//   for(TableIterator iter = tf->GetTableHeap()->Begin(nullptr);!iter.isNull(); iter++){
//     Row nowrow = *iter;
//     Row keyrow(nowrow,record);
//     auto ret = index_info->GetIndex()->InsertEntry(keyrow, nowrow.GetRowId(), txn);
//     if (ret == DB_FAILED){
//       // duplicated, rollback
//       buffer_pool_manager_->UnpinPage(pageID, false);
//       buffer_pool_manager_->DeletePage(pageID);
//       return DB_COLUMN_NOT_UNIQUE;
//     }      
//   }
//   if(is_set_unique == false){
//     for (auto &i : record){
//       col[i]->SetUnique(true);
//     }
//   }
//   mdata->SerializeTo(page->GetData());
//   buffer_pool_manager_->UnpinPage(pageID, true);
//   unordered_map<std::string, index_id_t> recordMap;
//   if(index_names_.count(table_name)){
//     recordMap=index_names_.at(table_name);
//   }
//   recordMap[index_name]=nextID;
//   index_names_[table_name]=recordMap;
//   indexes_[nextID]=index_info;
//   catalog_meta_->index_meta_pages_[nextID]=page->GetPageId();
//   return DB_SUCCESS;  
// }

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {

  if(this->index_names_.count(table_name)==0){
    return DB_TABLE_NOT_EXIST;
  }
  else if(this->index_names_.at(table_name).count(index_name)==0){
    return DB_INDEX_NOT_FOUND;
  }
  else{
    index_info=this->indexes_.at(this->index_names_.at(table_name).at(index_name));
    return DB_SUCCESS;    
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  unordered_map<std::string, index_id_t> tmp=this->index_names_.at(table_name);
  auto iter=tmp.begin();
  while(iter!=tmp.end()){
    indexes.push_back(this->indexes_.at(iter->second));
    iter++;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if(this->table_names_.count(table_name)==0){
    return DB_TABLE_NOT_EXIST;
  }
  unordered_map<std::string, index_id_t> tmp=index_names_.at(table_name);
  //把关于这个表的index删除
  auto iter=tmp.begin();
  while(iter!=tmp.end()){
    this->DropIndex(table_name, iter->first);
    iter++;
  }
  //从indexname中删除这张表
  this->index_names_.erase(table_name);
  //删除这张表
  buffer_pool_manager_->DeletePage(this->catalog_meta_->table_meta_pages_.at(this->table_names_.at(table_name)));
  this->catalog_meta_->table_meta_pages_.erase(this->table_names_.at(table_name));
  //删除TableHeap
  this->tables_[this->table_names_.at(table_name)]->GetTableHeap()->FreeTableHeap();
  //更新相应的值
  this->tables_.erase(this->table_names_.at(table_name));
  this->table_names_.erase(table_name);
  return DB_SUCCESS;  
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(this->index_names_.at(table_name).count(index_name)==0){
    return DB_INDEX_NOT_FOUND;
  }
  this->indexes_.at(this->index_names_.at(table_name).at(index_name))->GetIndex()->Destroy();
  //删除这个index
  buffer_pool_manager_->DeletePage( this->catalog_meta_->index_meta_pages_.at(this->index_names_.at(table_name).at(index_name)) );
  this->catalog_meta_->index_meta_pages_.erase(this->index_names_.at(table_name).at(index_name));
  this->indexes_.erase(this->index_names_.at(table_name).at(index_name));
  this->index_names_.at(table_name).erase(index_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // if(buffer_pool_manager_->DeletePage(CATALOG_META_PAGE_ID)){
  //   return DB_SUCCESS;
  // }
  // else{
  //   return DB_FAILED;
  // }
  auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  TableInfo *table = TableInfo::Create();
  Page *pge=buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *mdata=nullptr;
  //反序列化获取数据
  TableMetadata::DeserializeFrom(pge->GetData(),mdata);
  //更新数据
  table_names_[mdata->GetTableName()]=table_id;
  TableHeap *th=TableHeap::Create(buffer_pool_manager_,mdata->GetFirstPageId(),mdata->GetSchema(),log_manager_,lock_manager_);
  table->Init(mdata,th);
  tables_[table_id]=table;
  buffer_pool_manager_->UnpinPage(page_id,false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
//need to check
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  Page *page=buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *im=nullptr;
  //反序列化获取数据
  IndexMetadata::DeserializeFrom(page->GetData(),im);
  table_id_t table_id = im->GetTableId();
  std::string index_name = im->GetIndexName();
  //找出对应的表
  TableInfo *table_info = tables_[table_id];
  IndexInfo *index_info = IndexInfo::Create();
  std::string table_name = table_info->GetTableName();
  if(table_names_.find(table_name) == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  if(index_names_.find(table_name)==index_names_.end()){//没有这个table的index map
    unordered_map<std::string, index_id_t> map;
    map[index_name] = index_id;
    index_names_[table_name] = map; 
  }
  else{
    index_names_[table_name][index_name] = index_id;
  }
  index_info->Init(im, table_info, buffer_pool_manager_);
  indexes_[index_id] = index_info;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(this->tables_.count(table_id)==0){
    return DB_TABLE_NOT_EXIST;
  }  
  else{
    table_info = tables_.at(table_id);
    return DB_SUCCESS;
  }
}

