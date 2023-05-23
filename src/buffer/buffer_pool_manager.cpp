#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  try{
    //exist
    frame_id_t Pid = page_table_.at(page_id);
    //update
    pages_[Pid].pin_count_++;
    //use pin function
    replacer_->Pin(Pid);
    return &pages_[Pid];
  }
  catch(const std::out_of_range& e){
    //1.2
    frame_id_t frame_id;
    if(free_list_.empty()==true){
      if (!replacer_->Victim(&frame_id)) {
        return nullptr;
      }
    }
    else{
      frame_id = free_list_.front();
      free_list_.pop_front();
    }
    //2
    frame_id_t Rid;
    Rid = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()==true){
      FlushPage(Rid);
    }
    //3
    page_table_.erase(Rid);
    page_table_[page_id] = frame_id;
    Page *P = &pages_[frame_id];
    //4
    P->page_id_ = page_id;
    P->pin_count_ = 1;
    P->is_dirty_ = false;
    disk_manager_->ReadPage(page_id, P->GetData());
    return P;
  }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    frame_id_t frame_id;
    if(free_list_.empty()==true){
      if (!replacer_->Victim(&frame_id)) {
        return nullptr;
      }
      page_table_.erase(pages_[frame_id].GetPageId());
    }
    else{
      frame_id = free_list_.front();
      free_list_.pop_front();
      
    }
    page_id_t Pid;
    Pid = AllocatePage();
    Page *P = &pages_[frame_id];
    P->page_id_ = Pid;
    P->pin_count_ = 1;
    P->is_dirty_ = true;
    P->ResetMemory();
    page_table_[Pid] = frame_id;
    page_id = Pid;
    return P;
}

/**
 * TODO: Student Implement
 */

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  DeallocatePage(page_id);
    frame_id_t P_frame_id;
  try{ 
    P_frame_id = page_table_.at(page_id); // may throw err out_of_range
  }
  catch(const std::out_of_range& e){ 
    return true;
  }
  if (pages_[P_frame_id].GetPinCount()){
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Pin(P_frame_id);
  Page *P = &pages_[P_frame_id];
  P->page_id_ = INVALID_PAGE_ID;
  P->pin_count_ = 0;
  P->is_dirty_ = false;
  free_list_.push_back(P_frame_id);
  return true;
}
/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  frame_id_t frame_id = page_table_[page_id];
  // decrement pin count (lower bound is 0)
  pages_[frame_id].pin_count_ = pages_[frame_id].pin_count_-1;
  if(pages_[frame_id].pin_count_<0){
    pages_[frame_id].pin_count_ = 0;
  }
  if(!pages_[frame_id].pin_count_){
    replacer_->Unpin(frame_id);
  }
  if (is_dirty){
    pages_[frame_id].is_dirty_ = true;
  }
  return true;  
}


/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  //写入后更新脏数据
  pages_[frame_id].is_dirty_ = false;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}