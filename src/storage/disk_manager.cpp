#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>
#include <iostream>
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include <stdio.h>

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage* meta_inf = reinterpret_cast<DiskFileMetaPage*>(this->meta_data_);
  int mark = 0;
  page_id_t page_res = 0;
  uint32_t temp_offest = 0;
  uint32_t temp_page;
  char bitmap[PAGE_SIZE];
  uint32_t i;
  //已有的块进行遍历查询
  for(i=0;i<meta_inf->GetExtentNums();i++){
    //查看哪个块有剩余
    if(meta_inf->GetExtentUsedPage(i)<BITMAP_SIZE){
      mark = 1;
      temp_page = i*(BITMAP_SIZE + 1) + 1;
      ReadPhysicalPage(temp_page,bitmap);
      //找到位图信息
      BitmapPage<PAGE_SIZE>* Bitmap_inf = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap);
      //重新分配
      Bitmap_inf->AllocatePage(temp_offest);
      //更新结果
      page_res = i*BITMAP_SIZE + temp_offest;
      meta_inf->extent_used_page_[i]++;
      meta_inf->num_allocated_pages_++;
      WritePhysicalPage(temp_page,bitmap);
      break;
    }
  }
  if(mark==0){
    temp_page = meta_inf->num_extents_*(BITMAP_SIZE + 1) + 1;
    ReadPhysicalPage(temp_page,bitmap);
    BitmapPage<PAGE_SIZE>* Bitmap_inf = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap);
    Bitmap_inf->AllocatePage(temp_offest);
    page_res = meta_inf->num_extents_*BITMAP_SIZE + temp_offest;
    meta_inf->extent_used_page_[meta_inf->num_extents_]++;
    meta_inf->num_allocated_pages_++;
    meta_inf->num_extents_++;
    WritePhysicalPage(temp_page,bitmap);
  } 
  return page_res;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  uint32_t which_loc,which_pos;
  which_loc = logical_page_id/BITMAP_SIZE;
  which_pos = logical_page_id%BITMAP_SIZE;
  DiskFileMetaPage* meta_inf = reinterpret_cast<DiskFileMetaPage*>(this->meta_data_);
  char bitmap[PAGE_SIZE];
  uint32_t temp_page =  which_loc*(BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(temp_page,bitmap);
  BitmapPage<PAGE_SIZE>* Bitmap_inf = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap);  
  Bitmap_inf->DeAllocatePage(which_pos);
  meta_inf->num_allocated_pages_--;
  meta_inf->extent_used_page_[which_loc]--;
  WritePhysicalPage(temp_page,bitmap);
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t which_loc,which_pos;
  which_loc = logical_page_id/BITMAP_SIZE;
  which_pos = logical_page_id%BITMAP_SIZE;
  char bitmap[PAGE_SIZE];
  uint32_t temp_page =  which_loc*(BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(temp_page,bitmap);
  BitmapPage<PAGE_SIZE>* Bitmap_inf = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap);  
  return Bitmap_inf->IsPageFree(which_pos);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  uint32_t used_page = logical_page_id/BITMAP_SIZE + 1; //已用位图页数量
  return used_page + logical_page_id + 1;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  // std::cout<<"ReadPhysicalPage\n";
  // std::cout<<physical_page_id<<std::endl;
  // std::cout<<"--------------------------------\n";
  // for(int i=0;i<=8;i++){
  //   printf("%u\n",page_data[i]);
  // }
  // std::cout<<"--------------------------------\n";
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  // std::cout<<"WritePhysicalPage\n";
  // std::cout<<physical_page_id<<std::endl;
  // std::cout<<"--------------------------------\n";
  // for(int i=0;i<=8;i++){
  //   printf("%u\n",page_data[i]);
  // }
  // std::cout<<"--------------------------------\n";
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}