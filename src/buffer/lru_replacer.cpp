#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  //是否为空
  if(lru_list.size()==0){
    return false;
  }
  else{
    *frame_id = lru_list.back();
    hash_map.erase(*frame_id);
    lru_list.pop_back();
    return true;
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(hash_map.count(frame_id)==0){
    return ;
  }
  else{
    lru_list.erase(hash_map[frame_id]);
    hash_map.erase(frame_id);  
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(hash_map.count(frame_id)){
    return ;
  }
  else{
    lru_list.push_front(frame_id);
    hash_map[frame_id]=lru_list.begin();
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list.size();
}