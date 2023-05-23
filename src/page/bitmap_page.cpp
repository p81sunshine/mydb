#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  //check whether full
  if(MAX_CHARS*8==page_allocated_) return false;
  uint32_t which_byte,which_bit;
  //update
  page_offset = next_free_page_++;
  page_allocated_++;
  //get location
  which_byte = page_offset/8;
  which_bit = page_offset%8;
  //update the result
  bytes[which_byte] = (1<<which_bit) | bytes[which_byte];
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  //check whether empty
  if(IsPageFree(page_offset)==true) return false;
  uint32_t which_byte,which_bit;
  //update
  page_allocated_--;
  //get location
  which_byte = page_offset/8;
  which_bit = page_offset%8;
  //update the result
  bytes[which_byte] = (~(1<<which_bit)) & bytes[which_byte];
  uint32_t i,j;
  for(i=0;i<MAX_CHARS;i++){
    if(bytes[i]!=0xff){
      for(j = 0;j < 8;j++){
        if((bytes[i]>>j&1) == 0){
          next_free_page_ = i*8+j;
          break;
        }
      }
      break;
    }
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t which_byte,which_bit;
  which_byte = page_offset/8;
  which_bit = page_offset%8;
  return IsPageFreeLow(which_byte,which_bit);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t which_byte, uint8_t which_bit) const {
  uint32_t one_bit = this->bytes[which_byte]>>which_bit & 1;
  if(one_bit==0) return true;
  else return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;