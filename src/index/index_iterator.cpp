#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager* bpm, int index):current_page_id(page_id), item_index(index), buffer_pool_manager(bpm){
  page = reinterpret_cast<LeafPage*>(buffer_pool_manager->FetchPage(current_page_id));
}

IndexIterator::~IndexIterator() {
  if(current_page_id != INVALID_PAGE_ID) buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {
  if(item_index < page->GetSize() - 1){
    item_index++;
  }else{
    buffer_pool_manager->UnpinPage(current_page_id, false);
    current_page_id = page->GetNextPageId();
    item_index = 0;
    if(current_page_id == INVALID_PAGE_ID){
      return *this;
    }else {
      page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id));
    }
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}