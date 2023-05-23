// // #include "storage/table_heap.h"

// // /**
// //  * TODO: Student Implement
// //  */
// // bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
// //   return false;
// // }

// // bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
// //   // Find the page which contains the tuple.
// //   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
// //   // If the page could not be found, then abort the transaction.
// //   if (page == nullptr) {
// //     return false;
// //   }
// //   // Otherwise, mark the tuple as deleted.
// //   page->WLatch();
// //   page->MarkDelete(rid, txn, lock_manager_, log_manager_);
// //   page->WUnlatch();
// //   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
// //   return true;
// // }

// // /**
// //  * TODO: Student Implement
// //  */
// // bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
// //   return false;
// // }

// // /**
// //  * TODO: Student Implement
// //  */
// // void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
// //   // Step1: Find the page which contains the tuple.
// //   // Step2: Delete the tuple from the page.

// // }

// // void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
// //   // Find the page which contains the tuple.
// //   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
// //   assert(page != nullptr);
// //   // Rollback to delete.
// //   page->WLatch();
// //   page->RollbackDelete(rid, txn, log_manager_);
// //   page->WUnlatch();
// //   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
// // }

// // /**
// //  * TODO: Student Implement
// //  */
// // bool TableHeap::GetTuple(Row *row, Transaction *txn) {
// //   return false;
// // }

// // void TableHeap::DeleteTable(page_id_t page_id) {
// //   if (page_id != INVALID_PAGE_ID) {
// //     auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
// //     if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
// //       DeleteTable(temp_table_page->GetNextPageId());
// //     buffer_pool_manager_->UnpinPage(page_id, false);
// //     buffer_pool_manager_->DeletePage(page_id);
// //   } else {
// //     DeleteTable(first_page_id_);
// //   }
// // }

// // /**
// //  * TODO: Student Implement
// //  */
// // TableIterator TableHeap::Begin(Transaction *txn) {
// //   return TableIterator();
// // }

// // /**
// //  * TODO: Student Implement
// //  */
// // TableIterator TableHeap::End() {
// //   return TableIterator();
// // }

// #include "storage/table_heap.h"


// bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
//   page_id_t i = first_page_id_;
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(i));
//   int lastI = i;
//   // first fit
//   while(i!=INVALID_PAGE_ID){
//     page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(i));
//     if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
//       buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
//       return true;
//     }
//     lastI = i;
//     i = page->GetNextPageId();
//     buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false);
//   }
//   // need to allocate a new page, i=invalid page id
//   page=reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(i));
//   page->Init(i,lastI,log_manager_,txn);
//   // set next page id
//   auto lastPage=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(lastI));
//   lastPage->SetNextPageId(i);
//   buffer_pool_manager_->UnpinPage(lastI, true);
//   // insert to new page
//   if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
//     buffer_pool_manager_->UnpinPage(i, true);
//     return true;
//   }
//   buffer_pool_manager_->UnpinPage(i, true);
//   return false;
// }

// bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
//   // Find the page which contains the tuple.
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//   // If the page could not be found, then abort the transaction.
//   if (page == nullptr) {
//     return false;
//   }
//   // Otherwise, mark the tuple as deleted.
//   page->WLatch();
//   page->MarkDelete(rid, txn, lock_manager_, log_manager_);
//   page->WUnlatch();
//   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//   return true;
// }

// bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
//   auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_ ->FetchPage(rid.GetPageId()));//get the page
//   if(page==nullptr){
//     return false;
//   }
//   Row *oldRow=new Row(rid);//get the old row
//   int type=page->UpdateTuple(row,oldRow,schema_,txn,lock_manager_,log_manager_);//get the update result
//   switch(type){
//     case 1:buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);return false;
//     case 2:buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);return false;//invalid update
//     case 0:buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);return true;
//     //not enough space for update
//     case 3:if(page->MarkDelete(rid,txn,lock_manager_,log_manager_)){
//       TablePage *oldPage=page;
//       int i=first_page_id_;
//       int preI=i;
//       while(i!=INVALID_PAGE_ID){
//         page=reinterpret_cast<TablePage *>(buffer_pool_manager_ ->FetchPage(i));
//         if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
//           oldPage->ApplyDelete(rid,txn,log_manager_);
//           buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//           return true;
//         }else{
//           preI=i;
//           i=page->GetNextPageId();
//           buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
//         }
//       }
//       //create a new page
//       page=reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(i));
//       page->Init(preI,i,log_manager_,txn);
//       if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
//           oldPage->ApplyDelete(rid,txn,log_manager_);
//           buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//           return true;
//       }
//       buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//       return false;
//     }else{
//       buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
//       return false;
//     }
//   }
//   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
//   return false;
// }

// void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
//   // Step1: Find the page which contains the tuple.
//   // Step2: Delete the tuple from the page.
//   auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//   page->ApplyDelete(rid,txn,log_manager_);
//   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
// }

// void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
//   // Find the page which contains the tuple.
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//   assert(page != nullptr);
//   // Rollback the delete.
//   page->WLatch();
//   page->RollbackDelete(rid, txn, log_manager_);
//   page->WUnlatch();
//   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
// }


// bool TableHeap::GetTuple(Row *row, Transaction *txn) {
//   auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
//   if(page->GetTuple(row,schema_,txn,lock_manager_)){
//     buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//     return true;
//   }else{
//     buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
//     return false;
//   }
// }

// TableIterator TableHeap::Begin(Transaction *txn) {
//   return TableIterator(this);
// }

// TableIterator TableHeap::End() {
//   return TableIterator(this,nullptr);
// }

#include "storage/table_heap.h"


/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  page_id_t now_page,last_page;
  now_page = first_page_id_;
  last_page = first_page_id_;
  bool mark = false;
  while(now_page!=INVALID_PAGE_ID){
    //try one by one
    TablePage* page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(now_page));
    //check insert successful or not
    if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)==true){
      buffer_pool_manager_->UnpinPage(now_page,true);
      mark = true;
      break;
    }
    last_page = now_page;
    now_page = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(last_page,false);
  }
  if(mark==true){
    return true;
  }
  TablePage* page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(now_page));
  page->Init(now_page,last_page,log_manager_,txn);
  TablePage* lastPage=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page));
  lastPage->SetNextPageId(now_page);
  if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)==true){
    buffer_pool_manager_->UnpinPage(now_page, true);
    return true;
  }
  else{
    buffer_pool_manager_->UnpinPage(now_page, true);
    return false;    
  }
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {

  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_ ->FetchPage(rid.GetPageId()));//get the page
  if(page==nullptr) return false;
  Row *last_row = new Row(rid);
  int which_case = page->UpdateTuple(row,last_row,schema_,txn,lock_manager_,log_manager_);
  if(which_case==0){//正常插入
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
    return true;
  }
  else if(which_case==1){//异常
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
    return false;
  }
  else if(which_case==2){//异常
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
    return false;
  }
  else{
    if(!page->MarkDelete(rid,txn,lock_manager_,log_manager_)){
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        return false;
    }
    page_id_t now_page,last_page;
    now_page = first_page_id_;
    last_page = first_page_id_;
    TablePage *lastpage = page;
    bool mark = false;
    while(now_page!=INVALID_PAGE_ID){
      //try one by one
      TablePage* page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(now_page));
      //check insert successful or not
      if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)==true){
        lastpage->ApplyDelete(rid,txn,log_manager_);
        buffer_pool_manager_->UnpinPage(now_page,true);
        mark = true;
        break;
      }
      last_page = now_page;
      now_page = page->GetNextPageId();
      buffer_pool_manager_->UnpinPage(last_page,false);
    }
    if(mark==true){
      return true;
    }
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(now_page));
    page->Init(now_page,last_page,log_manager_,txn);
    if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)==true){
      lastpage->ApplyDelete(rid,txn,log_manager_);
      buffer_pool_manager_->UnpinPage(now_page, true);
      return true;
    }
    else{
      buffer_pool_manager_->UnpinPage(now_page, true);
      return false;    
    }
  }
}


/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  TablePage* page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->ApplyDelete(rid,txn,log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  TablePage* page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page->GetTuple(row,schema_,txn,lock_manager_)){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }
  else{
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

TableIterator TableHeap::Begin(Transaction *txn) {
  return TableIterator(this);
}

TableIterator TableHeap::End() {
  return TableIterator(this,nullptr);
}
