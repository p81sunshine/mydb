//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"
#include "executor/executors/values_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  exec_ctx->GetCatalog()->GetTableIndexes(plan->table_name_, indexes_);
}

// 主体函数是写到Init里面还是Next里面
void InsertExecutor::Init() {
  child_executor_->Init();  // 这里假设其实现了多态，即传入的valueExecutor,而标识的是abstraction_executor
}

/**
 * @brief To be solved: return what? 
 * 
 * @param row 
 * @param rid 
 * @return true 
 * @return false 
 */
bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row new_row;
  RowId new_row_id;
  if (child_executor_->Next(&new_row, &new_row_id)) {
    if (!table_info_->GetTableHeap()->InsertTuple(new_row, exec_ctx_->GetTransaction())) {
      return false;
    }

    for (auto index : indexes_) {
      Row key_row;
      new_row.GetKeyFromRow(table_info_->GetSchema(), index->GetIndexKeySchema(), key_row);
      index->index_->InsertEntry(key_row, new_row_id, exec_ctx_->GetTransaction());
    }

  } else {
    return false;
  }
}