//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
 * TODO: Student Implement
 */
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  exec_ctx->GetCatalog()->GetTable(plan->GetTableName(), table_info);
}

void SeqScanExecutor::Init() { table_iterator = table_info->table_heap_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  while (table_iterator != table_info->table_heap_->End()) {
    Field temp(kTypeInt, 1);
    if (temp.CompareEquals(plan_->GetPredicate()->Evaluate(table_iterator.GetRow()))) {  // 比较是否满足条件
      *row = Row(*table_iterator.GetRow());                                              // 副本
      *rid = row->GetRowId();
      return true;
    } else {
      table_iterator++;
    }
  }
  return false;
}
