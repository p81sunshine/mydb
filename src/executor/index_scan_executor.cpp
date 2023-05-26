#include "executor/executors/index_scan_executor.h"
#include <algorithm>
#include <set>

IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  exec_ctx->GetCatalog()->GetTable(plan->GetTableName(), table_info);
}

void IndexScanExecutor::Init() {
  /**
   * @brief 如果Indices那个数组，存的是有关这个查询的所有index的话
   * 先通过index，找到row,判断row符不符合谓词条件
   * 如果符合的话，加到一个set里面
   * 多个index查询之后的set,取一个交集，得到一个row id的集合
   * next按照顺序读取set里面的row
   *
   * @param index
   */
  // 对于每个Index,先将满足条件的row放到一个set里
  std::vector<std::set<RowId>> rows(plan_->indexes_.size());
  int i = 0;
  for (auto index : plan_->indexes_) {
    BPlusTreeIndex *table_iterator = reinterpret_cast<BPlusTreeIndex *>(index->GetIndex());
    auto it = table_iterator->GetBeginIterator();
    for (; it != table_iterator->GetEndIterator(); ++it) {
      Field temp(kTypeInt, 1);
      rows[i].insert((*it).second);
    }
    i++;
  }

  res = rows[0];
  for (int i = 1; i < rows.size(); i++) {
    std::set<RowId> temp;
    std::set_intersection(rows[i].begin(), rows[i].end(), res.begin(), res.end(), temp.begin());
    res = temp;
  }
  begin = res.begin();
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  // 输出set里面的一个元素
  while (begin != res.end()) {  // 读取行，然后根据行的内容，使用seqscan类似的方法进行谓词判断
    Row res_row(*begin);
    table_info->GetTableHeap()->GetTuple(&res_row, exec_ctx_->GetTransaction());
    Field temp(kTypeInt, 1);
    if (temp.CompareEquals(plan_->GetPredicate()->Evaluate(&res_row))) {  // 比较是否满足条件
      *row = res_row;
      *rid = *begin;
      return true;
    } else {
      begin++;
    }
  }

  return false;
}
