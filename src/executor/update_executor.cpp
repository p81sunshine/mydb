//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  exec_ctx->GetCatalog()->GetTableIndexes(plan->table_name_, index_info_);
}

/**
 * TODO: Student Implement
 */
void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row old_row;
  RowId old_row_id;
  Row new_row;
  if (child_executor_->Next(&old_row, &old_row_id)) {
    new_row = GenerateUpdatedTuple(old_row);
    RowId new_rid = {old_row_id.GetPageId(), old_row_id.GetSlotNum()};
    if (!table_info_->GetTableHeap()->UpdateTuple(new_row, new_rid, exec_ctx_->GetTransaction())) {
      return false;
    }
  } else {
    return false;
  }

  /**
   * @brief 更新索引
   *
   * @param index_info_
   */
  for (auto index : index_info_) {
    // index的key关联表中的key
    std::vector<uint32_t> key_attrs = index->meta_data_->GetKeyMapping();  // To be solved
    const auto &update_attrs = plan_->GetUpdateAttr();
    bool isUpdate = false;
    for (const auto key_attr : key_attrs) {
      if (update_attrs.find(key_attr) != update_attrs.end()) {
        isUpdate = true;
        break;
      }
    }

    if (!isUpdate)
      continue;
    else {
      Row old_key;  // 我们用Row来表示一个key
      Row new_key;
      old_row.GetKeyFromRow(table_info_->GetSchema(), index->key_schema_, old_key);
      new_row.GetKeyFromRow(table_info_->GetSchema(), index->key_schema_, old_key);
      index->index_->RemoveEntry(old_key, old_row_id, exec_ctx_->GetTransaction());
      index->index_->InsertEntry(new_key, new_row.GetRowId(), exec_ctx_->GetTransaction());
    }
  }

  return true;
}

/**
 * @brief 找到要更新的属性，替换掉原来的那一行中对应的属性
 *
 * @param src_row 之前的那行,因为我们更新要保留未被更新的属性
 * @return Row
 */
Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  std::vector<Field> values;
  const auto &update_attrs =
      plan_->GetUpdateAttr();  // 这个结构同上，hash map的key是表中的第几列，expression是要插入的新值（constant expression)
  for (uint32_t column_id = 0; column_id < table_info_->GetSchema()->GetColumnCount(); column_id++) {
    if (update_attrs.find(column_id) == update_attrs.cend()) {  // 没有找到,插入旧的
      values.emplace_back(src_row.GetField(column_id));
    } else {  // 获取新的值，根据文档，由SeqScanExecutor提供,文档写的非常不清晰，我们假设next提供了一个包含了跟新值的行，那么问题在于这时候为什么还要这个函数？
    // 而且，seqscan是从一个表中读一行，是怎么给update算子要更新的值的？
    // 而且，这样的话和上边的update_attrs的解释冲突了
      Row temp_row;
      RowId temp_row_id;
      child_executor_->Next(&temp_row, &temp_row_id);
      values.emplace_back(temp_row.GetField(column_id));
    }
  }

  return Row(values);
}