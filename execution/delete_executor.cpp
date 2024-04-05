//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
        plan_ = plan;
        child_executor_ = child_executor;
    }

void DeleteExecutor::Init() { 
    //throw NotImplementedException("DeleteExecutor is not implemented"); 
    ExecutorContext* exec_ctx = GetExecutorContext();
    Catalog *catalog = exec_ctx->GetCatalog();
    table_oid_t table_oid = plan_->GetTableOid(); 
    tableInfo_ = catalog->GetTable(table_oid); //TableInfo* 
    std::string tableName = tableInfo->name_;
    vec_ = catalog->GetTableIndexes(tableName);//vector<IndexInfo* > 
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //子执行器可能是SeqscanExecutor也可能是FilterExecutor
  //操作步骤和update类似，只是删除不需要插入
  bool flag = child_executor_->Next(tuple, rid);
  if (flag == false)
    return false;

  TupleMeta meta = {0, 0, true};
  table_info->table_->UpdateTupleMeta(meta, *rid); //删除原来的tuple

  //对索引进行更新。删除原tuple的索引
  for(int i = 0; i < vec_.size(); i++) {
    IndexInfo* indexInfo = vec_[i];
    Schema key_schema = indexInfo->key_schema_;

    //这个key_attrs应该自己构造？
    std::vector<uint32_t> key_attrs;
    uint32_t length = key_schema.GetLength();
    for (uint32_t i = 0; i < length; i++) {
      Column column = key_schema.GetColumn(i);
      std::string column_name = column.GetName();
      uint32_t idx = scheme.GetColIdx(column_name);
      key_attrs.push_back(idx);
    }

    //删除原来tuple的索引，插入新tuple的索引
    indexInfo->index_->DeleteEntry(tuple.KeyFromTuple(tableInfo_->schema_, key_schema, key_attrs), *rid, nullptr);
  }

  return true;
}

}  // namespace bustub
