//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
      plan_ = plan;
      child_executor_ = child_executor;
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { 
  //throw NotImplementedException("UpdateExecutor is not implemented"); 
  ExecutorContext* exec_ctx = GetExecutorContext();
  Catalog *catalog = exec_ctx->GetCatalog();
  table_oid_t table_oid = plan_->GetTableOid(); 
  tableInfo_ = catalog->GetTable(table_oid); //TableInfo* 
  std::string tableName = tableInfo->name_;
  vec_ = catalog->GetTableIndexes(tableName);//vector<IndexInfo* > 
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  //从子执行器获取tuple  这里的子执行器应该是SeqScanExecutor，也可能是来自FilterExecutor
  bool flag = child_executor_->Next(tuple, rid);
  if (flag == false)
    return false;

  //TablePage提供的几个函数
  //InsertTuple UpdateTupleMeta GetTuple GetTupleMeta还有一个不让用的UpdateTupleInPlaceUnsafe
  //还有两个成员变量 num_tuples_和num_deleted_tuples_
  //更新的话暂时不能使用UpdateTupleInPlaceUnsafe，就是对需要更改的Tuple原地更新，这样可能和事务有关，不安全
  //要求的更新方式是对要更新的Tuple先修改meta删除，然后再插入新的Tuple
  //还有一个问题是UpdataTupleMeta删除的话，num_deleted_tuple++，但是这个slot删除了别人也没再使用了？
  TupleMeta meta = {0, 0, true};
  table_info->table_->UpdateTupleMeta(meta, *rid); //删除原来的tuple

  meta = {0, 0, false};
  //to-do 对原来的tuple进行update  GenerateUpdatedTuple in plan node?
  //--------------------------------------------------------------------------------------------
  std::optional<RID> new_rid = tableInfo_->table_->InsertTuple(meta, *tuple); //插入新的update后的tuple
  tuple->SetRid(new_rid.value());

  //对索引进行更新。删除原tuple的索引，插入新tuple的索引
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
    indexInfo->index_->InsertEntry(tuple.KeyFromTuple(tableInfo_->schema_, key_schema, key_attrs), tuple.GetRid(), nullptr); //IndexInfo含有Index对象
  }

  return true;
}

}  // namespace bustub
