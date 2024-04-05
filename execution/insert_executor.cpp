//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
        plan_ = plan;
        child_executor_ = child_executor;
    }

void InsertExecutor::Init() { 
    //throw NotImplementedException("InsertExecutor is not implemented"); 
    //初始化主要获取tableInfo_和vec_这两个成员变量，不用每次调用Next都执行一遍来获取
    ExecutorContext* exec_ctx = GetExecutorContext();
    Catalog *catalog = exec_ctx->GetCatalog();
    table_oid_t table_oid = plan_->GetTableOid(); 
    tableInfo_ = catalog->GetTable(table_oid); //TableInfo*
    std::string tableName = tableInfo->name_;
    vec_ = catalog->GetTableIndexes(tableName);//vector<IndexInfo* >
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    bool flag = child_executor_->Next(tuple, rid); //从子执行器中获取要插入的tuple.这里的子执行器是ValueExecutor，从它返回的Tuple是没有rid的，因为没有插入到Table中
    if (flag == false)
        return false;


    //tableHeap对象用来表示一个table，向tableHeap中插入Tuple，获得Tuple的RID
    TupleMeta meta;
    std::optional<RID> new_rid = tableInfo_->table_->InsertTuple(meta, *tuple); //有个问题，这整个调用过程插入Tuple都没有为Tuple设置RID
    tuple->SetRid(new_rid.value());

    //插入Table之后要看该Table是否建立索引，要更新索引
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

        //Index是一个基类，子类可以是b_plus_tree_index也可以是extendible_hash_table_index
        //extendible_hash_table_index有一个成员变量DiskExtendibleHashTable container_,
        //应该是每个索引，都会对应一个Index 也都会有对应的一个具体的container
        indexInfo->index_->InsertEntry(tuple.KeyFromTuple(tableInfo_->schema_, key_schema, key_attrs), tuple.GetRid(), 0); //IndexInfo含有Index对象
    }
    return true;
}

    

}  // namespace bustub
