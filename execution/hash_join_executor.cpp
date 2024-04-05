//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(left_child->release()), right_child_(right_child->release()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

//思路
//hash join就是根据要join的属性作为key，建立hash表（先为outer table建立hash表）
//然后对于inner table，使用hash function找到对应的bucket，然后进行匹配。
//这里是假设整个hash表能放到内存中的
void HashJoinExecutor::Init() { 
  //throw NotImplementedException("HashJoinExecutor is not implemented"); 
  idx_ = 0;
  left_child_->Init();
  right_child_->Init();
  Tuple *left_tuple, *right_tuple;
  RID *left_rid, *right_rid;
  const Schema output_schema = GetOutputSchema();
  const Schema left_schema = left_child_->GetOutputSchema();
  const Schema right_schema = right_child_->GetOutputSchema();
  //为outer table创建哈希表
  while (!left_child_->Next(left_tuple, left_rid)) {
    AggregateKey leftKey = GetLeftAggregateKey(left_tuple);
    if (ht_.find(leftKey) == ht_.end()) {
      ht_.insert({leftKey, {*left_tuple}});
    } else {
      ht_[leftKey].push_back(*left_tuple);
    }
  }
  //遍历inner table，从哈希表中查找匹配的tuple
  while (!right_child_->Next(right_tuple, right_rid)) {
    AggregateKey rightKey = GetRightAggregateKey(right_tuple);
    if (ht_.find(rightKey) == ht_.end()) {
      continue;
    } else {
      vector<Tuple> vec = ht_[rightKey];
      //将两个Tuple进行组合
      for (int idx = 0; idx < vec.size(); i++) {
        *left_tuple = vec[i];
        vector<Value> res;
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
          res.push_back(left_tuple->GetValue(left_schema, i));
        }
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
          res.push_back(right_tuple->GetValue(right_schema, i));
        }
        Tuple tuple = Tuple(res, &output_schema);
        buffer_.push_back(tuple);
      }
    }
  }

}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (idx_ < buffer_.size()) {
    *tuple = buffer_[idx_];
    idx_++;
    return true;
  } else {
    return false;
  }
}

}  // namespace bustub
