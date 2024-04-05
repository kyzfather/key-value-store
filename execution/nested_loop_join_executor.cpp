//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor, 
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_executor_ = left_executor;
  right_executor_ = right_executor;
}

void NestedLoopJoinExecutor::Init() { 
  //Init()阶段是不是要不停的调用子执行器的Next()函数，获取到所有的Tuple后再执行合并？
  //这样子有个问题，就是table的那么多tuple能放到内存中吗，感觉不能
  //看了下网上的做法
  left_executor_->Init();
  right_executor_->Init();
  is_done_ = !left_executor_->Next(left_tuple, left_rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  //有个问题，对于outertable的一个tuple，要访问所有innertable的tuple
  //但是executor对象提供的是Next，返回下一个tuple，这样的话该怎么访问？
  Tuple* right_tuple;
  RID* right_rid;
  const Schema left_schema = left_executor_->GetOutputSchema();
  const Schema right_schema = right_executor_->GetOutputSchema();
  const AbstractExpressionRef predicate = plan_->Predicate();
  const Schema join_schema = NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());
  uint32_t left_column_count = left_schema.GetColumnCount();
  uint32_t right_colomn_count = right_schema.GetColumnCount();

  while(!is_done_) {
    while(!right_executor_->Next(right_tuple, right_rid)) {
      Value val = predicate->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema); //evaluetejoin的实现没有看，大概就是根据predicate的谓词还有两个tuple判断能否join
      if (val.GetAs<bool>() == true) {
        vector<Value> vec;
        for (uint32_t i = 0; i < left_column_count; i++) {
          vec.push_back(left_tuple->GetValue(left_schema, i));
        }
        for (uint32_t i = 0; i < right_column_count; i++) {
          vec.push_back(right_tuple->GetValue(right_schema, i));
        }
        tuple = new Tuple(vec, &join_schema);
        return true;
      }
    }
    is_done_ = !left_excecutor_->Next(left_tuple, left_rid);
    right_executor_->Init(); //inner table遍历完了，需要init，再从头开始遍历

  }
  return false;
  
}

}  // namespace bustub
