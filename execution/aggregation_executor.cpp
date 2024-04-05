//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(child_executor->release()),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      iter_(aht_.begin()) {}

void AggregationExecutor::Init() {
    //aggregation plan node中有三个成员变量 
    //vector<AbstractExpressionRef> group_bys_  代表的是聚合键（类似于group by A,B,C 这里的A,B,C）
    //vector<AbstractExpressionRef> aggregates_ 代表的是要聚合的值，应该比如count(A) max(B) 这里的A和B
    //vector<AggregateType> agg_types           代表的是聚合的类型 count max min sum 
    //select count(A),max(B) from xx group by C,D,E
    //关于AbstractExpression以及如何根据Expression生成相应的AggregateKey和Value没有看原理
    child_executor_->Init();
    Tuple* tuple;
    RID* rid;
    while (child_executor_->Next(tuple, rid)) {
        AggregateKey agg_key = MakeAggregateKey(tuple);
        AggregateValue agg_val = MakeAggregateValue(tuple);
        aht_.InsertCombine(agg_key, agg_val);
    }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    //对聚合后的结果，也就是从哈希表中遍历获得每个结果
    while (iter_ != aht_.end()) {
        vector<Value> vec;
        const AggregateValue val = iter_.Val();
        *tuple = Tuple(val.aggregates_, GetOutputSchema());
        iter_++;
        return true;
    }
    return false;
    
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
