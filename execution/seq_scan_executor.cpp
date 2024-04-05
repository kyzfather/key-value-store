//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
    plan_ = plan;
}

//脑子有点乱，写代码想各种读研以来不愉快的事情，就让这些事情过去吧，不要再想困扰自己。
//做好当下，把握好时间，努力不荒废时间，专注学习要有效率，踏实把每件小事做好。
//突然想到一些东西。我觉得确实不一定非要读博，读博很大可能就是在做自己觉得没有意义的事情，折磨自己
//我觉得还是要做自己喜欢做的事情，我就很想从事游戏相关的开发工作，也愿意写代码学习技术，那么为什么不考虑往这个方面发展呢？
//科研与工作的准备是可以同时进行的，只要肯努力。最后尝试一下能否搞科研，如果真的适合自己再考虑读博。
//如果把读博的努力与精力放在工作与学习技术上，那么回报会比读博小吗？
void SeqScanExecutor::Init() {
    //throw NotImplementedException("SeqScanExecutor is not implemented"); 
    //In Init, take a table lock. Get an iterator by using MakeEagerIterator instead of MakeIterator
    //这里init首先要给table上锁，SeqExecutor继承AbstractExecutor
    //AbstractExecutor有ExecutorContext类的成员变量 ExecutorContext中有Transtaction和LockManager的成员变量
    //调用LockManager的LockTable(txn, lock_mode, table_oid)试图给表上锁。
    //但是，SeqScan应该上什么锁呢？ S锁还是IS锁？

    //如果是scan整个表，需要对table上s锁
    //如果只是从整个表中读一个tuple，对table上IS锁，对对应的tuple上S锁
    //如果是从表中选择一个tuple进行更新，应该是对table上IX锁，对对应的tuple上X锁
    //SeqScan算子对应的就是Select,感觉应该是上S锁

    //project3
    table_oid_t table_oid = plan_->GetTableOid();
    Catalog* catalog = GetExecutorContext()->GetCatalog();
    TableInfo* tableInfo = catalog->GetTable(table_oid);
    iter = tableInfo->table_->MakeEagerIterator(); //有个问题，tableInfo的table是unique_ptr，能这样直接调用吗？

    //project4
    LockManager *LockManager = GetExecutorContext()->GetLockManager();
    Transaction *transaction = GetExecutorContext()->GetTransaction();
    //根据Transaction的隔离级别设置LockMode:对于REPEATABLE_READ和READ_COMMITTED读需要上S锁。但是对于READ_UNCOMMITED读不需要上任何锁
    if (transaction->GetIsolationLevel() != READ_UNCOMMITED) {
        lockManager->LockTable(transaction, SHARED, table_oid); 
    } //对于READ_UNCOMMITED不需要对表上锁


}


//顺序扫描
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    /* project4:
    Lock the tuple as needed for the isolation level
    Fetch the tuple. Check tuple meta.If the tuple should not be read by this transaction,
    force unlock the row. Otherwise, lock the row as needed for the isolation level.
    If the current operation is delete (by checking executor context IsDelete(), which will be set to true for DELETE and UPDATE),
    you should assume all tuples scanned will be deleted, and you should take X locks on the table and tuple as necessary in step 2.
    */
    pair<TupleMeta, Tuple> row = iter.GetTuple();

    tuple = &row.second;
    rid = &iter.GetRID();

    if (rid->GetPageId() == INVALID_PAGE_ID) {
        return false;
    }

    iter++; //只有iter没有到达最后的位置才能增长

    if (row.first.is_deleted_ == true) {
        return false;
    } else {
        return true;
    }

}

}  // namespace bustub
