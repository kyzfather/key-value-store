//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

//这里的UpgradeLockTable是更新Transaction维护的lock table？
auto UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::shared_ptr<std::unordered_set<table_oid_t>>  table_set;
  switch (lock_mode) {
    case SHARED:
      table_set = GetSharedTableLockSet();
      break;
    case EXCLUSIVE:
      table_set = GetExclusiveTableLockSet();
      break;
    case INTENTION_SHARED:
      table_set = GetIntentionSharedTableLockSet();
      break;
    case INTENTION_EXCLUSIVE:
      table_set =  GetIntentionExclusiveTableLockSet();
      break;
    case SHARED_INTENTION_EXCLUSIVE:
      table_set = GetSharedIntentionExclusiveTableLockSet();
      break;
    default:
      return false;
  }
  table_set->insert(oid);
  return true;
}

auto UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> row_set;
  switch (lock_mode) {
    case SHARED:
      row_set = GetSharedRowLockSet();
    case EXCLUSIVE:
      row_set = GetExclusiveRowLockSet();
    default:
      return false;
  }
  row_set[oid].insert(rid);
  return true;
}

//判断两个锁是否兼容
//enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };
auto AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case SHARED:
      if (l2 == SHARED || l2 == INTENTION_SHARED)
        return true;
      else
        reuturn false;
    case EXCLUSIVE:
      return false;
    case INTENTION_SHARED:
      if (l2 == EXCLUSIVE)
        return false;
      else
        return true;
    case INTENTION_EXCLUSIVE:
      if (l2 == INTENTION_SHARED || l2 == INTENTION_EXCLUSIVE)
        return true;
      else
        return false;
    case SHARED_INTENTION_EXCLUSIVE:
      if (l2 == INTENTION_SHARED)
        return true;
      else
        return false;
    default:
      return false;
  }
}

auto CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  //S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the 
  // TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
  if (txn->GetIsolationLevel() == READ_UNCOMMITTED && (lock_mode == SHARED || lock_mode == INTENTION_SHARED || lock_mode == SHARED_INTENTION_EXCLUSIVE)) {
    txn->SetState(ABORTED);
    throw(TransactionAbortException(oid, LOCK_SHARED_ON_READ_UNCOMMITTED));
  }

  // X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
  // should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
  if (txn->GetState() == SHRINKING && (lock_mode == EXCLUSIVE || lock_mode = INTENTION_EXCLUSIVE)) {
    txn->SetState(ABORTED);
    throw(TransactionAbortException(oid, LOCK_ON_SHRINKING));
  }

  if (txn->GetIsolationLevel() == REPEATABLE_READ && txn->GetState() == SHRINKING) {
    txn->SetState(ABORTED);
    return false;
  } 

  if (txn->GetIsolationLevel() == READ_COMMITTED && txn->GetState() == SHRINKING && lock_mode == SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(ABORTED);
    return false;
  }

  return true;
}


void GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {

}

auto CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  switch (curr_lock_mode) {
    case INTENTION_SHARED:
      if (requested_lock_mode == SHARED || requested_lock_mode == EXCLUSIVE || requested_lock_mode == INTENTION_EXCLUSIVE || requested_lock_mode == SHARED_INTENTION_EXCLUSIVE)
        return true;
      else
        return false;
    case SHARED:
      if (requested_lock_mode == INTENTION_EXCLUSIVE || requested_lock_mode == SHARED_INTENTION_EXCLUSIVE)
        return true;
      else
        return false;
    case INTENTION_EXCLUSIVE:
      if (requested_lock_mode == EXCLUSIVE || requested_lock_mode == SHARED_INTENTION_EXCLUSIVE)
        return true;
      else
        return false;
    case SHARED_INTENTION_EXCLUSIVE:
      if (requested_lock_mode == EXCLUSIVE)
        return true;
      else
        return false;
    default:
      return false;
  }
}

auto CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool;
auto FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                 std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool;
void UnlockAll();

/**
 * Acquire a lock on table_oid_t in the given lock_mode.
 * If the transaction already holds a lock on the table, upgrade the lock
 * to the specified lock_mode (if possible).
 *
 * This method should abort the transaction and throw a
 * TransactionAbortException under certain circumstances.
 * See [LOCK_NOTE] in header file.
 *
 * @param txn the transaction requesting the lock upgrade
 * @param lock_mode the lock mode for the requested lock
 * @param oid the table_oid_t of the table to be locked in lock_mode
 * @return true if the upgrade is successful, false otherwise
 */
//*    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
//*    If the transaction was aborted in the meantime, do not grant the lock and return false.
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  bool flag = CanTxnTakeLock(txn, lock_mode);
  if (!flag)
    return false;

  //将上锁的请求插入到该表对应的请求队列上。查询请求队列上前面的请求的上锁模式是否与当前请求兼容，如果兼容就可以上锁
  //否则就需要wait等待。
  std::unique_lock<mutex> tablemap_lock(table_lock_map_latch_);
  LockRequest lr(txn->GetTransactionId(), lock_mode, oid);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) { //该表还没有任何上锁请求，允许上锁
    LockRequestQueue lrq;
    lrq.request_queue_.push_back(std::shared_ptr<LockRequest>(&lr));
    table_lock_map_[oid] = std::shared_ptr<LockRequestQueue>(&lrq);

    UpgradeLockTable(txn, lock_mode, oid);

    tablemap_lock.unlock();
    return true;
  } else { //该表有上锁请求，查看前面的上锁请求和当前请求是否兼容
    //如果请求队列有获取IS锁 然后X锁被阻塞 然后后面的请求又有IS锁的 也就是队列中IS X IS,那么这里最后的IS能获取锁吗？
    //感觉应该不可以？要不然X可能会一直饥饿获取不了锁？


    //更新，将原有的RequestQueue删除，将transaction中原来的lock记录删除
    //将新的LockRequest插入到LockRequestQueue中。因为update的请求要优先处理，所以插入到第一个没有获得锁的请求的位置
    tablemap_lock.unlock();
    bool upgrade = false;
    std::shared_ptr<LockRequestQueue> lrq = table_lock_map_[oid];
    std::unique_lock<mutex> table_lock(lrq->latch_);
    for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue.end(); iter++) {
      std:shared_ptr<LockRequest> temp_lr = *iter;
      if (temp_lr->txn_id == txn->GetTransactionId()) { //说明是upgrade的情况
        if (lrq->upgrading_ != INVALID_TXN_ID) { //当前需要更新的不止一个
          txn->SetState(ABORTED); //只需要设置为ABORTED就行，因为transaction manager会负责释放锁
          throw(TransactionAbortException(txn->GetTransactionId(), UPGRADE_CONFLICT));
          return false;
        }
        LockMode curr_lock_mode = temp_lr->lock_mode_;
        if (CanLockUpgrade(curr_lock_mode, lock_mode) == false) {
          txn->SetState(ABORTED);
          throw(TransactionAbortException(txn->GetTransactionId(), INCOMPATIBLE_UPGRADE));
          return false;
        }

        //除了上面两种情况，可以进行更新
        //首先需要删除Transaction还有这里的queue中的记录。为什么不调用unlocktable是因为暂时还不想调用cv.notify_all()
        txn->GetSharedTableLockSet()->erase(oid); 
        txn->GetExclusiveTableLockSet()->erase(oid);
        txn->GetIntentionSharedTableLockSet()->erase(oid);
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
        lrq->request_queue_.erase(iter);
        lrq->upgrading_ = txn->GetTransactionId();
        upgrade = true;
        break;

      }
    }

    //如果是更新，需要将request插入到没有获取锁的最前面。也就是最后一个获取锁的请求的后一个位置
    if (upgrade) {
      for(auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue.end(); iter++) {
        std:shared_ptr<LockRequest> temp_lr = *iter;
        if (temp_lr->granted_ == true)
          continue;
        //第一个没有获取锁请求的位置
        lrq->request_queue_.insert(iter, std::shared_ptr<LockRequest>(&lrq));
        break;
      }
    } else {
      lrq->request_queue_.push_back(std::shared_ptr<LockRequest>(&lrq));
    }
    for (int i = 0; i < lrq->request_queue_.size(); i++) {
      std::shared_ptr<LockRequest> temp_lr = lrq->request_queue_[i];
      if (temp_lr->txn_id_ == txn->GetTransactionId()) {
        //说明可以获取锁
        UpgradeLockTable(txn, lock_mode, oid);
        temp_lr->granted_ = true;
        return true;
      }
      if (temp_lr->granted_ == false || !AreLocksCompatible(temp_lr->lock_mode_, lock_mode)) {
        //前面有请求没获取锁的，或者与前面的请求不兼容的。需要wait
        std::unique_lock<mutex> lock(lrq->latch_);
        lrq->cv_.wait(lock);
        if (txn->GetState() == ABORTED)
          return false;
        i = 0; //这样子写对吗？就是不能获得锁，条件变量wait，等着释放锁的线程唤醒，唤醒后再从头遍历请求队列查看是否可以获得锁
      } else {
        continue;
      }     
    }
  }


}


/**
 * Release the lock held on a table by the transaction.
 *
 * This method should abort the transaction and throw a
 * TransactionAbortException under certain circumstances.
 * See [UNLOCK_NOTE] in header file.
 *
 * @param txn the transaction releasing the lock
 * @param oid the table_oid_t of the table to be unlocked
 * @return true if the unlock is successful, false otherwise
 */
auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  //should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
  bool flag = txn->IsTableIntentionSharedLocked(oid) || txn->IsTableSharedLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid)
                || txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid);
  //If not, LockManager should set the TransactionState as ABORTED and throw a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
  if (!flag) {
    txn->SetState(TransactionState(ABORTED));
    throw(TransactionAbortException(txn->GetTransactionId(), AbortReason(ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)));
  }

  //unlocking a table should only be allowed if the transaction does not hold locks on any row on that table.
  //If the transaction holds locks on rows of the table, Unlock should set the Transaction State as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> s_row_lock_set = txn->GetSharedRowLockSet();
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (s_row_lock_set->find(oid) != s_row_lock_set->end() || x_row_lock_set->find(oid) != x_row_lock_set->end()) {
    txn->SetState(TransactionState(ABORTED));
    throw(TransactionAbortException(txn->GetTransactionId(), AbortReason(TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS)));
  }

  //Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
  //从table_lock_map_对应的list中删除这个Request，然后唤醒其他的线程。
  LockMode lock_mode;
  std::shared_ptr<LockRequestQueue> lrq = table_lock_map_[oid]; 
  std::unique_lock<mutex> lk(lrq->latch_); //不管是上锁，还是解锁，都需要访问LockRequestQueue，多线程同时访问需要上锁
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    std::shared_ptr<LockRequest> lr = *iter;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      lock_mode = lr->lock_mode_;
      lrq->request_queue_.erase(iter); //从LockManager中删除该请求
      txn->GetSharedTableLockSet()->erase(oid); //从Transaction中删除该锁的记录
      txn->GetExclusiveTableLockSet()->erase(oid);
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      lrq->cv_.notify_all();
    }
  }

  //TRANSACTION STATE UPDATE
  //需要根据该Transaction的隔离级别以及释放锁的Lock Mode进行两阶段锁阶段的转换
  if (lock_mode == SHRAED || lock_mode == EXCLUSIVE) {
    if (lock_mode == SHARED) {
      if (txn->GetIsolationLevel() == REPEATABLE_READ) {
        txn->SetState(SHRINKING);
      }
    } else {
      txn->SetState(SHRINKING);
    }
  }
  return true;
  
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode == INTENTION_SHARED || lock_mode == INTENTION_EXCLUSIVE || lock_mode == SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(ABORTED);
    throw(TransactionAbortException(txn->GetTransactionId(), ATTEMPTED_INTENTION_LOCK_ON_ROW));
  }

  if (!CanTxnTakeLock(txn, lock_mode))
    return false;

  //查看想要获取row的锁对应table有没有已经获取锁
  if (lock_mode == EXCLUSIVE) {
    if (!(txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid))) {
      txn->SetState(ABORTED);
      throw(TransactionAbortException(txn->GetTransactionId(), TABLE_LOCK_NOT_PRESENT));      
    }
  } else { //SHARED
    bool flag = txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid) ||
                txn->IsTableIntentionSharedLocked(oid) || txn->IsTableSharedLocked(oid);
    if (!flag) {
      txn->SetState(ABORTED);
      throw(TransactionAbortException(txn->GetTransactionId(), TABLE_LOCK_NOT_PRESENT));        
    }
  }

  //to-do--------------------------------------------
  //感觉逻辑和lock table差不多，赶时间，写的意义不是太大，先不写了。如果未来有机会再写


}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  //to-do--------------------------------------------
  //感觉逻辑和lock table差不多，赶时间，写的意义不是太大，先不写了。如果未来有机会再写
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  //感觉这里的AddEdge要根据Lock获取锁的策略。
  //上面实现的Lock获取锁的策略是 
  //1.先看LockRequesQueue 自己这个请求前面是否有没有granted的请求，如果有需要等待
  //2.如果前面的请求都是granted的，看是否与所有granted的请求兼容，兼容则可获取锁
  //就是即使当前请求与所有granted的请求兼容，也不一定能获取锁，因为前面可能有请求没有获取锁
  //如果允许没有获取锁的后面的请求获取锁，那么该请求可能就会一直处于饥饿状态
  //那么添加边的规则是什么呢？
  //对于某个请求，遍历前面的所有request，如果不兼容则添加边 
  //这里遍历lockManager中的table_lock_map和row_lock_map
  //  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  //t1 waits for t2
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_[t1] = {t2};
  } else {
    waits_for_[t1].push_back(t2);
  }

  if (waits_for_me.find(t2) == waits_for_me.end()) {
    waits_for_me[t2] = {t1};
  } else {
    waits_for_me[t2].push_back(t1);
  }

}

//  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::vector<txn_id_t> vec = waits_for_[t1];
  for (auto iter = vec.begin(); iter != vec.end(); iter++) {
    if (*iter == t2) {
      vec.erase(iter);
    }
  }
  waits_for_[t1] = vec;

  std::vector<txn_id_t> vecc = waits_for_me_[t2];
  for (auto iter = vecc.begin(); iter != vecc.end(); iter++) {
    if (*iter == t1) {
      vecc.erase(iter);
    }
  }
  waits_for_me_[t2] = vecc;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { 
  //dfs搜索waits_for_判断是否有环，返回youngest txn，时间原因不写了
  //可以用一个unordered_set在dfs过程中记录访问过哪些txn。重复出现则说明有环
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  //while(HasCycle)是否有环
  //将对应的txn通过transManager 进行abort还是只用setState()
  //
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      //先构建锁的wais_for图
      for (auto it = table_lock_map.begin(); it != table_lock_map.end(); it++) {
        std::shared_ptr<LockRequestQueue> lrq = it->second;
        for (int i = 1; i < lrq->request_queue_.size(); i++) { //遍历前面的请求，看是否与自己不兼容
          for (int j = 0; j < i; j++) {
            std::shared_ptr<LockRequest> outer = lrq->request_queue_[i];
            std::shared_ptr<LockRequest> inner = lrq->request_queue_[j];
            if (!AreLocksCompatible(inner->lock_mode_, outer->lock_mode_)) {
              AddEdge(outer->txn_id_, inner->txn_id_);
            }
          }
        }
      }

      for (auto it = row_lock_map.begin(); it != row_lock_map.end(); it++) {
        std::shared_ptr<LockRequestQueue> lrq = it->second;
        for (int i = 1; i < lrq->request_queue_.size(); i++) { //遍历前面的请求，看是否与自己不兼容
          for (int j = 0; j < i; j++) {
            std::shared_ptr<LockRequest> outer = lrq->request_queue_[i];
            std::shared_ptr<LockRequest> inner = lrq->request_queue_[j];
            if (!AreLocksCompatible(inner->lock_mode_, outer->lock_mode_)) {
              AddEdge(outer->txn_id_, inner->txn_id_);
            }
          }
        }
      }

      //不断检测是否存在环，
      txn_id_t *txn_id;
      while (HasCycle(txn_id)) {
        //return the youngest txn in the cycle
        txn_id->SetState(ABORTED); //txn的状态被设置为ABORTED 但是什么时候会调用transaction manager将txn的锁释放呢？
        txn_manager_->Abort(txn_manager_->GetTransaction(txn_id)); //释放锁，并且会唤醒锁对应的队列的其他transaction
        std::vector<txn_id_t> vec = waits_for_me_[txn_id];
        for (int i = 0; i < vec.size(); i++) {
          txn_id_t id = vec[i];
          RemoveEdge(id, *txn_id);
        }
        //remove edge?
        //When your detection thread wakes up, it must break all cycles that exist. If you follow the above requirements, 
        //When a transaction is aborted, set the transaction's state to ABORTED. 
        //The transaction manager should then take care of the explicit abort and rollback changes.
        //A transaction waiting for a lock may be aborted by the background cycle detection thread. 
        //You must have a way to notify waiting transactions that they've been aborted.
        //remove edge的时候，怎么检测有哪些txn正在等当前的txn呢？
        //感觉是不是需要再加一个unordered_map的成员变量，用来记录哪些txn在wait自己

      }
    }
  }
}

}  // namespace bustub
