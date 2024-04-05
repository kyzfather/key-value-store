//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  //throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  //构造函数中应该初始new header page
  BasicPageGuard bpg = bpm_->NewPageGuarded(header_page_id_);
  ExtendibleHTableHeaderPage* headPage = bpg.AsMut<ExtendibleHTableHeaderPage>();
  headPage->Init();

}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
  /** TODO(P2): Add implementation
   * Get the value associated with a given key in the hash table.
   *
   * Note(fall2023): This semester you will only need to support unique key-value pairs.
   *
   * @param key the key to look up
   * @param[out] result the value(s) associated with a given key
   * @param transaction the current transaction
   * @return the value(s) associated with the given key
   */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const -> bool {
  uint32_t hash = Hash(hash_fn_.GetHash(key));

  ReadPageGuard rpg_head = bpm_->FetchPageRead(header_page_id_);
  const ExtendibleHTableHeaderPage* headerPage = rpg_head.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = headerPage->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = headerPage->GetDirectoryPageId(directory_idx);
  rpg_head.Drop();

  if (directory_page_id == 0) 
    return false;

  ReadPageGuard rpg_directory = bpm_->FetchPageRead(directory_page_id);
  const ExtendibleHTableDirectoryPage* directoryPage = rpg_directory.As<ExtendibleHTableDirectoryPage>(); 
  uint32_t bucket_idx = directoryPage.HashToBucketIndex(hash);
  page_id_t bucket_page_id = directoryPage.GetBucketPageId(bucket_idx);   
  rpg_directory.Drop();

  if (bucket_page_id == 0)
    return false;

  ReadPageGuard rpg_bucket = bpm_->FetchPageRead(bucket_page_id);
  const ExtendibleHTableBucketPage* bucketPage = rpg_bucket.As<ExtendibleHTableBucketPage>();
  V value;
  bool flag = bucketPage->Lookup(key, value, cmp_);
  rpg_bucket.Drop();

  if (flag == false) {
    return false;
  } else {
    result->push_back(value);
    return true;
  }

}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

  /** TODO(P2): Add implementation
   * Inserts a key-value pair into the hash table.
   *
   * @param key the key to create
   * @param value the value to be associated with the key
   * @param transaction the current transaction
   * @return true if insert succeeded, false otherwise
   */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  //总结一下，写了好久，一直在Read锁和Write锁的切换，很麻烦
  //又重新看了下PPT，insert给每个page加写锁就行了，即使可能不需要对其进行写
  //采用index crabbing alogirth 因为这里的Extendible Hash也是一层一层的（head->directory->bucket）
  //所以在child上了锁之后，判断safe了，也就是不会进行insert或者merge从而修改parent，就可以将上一层parent的锁释放了
  //ppt上还讲了乐观锁，就是乐观地认为只有最后一层的leaf node会发生修改，这样就可以及时的释放parent的latch。
  //但是如果leaf node修改导致parent node也要修改的话，那么就重来一次，按照之前的判断safe的保守策略
  uint32_t hash = Hash(hash_fn_.GetHash(key));
  
  //操作header page
  WritePageGuard wpg_head = bpm_->FetchPageWrite(header_page_id);
  ExtendibleHTableHeaderPage* headerPage = wpg_head.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = headerPage->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = headerPage->GetDirectoryPageId(directory_idx);

  WritePageGuard wpg_directory;

  if (directory_page_id == 0) { //创建directory page
    wpg_directory = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
    headerPage->SetDirectoryPageId(directory_idx, directory_page_id);
  } else {
    wpg_directory = bpm_->FetchPageWrite(directory_page_id);
  }
  wpg_head.Drop();


  //操作directory page
  ExtendibleHTableDirectoryPage* directoryPage = wpg_directory.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directoryPage->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directoryPage->GetBucketPageId(bucket_idx);

  WritePageGuard wpg_bucket;

  if (bucket_page_id == 0) { 
    //想一下什么情况下directory page里记录的bucket page会为空
    //刚创建diretory page时，其global depth为0，
    wpg_bucket = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
    directoryPage->SetBucketPageId(bucket_idx, bucket_page_id);
    //to do----------------------------------------------------
    directoryPage->
  } else {
    wgp_bucket = bpm_->FetchPageWrite(&bucket_page_id).UpgradeWrite();
  }

  //操作bucket page
  ExtendibleHTableBucketPage* bucketPage = wpg_bucket.AsMut<ExtendibleHTableBucketPage>();
  bool flag = bucketPage->Insert(key, value, cmp_);

  //如果插入失败，也就是bucket空间不足，还需要考虑扩展哈希表的情况
  //因为要求是unique key 所以插入重复的key也会造成插入失败
  if (flag == false) {
    if (!bucketPage->IsFull()) {
      return false; //duplicate key
    } else {
        //先看当前的bucket的local depth是否等于directory的global depth
        //如果不等，就可以对bucket进行split
        //如果相等，则需要先对directory进行扩展，再对bucket进行split
        if (directoryPage->GetLocalDepth(bucket_idx) < directoryPage->GetGlobalDepth()) {
          //需要新建一个bucket page，然后找到该index的split index，然后将该bucket中的元素重新映射到两个bucket中
          //之后修改directory page中这两个index对应的local depth
          page_id_t* page_id;
          WritePageGuard wpg_bucket_new = bpm_->NewPageGuarded(page_id).UpgradeWrite();
          ExtendibleHTableBucketPage* bucketPageNew = wpg_bucket_new.AsMut<ExtendibleHTableBucketPage>();
          //rehash 将原来的page（wpg2）的部分key-val迁移到新建的page（ipg）上
          //split: 假如说当前global depth = 3，001发现bucket满了，要进行split
          //local_depths_[001] = 1 说明001 011 101 111这四个hash都会映射到同一个桶中
          //所以进行split时，local_depths[001]++  001与101映射到一个桶中、011与111映射到另一个桶中
          //然后需要修改001 011 101 111 ExtendibleHTableDirectoryPage的 bucket_page_ids_[]的映射关系
          uint8_t local_depth = directoryPage->GetLocalDepth(bucket_idx);
          uint32_t local_depth_mask = directoryPage->GetLocalDepthMask(bucket_idx);
          uint32_t num1 = bucket_idx & local_depth_mask;
          uint32_t num2 = bucket_idx & local_depth_mask + (1 << local_depth);
          uint32_t size = bucketPage->Size();
          uint32_t newLocalDepthMask = 1 << (local_depth + 1) - 1;
          for (uint32_t i = 0; i < size; i++) {
            std::pair<KeyType, ValueType> kv = bucketPage->EntryAt(i);
            if (kv.first & newLocalDepthMask == num2) {
              bucketPageNew->Insert(kv.first, kv.second, cmp_);
              bucketPage->RemoveAt(i);
            }
          }
          //修改ExtendibleHTableDirectoryPage的映射关系，并且修改local depth
          uint32_t global_depth = directoryPage->GetGlobalDepth(); 
          for (uint32_t i = 0; i < (1 << global_depth); i++) {
            if (i & newLocalDepthMask == num1) {
              directoryPage->SetLocalDepth(i, local_depth + 1);
              directortPage.SetBucketPageId(i, bucket_page_id);
            } else if (i & newLocalDepthMask == num2) {
              directoryPage->SetLocalDepth(i, local_depth + 1);
              directoryPage->SetBucketPageId(i, *page_id);
            }
          }
        } else {
          //local depth = global depth 说明需要扩展directory了
          //需要在bucket_page_ids_[]中填入新的项，
          /* 举个例子，加入当前的global_depth = 2  00 01 10 11
           * 假设00和10映射到一个桶，local depth = 1 然后01 11映射到两个桶中，local depth = 2
           * 之后101要插入01对应的桶，发现满了，扩展目录
           * 000 010 100 110仍然映射到那个桶 011 111映射到一个桶  001和101要分别映射
          **/
          page_id_t* page_id;
          WritePageGuard wpg_bucket_new = bpm_->NewPageGuarded(page_id).UpgradeWrite();
          ExtendibleHashTableBucketPage* bucketPageNew = wpg_bucket_new.AsMut<ExtendibleHTableBucketPage>();
          directoryPage->IncrGlobalDepth();         
          uint8_t local_depth = directoryPage->GetLocalDepth(bucket_idx);
          uint32_t local_depth_mask = directoryPage->GetLocalDepthMask(bucket_idx);
          uint32_t num1 = bucket_idx & local_depth_mask;
          uint32_t num2 = bucket_idx & local_depth_mask + (1 << local_depth);
          uint32_t size = bucketPage->Size();
          uint32_t newLocalDepthMask = 1 << (local_depth + 1) - 1;
          directoryPage->IncrLocalDepth(num1);
          directoryPage->IncrLocalDepth(num2);
          for (uint32_t i = 0; i < size; i++) {
            std::pair<KeyType, ValueType> kv = bucketPage->EntryAt(i);
            if (kv.first & newLocalDepthMask == num2) {
              bucketPageNew->Insert(kv.first, kv.second, cmp_);
              bucketPage->RemoveAt(i);
            }
          }
          //修改directory的映射 和 local depth、 当扩展了directory这里更新映射是有点复杂的
          bool visit[1 << directoryPage->GetGlobalDepth()];
          for (uint32_t i = 0; i < (1 << directoryPage->GetGlobalDepth() - 1); i++) {
            local_depth = directoryPage->GetLocalDepth(i);
            page_id_t page_id = directoryPage->GetBucketPageId(i);
            for (uint32_t j = (1 << directoryPage->GetGlobalDepth() - 1); j < (1 << directoryPage->GetGlobalDepth()); j++) {
              if (visit[j] == true)
                continue;
              if (j )
            }
          }

          
        }
    }
  } else {
    return true;
  }

}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
  /** TODO(P2): Add implementation
   * Removes a key-value pair from the hash table.
   *
   * @param key the key to delete
   * @param value the value to delete
   * @param transaction the current transaction
   * @return true if remove succeeded, false otherwise
   */
//插入，删除，读取的过程都类似，都是header page->directory page->bucket page 一步步的找
//最后调用ExtendibleHTableBucketPage类中提供的成员函数Lookup、Insert、Remove函数
//不同的是remove要考虑merge的情况，insert要考虑split的情况
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(hash_fn_.GetHash(key));

  //操作header page
  WritePageGuard wpg_head = bpm_->FetchPageWrite(header_page_id);
  ExtendibleHTableHeaderPage* headerPage = wpg_head.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = headerPage->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = headerPage->GetDirectoryPageId(directory_idx);

  if (directory_page_id == 0)
    return false;

  wpg_head.Drop(); 

  //操作directory page
  WritePageGuard wpg_directory = bpm_->FetchPageWrite(directory_page_id);
  ExtendibleHTableDirectoryPage* directoryPage = wpg_directory.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directoryPage->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directoryPage->GetBucketPageId(bucket_idx);

  if (bucket_page_id == 0)
    return false;

  //操作bucket page
  WritePageGuard wpg_bucket = bpm_->FetchPageWrite(bucket_page_id);
  ExtendibleHTableBucketPage* bucketPage = wpg_bucket.AsMut<ExtendibleHTableBucketPage>();
  bool flag = bucketPage->Remove(key, cmp_);

  if (flag == false) {
    wpg_directory.Drop();
    wpg_bucket.Drop();
    return false;
  }


  //要考虑删除元素后bucket变为empty，考虑merge的情况
  while (bucketPage->IsEmpty()) {
    uint8_t local_depth = directoryPage->GetLocalDepth(bucket_idx);
    uint32_t split_bucket_idx = directoryPage->GetSplitImageIndex(bucket_idx);
    //检查split image的local depth是否和当前的local depth相同
    if (directoryPage->GetLocalDepth(split_bucket_idx) == local_depth) {
      //set local depth
      directoryPage->DecrLocalDepth(bucket_idx);
      directoryPage->DecrLocalDepth(split_bucket_idx);

      //delete empty page
      wpg_bucket.Drop(); //unpin unlock
      bpm_->DeletePage(bucket_page_id);

      //set mapping
      page_id_t split_bucket_page_id = directoryPage->GetBucketPageId(split_bucket_idx);
      directoryPage->SetBucketPageId(bucket_idx, split_bucket_page_id);

      //recursively merge 修改变量名称
      bucket_idx = split_bucket_idx;
      bucket_page_id = split_bucket_page_id;
      wpg_bucket = bpm_->FetchPageWrite(bucket_page_id);
      bucketPage = wpg_bucket.AsMut<ExtendibleHTableBucketPage>();
    }
  } 

  //merge后检查directory是否能shrink
  if (directoryPage->CanShrink()) {
    Shrink();
  }

  wpg_bucket.Drop();
  wpg_directory.Drop();

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
