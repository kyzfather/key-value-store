//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    if (curr_size_ == 0) //LRUKReplacer的curr_size_表明有多少个frame是evictable的，如果等于0返回false
        return false;
    size_t timestamp1; //如果没有inf的，使用这个时间戳比较
    size_t timestamp2; //如果有inf的，也就是不足K次访问的，使用这个时间戳比较
    bool flag;
    for (auto it = node_store_.begin(); it != node_store_.end(); it++) {
        LRUKNode node = it->second;
        if (node.isEvictable() == false)
            continue;
        size_t time = node.getKdistance(flag);
        if (flag) {
            if (time < timestamp2) {
                *frame_id = node.getID();
                timestamp2 = time;
            }
        } else {
            if (time < timestamp1) {
                *frame_id = node.getID();
                timestamp1 = time;
            }
        } 
    }

    node_store_.erase(*frame_id);
    curr_size_--;
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
    //需要检查frame_id是否valid
    //to do
    if (node_store_.find(frame_id) == node_store_.end()) {
        node_store_[frame_id] = LRUKNode(current_timestamp_, frame_id);
    } else {
        LRUKNode node = node_store_[frame_id];
        node.addRecord(current_timestamp_);
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    //检查frame_id是否valid
    //to do
    LRUKNode node = node_store_[frame_id];
    if (set_evictable == true) {
        if (node.isEvictable() == false) {
            node.setEvictable(true);
            curr_size_++;
        } 
    }
    if (set_evictable == false) {
        if (node.isEvictable() == true) {
            node.setEvictable(false);
            curr_size_--;
        }
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    //检查该frame是否是non-evictable的
    //to do
    node_store_.earse(frame_id);
    curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
