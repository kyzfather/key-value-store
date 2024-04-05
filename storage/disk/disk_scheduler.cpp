//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  throw NotImplementedException(
      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
      "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

//这里的schedule就是将一个DiskRequest放入到队列中
//就这么简单
void DiskScheduler::Schedule(DiskRequest r) {
  request_queue_.Put(std::move(r));
}

//startworkerthread是作为一个backgroud worker thread线程运行，负责处理Disrequest
//它要做的事情就是从队列中取出DiskRequest，然后调用DiskManager处理这个请求
void DiskScheduler::StartWorkerThread() {
  while (true) {
    DiskRequest r = request_queue_.Get();
    if (r.is_write_) {
      disk_manager_->WritePage(r.page_id_, r.data_);
    } else {
      disk_manager_->ReadPage(r.page_id_, r.data_);
    }
    //执行结束了应该修改DiskRequest中的promise变量，以通知调用者完成了操作
    r.callback_.set_value(true);
  }
}




}  // namespace bustub
