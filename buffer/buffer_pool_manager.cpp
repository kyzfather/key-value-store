//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }


  /**
   * TODO(P1): Add implementation
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
//newpage是干什么用的？ 就是给一个页号在buffer pool中分配一个帧，但是这个帧中没有实际内容，磁盘上也没有
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  if (!free_list_.empty()) {
    *page_id = AllocatePage();
    frame_id_t frame_id = free_list_[free_list_.size() - 1];
    free_list_.pop_back();
    Page* page = &pages_[frame_id];
    page->page_id = *page_id;
    page->pin_count_ = 1;
    replacer_->RecordAccess(frame_id); 
  } else {
    frame_id_t* frame_id;
    bool flag = replacer_->Evict(frame_id);
    if (flag == false)
      return nullptr;
    Page* page = pages_[*frame_id];

    if (page->IsDirty()) { 
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest diskRequest = DiskRequest(true, page->GetData(), page->GetPageId(), std::move(promise));
      disk_scheduler_->Schedule(diskRequest);
      future.get();
    }
    *page_id = AllocatePage();
    page->ResetMemory();
    page->page_id_ = *page_id;
    page->count_ = 1;
    page->is_dirty_ = false;
    replacer_->RecordAccess(frame_id);
  }
  return page;
}



/**
   * TODO(P1): Add implementation
   *
   * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
   * but all frames are currently in use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
   * the replacer (always find from the free list first), read the page from disk by scheduling a read DiskRequest with
   * disk_scheduler_->Schedule(), and replace the old page in the frame. Similar to NewPage(), if the old page is dirty,
   * you need to write it back to disk and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
   *
   * @param page_id id of page to be fetched
   * @param access_type type of access to the page, only needed for leaderboard tests.
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
*/

//有个问题：fetchpage需要将page的pincount增加吗？
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  if (page_table_.find(page_id) == page_table_.end()) {  //page_table_是page_id到frame_id的映射，如果找不到page_id, 说明该页不在buffer pool中
    if (!free_list.empty()) {  //先查看free list中是否有空间，如果有需要从磁盘中读取，并且发出DiskRequest请求
      frame_id_t frame_id = free_list_[free_list_.size() - 1];
      free_list_.pop_back();
      Page* page = &pages_[frame_id];
      //fetch是否要将pin_count + 1 ? 
      //因为page的友元是BufferPoolManager 所以可以操作Page的私有成员变量
      page->pin_count_ = 1;
      page->page_id_ = page_id;

      auto promise = disk_scheduler->CreatePromise();
      auto future = promise.get_future();
      DiskRequest diskRequest = DiskRequest{false, page->GetData(), page_id, std::move(promise)};
      disk_scheduler_->Schedule(diskRequest);
      future.get(); //future的get函数会等待异步操作完成否则会阻塞，也就是等待diskScheduler的worker thread处理完这个请求。promise set value后即异步操作完成

      page_table_[page_id] = frame_id;
      replacer_->RecordAccess(frame_id);
      return page;                
    } else { //没有free frame，需要寻找替换的页，如果替换的页是dirty的还要将其写入到磁盘
      frame_id_t* frame_id;
      bool flag = replacer_->Evict(frame_id);
      if (flag == false) { //没有可以替换的页
        return nullptr;
      } else { //有可替换的页
        Page* page = &pages_[frame_id];
        if (page->is_dirty_() == true) { //如果是dirty的，需要先将其写回磁盘中
          auto promise = disk_scheduler->CreatePromise();
          auto future = promise.get_future();
          DiskRequest diskRequest = DiskRequest(true, page->GetData(), page->GetPageId(), std::move(promise));
          disk_scheduler_->Schedule(diskRequest);
          future.get(); //已经将受害者页写回磁盘
        }
        //将新的要fetch的页从磁盘中读出，并写到这个替换的页上。
        auto promise = disk_scheduler->CreatePromise();
        auto future = promise.get_future();
        DiskRequest diskRequest = DiskRequest(false, page->GetData(), page_id, std::move(promise));
        disk_scheduler_->Schedule(diskRequest);
        future.get();
        //需要将原来的page的内容修改为新的page的内容，包括dirty位、pin_count、page_id
        page->is_dirty_ = false;
        page->pin_count_ = 1;
        page->page_id_ = page_id;
        page_table_[page_id] = frame_id;
        replacer_->RecordAccess(frame_id);
        return page;
      }
    }
  } else { //需要fetch的页已经在bufferpool中
    frame_id_t frame_id = page_table_[page_id];
    Page* page = &pages_[frame_id];
    //获取一个页，需要增加该页的访问记录，并且设置为不可替换的
    replacer_->RecordAccess(frame_id);
    page->pin_count_++;
    //注意，如果page不在bufferpool manager中，调用replacer_->RecordAccess(frame_id)会创建新的LRUKNode,evictable标记是false
    //但是如果page已经在bufferpool manager中，该page有对应的LRUKNode，这时要修改evictable标记（之前的pin_count可能是0，所以是evictable的）
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

}

  /**
   * TODO(P1): Add implementation
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @param access_type type of access to the page, only needed for leaderboard tests.
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   */
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_table_.find(page_id) == page_table_.end())
    return false;
  frame_id_t frame_id = page_table_[page_id];
  Page* page = &pages_[frame_id];
  if (page->GetPinCount() == 0)
    return false;
  page->pin_count_--;
  if (page->GetPinCount() == 0)
    replacer_->SetEvictable(frame_id, true);
  if (is_dirty)
    page->is_dirty_ = true;
  return true;
}

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush the target page to disk.
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
   * Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  if (page_table_.find(page_id) == page_table_.end())
    return false;
  frame_id_t frame_id = page_table_[page_id];
  Page page = pages_[frame_id];

  //将对应的帧写回磁盘，并且将dirty位清空
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest diskRequest = DiskRequest(true, page.GetData(), page.GetPageId(), std::move(promise));
  disk_scheduler_->Schedule(diskRequest);
  future.get();

  page.is_dirty_ = false;
  return true;
}

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush all the pages in the buffer pool to disk.
   */
void BufferPoolManager::FlushAllPages() {
  for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
    page_id_t page_id = it->first;
    frame_id_t frame_id = it->second;
    Page page = pages_[frame_id];

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    DiskRequest diskRequest = DiskRequest(true, page.GetData(), page.GetPageId(), std::move(promise));
    disk_scheduler_->Schedule(diskRequest);
    future.get();    

    page.is_dirty_ = false;
  }
  return 0;
}

  /**
   * TODO(P1): Add implementation
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   * page is pinned and cannot be deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
//将一个页从buffer pool memory中删除，同时也从dist中删除
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end())
    return true;

  frame_id_t frame_id = page_table_[page_id];
  //Page page = pages_[frame_id]; 不能这样子写吧，然后后面再修改怕page，这样子相当于是拷贝，得用引用或者指针
  Page* page = &pages_[frame_id];
  if (page->GetPinCount() != 0)
    return false;
  
  //bufferpool manager中删除这条page记录
  page_table_.erase(page_id);

  //清空该页对应的帧的内容
  page->ResetMemory();
  page->pin_count_ = 0;
  page->page_id = INVALID_PAGE_ID;
  page->is_dirty_ = false;

  //将该帧重新放入到free list中，然后不再跟踪该帧的访问记录
  free_list_.push_back(frame_id);
  replacer_->Remove(frame_id);

  //将该页的内容从磁盘上清空
  DeallocatePage();

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

  /**
   * TODO(P2): Add implementation
   *
   * @brief PageGuard wrappers for FetchPage
   *
   * Functionality should be the same as FetchPage, except
   * that, depending on the function called, a guard is returned.
   * If FetchPageRead or FetchPageWrite is called, it is expected that
   * the returned page already has a read or write latch held, respectively.
   *
   * @param page_id, the id of the page to fetch
   * @return PageGuard holding the fetched page
   */
auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { 
  Page* page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page* page = FetchPage(page_id);
  page->RLatch(); //这里要上锁，ReadPageGuard析构的时候会自动解锁
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  Page* page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}


  /**
   * TODO(P2): Add implementation
   *
   * @brief PageGuard wrapper for NewPage
   *
   * Functionality should be the same as NewPage, except that
   * instead of returning a pointer to a page, you return a
   * BasicPageGuard structure.
   *
   * @param[out] page_id, the id of the new page
   * @return BasicPageGuard holding a new page
   */
auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { 
  Page* page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
