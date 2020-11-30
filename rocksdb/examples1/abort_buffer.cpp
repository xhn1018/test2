#include "abort_buffer.h"

namespace sim {

  AbortBuffer::AbortBuffer()
      : header_(nullptr),
        size_(0) {

  }

  AbortBuffer::~AbortBuffer() {
    auto *current = header_;
    while (current != nullptr) {
        auto *last = current;
        current = current->link_newer;
        delete last;
    }
  }

  void AbortBuffer::Put(Worker* worker) {
    auto *current = new AbortItem(worker);
    auto *header = header_;
    if (header == nullptr) {
      header_ = current;
    } else {
      while (header->link_newer != nullptr) {
        header = header->link_newer;
      }
      header->link_newer = current;
      current->link_older = header;
    }
    size_++;
  }

  void AbortBuffer::RunOld(Controller* controller, size_t idx) {
      auto *current = header_;
      while (current != nullptr) {
          Worker* worker = current->GetWorker();
          bool result = worker->Run();
          if (controller->recording) {
              if (result) controller->IncreaseCommit(idx);
              else controller->IncreaseAbort(idx);
          }
          controller->Finish(idx);
          if (result) { // What about controller->NeedRetry
              size_--;
              // remove the finished item - start
              if (current->link_older == nullptr) {
                  //head of the linked list
                  auto *new_header = current->link_newer;
                  if (new_header == nullptr) {
                      header_ = nullptr;
                      delete current;
                      return;
                  } else {
                      header_ = new_header;
                      new_header->link_older = nullptr;
                      delete current;
                      current = new_header;
                  }
              } else if (current->link_newer == nullptr) {
                  //end of the linked list, not single item in the linked list
                  auto *former = current->link_older;
                  former->link_newer = nullptr;
                  current->link_older = nullptr;
                  delete current;
                  current = nullptr;
              } else {
                  //middle of the linked list
                  auto *former = current->link_older;
                  auto *latter = current->link_newer;
                  former->link_newer = latter;
                  latter->link_older = former;
                  delete current;
                  current = latter;
              }
              // remove the finished item - end
          } else {
              current = current->link_newer;
          }
      }
  }

  AbortItem::AbortItem(Worker* worker)
      : worker_(worker) {

  }

  AbortItem::~AbortItem() {
    delete worker_;
  }

}
