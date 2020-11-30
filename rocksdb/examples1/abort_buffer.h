#pragma once
#include <atomic>
#include <memory>

#include "worker.h"
#include "controller.h"

namespace sim {

class AbortItem;

class AbortBuffer {
 public:
  explicit AbortBuffer();

  ~AbortBuffer();

  void Put(Worker* worker);

  void RunOld(Controller* controller, size_t idx);

  unsigned int getSize() { return size_; }
 private:
  AbortItem* header_;
  unsigned int size_;

  // No copying allowed
  AbortBuffer(const AbortBuffer&);
  AbortBuffer& operator=(const AbortBuffer&);
};

class AbortItem {
 public:
  explicit AbortItem(Worker* worker);

  ~AbortItem();

  inline Worker* GetWorker() { return worker_; };

 private:
  friend class AbortBuffer;

  Worker* worker_;

  AbortItem* link_older = nullptr;
  AbortItem* link_newer = nullptr;

  // No copying allowed
  AbortItem(const AbortItem&);
  AbortItem& operator=(const AbortItem&);
};

}
