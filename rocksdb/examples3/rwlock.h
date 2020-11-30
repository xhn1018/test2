#pragma once

#include <pthread.h>

namespace sim {

class RWMutex {
 public:
  RWMutex();
  ~RWMutex();

  void ReadLock();
  void WriteLock();
  void ReadUnlock();
  void WriteUnlock();
  void AssertHeld() { }

 private:
  pthread_rwlock_t mu_; // the underlying platform mutex

  // No copying allowed
  RWMutex(const RWMutex&);
  void operator=(const RWMutex&);
};

}