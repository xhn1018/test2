#pragma once
namespace rocksdb {

static constexpr uint8_t kTotalStates = 4;

static constexpr uint8_t kOptimisticReadIndex = 0;
static constexpr uint8_t kPessimisticReadIndex = 1;
static constexpr uint8_t kOptimisticWriteIndex = 2;
static constexpr uint8_t kPessimisticWriteIndex = 3;

template <bool read, bool is_optimistic>
static constexpr uint8_t GetStateIndex() {
  return read
         ? (is_optimistic ? kOptimisticReadIndex : kPessimisticReadIndex)
         : (is_optimistic ? kOptimisticWriteIndex : kPessimisticWriteIndex);
}

using StateUnit = uint16_t;
using StateInfoInternal = StateUnit[kTotalStates];

struct StateInfo {
  StateInfoInternal* handle;

  StateInfo(StateInfoInternal* info) : handle(info) {}
  StateInfo() : StateInfo(nullptr) {}

  void SetHandle(StateInfoInternal* h) { handle = h; }

  template <bool read, bool is_optimistic>
  inline void IncreaseAccess() {
    constexpr uint8_t index = GetStateIndex<read, is_optimistic>();
#define atomic_inc(P) __sync_add_and_fetch((P), 1)
    atomic_inc(((StateUnit*)handle) + index);
#undef atomic_inc
  }

  template <bool read, bool is_optimistic>
  inline void DecreaseAccess() {
    constexpr uint8_t index = GetStateIndex<read, is_optimistic>();
#define atomic_dec(P) __sync_add_and_fetch((P), -1) 
    atomic_dec(((StateUnit*)handle) + index);
#undef atomic_dec
  }
};
}
