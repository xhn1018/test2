#pragma once

#include <atomic>
#include <unordered_map>

#include "rocksdb_eval.h"
#include "../utilities/state_info.h"

namespace sim {
using rocksdb::StateUnit;
using rocksdb::StateInfo;
using rocksdb::StateInfoInternal;
using rocksdb::kOptimisticReadIndex;
using rocksdb::kPessimisticReadIndex;
using rocksdb::kOptimisticWriteIndex;
using rocksdb::kPessimisticWriteIndex;

struct TaskState {
	uint8_t     local_state;
	StateInfo   info;

	TaskState(StateInfoInternal* handle) 
            : local_state(0), 
              info(handle), 
              state_buf_(13) {
            state_buf_.reserve(14);
    }

	TaskState() : TaskState(nullptr) {}

	State& GetState();

    int EncodeState(int access_id);

    int EncodeStateForWaitDecision(int dep_piece_id);

    void Reset(StateInfoInternal* h) {
            local_state = 0;
            info.SetHandle(h);
    }

private:
    State   state_buf_;
};

using TaskStates = std::unordered_map<uint32_t, std::unordered_map<std::string, TaskState*>>;

extern const uint8_t kNoOpFlag;
extern const uint8_t kOptimisticReadFlag;
extern const uint8_t kPessimisticReadFlag;
extern const uint8_t kOptimisticWriteFlag;
extern const uint8_t kPessimisticWriteFlag;
}
