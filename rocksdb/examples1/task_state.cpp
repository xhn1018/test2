#include "task_state.h"
#include "config.h"

namespace sim {
const uint8_t kNoOpFlag = 0;
const uint8_t kOptimisticReadFlag = 1;
const uint8_t kPessimisticReadFlag = 2;
const uint8_t kOptimisticWriteFlag = 4;
const uint8_t kPessimisticWriteFlag = 8;

State& TaskState::GetState() {
        state_buf_.resize(13);
        for (int i = 0; i < 13; i++) state_buf_[i] = 0;

        StateUnit* ptr = (StateUnit*)info.handle;

        uint64_t opt_reads = ptr[kOptimisticReadIndex];
        if (opt_reads > 1) state_buf_[1] = 1;
        else if (opt_reads == 1) state_buf_[2] = 1;
        else state_buf_[3] = 1;

        uint64_t pes_reads = ptr[kPessimisticReadIndex];
        if (pes_reads > 1) state_buf_[4] = 1;
        else if (pes_reads == 1) state_buf_[5] = 1;
        else state_buf_[6] = 1;

        uint64_t opt_writes = ptr[kOptimisticWriteIndex];
        if (opt_writes > 1) state_buf_[7] = 1;
        else if (opt_writes == 1) state_buf_[8] = 1;
        else state_buf_[9] = 1;

        uint64_t pes_writes = ptr[kPessimisticWriteIndex];
        if (pes_writes > 1) state_buf_[10] = 1;
        else if (pes_writes == 1) state_buf_[11] = 1;
        else state_buf_[12] = 1;

        return state_buf_;
}

int TaskState::EncodeState(int access_id) {
        int rw = state_buf_[0] == 0 ? 0 : 1;
        int occ_r = 0;
        int pes_r = 0;
        int occ_w = 0;
        int pes_w = 0;

        if (state_buf_[1] == 1) occ_r = 0;
        else if (state_buf_[2] == 1) occ_r = 1;
        else if (state_buf_[3] == 1) occ_r = 2;

        if (state_buf_[4] == 1) pes_r = 0;
        else if (state_buf_[5] == 1) pes_r = 1;
        else if (state_buf_[6] == 1) pes_r = 2;

        if (state_buf_[7] == 1) occ_w = 0;
        else if (state_buf_[8] == 1) occ_w = 1;
        else if (state_buf_[9] == 1) occ_w = 2;

        if (state_buf_[10] == 1) pes_w = 0;
        else if (state_buf_[11] == 1) pes_w = 1;
        else if (state_buf_[12] == 1) pes_w = 2;

        return rw * 81 * TotalAccess +
        occ_r * 27 * TotalAccess +
        pes_r * 9 * TotalAccess +
        occ_w * 3 * TotalAccess +
        pes_w * 1 * TotalAccess +
        access_id * 1;
}

int TaskState::EncodeStateForWaitDecision(int dep_piece_id) {

        int occ_r = 0;
        int pes_r = 0;
        int occ_w = 0;
        int pes_w = 0;

        if (state_buf_[1] == 1) occ_r = 0;
        else if (state_buf_[2] == 1) occ_r = 1;
        else if (state_buf_[3] == 1) occ_r = 2;

        if (state_buf_[4] == 1) pes_r = 0;
        else if (state_buf_[5] == 1) pes_r = 1;
        else if (state_buf_[6] == 1) pes_r = 2;

        if (state_buf_[7] == 1) occ_w = 0;
        else if (state_buf_[8] == 1) occ_w = 1;
        else if (state_buf_[9] == 1) occ_w = 2;

        if (state_buf_[10] == 1) pes_w = 0;
        else if (state_buf_[11] == 1) pes_w = 1;
        else if (state_buf_[12] == 1) pes_w = 2;

        return OneAccessEntryCount * TotalAccess +
        occ_r * 27 * TotalDepPiece +
        pes_r * 9 * TotalDepPiece +
        occ_w * 3 * TotalDepPiece +
        pes_w * 1 * TotalDepPiece +
        dep_piece_id * 1;
}

} // namespace sim
