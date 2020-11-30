
#pragma once

#include <thread>
#include <vector>
#include <chrono>
#include <functional>
#include <cassert>

#include "config.h"
#include "rocksdb_eval.h"
#include "task_state.h"
#include "worker.h"
#include "cyclic_barrier.h"
#include "rocksdb/utilities/transaction.h"

namespace sim {
using std::vector;
using std::thread;
using std::map;
using std::function;
using std::chrono::steady_clock;
using t = steady_clock::time_point;
using rocksdb::Transaction;
using simpleutil::CyclicBarrier;

extern string dbPath;
extern string walPath;

class Worker;
class WorkerGen;

struct Decision {
    t       time;
    State   s;

    Decision(t timing, const State& state) : time(timing), s(state) {}
    Decision(Decision&& another) : time(another.time), s(std::move(another.s)) {}
    Decision(const Decision& another) : time(another.time), s(another.s) {}
    Decision& operator=(const Decision& another) {
            time = another.time;
            s = another.s;
            return *this;
    }
};

template <typename T, size_t SZ>
struct SimplePool {
        T* GetNext() { return &pool_[next_++]; }
        void Reset() { next_ = 0; }
        SimplePool() : next_(0) {}
private:
        T   pool_[SZ];
        size_t next_;
};

struct WorkerData {
        vector<Decision>    decisions;
        TaskStates          states;
        unsigned int        aborts;
        unsigned int        commits;
        unsigned int        occ_reads;
        unsigned int        occ_writes;
        unsigned int        ic3_reads;
        unsigned int        ic3_writes;
        unsigned int        tpl_reads;
        unsigned int        tpl_writes;
        unsigned int        waits;
        unsigned int        no_waits;
        SimplePool<TaskState, 400>  pool;
        WorkerData() : aborts(0), commits(0), occ_reads(0), occ_writes(0), ic3_reads(0), ic3_writes(0), tpl_reads(0), tpl_writes(0), waits(0), no_waits(0) {}
};

class Controller {
private:
    size_t              nworkers_;
    bool                direct_;
    bool                retry_;
    bool                record_decision_;
    Sample&             action_map_;
    vector<WorkerData>  data_;
    WorkerGen*          gen_;

    AgentDecision       decide_return;
    bool                profile;

    TaskState* GetInfo(uint32_t cfh_id, const std::string& key, size_t worker_idx);
    void DoRun(const function<void()>& timer);

public:
    volatile bool   running;
    volatile bool   recording;
    t               start_time;
    t               end_time;
    unsigned int    aborts = 0;
    unsigned int    commits = 0;

    unsigned int    occ_reads = 0;
    unsigned int    occ_writes = 0;
    unsigned int    ic3_reads = 0;
    unsigned int    ic3_writes = 0;
    unsigned int    tpl_reads = 0;
    unsigned int    tpl_writes = 0;
    unsigned int    waits = 0;
    unsigned int    no_waits = 0;


    Controller(size_t nworkers, bool retry, Sample& action_map);
    Controller(Workload_type w, unsigned int nkeys, size_t nworkers, bool retry,
               Sample& action_map, double theta, bool direct, bool profile);
    ~Controller();

    // decide clean read or not, privately buffered-write or not
    template <bool is_read>
    AgentDecision Decide(uint32_t cfh_id, const std::string& key, size_t worker_idx, int access_id = 0);

    // decide whether to wait for all the dependencies to commit
    AgentDecision DecideWait(uint32_t cfh_id, const std::string& key, size_t worker_idx, int dep_piece_id);

    State_Action GetActions();
    void Run(double arg, bool record_decision = false);
    void EvalRun(unsigned int arg, bool record_decision = false);

    unsigned int commit_sum();

    Worker* Next(size_t idx);
    void Finish(size_t worker_idx);
    void IncreaseCommit(size_t idx) { data_[idx].commits++; }
    void IncreaseAbort(size_t idx) { data_[idx].aborts++; }

    void IncreaseCCDecision(size_t idx, bool read, AgentDecision decision) {
        if (read) {
            if (decision == agent_occ) data_[idx].occ_reads++;
            else if (decision == agent_ic3) data_[idx].ic3_reads++;
            else if (decision == agent_tpl) data_[idx].tpl_reads++;
        } else {
            if (decision == agent_occ) data_[idx].occ_writes++;
            else if (decision == agent_ic3) data_[idx].ic3_writes++;
            else if (decision == agent_tpl) data_[idx].tpl_writes++;
        }
    }

    void IncreaseWaitDecision(size_t idx, AgentDecision decision) {
        if (decision == agent_ic3) data_[idx].waits++;
        else data_[idx].no_waits++;
    }

    bool NeedRetry() { return retry_; }
};

template <bool is_read>
AgentDecision Controller::Decide(uint32_t cfh_id, const std::string& key, size_t worker_idx, int access_id) {

    // if using single concurrency control, occ should return false as "NOT DIRTY"
    if (direct_) {
        return decide_return;
    }

    TaskState* state = GetInfo(cfh_id, key, worker_idx); // get local state

    uint8_t local_state = state->local_state;

    State& s = state->GetState(); // get db state
    s[0] = is_read ? 0 : 1;

    assert(access_id >= 0 && access_id < TotalAccess);
    AgentDecision agent_decision = action_map_(state->EncodeState(access_id));

    // dirty action(ic3), which also need validation phase,  should also be treated as optimistic
    bool is_opt = agent_decision == agent_tpl ? false : true;

    // modify local state
    state->local_state |= is_read
        ? (is_opt ? kOptimisticReadFlag : kPessimisticReadFlag)
        : (is_opt ? kOptimisticWriteFlag : kPessimisticWriteFlag);

    // we track the state at txn level.
    // i.e., if a txn access a key twice but in the same way, the state should count only 1.
    if (state->local_state !=  local_state) {
        if (is_opt) state->info.IncreaseAccess<is_read, true>();
        else state->info.IncreaseAccess<is_read, false>();
    }

    // track decision
    if (profile) IncreaseCCDecision(worker_idx, is_read, agent_decision);

    return agent_decision;
};

}
