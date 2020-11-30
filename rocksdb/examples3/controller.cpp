#include <unistd.h>
#include <string>
#include <algorithm>

#include "controller.h"
#include "large_worker.h"
#include "ycsb_worker.h"
#include "tpcc_worker.h"
#include "abort_buffer.h"

namespace sim {

string dbPath;
string walPath;

string simpleDbPath = "/tmp/simple_data";
string simpleWalPath = "/tmp/simple_data/wal";
string tpccDbPath = "/tmp/tpcc_data";
string tpccWalPath = "/tmp/tpcc_data/wal";

struct {
    bool operator () (const Decision& d1, const Decision& d2) {
        return d1.time < d2.time;
    }
} decision_comp;

Controller::Controller(size_t nworkers, bool retry, Sample& action_map) : 
            Controller(tpcc, 0, nworkers, retry, action_map, 0.6, false, false) { }

Controller::Controller(Workload_type w, unsigned int nkeys,
               size_t nworkers, bool retry, Sample& action_map, double theta,
               bool direct, bool profile) :
            nworkers_(nworkers), 
            direct_(direct), 
            retry_(retry), 
            record_decision_(false),
            action_map_(action_map), 
            data_(nworkers),
            gen_(w == large ? ((WorkerGen*)new LargeWorkerGen(this, nworkers, nkeys)) :
                w == tpcc ?  ((WorkerGen*)new TPCCWorkerGen(this, nworkers)) : 
                      ((WorkerGen*)new YCSBWorkerGen(this, nworkers, nkeys, theta))),
            profile(profile),
            running(false) {
    dbPath = w == tpcc ? tpccDbPath : simpleDbPath;
    walPath = w == tpcc ? tpccWalPath : simpleWalPath;
    gen_->Init();
    if (direct_)
      decide_return = action_map_(0);
}

Controller::~Controller() { delete gen_; }

unsigned int Controller::commit_sum() {
    unsigned int commits = 0;
    for (auto&& datum : data_) commits += datum.commits;
    return commits;
}

TaskState* Controller::GetInfo(uint32_t cfh_id, const std::string& key, size_t worker_idx) {
    WorkerData& data = data_[worker_idx];
    auto& keys = data.states[cfh_id];

    auto key_iter = keys.find(key);
    if (key_iter == keys.end()) {
        TaskState* t = data.pool.GetNext();
        //t->Reset(gen_->txn_db->DoGetState(cfh_id, key));

        key_iter = keys.emplace(key, t).first;
                 // std::forward_as_tuple(key),    
                 // std::forward_as_tuple(t)).first;
    } 

    return key_iter->second;
}

State_Action Controller::GetActions() {
    // We are not using RL anymore so there should never be a reason to record
    // actions.
    assert(!record_decision_);
    if (!record_decision_) return State_Action();

    vector<Decision> buffer;
    buffer.reserve(1000000);

    for (auto&& datum : data_) 
        for (auto&& d : datum.decisions)
            buffer.emplace_back(std::move(d));

    std::sort(buffer.begin(), buffer.end(), decision_comp);

    State_Action actions;
    actions.reserve(buffer.size());

    for (auto&& d : buffer) actions.emplace_back(std::move(d.s));

    return actions;
}


void Controller::Finish(size_t worker_idx) {
    WorkerData& data = data_[worker_idx];
    for (auto&& cf_and_keys : data.states) {
        auto& keys = cf_and_keys.second;
        for (auto&& key_and_state : keys) {
            TaskState* state = key_and_state.second;
            uint8_t local_state = state->local_state;
            StateInfo& info = state->info;
            if (local_state & kOptimisticReadFlag) info.DecreaseAccess<true, true>();
            if (local_state & kPessimisticReadFlag) info.DecreaseAccess<true, false>();
            if (local_state & kOptimisticWriteFlag) info.DecreaseAccess<false, true>();
            if (local_state & kPessimisticWriteFlag) info.DecreaseAccess<false, false>();
        }
        keys.clear();
    }
    data.pool.Reset();
}

void WorkerFunc(Controller* controller, size_t idx, CyclicBarrier& cb) {
    cb.await();

    AbortBuffer* buffer = new AbortBuffer();
    bool flag_new = false;
    unsigned int MAX_SIZE = 10;
    while (controller->running) {
        unsigned int size = buffer->getSize();
        if (size == 0 || size == 1 || flag_new) {
            Worker* worker = controller->Next(idx);
            bool result = worker->Run();
            if (controller->recording) {
                if (result) controller->IncreaseCommit(idx);
                else controller->IncreaseAbort(idx);
            }
            controller->Finish(idx);
            if (result || !controller->NeedRetry()) {
                delete worker;
            } else {
                // add to buffer
                buffer->Put(worker);
            }
            flag_new = false;
        } else {
            buffer->RunOld(controller, idx);
            if (size != MAX_SIZE) flag_new = true;
        }
    }
    delete buffer;
}

void TimingFunc(Controller* controller, const function<void()>& timer, CyclicBarrier& cb) {
	controller->running = true;
	controller->recording = false;
    cb.await();

    controller->recording = true;
	controller->start_time = steady_clock::now();
    timer();
    controller->end_time = steady_clock::now();
    controller->recording = false;
	controller->running = false;
}

void Controller::DoRun(const function<void()>& timer) {
    CyclicBarrier cb(nworkers_ + 1);

    thread timing_thread(TimingFunc, this, std::ref(timer), std::ref(cb));

    vector<thread> threads;
    for (size_t i = 0; i < nworkers_; i++)
        threads.emplace_back(WorkerFunc, this, i, std::ref(cb));

    timing_thread.join();

    for (size_t i = 0; i < nworkers_; i++) threads[i].join();

    for (auto&& datum : data_) {
            commits += datum.commits;
            aborts += datum.aborts;
            occ_reads += datum.occ_reads;
            occ_writes += datum.occ_writes;
            ic3_reads += datum.ic3_reads;
            ic3_writes += datum.ic3_writes;
            tpl_reads += datum.tpl_reads;
            tpl_writes += datum.tpl_writes;
            waits += datum.waits;
            no_waits += datum.no_waits;
    }
}

void Controller::Run(double arg, bool record_decision) {
    record_decision_ = record_decision;
    if (record_decision_)
            for (auto&& datum : data_) datum.decisions.reserve(800000);

    DoRun([=](){ usleep((int)(1000000 * arg)); });
}

void Controller::EvalRun(unsigned int arg, bool record_decision) {
    record_decision_ = record_decision;
    if (record_decision_)
            for (auto&& datum : data_) datum.decisions.reserve(800000);

    DoRun([=](){ while(this->commit_sum() < arg) ; });
}

Worker* Controller::Next(size_t idx) { return gen_->Next(idx); }

AgentDecision Controller::DecideWait(uint32_t cfh_id, const std::string& key, size_t worker_idx, int dep_piece_id) {

    // if using single concurrency control, occ should return false as "NO WAIT"
    if (direct_) {
        return decide_return;
    }

    assert(dep_piece_id >= 0 && dep_piece_id < TotalDepPiece);

    TaskState* state = GetInfo(cfh_id, key, worker_idx); // get local state

    int index_in_action_map = state->EncodeStateForWaitDecision(dep_piece_id);
    assert(index_in_action_map >= OneAccessEntryCount * TotalAccess && dep_piece_id < TotalEntryCount);
    AgentDecision agent_decision = action_map_(index_in_action_map);

    // track decision
    if (profile) IncreaseWaitDecision(worker_idx, agent_decision);

    return agent_decision;
};

} // namespace sim
