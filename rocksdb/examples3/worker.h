#pragma once

#include "rocksdb/db.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

#include "rocksdb_eval.h"

#include <unistd.h>
#include <string>

namespace sim {
  using namespace rocksdb;
  using std::string;

  class Controller;


// delegation to run a txn
  class Worker {
    friend class WorkerGen;

  protected:
    Controller* controller_; // use Controller::Decide to consult for access way
    size_t nworkers_;
    size_t idx_;

    TransactionDB* txn_db_;

    bool deadlock_detect = true;
    bool disable_wal = true;
    bool write_sync = false;

    inline bool Opt(AgentDecision decision) {
      switch(decision) {
        case agent_occ: return true;
        case agent_ic3: return true;
        case agent_tpl: return false;
        default: return true;
      }
    }

    inline bool Dirty(AgentDecision decision) {
      switch(decision) {
        case agent_occ: return false;
        case agent_ic3: return true;
        case agent_tpl: return false;
        default: return false;
      }
    }

    inline bool Wait(AgentDecision decision) {
      return decision == agent_ic3;
    }

  public:
    Worker(Controller* controller, size_t nworkers, size_t idx, TransactionDB* txn_db) :
        controller_(controller),
        nworkers_(nworkers),
        idx_(idx),
        txn_db_(txn_db) {}
    virtual ~Worker() {}

    // run a txn. return whether is successful
    virtual bool Run()= 0;
  };

// generator to produce workers, in charge of opening database and all other stuff
  class WorkerGen {
  protected:
    Controller* controller_;
    size_t nworkers_;

  public:
    WorkerGen(Controller* controller, size_t nworkers) :
        controller_(controller),
        nworkers_(nworkers) {}

    TransactionDB* txn_db;

    virtual ~WorkerGen() { delete txn_db; };

    virtual void Init()= 0; // initialization (if neccessary). called from controller.
    virtual Worker *Next(size_t idx)= 0; // generate a txn for thread [idx]. called from controller.
  };
}

