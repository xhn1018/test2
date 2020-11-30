#pragma once

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "worker.h"

#include <unistd.h>

namespace sim {
  using namespace rocksdb;
  using std::string;

  class Controller;

// delegation to run a txn
  class LargeWorker : public Worker {
    friend class LargeWorkerGen;

  protected:
    // number of key range size (table size)
    int keys_;

    const unsigned int txn_size = 30;
    const int write_size = txn_size / 6;

  public:
    LargeWorker(Controller *controller, size_t nworkers, size_t idx,
                TransactionDB *txn_db, int keys)
        : Worker(controller, nworkers, idx, txn_db),
          keys_(keys) {}

    // run a txn. return whether is successful
    bool Run() override;
  };

// generator to produce workers, in charge of opening database and all other stuff
  class LargeWorkerGen : public WorkerGen {

  protected:
    // number of key range size (table size)
    int keys_;

  public:
    LargeWorkerGen(Controller *controller, size_t nworkers, int keys)
        : WorkerGen(controller, nworkers),
          keys_(keys) {}

    void Init() override; // initialization (if neccessary). called from controller.
    Worker *Next(size_t idx) override; // generate a txn for thread [idx]. called from controller.
  };
}

