#pragma once

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "worker.h"

#include <unistd.h>
#include <vector>

namespace sim {
  using namespace rocksdb;
  using std::string;
  using std::vector;

  class Controller;

// delegation to run a txn
  class YCSBWorker : public Worker {
    friend class YCSBWorkerGen;

  protected:
    drand48_data *buffer_;
    drand48_data *buffer_rw_;

    // number of key range size (table size)
    int keys_;
    double g_zipf_theta_;
    const unsigned int txn_size = 16;

  public:
    YCSBWorker(Controller *controller, size_t nworkers, size_t idx,
               TransactionDB *txn_db, drand48_data *buffer, drand48_data *buffer_rw,
               int keys, double g_zipf_theta)
        : Worker(controller, nworkers, idx, txn_db),
          buffer_(buffer),
          buffer_rw_(buffer_rw),
          keys_(keys),
          g_zipf_theta_(g_zipf_theta) {}

    // run a txn. return whether is successful
    bool Run() override;
  };

// generator to produce workers, in charge of opening database and all other stuff
  class YCSBWorkerGen : public WorkerGen {

  protected:
    vector<drand48_data *> v_buffer_;
    vector<drand48_data *> v_buffer_rw_;

    // number of key range size (table size)
    int keys_;
    double g_zipf_theta_;

  public:
    YCSBWorkerGen(Controller *controller, size_t nworkers,
                  int keys, double g_zipf_theta)
        : WorkerGen(controller, nworkers),
          keys_(keys),
          g_zipf_theta_(g_zipf_theta) {}

    ~YCSBWorkerGen();

    void Init() override; // initialization (if neccessary). called from controller.
    Worker *Next(size_t idx) override; // generate a txn for thread [idx]. called from controller.
  };
}

