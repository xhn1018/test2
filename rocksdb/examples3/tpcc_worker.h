#pragma once

#include <unistd.h>
#include <cinttypes>
#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <random>

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rwlock.h"
#include "spinlock.h"
#include "util.h"
#include "worker.h"

namespace sim {
  using namespace rocksdb;
  using std::mutex;
  using std::vector;
  using std::string;
  using std::unordered_map;
  using util::fast_random;
  using spinlock_map = unordered_map<string, RWMutex*>;

  class Controller;

  class abort_exception {
  };

// delegation to run a txn
class TPCCWorker : public Worker {
friend class TPCCWorkerGen;
typedef bool(TPCCWorker::*txn_func)();

private:
    static const txn_func txn_funcs[5];
    vector<vector<mutex>*> *update_locks;
    // 0 - txn_new_order, 1 - txn_payment, 2 - txn_delivery
    // 3 - txn_order_status, 4 - txn_stock_level
    const int tpcc_type_;
    const unsigned long seed_;
    vector<ColumnFamilyHandle*> handles;
    mutex *get_update_lock(const string &key, int cfd = 0);
    int32_t *last_no_o_ids;

    bool txn_new_order();
    bool txn_delivery();
    bool txn_payment();
    bool txn_order_status();
    bool txn_stock_level();

    void do_wait(Transaction *txn, unsigned int p_idx);

public:
    TPCCWorker(Controller* controller, size_t nworkers, size_t idx,
               int tpcc_type, unsigned long seed, TransactionDB *txn_db,
               vector<ColumnFamilyHandle*> cf_handles)
        : Worker(controller, nworkers, idx, txn_db),
          tpcc_type_(tpcc_type),
          seed_(seed),
          handles(std::move(cf_handles)) {}

    // run a txn. return whether is successful
    bool Run() override;
};

// generator to produce workers, in charge of opening database and all other stuff
class TPCCWorkerGen : public WorkerGen {
private:
    vector<ColumnFamilyDescriptor> column_families;
    vector<ColumnFamilyHandle*> handles;
    vector<vector<mutex>*> update_locks;

    fast_random* r;

    std::vector<int32_t*> hack_map;

    int get_tpcc_txn_type(unsigned long seed);

public:
    TPCCWorkerGen(Controller *controller, size_t nworkers);

    ~TPCCWorkerGen();
    
    void Init() override; // initialization (if neccessary). called from controller.
    Worker* Next(size_t idx) override; // generate a txn for thread [idx]. called from controller.
};
}
