#include "tpcc_worker.h"

#include <unistd.h>
#include <cassert>
#include <string>
#include <thread>
#include <vector>
#include <iostream>
#include <set>
#include <mutex>
#include <algorithm>

#include "str_arena.h"
#include "spinlock.h"
#include "tpcc.h"
#include "tpcc_util.h"
#include "controller.h"
#include "murmurhash.h"
#include "rocksdb_util.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "../utilities/dirty_buffer_scan.h"

using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::Options;
using rocksdb::TxnDBWritePolicy;
using rocksdb::TransactionDBOptions;
using rocksdb::TransactionDB;
using rocksdb::TransactionOptions;
using rocksdb::Transaction;
using rocksdb::WriteOptions;
using rocksdb::ReadOptions;
using rocksdb::Iterator;
using rocksdb::Status;
using rocksdb::Slice;
using rocksdb::kDefaultColumnFamilyName;
using rocksdb::DirtyBufferScanCallback;
using rocksdb::Comparator;
using rocksdb::Snapshot;

using std::thread;
using std::mutex;
using std::lock_guard;
using std::vector;
using std::set;
using std::cerr;
using std::cout;
using std::min;
using std::endl;

namespace sim {

  const TPCCWorker::txn_func TPCCWorker::txn_funcs[5] = {
          &TPCCWorker::txn_new_order,
          &TPCCWorker::txn_payment,
          &TPCCWorker::txn_delivery,
          &TPCCWorker::txn_order_status,
          &TPCCWorker::txn_stock_level
  };

  inline ALWAYS_INLINE string &
  str(str_arena *arena) { return *arena->next(); }

  inline void ASSERT_STATUS(Status status) {
    if (!status.ok()) throw abort_exception();
  }

#define BEFORE_ACCESS(txn, p_idx)  \
  do_wait(txn, p_idx);

#define AFTER_ACCESS(txn, p_idx)  \
  txn->SetTxnPieceIdx(p_idx);

  bool TPCCWorker::Run() { return (this->*txn_funcs[tpcc_type_])(); }

  bool TPCCWorker::txn_new_order() {
      return 0;
  }

  bool TPCCWorker::txn_payment() {
      return 0;
  }

class NewOrderScanCallback : public DirtyBufferScanCallback {
public:
  explicit NewOrderScanCallback() {
    deletion_array_.reserve(16);
    txn_id = 0;
  }

  ~NewOrderScanCallback() override {
  }

  Status Invoke(const string &key, const string &value, const uint64_t dep_txn_id) override {
    // hack - only have to check the deletion array to guarantee a key's latest version already found -
    // hack - "a new_order only have two operations: 1. insert 2. deletion"
    if (CheckExistInDeletionSet(key)) {
      return Status::OK();
    }

    Slice target(key);
    (void) value;

    if (!key_found) {
      key_found = true;
      current_smallest = target;
      smallest_new_order_k = key;
      txn_id = dep_txn_id;
      return Status::OK();
    }

    if (comparator->Compare(target, current_smallest) < 0) {
      current_smallest = target;
      smallest_new_order_k = key;
      txn_id = dep_txn_id;
    }

    return Status::OK();
  }

  Status InvokeDeletion(const string &key) override {
    // a deletion of a new_order will not appear twice
    if(std::find(deletion_array_.begin(), deletion_array_.end(), key) == deletion_array_.end()) {
      deletion_array_.emplace_back(key);
    }
    return Status::OK();
  }

private:

  friend class TPCCWorker;

  const Comparator *comparator = BytewiseComparator();
  bool key_found = false;

  Slice current_smallest;
  string smallest_new_order_k;
  uint64_t txn_id;
  vector<string> deletion_array_;

  // return true if the key is in the deletion set of dirty buffer
  bool CheckExistInDeletionSet(const string &key) {
    if(std::find(deletion_array_.begin(), deletion_array_.end(), key) != deletion_array_.end()) {
      return true;
    }
    return false;
  }
};

class OrderLineScanCallback : public DirtyBufferScanCallback {
public:
  explicit OrderLineScanCallback()
      : n(0) {
    key_array_.reserve(15);
    value_array_.reserve(15);
    txn_id_array_.reserve(15);
    deletion_array_.reserve(15);
  }

  ~OrderLineScanCallback() override {
  }

  Status Invoke(const string &key, const string &value, const uint64_t dep_txn_id) override {
    if (CheckExist(key)) {
      return Status::OK();
    }

    ++n;
    key_array_.emplace_back(key);
    value_array_.emplace_back(value);
    txn_id_array_.emplace_back(dep_txn_id);

    return Status::OK();
  }

  Status InvokeDeletion(const string &key) override {
    if (CheckExist(key)) {
      return Status::OK();
    }

    if(std::find(deletion_array_.begin(), deletion_array_.end(), key) == deletion_array_.end()) {
      deletion_array_.emplace_back(key);
    }
    return Status::OK();
  }

private:

  friend class TPCCWorker;

  vector<string> key_array_;
  vector<string> value_array_;
  vector<uint64_t> txn_id_array_;
  vector<string> deletion_array_;
  size_t n;

  // return true if the key's latest version has already been found
  bool CheckExist(const string &key) {
    if(std::find(key_array_.begin(), key_array_.end(), key) != key_array_.end()) {
      return true;
    }
    if(std::find(deletion_array_.begin(), deletion_array_.end(), key) != deletion_array_.end()) {
      return true;
    }
    return false;
  }
};

  bool TPCCWorker::txn_delivery() {
     return 0;
  }

  bool TPCCWorker::txn_order_status() {
    return 0;
  }

  bool TPCCWorker::txn_stock_level() {
    return 0;
  }

  void TPCCWorker::do_wait(Transaction *txn, unsigned int p_idx) {
    // avoid cyclic dependency
    //ASSERT_STATUS(txn->DoWait(tpcc_type_, p_idx));
  }

  TPCCWorkerGen::TPCCWorkerGen(Controller *controller, size_t nworkers) : WorkerGen(controller, nworkers) {
    hack_map.reserve(nworkers);
    for (int i = 0; i < nworkers; ++i) {
      hack_map[i] = new int32_t[NumWarehouses * NumDistrictsPerWarehouse + 64]();
    }
  }

  TPCCWorkerGen::~TPCCWorkerGen() {
    delete r;
    for (auto&& handle : handles) delete handle;
    for (int i = 0; i < nworkers_; ++i)
        delete[] hack_map[i];
  }

  void TPCCWorkerGen::Init() {
    r = new fast_random(23984543);

    //Column Family
  

    Status status = TransactionDB::Open(InitDBOptions(walPath), InitTxnDBOptions(), dbPath, column_families, &handles,
                                        &txn_db);
    if (!status.ok()) cerr << "Error initialize: " << status.ToString() << endl;
    assert(status.ok());

    //cfd_id
    for (auto&& it : cf_map) cfh_id_map[it.first] = handles[it.second]->GetID();
  }

  int TPCCWorkerGen::get_tpcc_txn_type(unsigned long seed) {
    fast_random r(seed);
    uint res = RandomNumber(r, 1, 100);
    for (int i = 0; i < 5; ++i) {
      if (res <= g_txn_workload_mix[i]) return i;
      else res -= g_txn_workload_mix[i];
    }
    return 0;
  }

  Worker* TPCCWorkerGen::Next(size_t idx) {
    int type = get_tpcc_txn_type(r->next());

    TPCCWorker* worker = new TPCCWorker(controller_, nworkers_, idx, type, r->next(), txn_db, handles);

//    worker->last_no_o_ids = new int32_t[NumWarehouses * NumDistrictsPerWarehouse + 64];
//    NDB_MEMSET(&worker->last_no_o_ids[0], 0, sizeof(worker->last_no_o_ids));
    worker->last_no_o_ids = hack_map[idx];
    return worker;
  }
}


