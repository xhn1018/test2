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

//#include "str_arena.h"
#include "spinlock.h"
//#include "tpcc.h"
//#include "tpcc_util.h"
//#include "controller.h"
//#include "murmurhash.h"
//#include "rocksdb_util.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/dirty_buffer_scan.h"

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
  bool TPCCWorker::Run() { return (this->*txn_funcs[tpcc_type_])(); }

  bool TPCCWorker::txn_new_order() {
       return 0;
  }





   bool TPCCWorker::txn_delivery() {return 0;}
   bool TPCCWorker::txn_payment()  {return 0;}
   bool TPCCWorker::txn_order_status() {return 0;}
   bool TPCCWorker::txn_stock_level()  {return 0;}




  TPCCWorkerGen::TPCCWorkerGen( size_t nworkers) : WorkerGen( nworkers) {
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
    column_families.emplace_back(kDefaultColumnFamilyName, InitColumnFamilyOptions());
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_warehouse, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_item, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_stock, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_stock_data, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_district, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_customer, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_customer_name_idx, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_history, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_oorder, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_oorder_c_id_idx, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_new_order, InitColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_order_line, InitColumnFamilyOptions()));

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
