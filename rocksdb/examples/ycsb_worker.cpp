#include <unistd.h>
#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cinttypes>
#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <unordered_set>
#include <random>

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "ycsb_worker.h"
#include "controller.h"
#include "rocksdb_util.h"

using std::unordered_set;
using std::string;
using std::to_string;
using std::cerr;
using std::endl;
using namespace rocksdb;

namespace sim {
  static double g_read_perc = 0.9;
  static double g_write_perc = 0.1;
  static double zeta_2_theta;
  static double denom = 0;
  static uint64_t the_n;

  uint64_t zipf(uint64_t n, double theta, drand48_data *buffer) {
    assert(the_n == n);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) /
                 (1 - zeta_2_theta / zetan);
    double u;
    drand48_r(buffer, &u);
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, theta)) return 2;
    return 1 + (uint64_t) (n * pow(eta * u - eta + 1, alpha));
  }

  double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++)
      sum += pow(1.0 / i, theta);
    return sum;
  }

  void initialParameter(int keys, double g_zipf_theta) {
    zeta_2_theta = zeta(2, g_zipf_theta);
    auto table_size = static_cast<uint64_t>(keys);
    the_n = table_size - 1;
    denom = zeta(the_n, g_zipf_theta);
  }

  bool YCSBWorker::Run() {
    Status s;
    bool succ = true;

    unordered_set<uint64_t> ids_pool;
    ids_pool.reserve(txn_size);
    while (ids_pool.size() < txn_size) {
      uint64_t id = zipf(static_cast<uint64_t>(keys_ - 1), g_zipf_theta_, buffer_);
      ids_pool.insert(id);
    }
    vector<uint64_t> ids(ids_pool.begin(), ids_pool.end());
    sort(ids.begin(), ids.end());

    WriteOptions writeOptions;
    writeOptions.sync = write_sync;
    writeOptions.disableWAL = disable_wal;
    TransactionOptions txn_option;
    txn_option.deadlock_detect = deadlock_detect;

    Transaction *txn = txn_db_->BeginTransaction(writeOptions, txn_option);
    ReadOptions readOptions;
    for (uint i = 0; i < txn_size; ++i) {
      string id = to_string(ids[i]);
      const char *key = id.c_str();

      double r;
      drand48_r(buffer_rw_, &r);

      if (r < g_read_perc) {
        // read access
        string tmp_string;
        bool decision = controller_->Decide<true>(0 /*default cfh_id */, key, idx_);
        //s = txn->DoGet(readOptions, id.c_str(), &tmp_string, decision);
      } else if (r >= g_read_perc && r <= g_write_perc + g_read_perc) {
        // write access
        bool decision = controller_->Decide<false>(0 /*default cfh_id */, key, idx_);
        //s = txn->DoPut(key, id.c_str(), decision);
      }

      if (!s.ok()) {
        succ = false;
        break;
      }
    }

    if (succ) s = txn->Commit();
    else s = txn->Rollback();

    if (!s.ok())
      succ = false;

    delete txn;
    return succ;
  }

  YCSBWorkerGen::~YCSBWorkerGen() {
    for (int i = 0; i < nworkers_; ++i) {
      delete v_buffer_[i];
      delete v_buffer_rw_[i];
    }
  }

  void YCSBWorkerGen::Init() {
    Status status = TransactionDB::Open(InitDBOptions(walPath), InitTxnDBOptions(), dbPath, &txn_db);
    if (!status.ok()) cerr << "Error initialize: " << status.ToString() << endl;
    assert(status.ok());
    
    v_buffer_.reserve(nworkers_);
    v_buffer_rw_.reserve(nworkers_);
    for (int i = 0; i < nworkers_; ++i) {
      v_buffer_[i] = new drand48_data();
      v_buffer_rw_[i] = new drand48_data();
    }
    initialParameter(keys_, g_zipf_theta_);
  }

  Worker *YCSBWorkerGen::Next(size_t idx) {
    return new YCSBWorker(controller_, nworkers_, idx, txn_db, v_buffer_[idx], v_buffer_rw_[idx], keys_, g_zipf_theta_);
  }
}



