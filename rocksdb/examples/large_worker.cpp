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
#include "large_worker.h"
#include "controller.h"
#include "rocksdb_util.h"

using std::unordered_set;
using std::string;
using std::to_string;
using std::cerr;
using std::endl;
using namespace rocksdb;

namespace sim {

  bool LargeWorker::Run() {
    std::mt19937 gen;
    std::random_device rd;
    gen.seed(rd());

    Status s;
    bool succ = true;

    std::uniform_int_distribution<> dist(0, keys_ - 1);

    vector<uint> write_pos_pool(txn_size);
    for (uint i = 0; i < txn_size; i++) {
      write_pos_pool[i] = i;
    }

    unordered_set<int> ids_pool;
    ids_pool.reserve(txn_size);
    while (ids_pool.size() < txn_size) {
      ids_pool.insert(dist(gen));
    }

    vector<int> ids;
    ids.insert(ids.end(), ids_pool.begin(), ids_pool.end());
    sort(ids.begin(), ids.end());

    random_shuffle(write_pos_pool.begin(), write_pos_pool.end());
    vector<uint> write_pos(write_pos_pool.begin(), write_pos_pool.begin() + write_size);
    sort(write_pos.begin(), write_pos.end());

    WriteOptions writeOptions;
    writeOptions.sync = write_sync;
    writeOptions.disableWAL = disable_wal;
    TransactionOptions txn_option;
    txn_option.deadlock_detect = deadlock_detect;

    Transaction *txn = txn_db_->BeginTransaction(writeOptions, txn_option);
    ReadOptions readOptions;
    auto iter = write_pos.begin();
    for (uint i = 0; i < txn_size; ++i) {
      string id = to_string(ids[i]);
      const char *key = id.c_str();

      if (*iter == i) {
        // write access
        bool decision = controller_->Decide<false>(0 /*default cfh_id */, key, idx_);
        //s = txn->DoPut(key, id.c_str(), decision);
        iter++;
      } else {
        // read access
        string tmp_string;
        bool decision = controller_->Decide<true>(0 /*default cfh_id */, key, idx_);
        //s = txn->DoGet(readOptions, id.c_str(), &tmp_string, decision);
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

  void LargeWorkerGen::Init() {
    Status status = TransactionDB::Open(InitDBOptions(walPath), InitTxnDBOptions(), dbPath, &txn_db);
    if (!status.ok()) cerr << "Error initialize: " << status.ToString() << endl;
    assert(status.ok());
  }

  Worker *LargeWorkerGen::Next(size_t idx) {
    return new LargeWorker(controller_, nworkers_, idx, txn_db, keys_);
  }
}



