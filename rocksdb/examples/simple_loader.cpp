#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cinttypes>
#include <string>
#include <iostream>
#include <sstream>

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "simple_loader.h"

using std::string;
using std::to_string;
using std::cerr;
using std::endl;
using namespace rocksdb;

namespace sim {
  void SimpleLoader::Init() {
    Status status = TransactionDB::Open(InitDBOptions(walPath_), InitTxnDBOptions(), dbPath_, &txn_db);
    if (!status.ok()) cerr << "Error initialize: " << status.ToString() << endl;
    assert(status.ok());
  }

  void SimpleLoader::Prepare() {
    Status s;
    WriteOptions writeOptions;
    writeOptions.sync = false;
    writeOptions.disableWAL = true;
    Transaction *txn = txn_db->BeginTransaction(writeOptions, TransactionOptions());
    for (uint64_t i = 0; i < keys_; ++i) {
//      if (i % 1000 == 0) printf("insert %llu-th\n", i);

      const char *key = to_string(i).c_str();
      const char *value = to_string(rand() % 100000).c_str();

      s = txn->Put(key, value);
      if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;
    }
    s = txn->Commit();
    if (!s.ok()) cerr << "Error commit: " << s.ToString() << endl;

    delete txn;
    txn_db->Flush(FlushOptions());
  }
}