#pragma once

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_util.h"

#include <unistd.h>
#include <string>
#include <utility>

namespace sim {
  using namespace rocksdb;
  using std::string;

// delegation to run a txn
  class Loader {

  protected:
    TransactionDB *txn_db;

    string dbPath_;
    string walPath_;

  public:
    Loader(string dbPath, string walPath) :
        dbPath_(std::move(dbPath)),
        walPath_(std::move(walPath)) {};

    virtual ~Loader() { delete txn_db; };

    virtual void Init()= 0;

    virtual void Prepare()=0;
  };
}

