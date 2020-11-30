#pragma once

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "loader.h"

#include <unistd.h>

namespace sim {
  using namespace rocksdb;
  using std::string;

// delegation to run a txn
  class SimpleLoader : public Loader {

  protected:
    int keys_;

  public:
    SimpleLoader(int keys, const string &dbPath, const string &walPath)
        : Loader(dbPath, walPath),
          keys_(keys) {}

    void Init() override;

    void Prepare() override;
  };
}

