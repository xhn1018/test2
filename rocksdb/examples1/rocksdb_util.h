#pragma once

#include "rocksdb/db.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

#include <unistd.h>
#include <string>

namespace sim {
  using namespace rocksdb;
  using std::string;

  Options InitDBOptions(string walPath);

  TransactionDBOptions InitTxnDBOptions();

  ColumnFamilyOptions InitColumnFamilyOptions();
}
