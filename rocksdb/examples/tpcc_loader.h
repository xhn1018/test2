#pragma once

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "loader.h"
#include "util.h"

#include <unistd.h>

namespace sim {
  using namespace rocksdb;
  using namespace util;
  using std::string;
  using std::vector;

// delegation to run a txn
  class TPCCLoader : public Loader {

  protected:

    size_t NumWarehouses;

    vector<ColumnFamilyDescriptor> column_families;
    vector<ColumnFamilyHandle *> handles;

    void prepare_warehouse(TransactionDB *db, fast_random &r);

    void prepare_item(TransactionDB *db, fast_random &r);

    void prepare_stock(TransactionDB *db, fast_random &r);

    void prepare_district(TransactionDB *db, fast_random &r);

    void prepare_customer(TransactionDB *db, fast_random &r);

    void prepare_order(TransactionDB *db, fast_random &r);

  public:
    TPCCLoader(size_t warehouse_number, const string &dbPath, const string &walPath)
        : Loader(dbPath, walPath),
          NumWarehouses(warehouse_number) {}

    ~TPCCLoader();

    void InitializeCF();

    void Init() override;

    void Prepare() override;
  };
}

