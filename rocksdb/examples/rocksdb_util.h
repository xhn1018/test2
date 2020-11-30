#include "rocksdb/db.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

#include <unistd.h>
#include <string>

namespace sim {
  using namespace rocksdb;
  using std::string;

  static Options InitDBOptions(string walPath) {
    Options options;
    options.IncreaseParallelism();
    options.wal_dir = std::move(walPath);
    options.manual_wal_flush = false;
    options.create_if_missing = true;

    // db logging
    options.db_log_dir = "/dev/null";
    options.keep_log_file_num = 1;
    options.info_log_level=rocksdb::FATAL_LEVEL;

    // memtable
    options.write_buffer_size = 1 << 30;
    options.max_write_buffer_number = 10;
    //flush rate
    std::shared_ptr<RateLimiter> p(NewGenericRateLimiter(1024 * 1024 * 1024));
    options.rate_limiter = p;
    options.bytes_per_sync = 1024 * 1024 * 1024;
    //compaction
    options.disable_auto_compactions = true;
    options.level0_file_num_compaction_trigger = 1024;
    options.level0_slowdown_writes_trigger = 1024;
    options.level0_stop_writes_trigger = 1024;
    options.max_bytes_for_level_base = static_cast<uint64_t>(2 << 30);
    return options;
  }

  static TransactionDBOptions InitTxnDBOptions() {
    TransactionDBOptions txn_db_options;
    txn_db_options.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
    txn_db_options.num_stripes = 256;
    return txn_db_options;
  }
}