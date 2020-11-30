// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "worker.h"
#include "tpcc_worker.h"
#include "controller.h"
//#include "controller.h"
using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_transaction_example";

int main() {
  // open DB

  Options options;
  TransactionDBOptions txn_db_options;
  options.create_if_missing = true;
  TransactionDB* txn_db;
 // sim::TPCCWorkerGen(3);
  Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
  assert(s.ok());

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
 // sim::Sample a;
 // sim::Controller b(1,0,&a);
 // 
  ////////////////////////////////////////////////////////
  //
  // Simple Transaction Example ("Read Committed")
  //
  ////////////////////////////////////////////////////////
  //s = txn_db->Get(read_options, "ac", &value);
//std::cout<<"value 1 "<<value<<std::endl;
  // Start a transaction
 Transaction* txn = txn_db->BeginTransaction(write_options);
 // Transaction* txn1 = txn_db->BeginTransaction(write_options);
  Transaction* txn2 = txn_db->BeginTransaction(write_options);
  assert(txn);
  s = txn2->Put("ac", "deedf"); 
  txn2->Commit();
  // Read a key in this transaction
  s = txn->Get(read_options, "adfc", &value);
  s = txn->Put("ac", "deasdsaf");

  s = txn->Commit();
 
  //s = txn1->Put("asc", "deedf"); 
//  txn1->Commit();
  // Write a key OUTSIDE of this transaction.
  // Does not affect txn since this is an unrelated key.  If we wrote key 'abc'
  // here, the transaction would fail to commit.
   


 //assert(s.ok());
 
  // Commit transaction

  
 //s = txn_db->Get(read_options, "ac", &value);
//std::cout<<"value 2 " <<value<<std::endl;
 // delete txn;

  // Cleanup
  delete txn_db;
  //DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
