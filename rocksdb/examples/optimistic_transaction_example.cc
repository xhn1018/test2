// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

//#include "mutex.hpp"

#include <mutex>
#include <unistd.h>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include <thread>
//#include "worker.h"
//#include "tpcc_worker.h"
//#include "controller.h"
//#include "controller.h"
using namespace rocksdb;


static int runtime = 10;
volatile bool starting, running;


std::mutex mtx;   

int count1=0;
int count2=0;
int count3=0;
int count4=0;

clock_t start,end,start1; 

std::string kDBPath = "/tmp/rocksdb_transaction_example";


void timing_thread_func() {
	//sleep(3);
	starting = true;
	//sleep(runtime);
	running = false;
}

void do_run(TransactionDB *db) {
     ReadOptions read_options;
     WriteOptions write_options;
     std::string value;
    
     Status s; 
    
//mtx.lock();
     for(int i=0;i<10;i++){
     Transaction* txn = db->BeginTransaction(write_options);
     int k;
     k =rand()%10000;
     value ="123";
     //txn->Get(read_options, std::to_string(i+k), &value);
    // txn->Get(read_options, "1", &value);
    // value =std::to_string(atoi(value.c_str()) -1);
    // std::cout<<"put value1  "<<value<<std::endl;
     s=txn->Put("1", std::to_string(i)); 
    // txn->Get(read_options, "2", &value);
    // value =std::to_string(atoi(value.c_str()) +1);
    // std::cout<<"put value2  "<<value<<std::endl;
     s=txn->Put("2", std::to_string(10-i)); 

     
     s = txn->Commit();
      
     if(s.ok())  {
       count1++;

     }
     else{  std::cout<<"error1"<<std::endl;
    }


    }
  //mtx.unlock();

      Transaction* txn1 = db->BeginTransaction(write_options);

 
   // txn->Get(read_options, "2", &value); 
   // std::cout<<"1  :"<<value.c_str()<<"  "; 
   //  txn->Get(read_options, "2", &value);
    //std::cout<<"2  :"<<value.c_str()<<"  ";
 
     txn1->Get(read_options, "2", &value);
     std::cout<<"value :"<<value<<std::endl;
     s = txn1->Commit();

}



void do_run1(TransactionDB *db) {
     ReadOptions read_options;
     WriteOptions write_options;
     std::string value;
     
     Status s; 
   
    value ="123";  
  //   mtx.lock();
     for(int i=0;i<10;i++){

     Transaction* txn = db->BeginTransaction(write_options);

 
   // txn->Get(read_options, "2", &value); 
   // std::cout<<"1  :"<<value.c_str()<<"  "; 
   //  txn->Get(read_options, "2", &value);
    //std::cout<<"2  :"<<value.c_str()<<"  ";
 
     
     s = txn->Commit();
         
     if(s.ok())  {
       count1++;

     }
     else{  std::cout<<"error2"<<std::endl;
    }


    }
     //     mtx.unlock(); 
}




int main() {
  // open DB
  //sim::Sample k;
  //sim::Controller m(1,false,k);
//  sim::TPCCWorker c;
  //sim::TPCCWorkerGen sim(&m,3);
  //sim.Next(3);
  Options options;
  TransactionDBOptions txn_db_options;
  options.create_if_missing = true;
  TransactionDB* txn_db;
 // sim::TPCCWorkerGen(3);
 	options.IncreaseParallelism();
	//options.wal_dir = wal_path;
	options.manual_wal_flush = false;
	options.create_if_missing = true;
	options.write_buffer_size = 512 << 20;
	options.max_write_buffer_number = 10;

  Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
  assert(s.ok());
 
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;

 Transaction* txn = txn_db->BeginTransaction(write_options);
  Transaction* txn1 = txn_db->BeginTransaction(write_options);
  Transaction* txn2 = txn_db->BeginTransaction(write_options);
  Transaction* txn3 = txn_db->BeginTransaction(write_options);
  start1 = clock();

s = txn2->Put("2", "100"); 
s = txn2->Put("1", "200"); 
s = txn2->Commit();

assert(s.ok());

std::thread t1(do_run,txn_db);
//std::thread t2(do_run1,txn_db);


t1.join();
//t2.join();


//do_run(txn_db);

////do_run1(txn_db);
//sleep(1);
txn->Get(read_options, "2", &value);
std::cout<<"value :"<<value<<std::endl;
//s = txn->Put("4", "200"); 
s=txn->Commit();
assert(s.ok());

 start1 = clock();


 double duration =(double)(end-start1)/CLOCKS_PER_SEC;
// printf("total  %f\n",duration); // 4.015



 


   
  // Cleanup
  delete txn_db;
  DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
