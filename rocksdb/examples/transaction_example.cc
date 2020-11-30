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
#include <thread>
//#include "worker.h"
//#include "tpcc_worker.h"
//#include "controller.h"
//#include "controller.h"
using namespace rocksdb;


static int runtime = 10;
volatile bool starting, running;




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
   

     for (int j=0 ;j<1000;j++){

     Transaction* txn = db->BeginTransaction(write_options);
     int k;
     k =rand()%10000;
     //std::cout<<"k ="<<k<<std::endl;
     for(int i=0;i<100;i++){
     //s = txn->Get(std::to_string(i), std::to_string(i)); 
      txn->Get(read_options, std::to_string(i+k), &value);
     // std::cout<<i<<"    =    "<<value<<std::endl;
     }
     //txn->Get(read_options, "2", &value);
     s=txn->Put(std::to_string(rand()%10000), "deasdsaf"); 
     s = txn->Commit();
     if(s.ok())  {
       count1++;

     }
     else{  std::cout<<"error"<<std::endl;
    }
   }

     
}


void do_run3(TransactionDB *db) {
     ReadOptions read_options;
     WriteOptions write_options;
     std::string value;
    
     Status s; 
   

     for (int j=0 ;j<100000;j++){
     Transaction* txn = db->BeginTransaction(write_options);
     int k;
     k =rand()%10000;
     //std::cout<<"k ="<<k<<std::endl;

     if(1){

     for(int i=0;i<1;i++){
   //  s = txn->Get(std::to_string(i), std::to_string(i)); 
      txn->Get(read_options, std::to_string(i+k), &value);
    //  std::cout<<i<<"    =    "<<value<<std::endl;
     }


     }
     else{
     //txn->Get(read_options, "2", &value);
   //  s=txn->Put(std::to_string(rand()%100), "deasdsaf"); 
      s=txn->Put("1", "deasdsaf"); 
     }
     s = txn->Commit();
     if(s.ok())  {
       count2++;
     }
     
     else{  std::cout<<"error"<<std::endl;
    }
   }

     
}

void do_run2(TransactionDB *db) {
     ReadOptions read_options;
     WriteOptions write_options;
     std::string value;
    
     Status s; 
   

     for (int j=0 ;j<100000;j++){
     Transaction* txn = db->BeginTransaction(write_options);
     int k;
     k =rand()%1000;
     //std::cout<<"k ="<<k<<std::endl;

     if(0){

     for(int i=0;i<1;i++){
     //s = txn->Get(std::to_string(i), std::to_string(i)); 
     // txn->Get(read_options, std::to_string(i+k), &value);
      txn->Get(read_options, "1", &value);
    //  std::cout<<i<<"    =    "<<value<<std::endl;
     }


     }
     else{
     //txn->Get(read_options, "2", &value);
     s=txn->Put(std::to_string(rand()%10000), "deasdsaf"); 
     }
     s = txn->Commit();
     if(s.ok())  {
       count1++;
     }
     
     else{  std::cout<<"error"<<std::endl;
    }
   }

     
}







void do_run4(TransactionDB *db) {
        int count10=0;
     ReadOptions read_options;
     WriteOptions write_options;
     std::string value;
    
     Status s; 
   
     clock_t start3,end3; 

     start3 = clock();
    
     for (int j=0 ;j<100000;j++){   
     Transaction* txn = db->BeginTransaction(write_options); 
     if(rand()%2==0){
    
     
               
     int k;
     k =rand()%4;

     txn->Get(read_options, std::to_string(k), &value);
     //s=txn->Put("1", "deasdsaf"); 
     s=txn->Put("1", "deasdsaf");
     s = txn->Commit();
     }


     else{

     int k;
     k =rand()%4;
     std::string a;
     //txn->Get(read_optons, "3", &value);
     a = std::to_string(j);
     s = txn->Put("1", "deasdsaf");
     s = txn->Put("2", "deasdsaf");
     s = txn->Put("3", "deasdsaf");
     s = txn->Put("4", "deasdsaf");
     s = txn->Commit();
     
     }
     if(s.ok())  {
       count10++;
     }
     
     else{ 
      s = txn->Rollback();
       if(s.ok())  {
       count10++;
    } //std::cout<<"error"<<std::endl;
    }
     
   }
     end3 =clock();
     double duration =(double)(end3-start3)/CLOCKS_PER_SEC;
     printf("Duratuion 4  : %f\n",duration); // 4.015
         std::cout<<"Count 10:"<<count10   <<std::endl; // 4.015
    
     
}
void do_run6(TransactionDB *db) {
     ReadOptions read_options;
     WriteOptions write_options;
     std::string value;
    
     Status s; 
   

     for (int j=0 ;j<10;j++){
     Transaction* txn = db->BeginTransaction(write_options);
     int k;
     k =rand()%10000;

    // txn->Get(read_options, "1", &value);
     //s=txn->Put("1", "deasdsaf"); 
     //s=txn->Put("1", "deasdsaf");
     s = txn->Commit();
     if(s.ok())  {
       count3++;
     }
     
     else{  //std::cout<<"error"<<std::endl;
    }
   }

     
}

void do_run5(TransactionDB *db) {
     ReadOptions read_options;
     WriteOptions write_options;
     std::string value;
    
     Status s; 
   
      clock_t start3,end3; 

     start3 = clock();
     for (int j=0 ;j<1000000;j++){
     Transaction* txn = db->BeginTransaction(write_options);   
      if(rand()%2==0){
    
     
                  
     int k;
     k =rand()%4;

     txn->Get(read_options, std::to_string(k), &value);
     //s=txn->Put("1", "deasdsaf"); 
     s=txn->Put("1", "deasdsaf");
     s = txn->Commit();
     }


     else{
 
     int k;
     k =rand()%4;
     std::string a;
     //txn->Get(read_optons, "3", &value);
     a = std::to_string(j);
     s = txn->Put("1", "deasdsaf");
     s = txn->Put("2", "deasdsaf");
     s = txn->Put("3", "deasdsaf");
     s = txn->Put("4", "deasdsaf");
     s = txn->Commit();
     
     }
     if(s.ok())  {
       count2++;
      // std::cout<<"done"<<std::endl;
     }
     
     else{  

       s = txn->Rollback();
       if(s.ok())  {
       count2++;
    }//std::cout<<"error"<<std::endl;
    }
   }
    end3 =clock();
    double duration =(double)(end3-start3)/CLOCKS_PER_SEC;
    printf("Duratuion 4  : %f\n",duration); // 4.015
     
}



void do_run7(TransactionDB *db) {
     ReadOptions read_options;
     WriteOptions write_options;
     std::string value;
    
     Status s; 
   
      clock_t start3,end3; 

     start3 = clock();
     for (int j=0 ;j<1000000;j++){


      Transaction* txn = db->BeginTransaction(write_options);   
      if(rand()%2==0){
    
     
                  
     int k;
     k =rand()%4;

     txn->Get(read_options, std::to_string(k), &value);
     //s=txn->Put("1", "deasdsaf"); 
     s=txn->Put("1", "deasdsaf");
     s = txn->Commit();
     }


     else{
   
     int k;
     k =rand()%4;
     std::string a;
     //txn->Get(read_optons, "3", &value);
     a = std::to_string(j);
     s = txn->Put("1", "deasdsaf");
     s = txn->Put("2", "deasdsaf");
     s = txn->Put("3", "deasdsaf");
     s = txn->Put("4", "deasdsaf");
     s = txn->Commit();
     
     }
     if(s.ok())  {
       count3++;
      // std::cout<<"done"<<std::endl;
     }
     
     else{  

       s = txn->Rollback();
       if(s.ok())  {
       count3++;
    }//std::cout<<"error"<<std::endl;
    }
   }
    end3 =clock();
    double duration =(double)(end3-start3)/CLOCKS_PER_SEC;
    printf("Duratuion 4  : %f\n",duration); // 4.015
     
}


void do_run8(TransactionDB *db) {
     ReadOptions read_options;
     WriteOptions write_options;
     std::string value;
    
     Status s; 
   
      clock_t start3,end3; 

     start3 = clock();
     for (int j=0 ;j<1000000;j++){
      Transaction* txn = db->BeginTransaction(write_options);  
      if(rand()%2==0){
    
     
                   
     int k;
     k =rand()%4;

     txn->Get(read_options, std::to_string(k), &value);
     //s=txn->Put("1", "deasdsaf"); 
     s=txn->Put("1", "deasdsaf");
     s = txn->Commit();
     }


     else{

     int k;
     k =rand()%4;
     std::string a;
     //txn->Get(read_optons, "3", &value);
     a = std::to_string(j);
     s = txn->Put("1", "deasdsaf");
     s = txn->Put("2", "deasdsaf");
     s = txn->Put("3", "deasdsaf");
     s = txn->Put("4", "deasdsaf");
     s = txn->Commit();
     
     }
     if(s.ok())  {
       count4++;
      // std::cout<<"done"<<std::endl;
     }
     
     else{  

       s = txn->Rollback();
       if(s.ok())  {
       count4++;
    }//std::cout<<"error"<<std::endl;
    }
   }
    end3 =clock();
    double duration =(double)(end3-start3)/CLOCKS_PER_SEC;
    printf("Duratuion 4  : %f\n",duration); // 4.015
     
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
 // sim::Sample a;
 // sim::Controller b(1,0,&a);
 // 
  ////////////////////////////////////////////////////////
  //
  // Simple Transaction Example ("Read Committed")
  //
  ////////////////////////////////////////////////////////

  // Start a transaction
 Transaction* txn = txn_db->BeginTransaction(write_options);
  Transaction* txn1 = txn_db->BeginTransaction(write_options);
  Transaction* txn2 = txn_db->BeginTransaction(write_options);
  Transaction* txn3 = txn_db->BeginTransaction(write_options);
  start1 = clock();

 for(int i=0;i<1000000;i++){
  //  start = clock();
     s = txn2->Put(std::to_string(i), std::to_string(i)); 
  }

txn2->Commit(); 
 //  end = clock();
   

  start1 = clock();
std::thread t1(do_run4,txn_db);
std::thread t2(do_run4,txn_db);
std::thread t3(do_run4,txn_db);
std::thread t4(do_run4,txn_db);
std::thread t5(do_run4,txn_db);
std::thread t6(do_run4,txn_db);
std::thread t7(do_run4,txn_db);
std::thread t8(do_run4,txn_db);
std::thread t11(do_run4,txn_db);
std::thread t12(do_run4,txn_db);
std::thread t13(do_run4,txn_db);
std::thread t14(do_run4,txn_db);
std::thread t15(do_run4,txn_db);
std::thread t16(do_run4,txn_db);
std::thread t17(do_run4,txn_db);
std::thread t18(do_run4,txn_db);
t1.join();
t2.join();
t3.join();
t4.join();
t5.join();
t6.join();
t7.join();
t8.join();
t11.join();
t12.join();
t13.join();
t14.join();
t15.join();
t16.join();
t17.join();
t18.join();
   double duration =(double)(end-start1)/CLOCKS_PER_SEC;
    printf("total  %f\n",duration); // 4.015



 
 // Commit transaction

  //txn1->Get(read_options, "1", &value);
  //std::cout<<"value"<<value<<std::endl;

//std::cout<<value<<std::endl;// s = txn_db->Get(read_options, "abc", &value);
 // delete txn;
  std::cout<<"total :"<<count1+count2+count3<<std::endl;
  std::cout<<"count1 :"<<count1<<std::endl;
  std::cout<<"count2 :"<<count2<<std::endl;
  std::cout<<"count3 :"<<count3<<std::endl;
  std::cout<<"count4 :"<<count4<<std::endl;
   
  // Cleanup
  delete txn_db;
  DestroyDB(kDBPath, options);
  return 0;
}

#endif  // ROCKSDB_LITE
