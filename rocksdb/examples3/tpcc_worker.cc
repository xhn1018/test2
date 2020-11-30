#include "tpcc_worker.h"

#include <unistd.h>
#include <cassert>
#include <string>
#include <thread>
#include <vector>
#include <iostream>
#include <set>
#include <mutex>
#include <algorithm>

#include "str_arena.h"
#include "spinlock.h"
#include "tpcc.h"
#include "tpcc_util.h"
#include "controller.h"
#include "murmurhash.h"
#include "rocksdb_util.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "../utilities/dirty_buffer_scan.h"

using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::Options;
using rocksdb::TxnDBWritePolicy;
using rocksdb::TransactionDBOptions;
using rocksdb::TransactionDB;
using rocksdb::TransactionOptions;
using rocksdb::Transaction;
using rocksdb::WriteOptions;
using rocksdb::ReadOptions;
using rocksdb::Iterator;
using rocksdb::Status;
using rocksdb::Slice;
using rocksdb::kDefaultColumnFamilyName;
using rocksdb::DirtyBufferScanCallback;
using rocksdb::Comparator;
using rocksdb::Snapshot;

using std::thread;
using std::mutex;
using std::lock_guard;
using std::vector;
using std::set;
using std::cerr;
using std::cout;
using std::min;
using std::endl;

namespace sim {
  const int update_locks_num = 10000;
  const TPCCWorker::txn_func TPCCWorker::txn_funcs[5] = {
          &TPCCWorker::txn_new_order,
          &TPCCWorker::txn_payment,
          &TPCCWorker::txn_delivery,
          &TPCCWorker::txn_order_status,
          &TPCCWorker::txn_stock_level
  };
  mutex *TPCCWorker::get_update_lock(const string &key, int cfd) {
    static murmur_hash hash;
    int pos = static_cast<int>(hash(key) % update_locks_num);
    return &((*(*update_locks)[cfd])[pos]);
  }
  inline ALWAYS_INLINE string &
  str(str_arena *arena) { return *arena->next(); }

  inline void ASSERT_STATUS(Status status) {
    if (!status.ok()) throw abort_exception();
  }

#define BEFORE_ACCESS(txn, p_idx)  \
  do_wait(txn, p_idx);

#define AFTER_ACCESS(txn, p_idx)  \
  txn->SetTxnPieceIdx(p_idx);

  bool TPCCWorker::Run() { printf("run\n");return (this->*txn_funcs[tpcc_type_])(); }

  bool TPCCWorker::txn_new_order() {
    string obj_key0;
    string obj_key1;
    string obj_v;
    str_arena arena;

    fast_random r(seed_);
    r.next_uniform();

    const uint warehouse_id = PickWarehouseId(r, 1, NumWarehouses + 1);
    const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse);
    const uint customerID = GetCustomerId(r);
    const uint numItems = RandomNumber(r, 5, 15);

    vector<uint> itemIDs(numItems);
    vector<uint> supplierWarehouseIDs(numItems);
    uint orderQuantities[numItems];
    bool allLocal = true;
    for (uint i = 0; i < numItems; i++) {

      while (true) {
        uint candidate_item_id = GetItemId(r);
        if(std::find(itemIDs.begin(), itemIDs.end(), candidate_item_id) == itemIDs.end()) {
          // new item id
          itemIDs[i] = candidate_item_id;
          break;
        }
      }

      if (likely(NumWarehouses == 1 ||
                 RandomNumber(r, 1, 100) > g_new_order_remote_item_pct)) {
        supplierWarehouseIDs[i] = warehouse_id;
      } else {
        do {
          supplierWarehouseIDs[i] = RandomNumber(r, 1, NumWarehouses);
        } while (supplierWarehouseIDs[i] == warehouse_id);
        allLocal = false;
      }
      orderQuantities[i] = RandomNumber(r, 1, 10);
    }
    sort(itemIDs.begin(), itemIDs.end());
    sort(supplierWarehouseIDs.begin(), supplierWarehouseIDs.end());

    Status s;

    WriteOptions writeOptions;
    writeOptions.sync = write_sync;
    writeOptions.disableWAL = disable_wal;

    ReadOptions readOptions;

    TransactionOptions txn_option;
    txn_option.deadlock_detect = deadlock_detect;

    Transaction* txn = txn_db_->BeginTransaction(writeOptions, txn_option);
   // txn->SetTxnType(0);
    ssize_t ret = 0;

    AgentDecision decision;
    try {
//      BEFORE_ACCESS(txn, 1);
      {
        //read warehouse
        const warehouse::key k_w(warehouse_id);

        // dep_piece_id 0
        //decision = controller_->DecideWait(cfh_id_map.at(tbl_warehouse), Encode(str(&arena), k_w), idx_, 0);
      //  if (Wait(decision)) do_wait(txn, 1);

      //  lock_guard<mutex> w_l(*get_update_lock(Encode(str(&arena), k_w), cfh_id_map.at(tbl_warehouse)));
        // access_id 0 - read warehouse
     //   decision = controller_->Decide<true>(cfh_id_map.at(tbl_warehouse), Encode(obj_key0, k_w), idx_, 0);
        ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_warehouse)], Encode(obj_key0, k_w), &obj_v));
        warehouse::value v_w_temp;
        const warehouse::value *v_w = Decode(obj_v, v_w_temp);
       checker::SanityCheckWarehouse(&k_w, v_w);
      }
       int32_t my_next_o_id;
      {
        // read district for next_o_id
        const district::key k_d(warehouse_id, districtID);

        // dep_piece_id 1
        //decision = controller_->DecideWait(cfh_id_map.at(tbl_district), Encode(obj_key0, k_d), idx_, 1);
        //if (Wait(decision)) do_wait(txn, 2);

        lock_guard<mutex> d_l(*get_update_lock(Encode(str(&arena), k_d), cfh_id_map.at(tbl_district)));
        // ExGet 0
        // access_id 1 - read district
        //decision = controller_->Decide<true>(cfh_id_map.at(tbl_district), Encode(str(&arena), k_d), idx_, 1);
        if (1)
          ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_district)], Encode(str(&arena), k_d), &obj_v));
        //else 
        //  ASSERT_STATUS(
          //    txn->GetForUpdate(readOptions, handles[cf_map.at(tbl_district)], Encode(str(&arena), k_d), &obj_v));
        district::value v_d_temp;
        const district::value *v_d = Decode(obj_v, v_d_temp);
        checker::SanityCheckDistrict(&k_d, v_d);

        my_next_o_id = v_d->d_next_o_id;

        if (!g_new_order_fast_id_gen) {
          district::value v_d_new(*v_d);
          v_d_new.d_next_o_id++;
          // access_id 2 - write district
          //decision = controller_->Decide<false>(cfh_id_map.at(tbl_district), Encode(str(&arena), k_d), idx_, 2);
          ASSERT_STATUS(
              txn->Put(handles[cf_map.at(tbl_district)], Encode(str(&arena), k_d), Encode(str(&arena), v_d_new)));
        }
      }


      item::value item_values[numItems];
      for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
        // read item
        const uint ol_i_id = itemIDs[ol_number - 1];
        const item::key k_i(ol_i_id);
        // access_id 3 - read item
        //decision = controller_->Decide<true>(cfh_id_map.at(tbl_item), Encode(obj_key0, k_i), idx_, 3);
        ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_item)], Encode(str(&arena), k_i), &obj_v));

        const item::value *v_i = Decode(obj_v, item_values[ol_number - 1]);
        checker::SanityCheckItem(&k_i, v_i);
      }


      for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
        // Put inside loop to prevent cyclic dependency
        // BEFORE_ACCESS(txn, 4);
        // read stock
        const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
        const uint ol_i_id = itemIDs[ol_number - 1];
        const uint ol_quantity = orderQuantities[ol_number - 1];

        {
          const stock::key k_s(ol_supply_w_id, ol_i_id);

          // dep_piece_id 2
          //decision = controller_->DecideWait(cfh_id_map.at(tbl_stock), Encode(obj_key0, k_s), idx_, 2);
          //if (Wait(decision)) do_wait(txn, 4);

          lock_guard<mutex> s_l(*get_update_lock(Encode(str(&arena), k_s), cfh_id_map.at(tbl_stock)));
          // access_id 4 - read stock
          //decision = controller_->Decide<true>(cfh_id_map.at(tbl_stock), Encode(str(&arena), k_s), idx_, 4);
          if (1)
            ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_stock)], Encode(str(&arena), k_s), &obj_v));
         // else
         //  ASSERT_STATUS(
           //     txn->GetForUpdate(readOptions, handles[cf_map.at(tbl_stock)], Encode(str(&arena), k_s), &obj_v));
//
          stock::value v_s_temp;
          const stock::value *v_s = Decode(obj_v, v_s_temp);
          checker::SanityCheckStock(&k_s, v_s);

          // update stock
          stock::value v_s_new(*v_s);
          if (v_s_new.s_quantity - ol_quantity >= 10)
            v_s_new.s_quantity -= ol_quantity;
          else
            v_s_new.s_quantity += -int32_t(ol_quantity) + 91;
          v_s_new.s_ytd += ol_quantity;
          v_s_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

          // access_id 5 - write stock
         // decision = controller_->Decide<false>(cfh_id_map.at(tbl_stock), Encode(str(&arena), k_s), idx_, 5);
          ASSERT_STATUS(
              txn->Put(handles[cf_map.at(tbl_stock)], Encode(str(&arena), k_s), Encode(str(&arena), v_s_new)));
        }
      }


      {
        const new_order::key k_no(warehouse_id, districtID, my_next_o_id);

        // dep_piece_id 3
        //decision = controller_->DecideWait(cfh_id_map.at(tbl_new_order), Encode(obj_key0, k_no), idx_, 3);
        //if (Wait(decision)) do_wait(txn, 5);

        // lock corresponding warehouse, delivery transaction has scan on this table
        // TODO - need to smaller the granularity - txn_new_order may be unnecessary for each other
        const new_order::key k_n_lock(warehouse_id, 0, 0);
        lock_guard<mutex> n_l(*get_update_lock(Encode(str(&arena), k_n_lock), cfh_id_map.at(tbl_new_order)));

        const new_order::value v_no;
        const size_t new_order_sz = Size(v_no);
        // access_id 6 - write new order
        //decision = controller_->Decide<false>(cfh_id_map.at(tbl_new_order), Encode(str(&arena), k_no), idx_, 6);
      //  ASSERT_STATUS(txn->DoInsert(handles[cf_map.at(tbl_new_order)], Encode(str(&arena), k_no), Encode(str(&arena), v_no),
      //                          Opt(decision), Dirty(decision)));
        ret += new_order_sz;
      }


      {
        const oorder::key k_oo(warehouse_id, districtID, my_next_o_id);

        // dep_piece_id 4
        //decision = controller_->DecideWait(cfh_id_map.at(tbl_oorder), Encode(obj_key0, k_oo), idx_, 4);
        //if (Wait(decision)) do_wait(txn, 6);

        lock_guard<mutex> oo_l(*get_update_lock(Encode(str(&arena), k_oo), cfh_id_map.at(tbl_oorder)));
        oorder::value v_oo;
        v_oo.o_c_id = int32_t(customerID);
        v_oo.o_carrier_id = 0; // seems to be ignored
        v_oo.o_ol_cnt = int8_t(numItems);
        v_oo.o_all_local = allLocal;
        v_oo.o_entry_d = GetCurrentTimeMillis();

        const size_t oorder_sz = Size(v_oo);
        // access_id 7 - write order
        //decision = controller_->Decide<false>(cfh_id_map.at(tbl_oorder), Encode(str(&arena), k_oo), idx_, 7);
        //ASSERT_STATUS(txn->DoInsert(handles[cf_map.at(tbl_oorder)], Encode(str(&arena), k_oo), Encode(str(&arena), v_oo),
        //                         Opt(decision), Dirty(decision)));
        ret += oorder_sz;

        const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID, my_next_o_id);
        const oorder_c_id_idx::value v_oo_idx(0);
        // access_id 8 - write order index
        //decision = controller_->Decide<false>(cfh_id_map.at(tbl_oorder_c_id_idx), Encode(str(&arena), k_oo_idx), idx_, 8);
        //ASSERT_STATUS(txn->DoInsert(handles[cf_map.at(tbl_oorder_c_id_idx)], Encode(str(&arena), k_oo_idx),
        //                         Encode(str(&arena), v_oo_idx),
        //                         Opt(decision), Dirty(decision)));
      }

      {
        // dep_piece_id 5
        const order_line::key k_oo_decide(warehouse_id, 0, 0, 0);
        //decision = controller_->DecideWait(cfh_id_map.at(tbl_order_line), Encode(obj_key0, k_oo_decide), idx_, 5);
        //if (Wait(decision)) do_wait(txn, 7);

        // lock corresponding warehouse, delivery transaction has scan on this table
        // TODO - need to smaller the granularity - txn_new_order may be unnecessary for each other
        const order_line::key k_oo_lock(warehouse_id, 0, 0, 0);
        lock_guard<mutex> oo_l(*get_update_lock(Encode(str(&arena), k_oo_lock), cfh_id_map.at(tbl_order_line)));

        for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
          const uint ol_i_id = itemIDs[ol_number - 1];
          const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
          const uint ol_quantity = orderQuantities[ol_number - 1];

          const order_line::key k_ol(warehouse_id, districtID, my_next_o_id, ol_number);

          // insert into order_line
          order_line::value v_ol;
          v_ol.ol_i_id = int32_t(ol_i_id);
          v_ol.ol_delivery_d = 0; // not delivered yet
          v_ol.ol_amount = float(ol_quantity) * item_values[ol_number - 1].i_price;
          v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
          v_ol.ol_quantity = int8_t(ol_quantity);

          const size_t order_line_sz = Size(v_ol);

          // access_id 9 - write order line
          //decision = controller_->Decide<false>(cfh_id_map.at(tbl_order_line), Encode(str(&arena), k_ol), idx_, 9);
          //ASSERT_STATUS(
          //    txn->DoInsert(handles[cf_map.at(tbl_order_line)], Encode(str(&arena), k_ol), Encode(str(&arena), v_ol),
          //               Opt(decision), Dirty(decision)));
          ret += order_line_sz;
        }
      }
      


      {
        //read customer
        const customer::key k_c(warehouse_id, districtID, customerID);

        // dep_piece_id 6
        //decision = controller_->DecideWait(cfh_id_map.at(tbl_customer), Encode(str(&arena), k_c), idx_, 6);
        //if (Wait(decision)) do_wait(txn, 8);

        lock_guard<mutex> c_l(*get_update_lock(Encode(str(&arena), k_c), cfh_id_map.at(tbl_customer)));
        // access_id 10 - read customer
       // decision = controller_->Decide<true>(cfh_id_map.at(tbl_customer), Encode(obj_key0, k_c), idx_, 10);
        ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_customer)], Encode(obj_key0, k_c), &obj_v));
        customer::value v_c_temp;
        const customer::value *v_c = Decode(obj_v, v_c_temp);
        checker::SanityCheckCustomer(&k_c, v_c);
      }
      

      s = txn->Commit();
      delete txn;
      return s.ok();

    }
    catch (abort_exception &ex) {
      txn->Rollback();
      delete txn;
      return false;
    }

 }
  bool TPCCWorker::txn_payment() {
    printf("run txn_payment\n");
    string obj_key0;
    string obj_key1;
    string obj_v;
    str_arena arena;

    fast_random r(seed_);
    r.next_uniform();

    const uint warehouse_id = PickWarehouseId(r, 1, NumWarehouses + 1);
    const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse);
    uint customerDistrictID, customerWarehouseID;
    if (likely(NumWarehouses == 1 ||
               RandomNumber(r, 1, 100) <= 85)) {
      customerDistrictID = districtID;
      customerWarehouseID = warehouse_id;
    } else {
      customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse);
      do {
        customerWarehouseID = RandomNumber(r, 1, NumWarehouses);
      } while (customerWarehouseID == warehouse_id);
    }
    const float paymentAmount = (float) (RandomNumber(r, 100, 500000) / 100.0);
    const uint32_t ts = GetCurrentTimeMillis();

    scoped_str_arena s_arena(arena);

    Status s;

    WriteOptions writeOptions;
    writeOptions.sync = write_sync;
    writeOptions.disableWAL = disable_wal;

    ReadOptions readOptions;

    TransactionOptions txn_option;
    txn_option.deadlock_detect = deadlock_detect;

    Transaction *txn = txn_db_->BeginTransaction(writeOptions, txn_option);
    //txn->SetTxnType(1);

    ssize_t ret = 0;
    AgentDecision decision;

    try {
      // BEFORE_ACCESS(txn, 1);
      // read warehouse
      const warehouse::key k_w(warehouse_id);
      const warehouse::value *v_w;
      {
        // dep_piece_id 7
        //decision = controller_->DecideWait(cfh_id_map.at(tbl_warehouse), Encode(obj_key0, k_w), idx_, 7);
        //if (Wait(decision)) do_wait(txn, 1);

        //lock_guard<mutex> w_l(*get_update_lock(Encode(str(&arena), k_w), cfh_id_map.at(tbl_warehouse)));
        // ExGet 2
        // access_id 11 - read warehouse
        //decision = controller_->Decide<true>(cfh_id_map.at(tbl_warehouse), Encode(str(&arena), k_w), idx_, 11);
        if (1)
          ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_warehouse)], Encode(str(&arena), k_w), &obj_v ));
        //else
        //  ASSERT_STATUS(
        //      txn->GetForUpdate(readOptions, handles[cf_map.at(tbl_warehouse)], Encode(str(&arena), k_w), &obj_v));

        warehouse::value v_w_temp;
        v_w = Decode(obj_v, v_w_temp);
        //checker::SanityCheckWarehouse(&k_w, v_w);

        // update warehouse
        warehouse::value v_w_new(*v_w);
        v_w_new.w_ytd += paymentAmount;
        // access_id 12 - write warehouse
        //decision = controller_->Decide<false>(cfh_id_map.at(tbl_warehouse), Encode(str(&arena), k_w), idx_, 12);
        ASSERT_STATUS(
            txn->Put(handles[cf_map.at(tbl_warehouse)], Encode(str(&arena), k_w), Encode(str(&arena), v_w_new)));
      }

      const district::value *v_d;
      {
        // read district
        const district::key k_d(warehouse_id, districtID);

        // dep_piece_id 8
        //decision = controller_->DecideWait(cfh_id_map.at(tbl_district), Encode(obj_key0, k_d), idx_, 8);
        //if (Wait(decision)) do_wait(txn, 2);

        //lock_guard<mutex> d_l(*get_update_lock(Encode(str(&arena), k_d), cfh_id_map.at(tbl_district)));
        // ExGet 3
        // access_id 13 - read district
        //decision = controller_->Decide<true>(cfh_id_map.at(tbl_district), Encode(str(&arena), k_d), idx_, 13);
        if (1)
          ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_district)], Encode(str(&arena), k_d), &obj_v));
        //else
          //ASSERT_STATUS(
         //     txn->GetForUpdate(readOptions, handles[cf_map.at(tbl_district)], Encode(str(&arena), k_d), &obj_v));

        district::value v_d_temp;
        v_d = Decode(obj_v, v_d_temp);
        checker::SanityCheckDistrict(&k_d, v_d);

        // update district
        district::value v_d_new(*v_d);
        v_d_new.d_ytd += paymentAmount;
        // access_id 14 - write district
       //decision = controller_->Decide<false>(cfh_id_map.at(tbl_district), Encode(str(&arena), k_d), idx_, 14);
        ASSERT_STATUS(
            txn->Put(handles[cf_map.at(tbl_district)], Encode(str(&arena), k_d), Encode(str(&arena), v_d_new)));
      }

      
      customer::key k_c;
      customer::value v_c;
      if (RandomNumber(r, 1, 100) <= 60) {
        // cust by name
        uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
        static_assert(sizeof(lastname_buf) == 16, "xx");
        NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
        GetNonUniformCustomerLastNameRun(lastname_buf, r);

        static const string zeros(16, 0);
        static const string ones(16, 255);

        customer_name_idx::key k_c_idx_0;
        k_c_idx_0.c_w_id = customerWarehouseID;
        k_c_idx_0.c_d_id = customerDistrictID;
        k_c_idx_0.c_last.assign((const char *) lastname_buf, 16);
        k_c_idx_0.c_first.assign(zeros);

        customer_name_idx::key k_c_idx_1;
        k_c_idx_1.c_w_id = customerWarehouseID;
        k_c_idx_1.c_d_id = customerDistrictID;
        k_c_idx_1.c_last.assign((const char *) lastname_buf, 16);
        k_c_idx_1.c_first.assign(ones);

        ReadOptions c_n_idx_range;

        Slice s_c_n_idx_lower(Encode(obj_key0, k_c_idx_0));
        Slice s_c_n_idx_upper(Encode(obj_key1, k_c_idx_1));
        c_n_idx_range.iterate_lower_bound = &s_c_n_idx_lower;
        c_n_idx_range.iterate_upper_bound = &s_c_n_idx_upper;

        Iterator *it_customer_name_idx = txn->GetIterator(c_n_idx_range, handles[cf_map.at(tbl_customer_name_idx)]);

        int count = 0;
        for (it_customer_name_idx->SeekToFirst(); it_customer_name_idx->Valid(); it_customer_name_idx->Next()) {
          ++count;
        }
        ALWAYS_ASSERT(count > 0);
        INVARIANT(c.size() < NMaxCustomerIdxScanElems); // we should detect this
        int index = count / 2;

        if (count % 2 == 0)
          index--;

        int n = 0;
        customer_name_idx::value v_c_idx_temp;
        const customer_name_idx::value *v_c_idx;
        for (it_customer_name_idx->SeekToFirst(); it_customer_name_idx->Valid(); it_customer_name_idx->Next()) {
          if (n == index) {
            v_c_idx = Decode(it_customer_name_idx->value().ToString(), v_c_idx_temp);
            break;
          }
          n++;
        }

        delete it_customer_name_idx;

        k_c.c_w_id = customerWarehouseID;
        k_c.c_d_id = customerDistrictID;
        k_c.c_id = v_c_idx->c_id;
      } else {
        // cust by ID
        const uint customerID = GetCustomerId(r);
        k_c.c_w_id = customerWarehouseID;
        k_c.c_d_id = customerDistrictID;
        k_c.c_id = customerID;
      }
      {
        // dep_piece_id 9
        //decision = controller_->DecideWait(cfh_id_map.at(tbl_customer), Encode(obj_key0, k_c), idx_, 9);
        //if (Wait(decision)) do_wait(txn, 3);

        //lock_guard<mutex> c_l(*get_update_lock(Encode(str(&arena), k_c), cfh_id_map.at(tbl_customer)));
        // ExGet 4, 5
        // access_id 15 - read customer
       // decision = controller_->Decide<true>(cfh_id_map.at(tbl_customer), Encode(str(&arena), k_c), idx_, 15);
        if (1)
          ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_customer)], Encode(str(&arena), k_c), &obj_v));
        //else
        //  ASSERT_STATUS(
        //      txn->GetForUpdate(readOptions, handles[cf_map.at(tbl_customer)], Encode(str(&arena), k_c), &obj_v));
        Decode(obj_v, v_c);
        checker::SanityCheckCustomer(&k_c, &v_c);
        customer::value v_c_new(v_c);

        // update customer
        v_c_new.c_balance -= paymentAmount;
        v_c_new.c_ytd_payment += paymentAmount;
        v_c_new.c_payment_cnt++;
        if (strncmp(v_c.c_credit.data(), "BC", 2) == 0) {
          char buf[501];
          int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s",
                           k_c.c_id,
                           k_c.c_d_id,
                           k_c.c_w_id,
                           districtID,
                           warehouse_id,
                           paymentAmount,
                           v_c.c_data.c_str());
          v_c_new.c_data.resize_junk(
              min(static_cast<size_t>(n), v_c_new.c_data.max_size()));
          NDB_MEMCPY((void *) v_c_new.c_data.data(), &buf[0], v_c_new.c_data.size());
        }
        // access_id 16 - write customer
       // decision = controller_->Decide<false>(cfh_id_map.at(tbl_customer), Encode(str(&arena), k_c), idx_, 16);
        ASSERT_STATUS(
            txn->Put(handles[cf_map.at(tbl_customer)], Encode(str(&arena), k_c), Encode(str(&arena), v_c_new)));
      }



      const history::key k_h(k_c.c_d_id, k_c.c_w_id, k_c.c_id, districtID, warehouse_id, ts);
      history::value v_h;
      v_h.h_amount = paymentAmount;
      v_h.h_data.resize_junk(v_h.h_data.max_size());
      int n = snprintf((char *) v_h.h_data.data(), v_h.h_data.max_size() + 1,
                       "%.10s    %.10s",
                       v_w->w_name.c_str(),
                       v_d->d_name.c_str());
      v_h.h_data.resize_junk(min(static_cast<size_t>(n), v_h.h_data.max_size()));

      const size_t history_sz = Size(v_h);
      // access_id 17 - write history
      decision = controller_->Decide<false>(cfh_id_map.at(tbl_history), Encode(str(&arena), k_h), idx_, 17);
     // ASSERT_STATUS(txn->DoInsert(handles[cf_map.at(tbl_history)], Encode(str(&arena), k_h), Encode(str(&arena), v_h),
     //                          Opt(decision), Dirty(decision)));
      ret += history_sz;
      s = txn->Commit();
      delete txn;
      return s.ok();
   }
     catch (abort_exception &ex) {
     txn->Rollback();
     delete txn;
     return false;
     }

  }

class NewOrderScanCallback : public DirtyBufferScanCallback {
public:
  explicit NewOrderScanCallback() {
    deletion_array_.reserve(16);
    txn_id = 0;
  }

  ~NewOrderScanCallback() override {
  }

  Status Invoke(const string &key, const string &value, const uint64_t dep_txn_id) override {
    // hack - only have to check the deletion array to guarantee a key's latest version already found -
    // hack - "a new_order only have two operations: 1. insert 2. deletion"
    if (CheckExistInDeletionSet(key)) {
      return Status::OK();
    }

    Slice target(key);
    (void) value;

    if (!key_found) {
      key_found = true;
      current_smallest = target;
      smallest_new_order_k = key;
      txn_id = dep_txn_id;
      return Status::OK();
    }

    if (comparator->Compare(target, current_smallest) < 0) {
      current_smallest = target;
      smallest_new_order_k = key;
      txn_id = dep_txn_id;
    }

    return Status::OK();
  }

  Status InvokeDeletion(const string &key) override {
    // a deletion of a new_order will not appear twice
    if(std::find(deletion_array_.begin(), deletion_array_.end(), key) == deletion_array_.end()) {
      deletion_array_.emplace_back(key);
    }
    return Status::OK();
  }

private:

  friend class TPCCWorker;

  const Comparator *comparator = BytewiseComparator();
  bool key_found = false;

  Slice current_smallest;
  string smallest_new_order_k;
  uint64_t txn_id;
  vector<string> deletion_array_;

  // return true if the key is in the deletion set of dirty buffer
  bool CheckExistInDeletionSet(const string &key) {
    if(std::find(deletion_array_.begin(), deletion_array_.end(), key) != deletion_array_.end()) {
      return true;
    }
    return false;
  }
};

class OrderLineScanCallback : public DirtyBufferScanCallback {
public:
  explicit OrderLineScanCallback()
      : n(0) {
    key_array_.reserve(15);
    value_array_.reserve(15);
    txn_id_array_.reserve(15);
    deletion_array_.reserve(15);
  }

  ~OrderLineScanCallback() override {
  }

  Status Invoke(const string &key, const string &value, const uint64_t dep_txn_id) override {
    if (CheckExist(key)) {
      return Status::OK();
    }

    ++n;
    key_array_.emplace_back(key);
    value_array_.emplace_back(value);
    txn_id_array_.emplace_back(dep_txn_id);

    return Status::OK();
  }

  Status InvokeDeletion(const string &key) override {
    if (CheckExist(key)) {
      return Status::OK();
    }

    if(std::find(deletion_array_.begin(), deletion_array_.end(), key) == deletion_array_.end()) {
      deletion_array_.emplace_back(key);
    }
    return Status::OK();
  }

private:

  friend class TPCCWorker;

  vector<string> key_array_;
  vector<string> value_array_;
  vector<uint64_t> txn_id_array_;
  vector<string> deletion_array_;
  size_t n;

  // return true if the key's latest version has already been found
  bool CheckExist(const string &key) {
    if(std::find(key_array_.begin(), key_array_.end(), key) != key_array_.end()) {
      return true;
    }
    if(std::find(deletion_array_.begin(), deletion_array_.end(), key) != deletion_array_.end()) {
      return true;
    }
    return false;
  }
};

  bool TPCCWorker::txn_delivery() {
   
    string obj_key0;
    string obj_key1;
    string obj_v;
    str_arena arena;

    fast_random r(seed_);
    r.next_uniform();
    const uint warehouse_id = PickWarehouseId(r, 1, NumWarehouses + 1);
    const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse);
    const uint32_t ts = GetCurrentTimeMillis();

    scoped_str_arena s_arena(arena);

    Status s;
    
    WriteOptions writeOptions;
    writeOptions.sync = write_sync;
    writeOptions.disableWAL = disable_wal;

    ReadOptions readOptions;

    TransactionOptions txn_option;
    txn_option.deadlock_detect = deadlock_detect;

    Transaction *txn = txn_db_->BeginTransaction(writeOptions, txn_option);
    //txn->SetTxnType(2)f;
    ssize_t ret = 0;

    AgentDecision decision;

    int32_t last_no_o_ids_query[10];
    uint c_id[10];
    float ol_total[10];
    try {

      // BEFORE_ACCESS(txn, 1); // NEW_ORDER

      // dep_piece_id 10
      // TODO - track this one on warehouse level
      const new_order::key k_n_decide(warehouse_id, 0, 0);
      //decision = controller_->DecideWait(cfh_id_map.at(tbl_new_order), Encode(obj_key0, k_n_decide), idx_, 10);
      //if (Wait(decision)) do_wait(txn, 1);

      {
        // lock corresponding warehouse, keep at warehouse granularity to avoid deadlock in benchmark side
        const new_order::key k_n_lock(warehouse_id, 0, 0);
        //lock_guard<mutex> k_n_l(*get_update_lock(Encode(str(&arena), k_n_lock), cfh_id_map.at(tbl_new_order)));

        for (uint d = 2; d <= NumDistrictsPerWarehouse; d++) {
          std::cout<<(warehouse_id - 1) * NumDistrictsPerWarehouse + d - 1<<std::endl;   
          std::cout<<"delviery"<<std::endl;
          std::cout<< last_no_o_ids[(warehouse_id - 1) * NumDistrictsPerWarehouse + d - 1]  <<std::endl;
          const new_order::key k_no_0(warehouse_id, d, last_no_o_ids[(warehouse_id - 1) * NumDistrictsPerWarehouse + d - 1]);
          const new_order::key k_no_1(warehouse_id, d, numeric_limits<int32_t>::max());

          ReadOptions new_order_range;
       
          Slice s_lower(Encode(obj_key0, k_no_0));
          Slice s_upper(Encode(obj_key1, k_no_1));
          new_order_range.iterate_lower_bound = &s_lower; 
          new_order_range.iterate_upper_bound = &s_upper;

          new_order::key n_o_temp;
          const new_order::key *k_no;
          bool found_valid_smallest_id = false;

          {
          //  bool using_occ = decision == agent_occ;
            // indicate using OCC or 2PL protocol
            // scan new order in db, find the one with smallest order id
            //if (using_occ) txn->TrackHeadNode(handles[cf_map.at(tbl_new_order)]);
           // Iterator *it = txn->GetIterator(new_order_range, handles[cf_map.at(tbl_new_order)]);
          //  it->SeekToFirst();
            // iterator found nothing in the db part
         //   if (!it->Valid()) {
        //      delete it;
         //     last_no_o_ids_query[d - 1] = -1;
         //     printf("wrong!!!!!\n");
         //     continue;
         //   }
            // get the smallest from db
        //    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      //        k_no = Decode(it->key().ToString(), n_o_temp);
      //        //txn->TrackScanKey(handles[cf_map.at(tbl_new_order)], it->key(), it->seq(), using_occ /*occ or 2pl*/);



              //if (!using_occ) {
               // // if using 2pl, have to get() the key again to make sure value is retrieved after read lock is on
               // s = txn->DoGet(readOptions, handles[cf_map.at(tbl_new_order)], it->key(), &obj_v,
              //                     false/*2pl*/, false/*not dirty*/);
              //  // no need to decode the value, we just want to guarantee the key has not been deleted, otherwise DoGet will return NotFound
              //  if (!s.ok()) {
              //    delete it;
              //    ASSERT_STATUS(s);
              //  }
              //}
         //     found_valid_smallest_id = true;
        //      break;
      //      }
      //      delete it;
          }

          if (!found_valid_smallest_id) {
            last_no_o_ids_query[d - 1] = -1;
            printf("wrong!!!!!\n");
            continue;
          }

          if (unlikely(!k_no)) { last_no_o_ids_query[d - 1] = -1; printf("wrong!!!!!\n");continue; }

          last_no_o_ids_query[d - 1] = k_no->no_o_id;

          // access_id 18 - delete new order
          //decision = controller_->Decide<false>(cfh_id_map.at(tbl_new_order), Encode(str(&arena), *k_no), idx_, 18);
          //ASSERT_STATUS(txn->DoDelete(handles[cf_map.at(tbl_new_order)], Encode(str(&arena), *k_no),
          //                            Opt(decision), Dirty(decision)));
          ret -= 0 /*new_order_c.get_value_size()*/;

        }
      }
    }
    catch (abort_exception &ex) {
      s = Status::NotFound();
      txn->Rollback();
    }
    delete txn;

    return s.ok();
  }

  bool TPCCWorker::txn_order_status() {
    std::cout<<"order_status"<<std::endl;
    string obj_key0;
    string obj_key1;
    string obj_v;
    str_arena arena;

    fast_random r(seed_);
    r.next_uniform();
    const uint warehouse_id = PickWarehouseId(r, 1, NumWarehouses + 1);
    const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse);

    scoped_str_arena s_arena(arena);

    Status s;
    WriteOptions writeOptions;
    writeOptions.sync = write_sync;
    writeOptions.disableWAL = disable_wal;

    ReadOptions readOptions;

    TransactionOptions txn_option;
    txn_option.deadlock_detect = deadlock_detect;

    Transaction *txn = txn_db_->BeginTransaction(writeOptions, txn_option);
   // txn->SetTxnType(3);

    const Snapshot* snapshot = txn_db_->GetSnapshot();
    readOptions.snapshot = snapshot;
    try {
      customer::key k_c;
      customer::value v_c;
      if (RandomNumber(r, 1, 100) <= 60) {
        // cust by name
        uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
        static_assert(sizeof(lastname_buf) == 16, "xx");
        NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
        GetNonUniformCustomerLastNameRun(lastname_buf, r);

        static const string zeros(16, 0);
        static const string ones(16, 255);

        customer_name_idx::key k_c_idx_0;
        k_c_idx_0.c_w_id = warehouse_id;
        k_c_idx_0.c_d_id = districtID;
        k_c_idx_0.c_last.assign((const char *) lastname_buf, 16);
        k_c_idx_0.c_first.assign(zeros);

        customer_name_idx::key k_c_idx_1;
        k_c_idx_1.c_w_id = warehouse_id;
        k_c_idx_1.c_d_id = districtID;
        k_c_idx_1.c_last.assign((const char *) lastname_buf, 16);
        k_c_idx_1.c_first.assign(ones);

        ReadOptions c_n_idx_range;

        Slice s_c_n_idx_lower(Encode(obj_key0, k_c_idx_0));
        Slice s_c_n_idx_upper(Encode(obj_key1, k_c_idx_1));
        c_n_idx_range.iterate_lower_bound = &s_c_n_idx_lower;
        c_n_idx_range.iterate_upper_bound = &s_c_n_idx_upper;

        Iterator *it_customer_name_idx = txn->GetIterator(c_n_idx_range, handles[cf_map.at(tbl_customer_name_idx)]);

        int n = 0;
        for (it_customer_name_idx->SeekToFirst(); it_customer_name_idx->Valid(); it_customer_name_idx->Next()) {
          ++n;
        }
        ALWAYS_ASSERT(n > 0);
        INVARIANT(c.size() < NMaxCustomerIdxScanElems); // we should detect this
        int index = n / 2;
        if (n % 2 == 0)
          index--;

        n = 0;
        customer_name_idx::value v_c_idx_temp;
        const customer_name_idx::value *v_c_idx;
        for (it_customer_name_idx->SeekToFirst(); it_customer_name_idx->Valid(); it_customer_name_idx->Next()) {
          if (n == index) {
            v_c_idx = Decode(it_customer_name_idx->value().ToString(), v_c_idx_temp);
            break;
          }
          n++;
        }

        delete it_customer_name_idx;

        k_c.c_w_id = warehouse_id;
        k_c.c_d_id = districtID;
        k_c.c_id = v_c_idx->c_id;
      } else {
        // cust by ID
        const uint customerID = GetCustomerId(r);
        k_c.c_w_id = warehouse_id;
        k_c.c_d_id = districtID;
        k_c.c_id = customerID;
      }

      ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_customer)], Encode(str(&arena), k_c), &obj_v));
      Decode(obj_v, v_c);
      checker::SanityCheckCustomer(&k_c, &v_c);

      const oorder_c_id_idx::key k_oo_idx_hi_l(warehouse_id, districtID, k_c.c_id, 0);
      const oorder_c_id_idx::key k_oo_idx_hi(warehouse_id, districtID, k_c.c_id, numeric_limits<int32_t>::max());

      ReadOptions oorder_c_id_idx_range;
      oorder_c_id_idx_range.snapshot = snapshot;

      Slice s_oorder_c_id_idx_lower(Encode(obj_key0, k_oo_idx_hi_l));
      Slice s_oorder_c_id_idx_upper(Encode(obj_key1, k_oo_idx_hi));
      oorder_c_id_idx_range.iterate_lower_bound = &s_oorder_c_id_idx_lower;
      oorder_c_id_idx_range.iterate_upper_bound = &s_oorder_c_id_idx_upper;

      uint o_id;
      
      Iterator *it_oorder_c_id_idx = txn->GetIterator(oorder_c_id_idx_range, handles[cf_map.at(tbl_oorder_c_id_idx)]);

      it_oorder_c_id_idx->SeekToLast();

      oorder_c_id_idx::key k_oo_idx_temp;
      const oorder_c_id_idx::key *k_oo_idx = Decode(it_oorder_c_id_idx->key().ToString(), k_oo_idx_temp);
      o_id = k_oo_idx->o_o_id;

      delete it_oorder_c_id_idx;
     

      const order_line::key k_ol_0(warehouse_id, districtID, o_id, 0);
      const order_line::key k_ol_1(warehouse_id, districtID, o_id, numeric_limits<int32_t>::max());
      ReadOptions order_line_range;
      order_line_range.snapshot = snapshot;

      Slice s_order_line_lower(Encode(obj_key0, k_ol_0));
      Slice s_order_line_upper(Encode(obj_key1, k_ol_1));
      order_line_range.iterate_lower_bound = &s_order_line_lower;
      order_line_range.iterate_upper_bound = &s_order_line_upper;

      {
        Iterator *it_order_line = txn->GetIterator(order_line_range, handles[cf_map.at(tbl_order_line)]);

        int n = 0;
        for (it_order_line->SeekToFirst(); it_order_line->Valid(); it_order_line->Next()) {
            // read some data
            order_line::value v_ol_temp;
            const order_line::value *v_ol = Decode(it_order_line->value().ToString(), v_ol_temp);
            ++n;
          }
          ALWAYS_ASSERT(n >= 5 && n <= 15);
    
          delete it_order_line;
      }

      s = txn->Commit();
      txn_db_->ReleaseSnapshot(snapshot);
      delete txn;
      return s.ok();
    } catch (abort_exception &ex) {
      txn->Rollback();
      txn_db_->ReleaseSnapshot(snapshot);
      delete txn;
      return false;
    }
    return 0;
  }

  bool TPCCWorker::txn_stock_level() {
    std::cout<<"stock"<<std::endl;
    string obj_key0;
    string obj_key1;
    string obj_v;
    str_arena arena;

    fast_random r(seed_);
    r.next_uniform();

    const uint warehouse_id = PickWarehouseId(r, 1, NumWarehouses + 1);
    const uint threshold = RandomNumber(r, 10, 20);
    const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse);

    scoped_str_arena s_arena(arena);

    Status s;

    WriteOptions writeOptions;
    writeOptions.sync = write_sync;
    writeOptions.disableWAL = disable_wal;

    ReadOptions readOptions;

    TransactionOptions txn_option;
    txn_option.deadlock_detect = deadlock_detect;

    Transaction *txn = txn_db_->BeginTransaction(writeOptions, txn_option);
    //txn->SetTxnType(4);

    const Snapshot* snapshot = txn_db_->GetSnapshot();
    readOptions.snapshot = snapshot;

    int32_t lower = 0;
    int32_t cur_next_o_id = 0;

    try {
      const district::value *v_d;
      // read district
      const district::key k_d(warehouse_id, districtID);

      ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_district)], Encode(str(&arena), k_d), &obj_v));
      district::value v_d_temp;
      v_d = Decode(obj_v, v_d_temp);
      checker::SanityCheckDistrict(&k_d, v_d);

      cur_next_o_id = v_d->d_next_o_id;

      lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
      const order_line::key k_ol_0(warehouse_id, districtID, lower, 0);
      const order_line::key k_ol_1(warehouse_id, districtID, cur_next_o_id, 0);

      ReadOptions order_line_range;
      order_line_range.snapshot = snapshot;

      Slice s_order_line_lower(Encode(obj_key0, k_ol_0));
      Slice s_order_line_upper(Encode(obj_key1, k_ol_1));
      order_line_range.iterate_lower_bound = &s_order_line_lower;
      order_line_range.iterate_upper_bound = &s_order_line_upper;


      Iterator *it_order_line = txn->GetIterator(order_line_range, handles[cf_map.at(tbl_order_line)]);

      vector<int32_t> itemIDs;

      for (it_order_line->SeekToFirst(); it_order_line->Valid(); it_order_line->Next()) {
        order_line::value ol_v_temp;
        const order_line::value *ol_v = Decode(it_order_line->value().ToString(), ol_v_temp);
        // check duplicate key
        if(std::find(itemIDs.begin(), itemIDs.end(), ol_v->ol_i_id) == itemIDs.end()) {
          itemIDs.push_back(ol_v->ol_i_id);
        }
      }

      sort(itemIDs.begin(), itemIDs.end());
      unordered_map<uint, bool> s_i_ids_distinct;

      for (int itemID : itemIDs) {
        const size_t nbytesread = serializer<int16_t, true>::max_nbytes();
        const stock::key k_s(warehouse_id, itemID);
        INVARIANT(p.first >= 1 && p.first <= NumItems);

        ASSERT_STATUS(txn->Get(readOptions, handles[cf_map.at(tbl_stock)], Encode(str(&arena), k_s), &obj_v));
        INVARIANT(obj_v.size() <= nbytesread);

        const uint8_t *ptr = (const uint8_t *) obj_v.data();
        int16_t i16tmp;
        ptr = serializer<int16_t, true>::read(ptr, &i16tmp);
        if (i16tmp < int(threshold)) s_i_ids_distinct[itemID] = 1;
      }

      delete it_order_line;

      s = txn->Commit();
      txn_db_->ReleaseSnapshot(snapshot);
      delete txn;
      return s.ok();
    } catch (abort_exception &ex) {
      txn->Rollback();
      txn_db_->ReleaseSnapshot(snapshot);
      delete txn;
      return false;
    }
    return 0;
  }

  void TPCCWorker::do_wait(Transaction *txn, unsigned int p_idx) {
    // avoid cyclic dependency
    //ASSERT_STATUS(txn->DoWait(tpcc_type_, p_idx));
  }

  TPCCWorkerGen::TPCCWorkerGen(Controller *controller, size_t nworkers) : WorkerGen(controller, nworkers) {
    hack_map.reserve(nworkers);
    for (int i = 0; i < nworkers; ++i) {
      hack_map[i] = new int32_t[NumWarehouses * NumDistrictsPerWarehouse + 64]();
    }
     update_locks.reserve(cfd_count);
    for (int i = 0; i < cfd_count; ++i) {
      update_locks[i] = new vector<mutex>(10000);
    }
  }

  TPCCWorkerGen::~TPCCWorkerGen() {
     
   // delete r;
    //std::cout<<"generate"<<std::endl;
    for (auto&& handle : handles) delete handle;
    for (int i = 0; i < nworkers_; ++i)
        delete[] hack_map[i];

  //   std::cout<<"generate"<<std::endl;
  }

  void TPCCWorkerGen::Init() {
    r = new fast_random(23984543);

    //Column Family
        column_families.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_warehouse, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_item, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_stock, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_stock_data, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_district, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_customer, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_customer_name_idx, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_history, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_oorder, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_oorder_c_id_idx, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_new_order, ColumnFamilyOptions()));
    column_families.emplace_back(ColumnFamilyDescriptor(tbl_order_line, ColumnFamilyOptions()));


    Status status = TransactionDB::Open(InitDBOptions(walPath), InitTxnDBOptions(), dbPath, column_families, &handles,
                                        &txn_db);
    if (!status.ok()) cerr << "Error initialize: " << status.ToString() << endl;
    assert(status.ok());

    //cfd_id
    for (auto&& it : cf_map) cfh_id_map[it.first] = handles[it.second]->GetID();
  }

  int TPCCWorkerGen::get_tpcc_txn_type(unsigned long seed) {
  
    fast_random r(seed);
    uint res = RandomNumber(r, 1, 100);
    for (int i = 0; i < 5; ++i) {
      if (res <= g_txn_workload_mix[i]) return i;
      else res -= g_txn_workload_mix[i];
    }
    return 0;
  }

  Worker* TPCCWorkerGen::Next(size_t idx) {
    int type =2;
   //  std::cout<<"generate"<<std::endl;
    TPCCWorker* worker = new TPCCWorker(controller_, nworkers_, idx, type, r->next(), txn_db, handles);
    // std::cout<<"generate"<std::endl;
//    worker->last_no_o_ids = new int32_t[NumWarehouses * NumDistrictsPerWarehouse + 64];
//    NDB_MEMSET(&worker->last_no_o_ids[0], 0, sizeof(worker->last_no_o_ids));
    worker->last_no_o_ids = hack_map[idx];




    return worker;
  }
}


