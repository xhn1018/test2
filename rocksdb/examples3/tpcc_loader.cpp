#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cinttypes>
#include <string>
#include <iostream>
#include <sstream>
#include <set>

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "tpcc_loader.h"
#include "tpcc.h"
#include "tpcc_util.h"

using std::set;
using std::string;
using std::to_string;
using std::cerr;
using std::endl;
using namespace rocksdb;

namespace sim {

  void TPCCLoader::InitializeCF() {
    Options options;
    options.create_if_missing = true;
    DB *db;
    Status status = DB::Open(options, dbPath_, &db);
    if (!status.ok()) cerr << "Error open: " << status.ToString() << endl;

    // create column family
    for (auto kv : cf_map) {
      ColumnFamilyHandle *cf;
      status = db->CreateColumnFamily(ColumnFamilyOptions(), kv.first, &cf);
      if (!status.ok()) cerr << "Error initialize CF: " << status.ToString() << endl;
      delete cf;
    }
    // close DB
    delete db;
  }

  void TPCCLoader::Init() {

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

    Status status = TransactionDB::Open(InitDBOptions(walPath_), InitTxnDBOptions(), dbPath_, column_families, &handles,
                                        &txn_db);
    if (!status.ok()) cerr << "Error initialize: " << status.ToString() << endl;
    assert(status.ok());
  }

  void TPCCLoader::prepare_warehouse(TransactionDB *db, fast_random &r) {
    Status s;
    WriteOptions writeOptions;
    writeOptions.sync = false;
    writeOptions.disableWAL = true;
    TransactionOptions txn_option = TransactionOptions();
    txn_option.deadlock_detect = true;
    Transaction *txn = db->BeginTransaction(writeOptions, txn_option);

    for (uint i = 1; i <= NumWarehouses; i++) {
      const warehouse::key k(i);

      const string w_name = RandomStr(r, RandomNumber(r, 6, 10));
      const string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
      const string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
      const string w_city = RandomStr(r, RandomNumber(r, 10, 20));
      const string w_state = RandomStr(r, 3);
      const string w_zip = "123456789";

      warehouse::value v;
      v.w_ytd = 300000;
      v.w_tax = (float) RandomNumber(r, 0, 2000) / 10000.0;
      v.w_name.assign(w_name);
      v.w_street_1.assign(w_street_1);
      v.w_street_2.assign(w_street_2);
      v.w_city.assign(w_city);
      v.w_state.assign(w_state);
      v.w_zip.assign(w_zip);

      checker::SanityCheckWarehouse(&k, &v);

      string obj_buf;
      string key = Encode(k);
      string value = Encode(obj_buf, v);
      s = txn->Put(handles[cf_map.at(tbl_warehouse)], key, value);
      if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;
    }

    s = txn->Commit();
    if (!s.ok()) cerr << "Error commit: " << s.ToString() << endl;
    delete txn;
  }

  void TPCCLoader::prepare_item(TransactionDB *db, fast_random &r) {
    Status s;
    WriteOptions writeOptions;
    writeOptions.sync = false;
    writeOptions.disableWAL = true;
    TransactionOptions txn_option = TransactionOptions();
    txn_option.deadlock_detect = true;
    Transaction *txn = db->BeginTransaction(writeOptions, txn_option);

    for (uint i = 1; i <= NumItems; i++) {
      // items don't "belong" to a certain warehouse, so no pinning
      const item::key k(i);

      item::value v;
      const string i_name = RandomStr(r, RandomNumber(r, 14, 24));
      v.i_name.assign(i_name);
      v.i_price = (float) RandomNumber(r, 100, 10000) / 100.0;
      const int len = RandomNumber(r, 26, 50);
      if (RandomNumber(r, 1, 100) > 10) {
        const string i_data = RandomStr(r, len);
        v.i_data.assign(i_data);
      } else {
        const int startOriginal = RandomNumber(r, 2, (len - 8));
        const string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
        v.i_data.assign(i_data);
      }
      v.i_im_id = RandomNumber(r, 1, 10000);

      checker::SanityCheckItem(&k, &v);

      string obj_buf;
      string key = Encode(k);
      string value = Encode(obj_buf, v);

      s = txn->Put(handles[cf_map.at(tbl_item)], key, value);
      if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;
    }

    s = txn->Commit();
    if (!s.ok()) cerr << "Error commit: " << s.ToString() << endl;
    delete txn;
  }

  void TPCCLoader::prepare_stock(TransactionDB *db, fast_random &r) {

    for (uint w = 1; w <= NumWarehouses; w++) {
      const size_t batchsize = NumItems;
      const size_t nbatches = 1;

      for (uint b = 0; b < nbatches; b++) {
        Status s;
        WriteOptions writeOptions;
        writeOptions.sync = false;
        writeOptions.disableWAL = true;
        TransactionOptions txn_option = TransactionOptions();
        txn_option.deadlock_detect = true;
        Transaction *txn = db->BeginTransaction(writeOptions, txn_option);

        const size_t iend = std::min((b + 1) * batchsize + 1, NumItems);
        for (uint i = (b * batchsize + 1); i <= iend; i++) {
          const stock::key k(w, i);
          const stock_data::key k_data(w, i);

          stock::value v;
          v.s_quantity = RandomNumber(r, 10, 100);
          v.s_ytd = 0;
          v.s_order_cnt = 0;
          v.s_remote_cnt = 0;

          stock_data::value v_data;
          const int len = RandomNumber(r, 26, 50);
          if (RandomNumber(r, 1, 100) > 10) {
            const string s_data = RandomStr(r, len);
            v_data.s_data.assign(s_data);
          } else {
            const int startOriginal = RandomNumber(r, 2, (len - 8));
            const string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
            v_data.s_data.assign(s_data);
          }
          v_data.s_dist_01.assign(RandomStr(r, 24));
          v_data.s_dist_02.assign(RandomStr(r, 24));
          v_data.s_dist_03.assign(RandomStr(r, 24));
          v_data.s_dist_04.assign(RandomStr(r, 24));
          v_data.s_dist_05.assign(RandomStr(r, 24));
          v_data.s_dist_06.assign(RandomStr(r, 24));
          v_data.s_dist_07.assign(RandomStr(r, 24));
          v_data.s_dist_08.assign(RandomStr(r, 24));
          v_data.s_dist_09.assign(RandomStr(r, 24));
          v_data.s_dist_10.assign(RandomStr(r, 24));

          checker::SanityCheckStock(&k, &v);

          string obj_buf;
          s = txn->Put(handles[cf_map.at(tbl_stock)], Encode(k), Encode(obj_buf, v));
          if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;

          string obj_buf1;
          s = txn->Put(handles[cf_map.at(tbl_stock_data)], Encode(k_data), Encode(obj_buf1, v_data));
          if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;
        }
        s = txn->Commit();
        if (!s.ok()) cerr << "Error commit: " << s.ToString() << endl;
        delete txn;
      }
    }
  }

  void TPCCLoader::prepare_district(TransactionDB *db, fast_random &r) {
    for (uint w = 1; w <= NumWarehouses; w++) {
      Status s;
      WriteOptions writeOptions;
      writeOptions.sync = false;
      writeOptions.disableWAL = true;
      TransactionOptions txn_option = TransactionOptions();
      txn_option.deadlock_detect = true;
      Transaction *txn = db->BeginTransaction(writeOptions, txn_option);

      for (uint d = 1; d <= NumDistrictsPerWarehouse; d++) {
        const district::key k(w, d);
        district::value v;
        v.d_ytd = 30000;
        v.d_tax = (float) (RandomNumber(r, 0, 2000) / 10000.0);
        v.d_next_o_id = 3001;
        v.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
        v.d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_state.assign(RandomStr(r, 3));
        v.d_zip.assign("123456789");

        checker::SanityCheckDistrict(&k, &v);

        string obj_buf;
        s = txn->Put(handles[cf_map.at(tbl_district)], Encode(k), Encode(obj_buf, v));
        if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;
      }
      s = txn->Commit();
      if (!s.ok()) cerr << "Error commit: " << s.ToString() << endl;
      delete txn;
    }
  }

  void TPCCLoader::prepare_customer(TransactionDB *db, fast_random &r) {
    for (uint w = 1; w <= NumWarehouses; w++) {
      const size_t batchsize = NumCustomersPerDistrict;
      const size_t nbatches = 1;

      for (uint d = 1; d <= NumDistrictsPerWarehouse; d++) {
        for (uint batch = 0; batch < nbatches; batch++) {
          Status s;
          WriteOptions writeOptions;
          writeOptions.sync = false;
          writeOptions.disableWAL = true;
          TransactionOptions txn_option = TransactionOptions();
          txn_option.deadlock_detect = true;
          Transaction *txn = db->BeginTransaction(writeOptions, txn_option);

          const size_t cstart = batch * batchsize;
          const size_t cend = std::min((batch + 1) * batchsize, NumCustomersPerDistrict);
          for (uint cidx0 = cstart; cidx0 < cend; cidx0++) {
            const uint c = cidx0 + 1;
            const customer::key k(w, d, c);

            customer::value v;
            v.c_discount = (float) (RandomNumber(r, 1, 5000) / 10000.0);
            if (RandomNumber(r, 1, 100) <= 10)
              v.c_credit.assign("BC");
            else
              v.c_credit.assign("GC");

            if (c <= 1000)
              v.c_last.assign(GetCustomerLastName(r, c - 1));
            else
              v.c_last.assign(GetNonUniformCustomerLastNameLoad(r));

            v.c_first.assign(RandomStr(r, RandomNumber(r, 8, 16)));
            v.c_credit_lim = 50000;

            v.c_balance = -10;
            v.c_ytd_payment = 10;
            v.c_payment_cnt = 1;
            v.c_delivery_cnt = 0;

            v.c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_state.assign(RandomStr(r, 3));
            v.c_zip.assign(RandomNStr(r, 4) + "11111");
            v.c_phone.assign(RandomNStr(r, 16));
            v.c_since = GetCurrentTimeMillis();
            v.c_middle.assign("OE");
            v.c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

            checker::SanityCheckCustomer(&k, &v);

            string obj_buf1;
            s = txn->Put(handles[cf_map.at(tbl_customer)], Encode(k), Encode(obj_buf1, v));
            if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;

            // customer name index
            const customer_name_idx::key k_idx(k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));
            const customer_name_idx::value v_idx(k.c_id);

            // index structure is:
            // (c_w_id, c_d_id, c_last, c_first) -> (c_id)

            string obj_buf2;
            s = txn->Put(handles[cf_map.at(tbl_customer_name_idx)], Encode(k_idx), Encode(obj_buf2, v_idx));
            if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;

            history::key k_hist;
            k_hist.h_c_id = c;
            k_hist.h_c_d_id = d;
            k_hist.h_c_w_id = w;
            k_hist.h_d_id = d;
            k_hist.h_w_id = w;
            k_hist.h_date = GetCurrentTimeMillis();

            history::value v_hist;
            v_hist.h_amount = 10;
            v_hist.h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

            string obj_buf3;
            s = txn->Put(handles[cf_map.at(tbl_history)], Encode(k_hist), Encode(obj_buf3, v_hist));
            if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;
          }
          s = txn->Commit();
          if (!s.ok()) cerr << "Error commit: " << s.ToString() << endl;
          delete txn;
        }
      }
    }
  }

  void TPCCLoader::prepare_order(TransactionDB *db, fast_random &r) {
    string obj_buf;
    for (uint w = 1; w <= NumWarehouses; w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse; d++) {
        set<uint> c_ids_s;
        vector<uint> c_ids;
        while (c_ids.size() != NumCustomersPerDistrict) {
          const auto x = (r.next() % NumCustomersPerDistrict) + 1;
          if (c_ids_s.count(x))
            continue;
          c_ids_s.insert(x);
          c_ids.emplace_back(x);
        }
        for (uint c = 1; c <= NumCustomersPerDistrict; c++) {
          Status s;
          WriteOptions writeOptions;
          writeOptions.sync = false;
          writeOptions.disableWAL = true;
          TransactionOptions txn_option = TransactionOptions();
          txn_option.deadlock_detect = true;
          Transaction *txn = db->BeginTransaction(writeOptions, txn_option);

          const oorder::key k_oo(w, d, c);

          oorder::value v_oo;
          v_oo.o_c_id = c_ids[c - 1];
          if (k_oo.o_id < 2101)
            v_oo.o_carrier_id = RandomNumber(r, 1, 10);
          else
            v_oo.o_carrier_id = 0;
          v_oo.o_ol_cnt = RandomNumber(r, 5, 15);
          v_oo.o_all_local = 1;
          v_oo.o_entry_d = GetCurrentTimeMillis();

          checker::SanityCheckOOrder(&k_oo, &v_oo);

          s = txn->Put(handles[cf_map.at(tbl_oorder)], Encode(k_oo), Encode(obj_buf, v_oo));
          if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;

          const oorder_c_id_idx::key k_oo_idx(k_oo.o_w_id, k_oo.o_d_id, v_oo.o_c_id, k_oo.o_id);
          const oorder_c_id_idx::value v_oo_idx(0);

          s = txn->Put(handles[cf_map.at(tbl_oorder_c_id_idx)], Encode(k_oo_idx), Encode(obj_buf, v_oo_idx));
          if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;

          if (c >= 2101) {
            const new_order::key k_no(w, d, c);
            const new_order::value v_no;

            checker::SanityCheckNewOrder(&k_no, &v_no);
            s = txn->Put(handles[cf_map.at(tbl_new_order)], Encode(k_no), Encode(obj_buf, v_no));
            if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;
          }
          for (uint l = 1; l <= uint(v_oo.o_ol_cnt); l++) {
            const order_line::key k_ol(w, d, c, l);

            order_line::value v_ol;
            v_ol.ol_i_id = RandomNumber(r, 1, 100000);
            if (k_ol.ol_o_id < 2101) {
              v_ol.ol_delivery_d = v_oo.o_entry_d;
              v_ol.ol_amount = 0;
            } else {
              v_ol.ol_delivery_d = 0;
              // random within [0.01 .. 9,999.99]
              v_ol.ol_amount = (float) (RandomNumber(r, 1, 999999) / 100.0);
            }

            v_ol.ol_supply_w_id = k_ol.ol_w_id;
            v_ol.ol_quantity = 5;
            // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
//          v_ol.ol_dist_info = RandomStr(r, 24);

            checker::SanityCheckOrderLine(&k_ol, &v_ol);
            s = txn->Put(handles[cf_map.at(tbl_order_line)], Encode(k_ol), Encode(obj_buf, v_ol));
            if (!s.ok()) cerr << "Error put: " << s.ToString() << endl;
          }
          s = txn->Commit();
          if (!s.ok()) cerr << "Error commit: " << s.ToString() << endl;
          delete txn;
        }
      }
    }
  }

  void TPCCLoader::Prepare() {
    util::fast_random r_warehouse(9324);
    prepare_warehouse(txn_db, r_warehouse);

    util::fast_random r_item(235443);
    prepare_item(txn_db, r_item);

    util::fast_random r_stock(89785943);
    prepare_stock(txn_db, r_stock);

    util::fast_random r_district(129856349);
    prepare_district(txn_db, r_district);

    util::fast_random r_customer(923587856425);
    prepare_customer(txn_db, r_customer);

    util::fast_random r_order(2343352);
    prepare_order(txn_db, r_order);

    for (auto handle : handles) {
      txn_db->Flush(FlushOptions(), handle);
    }
  }

  TPCCLoader::~TPCCLoader() {
    for (auto&& handle : handles) delete handle;
  }
}
