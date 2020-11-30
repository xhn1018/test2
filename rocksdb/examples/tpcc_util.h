#pragma once
#include <string>

#include "macros.h"
#include "util.h"

using namespace util;
using namespace std;

struct checker {
  // these sanity checks are just a few simple checks to make sure
  // the data is not entirely corrupted

  static inline ALWAYS_INLINE void
  SanityCheckCustomer(const customer::key *k, const customer::value *v)
  {
    INVARIANT(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= NumWarehouses());
    INVARIANT(k->c_d_id >= 1 && static_cast<size_t>(k->c_d_id) <= NumDistrictsPerWarehouse());
    INVARIANT(k->c_id >= 1 && static_cast<size_t>(k->c_id) <= NumCustomersPerDistrict());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->c_credit == "BC" || v->c_credit == "GC");
    INVARIANT(v->c_middle == "OE");
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckWarehouse(const warehouse::key *k, const warehouse::value *v)
  {
    INVARIANT(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= NumWarehouses());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->w_state.size() == 2);
    INVARIANT(v->w_zip == "123456789");
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckDistrict(const district::key *k, const district::value *v)
  {
    INVARIANT(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= NumWarehouses());
    INVARIANT(k->d_id >= 1 && static_cast<size_t>(k->d_id) <= NumDistrictsPerWarehouse());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->d_next_o_id >= 3001);
    INVARIANT(v->d_state.size() == 2);
    INVARIANT(v->d_zip == "123456789");
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckItem(const item::key *k, const item::value *v)
  {
    INVARIANT(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= NumItems());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->i_price >= 1.0 && v->i_price <= 100.0);
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckStock(const stock::key *k, const stock::value *v)
  {
    INVARIANT(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= NumWarehouses());
    INVARIANT(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= NumItems());
  }

  static inline ALWAYS_INLINE void
  SanityCheckNewOrder(const new_order::key *k, const new_order::value *v)
  {
    INVARIANT(k->no_w_id >= 1 && static_cast<size_t>(k->no_w_id) <= NumWarehouses());
    INVARIANT(k->no_d_id >= 1 && static_cast<size_t>(k->no_d_id) <= NumDistrictsPerWarehouse());
  }

  static inline ALWAYS_INLINE void
  SanityCheckOOrder(const oorder::key *k, const oorder::value *v)
  {
    INVARIANT(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= NumWarehouses());
    INVARIANT(k->o_d_id >= 1 && static_cast<size_t>(k->o_d_id) <= NumDistrictsPerWarehouse());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->o_c_id >= 1 && static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
    INVARIANT(v->o_carrier_id >= 0 && static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
    INVARIANT(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckOrderLine(const order_line::key *k, const order_line::value *v)
  {
    INVARIANT(k->ol_w_id >= 1 && static_cast<size_t>(k->ol_w_id) <= NumWarehouses());
    INVARIANT(k->ol_d_id >= 1 && static_cast<size_t>(k->ol_d_id) <= NumDistrictsPerWarehouse());
    INVARIANT(k->ol_number >= 1 && k->ol_number <= 15);
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems());
#endif
  }
};

static inline ALWAYS_INLINE int
CheckBetweenInclusive(int v, int lower, int upper)
{
  INVARIANT(v >= lower);
  INVARIANT(v <= upper);
  return v;
}
static inline ALWAYS_INLINE int
RandomNumber(fast_random &r, int min, int max)
{
  return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
}

static inline ALWAYS_INLINE int
NonUniformRandom(fast_random &r, int A, int C, int min, int max)
{
  return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
}

static inline ALWAYS_INLINE int
GetItemId(fast_random &r)
{
  return CheckBetweenInclusive(
      g_uniform_item_dist ?
      RandomNumber(r, 1, NumItems) :
      NonUniformRandom(r, 8191, 7911, 1, NumItems),
      1, NumItems);
}

static inline ALWAYS_INLINE int
GetCustomerId(fast_random &r)
{
  return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict), 1, NumCustomersPerDistrict);
}

// pick a number between [start, end)
static inline ALWAYS_INLINE unsigned
PickWarehouseId(fast_random &r, unsigned start, unsigned end)
{
  INVARIANT(start < end);
  const unsigned diff = end - start;
  if (diff == 1)
    return start;
  return (r.next() % diff) + start;
}

static string NameTokens[] =
    {
    string("BAR"),
    string("OUGHT"),
    string("ABLE"),
    string("PRI"),
    string("PRES"),
    string("ESE"),
    string("ANTI"),
    string("CALLY"),
    string("ATION"),
    string("EING"),
};

// all tokens are at most 5 chars long
static const size_t CustomerLastNameMaxSize = 5 * 3;

static inline size_t
GetCustomerLastName(uint8_t *buf, fast_random &r, int num)
{
  const string &s0 = NameTokens[num / 100];
  const string &s1 = NameTokens[(num / 10) % 10];
  const string &s2 = NameTokens[num % 10];
  uint8_t *const begin = buf;
  const size_t s0_sz = s0.size();
  const size_t s1_sz = s1.size();
  const size_t s2_sz = s2.size();
  NDB_MEMCPY(buf, s0.data(), s0_sz); buf += s0_sz;
  NDB_MEMCPY(buf, s1.data(), s1_sz); buf += s1_sz;
  NDB_MEMCPY(buf, s2.data(), s2_sz); buf += s2_sz;
  return buf - begin;
}

static inline ALWAYS_INLINE size_t
GetCustomerLastName(char *buf, fast_random &r, int num)
{
  return GetCustomerLastName((uint8_t *) buf, r, num);
}

static inline string
GetCustomerLastName(fast_random &r, int num)
{
  string ret;
  ret.resize(CustomerLastNameMaxSize);
  ret.resize(GetCustomerLastName((uint8_t *) &ret[0], r, num));
  return ret;
}

static inline ALWAYS_INLINE string
GetNonUniformCustomerLastNameLoad(fast_random &r)
{
  return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
}

static inline ALWAYS_INLINE size_t
GetNonUniformCustomerLastNameRun(uint8_t *buf, fast_random &r)
{
  return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
}

static inline ALWAYS_INLINE size_t
GetNonUniformCustomerLastNameRun(char *buf, fast_random &r)
{
  return GetNonUniformCustomerLastNameRun((uint8_t *) buf, r);
}

static inline ALWAYS_INLINE string
GetNonUniformCustomerLastNameRun(fast_random &r)
{
  return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
}

// following oltpbench, we really generate strings of len - 1...
static inline string
RandomStr(fast_random &r, uint len)
{
  // this is a property of the oltpbench implementation...
  if (!len)
    return "";

  uint i = 0;
  string buf(len - 1, 0);
  while (i < (len - 1)) {
    const char c = (char) r.next_char();
    // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
    // is a less restrictive filter than isalnum()
    if (!isalnum(c))
      continue;
    buf[i++] = c;
  }
  return buf;
}

// RandomNStr() actually produces a string of length len
static inline string
RandomNStr(fast_random &r, uint len)
{
  const char base = '0';
  string buf(len, 0);
  for (uint i = 0; i < len; i++)
    buf[i] = (char)(base + (r.next() % 10));
  return buf;
}

static inline uint32_t
GetCurrentTimeMillis()
{
  //struct timeval tv;
  //ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
  //return tv.tv_sec * 1000;

  // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
  // for now, we just give each core an increasing number

  static __thread uint32_t tl_hack = 0;
  return tl_hack++;
}

static aligned_padded_elem<atomic<uint64_t>> *g_district_ids = nullptr;

static inline atomic<uint64_t> &
NewOrderIdHolder(unsigned warehouse, unsigned district)
{
  INVARIANT(warehouse >= 1 && warehouse <= NumWarehouses());
  INVARIANT(district >= 1 && district <= NumDistrictsPerWarehouse());
  const unsigned idx =
      (warehouse - 1) * NumDistrictsPerWarehouse + (district - 1);
  return g_district_ids[idx].elem;
}

static inline uint64_t
FastNewOrderIdGen(unsigned warehouse, unsigned district)
{
  return NewOrderIdHolder(warehouse, district).fetch_add(1, memory_order_acq_rel);
}
