#pragma once

#include "stddef.h"

// TPCC variables
extern size_t NumWarehouses;
extern unsigned g_txn_workload_mix[5];

// Policy map related
extern int TotalEntryCount;

extern int OneAccessEntryCount;
extern int TotalAccess;

extern int OneDepPieceEntryCount;
extern int TotalDepPiece;
