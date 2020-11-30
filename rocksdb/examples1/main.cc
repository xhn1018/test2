#include <cstdlib>
#include <string>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <getopt.h>

#include "tpcc.h"
#include "tpcc_loader.h"
#include "simple_loader.h"

using std::string;
using namespace sim;

static const struct option prepare_long_opts[] = {
    {"wal_path", required_argument, NULL, 3},
    {"db_path",  required_argument, NULL, 4},
};

const char *prepare_optstr = "k:";

int main(int argc, char *argv[]) {
  string command = argv[1];
  long number = strtol(argv[2], nullptr, 0);

  // Default DB path
  string dbPath = command == "tpcc" ? "/tmp/tpcc_data" : "/tmp/simple_data";
  string walPath = command == "tpcc" ? "/tmp/tpcc_data/wal" : "/tmp/simple_data/wal";

  int optc;
  while (-1 != (optc = getopt_long(argc, argv, prepare_optstr, prepare_long_opts, nullptr))) {
    switch (optc) {
      case 3:
        walPath = optarg;
        break;
      case 4:
        dbPath = optarg;
        break;
      default:
        printf("warning: unknown arg");
    }
  }

  if (command == "tpcc") {
    TPCCLoader loader(static_cast<size_t >(number), dbPath, walPath);
    loader.InitializeCF();
    loader.Init();
    loader.Prepare();
  } else if (command == "simple") {
    SimpleLoader loader(static_cast<int>(number), dbPath, walPath);
    loader.Init();
    loader.Prepare();
  } else {
    throw 0;
  }

  return 0;
}