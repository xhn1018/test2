#include <iostream>
#include <algorithm>
#include <signal.h>
#include <cassert>
#include "simulator.h"

#include "config.h"

using std::cerr;
using std::endl;
using namespace sim;

Simulator::Simulator(std::string workload_type, std::string env_type_, 
                     int num_workers, int num_keys, int num_wh,
                     double theta, std::vector<int> tpcc_breakdown,
                     bool profile) :
    dbeval(workload_type == "ycsb" ? ycsb:
            workload_type == "tpcc" ? tpcc : large,
            static_cast<unsigned int>(num_workers),
            static_cast<unsigned int>(num_keys), theta, profile),
    env_type(env_type_ == "ntime" ? ntime : ntrans),
    probability_map(std::map<State, OCC_prob>()),
    info(std::vector<std::vector<double> >()) {
        // TODO(Conrad): These assertions are ignored in cython...
        //assert(workload_type == "ycsb" || workload_type == "tpcc");
        //assert(env_type_ == "ntime" || workload_type == "ntrans");

        info.push_back(std::vector<double>());
        info.push_back(std::vector<double>());
        info.push_back(std::vector<double>());

        if (num_wh > 0)
            NumWarehouses = num_wh;
        std::move(tpcc_breakdown.begin(), tpcc_breakdown.end(),
                g_txn_workload_mix);
}

std::vector<AgentDecision>& Simulator::prob_map(std::vector<int>& prob,
        std::vector<double>* vals=NULL) {

  decisions_.resize(TotalEntryCount);

  std::transform (prob.begin(), prob.end(), decisions_.begin(),
          [](int choice) -> AgentDecision {
      // NOTE: Requires AgentDecision to be ordered occ, ic3, 2pl
      return static_cast<AgentDecision>(choice); 
  });

  return decisions_;
}

std::vector<double> Simulator::evaluate(std::vector<int> prob,
    double seconds, bool direct) {
  assert(prob.size() == TotalEntryCount);

  Sample sample_f = Sample{prob_map(prob)};
  std::pair<unsigned int, std::vector<unsigned int> > commits_totals = 
      dbeval.eval_time(seconds, sample_f, direct);

  auto info = dbeval.last_info();

  std::vector<double> ret{static_cast<double>(info.first), static_cast<double>(info.second.first),
      static_cast<double>(info.second.second)};
  for (unsigned int action_count: commits_totals.second)
      ret.emplace_back(static_cast<double>(action_count));

  return ret;
}

std::vector<std::vector<double> > Simulator::last_info() {
    return info;
}
