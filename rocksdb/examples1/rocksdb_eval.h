#pragma once

#include <iostream>
#include <vector>
#include <utility>
#include <map>
#include <random>

namespace sim {

enum Workload_type {ycsb, tpcc, large};
typedef double OCC_prob;
typedef std::vector<unsigned char> State;
typedef std::vector<std::vector <unsigned char> > State_Action;

enum AgentDecision : unsigned char {
  agent_occ = 0,
  agent_ic3 = 1,
  agent_tpl = 2
};

struct Sample {
//    double epsilon;
//    std::mt19937 gen;
//    std::uniform_real_distribution<> dis;
//    std::map<State, OCC_prob> m;
    std::vector<AgentDecision> decisions;
    AgentDecision operator() (const int pos) {
        return decisions[pos];
    };

//    bool get(const int pos) {
//        return decisions[pos];
//    }
};

class RocksDBEval {
    public:
        unsigned int commits;
        unsigned int aborts;
        unsigned int occ_reads;
        unsigned int occ_writes;
        unsigned int ic3_reads;
        unsigned int ic3_writes;
        unsigned int tpl_reads;
        unsigned int tpl_writes;
        unsigned int waits;
        unsigned int no_waits;
        double running_time;
        RocksDBEval(Workload_type workload_type, unsigned int num_workers);

        // Large and hotspot workoad type take num_keys args
        RocksDBEval(Workload_type workload_type, unsigned int num_workers,
                unsigned int num_keys, double theta, bool profile);

        ~RocksDBEval() = default;

        // First return element is the commit count for the duration
        // Second: Vector of clean reads, dirty reads, clean writes, 
        //                   dirty writes, no waits, waits
        std::pair<unsigned int, std::vector<unsigned int> >
            eval_time(double time, Sample&, bool direct);

        // First return element is running time to complete the trans count
        // Second: Outer vector is size T for a T step episode. Keep the order
        //         Inner vector is: vec[0:13] is state, vec[13] is action
        std::pair<double, State_Action>
            eval_transcount(unsigned int transaction_count,
                    Sample& action_map);

        // Return running time, <commit count, abort count>
        std::pair<double, std::pair<unsigned int, unsigned int> > last_info();
    private:
	Workload_type w_;
	unsigned int num_keys_;
	size_t num_workers_;
    double theta;
    bool profile;
};
}
