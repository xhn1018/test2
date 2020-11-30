#pragma once

#include <mutex>
#include <condition_variable>

namespace simpleutil {
class CyclicBarrier {
public:
        CyclicBarrier(int n) : count_(n) {}
        void await() {
                
                if (__sync_add_and_fetch(&count_, -1) <= 0) cv_.notify_all();
                else { 
                        std::unique_lock<std::mutex> lock(mu_);
                        cv_.wait(lock, [this]{ return count_ <= 0; });
                }
        }
private:
        int count_;
        std::mutex mu_;
        std::condition_variable cv_;
};
}
