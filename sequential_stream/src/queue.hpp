#pragma once
#include <iostream>
#include <assert.h>    

#include <queue>
#include <mutex>
#include <condition_variable>

#define MAX_CAPACITY 10

namespace queues {
    template<typename T>
    class blocking_queue {
    private:
        blocking_queue(const blocking_queue& rhs);
        blocking_queue& operator= (const blocking_queue& rhs);

    private:
        mutable std::mutex mtx;
        std::condition_variable full_;
        std::condition_variable empty_;
        std::queue<T> queue_;
        size_t capacity_; 

    public:
        blocking_queue() :
            mtx(), 
            full_(), 
            empty_(), 
            capacity_(MAX_CAPACITY) 
        {}
        
        void enqueue(const T& data) {
            std::unique_lock<std::mutex> lock(mtx);
            while(queue_.size() == capacity_){
                full_.wait(lock);
            }

            assert(queue_.size() < capacity_);
            queue_.push(data);
            empty_.notify_all(); 
        }
        // pops from the front of the queue
        T dequeue() {
            std::unique_lock<std::mutex> lock(mtx);
            while(queue_.empty()) {
                empty_.wait(lock);
            }

            assert(!queue_.empty());
            T front(queue_.front()); // store the value
            queue_.pop(); // pop it from start
            full_.notify_all();
            return front;
        }

        T front() {
            std::unique_lock<std::mutex> lock(mtx);
            while(queue_.empty()) {
                empty_.wait(lock );
            }

            assert(!queue_.empty());
            T front(queue_.front());
            return front;
        }

        T back() {
            std::unique_lock<std::mutex> lock(mtx);
            while(queue_.empty()) {
                empty_.wait(lock );
            }

            assert(!queue_.empty());
            T back(queue_.back());
            return back;
        }

        size_t Size() {
            std::lock_guard<std::mutex> lock(mtx);
            return queue_.size();
        }

        bool empty() {
            std::unique_lock<std::mutex> lock(mtx);
            return queue_.empty();
        }

        void setCapacity(const size_t capacity) {
            capacity_ = (capacity > 0 ? capacity : MAX_CAPACITY);
        }
    };
}


