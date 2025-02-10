/**
 * @ Description: Original code from data player of Sthereo dataset
 */

#ifndef __DATATHREAD_H__
#define __DATATHREAD_H__

#include <queue>
#include <mutex>
#include <ctime>
#include <thread>
#include <condition_variable>

template <typename T>
struct DataThread{
    std::mutex mutex;
    std::queue<T> data_queue;
    std::condition_variable cv;
    std::thread thread;
    bool active;

    DataThread() : active(true){}

    void push(T data){
        mutex.lock();
        data_queue.push(data);
        mutex.unlock();
    }

    T pop(){
        T result;
        mutex.lock();
        result = data_queue.front();
        data_queue.pop();
        mutex.unlock();
        return result;
    }
};

#endif // __DATATHREAD_H__
