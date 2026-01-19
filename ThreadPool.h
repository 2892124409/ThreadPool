#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>
#include "RingBuffer.h"
#include "SpinLock.h"

class ThreadPool
{
public:
    // 构造函数：创建并启动指定数量的线程
    ThreadPool(size_t threads, size_t capacity = 100);

    // 析构函数：停止并销毁线程池
    ~ThreadPool();

    // 提交任务到队列，并返回一个 future 以获取结果
    template <class F, class... Args>
    auto submit(F &&f, Args &&...args)
        -> std::future<typename std::result_of<F(Args...)>::type>;

private:
    // 工作线程的主循环函数
    void worker_loop();

    // 工作线程的容器
    std::vector<std::thread> m_workers;

    // 环形任务队列
    RingBuffer<std::function<void()>> m_tasks;

    // 同步原语
    SpinLock m_queue_mutex; // 自旋锁，不需要condition
    // std::condition_variable m_condition;

    // 线程池是否停止标志
    bool m_stop;
};

// 构造函数实现
inline ThreadPool::ThreadPool(size_t threads, size_t capacity) : m_stop(false), m_tasks(capacity)
{
    for (size_t i = 0; i < threads; ++i)
    {
        m_workers.emplace_back([this]
                               { this->worker_loop(); });
    }
}

// 工作线程循环实现
inline void ThreadPool::worker_loop()
{
    while (true)
    {
        std::function<void()> task;
        bool got_task = false; // 标志位
        {
            // 当前线程拿到锁
            m_queue_mutex.lock();
            // 如果线程池停止且队列为空
            if (this->m_stop && this->m_tasks.isEmpty())
            {
                m_queue_mutex.unlock(); // 释放锁
                return;                 // 返回
            }

            // 任务队列非空则从任务队列取任务
            if (!m_tasks.isEmpty())
            {
                this->m_tasks.pop(task);
                got_task = true;
            }
            m_queue_mutex.unlock();
        }
        // 取到了任务才会执行任务
        if (got_task)
            task();
    }
}

// 任务提交函数实现
template <class F, class... Args>
auto ThreadPool::submit(F &&f, Args &&...args)
    -> std::future<typename std::result_of<F(Args...)>::type>
{

    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();
    while (true)//循环
    {
        m_queue_mutex.lock(); // 获取自旋锁
        if (m_stop)
        {
            m_queue_mutex.unlock();
            throw std::runtime_error("submit on stopped ThreadPool");
        }

        if (m_tasks.push([task]()
                         { (*task)(); }))
        {
            m_queue_mutex.unlock();
            break;//只有完成了将task放入任务队列才退出循环
        }
        m_queue_mutex.unlock();
    }
    return res;
}

// 析构函数实现
inline ThreadPool::~ThreadPool()
{
    {
        m_queue_mutex.lock(); // 获取锁
        m_stop = true;
        m_queue_mutex.unlock(); // 释放锁
    }
    for (std::thread &worker : m_workers)
    {
        worker.join();
    }
}

#endif // THREAD_POOL_H