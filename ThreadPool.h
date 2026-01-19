#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <vector>
#include <memory>
#include <thread>
#include <future>
#include <functional>
#include <stdexcept>
#include "RingBuffer.h"
// SpinLock 已经不需要了

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

    // 无锁环形任务队列 (MPMC Safe)
    RingBuffer<std::function<void()>> m_tasks;

    // 线程池是否停止标志 (使用 atomic 以保证线程安全)
    std::atomic<bool> m_stop;
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
        
        // 1. 检查停止信号
        // 如果停止了且队列大概率是空的，就退出
        // 注意：isEmpty 只是一个参考，但在 stop 为 true 时，即使有少量误判也没关系
        if (m_stop.load(std::memory_order_relaxed) && m_tasks.isEmpty())
        {
            return;
        }

        // 2. 尝试无锁 pop
        if (m_tasks.pop(task))
        {
            // 成功取到任务，执行
            task();
        }
        else
        {
            // 没取到任务 (队列空)，让出 CPU 时间片，避免死循环空转导致 CPU 100%
            // 在生产环境中，这里通常会配合 _mm_pause() 或 yield
            std::this_thread::yield();
        }
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
    
    // 忙等待直到放入
    while (true)
    {
        if (m_stop.load(std::memory_order_relaxed))
        {
            throw std::runtime_error("submit on stopped ThreadPool");
        }

        // 尝试无锁 push
        if (m_tasks.push([task](){ (*task)(); }))
        {
            // 成功放入，直接返回
            break;
        }

        // 队列满了，让出 CPU，稍后重试
        std::this_thread::yield();
    }
    
    return res;
}

// 析构函数实现
inline ThreadPool::~ThreadPool()
{
    // 原子地设置停止标志
    m_stop.store(true, std::memory_order_relaxed);

    for (std::thread &worker : m_workers)
    {
        if(worker.joinable())
            worker.join();
    }
}

#endif // THREAD_POOL_H