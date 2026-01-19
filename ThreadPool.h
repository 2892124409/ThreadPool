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
    std::mutex m_queue_mutex;
    std::condition_variable m_condition;
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
        {
            std::unique_lock<std::mutex> lock(this->m_queue_mutex);
            // 当线程池停止或者任务队列非空就继续执行，否则该工作线程会释放锁并阻塞等待
            this->m_condition.wait(lock, [this]
                                   { return this->m_stop || !this->m_tasks.isEmpty(); });
            // 判断如果线程池停止并且任务队列为空就直接返回，此时自动释放锁
            if (this->m_stop && this->m_tasks.isEmpty())
            {
                return;
            }
            // 从任务队列取任务
            this->m_tasks.pop(task);
        }
        // 通知生产者（可能在取任务前，队列是满的，生产者正在沉睡）
        this->m_condition.notify_all();
        // 执行任务
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
    {
        std::unique_lock<std::mutex> lock(m_queue_mutex);
        // 当线程池停止或任务队列非满就持有锁继续执行；如果线程池没有停止且任务队列满了当前线程就会释放锁并进入阻塞
        m_condition.wait(lock, [this]
                         { return m_stop || !m_tasks.isFull(); });
        if (m_stop)
        {
            throw std::runtime_error("submit on stopped ThreadPool");
        }

        m_tasks.push([task]()
                     { (*task)(); });
    }
    // 通知消费者
    m_condition.notify_one();
    return res;
}

// 析构函数实现
inline ThreadPool::~ThreadPool()
{
    {
        std::unique_lock<std::mutex> lock(m_queue_mutex);
        m_stop = true;
    }
    m_condition.notify_all();
    for (std::thread &worker : m_workers)
    {
        worker.join();
    }
}

#endif // THREAD_POOL_H