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

class ThreadPool
{
public:
    // 构造函数：创建并启动指定数量的线程
    ThreadPool(size_t threads);

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

    // 任务队列
    std::queue<std::function<void()>> m_tasks;

    // 同步原语
    std::mutex m_queue_mutex;
    std::condition_variable m_condition;
    bool m_stop;
};

// 构造函数实现
inline ThreadPool::ThreadPool(size_t threads) : m_stop(false)
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
            this->m_condition.wait(lock, [this]
                                   { return this->m_stop || !this->m_tasks.empty(); });

            if (this->m_stop && this->m_tasks.empty())
            {
                return;
            }

            task = std::move(this->m_tasks.front());
            this->m_tasks.pop();
        }
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
        if (m_stop)
        {
            throw std::runtime_error("submit on stopped ThreadPool");
        }
        m_tasks.emplace([task]()
                        { (*task)(); });
    }
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