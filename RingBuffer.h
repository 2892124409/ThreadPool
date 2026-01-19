#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#include <vector>
#include <atomic>
#include <cstddef>
#include <utility>
#include <stdexcept>

template <typename T>
class RingBuffer {
private:
    struct Cell {
        std::atomic<size_t> sequence; // 序列号，用于协调读写顺序
        T data;                       // 实际数据
    };

    std::vector<Cell> m_buffer;    // 存储单元数组
    size_t m_buffer_mask;         // 掩码，用于快速计算索引 (pos & m_buffer_mask)
    std::atomic<size_t> m_head;   // 原子读指针 (消费者共享)
    std::atomic<size_t> m_tail;   // 原子写指针 (生产者共享)

public:
    explicit RingBuffer(size_t capacity) 
        : m_buffer(capacity), m_buffer_mask(capacity - 1), m_head(0), m_tail(0) 
    {
        // 强制要求 capacity 是 2 的幂，这是为了能使用位运算 m_buffer_mask 来替代取模
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            throw std::invalid_argument("Capacity must be a power of 2");
        }

        // 初始化所有 cell 的 sequence。
        // 第 i 个位置的 sequence 初始值应该是 i。
        // 这意味着：位置 0 等待第 0 次写入，位置 1 等待第 1 次写入...
        for (size_t i = 0; i < capacity; ++i) {
            m_buffer[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    // 无锁 Push 操作
    // 返回 true 表示成功写入，false 表示队列已满
    bool push(T item) {
        Cell* cell;
        size_t pos = m_tail.load(std::memory_order_relaxed); // 获取当前的写位置

        while (true) {
            cell = &m_buffer[pos & m_buffer_mask]; // 通过掩码找到对应的 Cell 指针
            
            // 1. 获取当前坑位的门票号 (sequence)
            // 使用 acquire 序，保证如果是别的线程更新了 sequence，我能看到相关的 data 写入
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            
            // 2. 计算 diff。如果 seq == pos，说明这个坑位是空的，且正好轮到这一次写入
            intptr_t dif = (intptr_t)seq - (intptr_t)pos;

            if (dif == 0) {
                // 情况A：坑位空闲，且轮到我。尝试原子地把 m_tail 加 1 (pos -> pos + 1)
                // 如果成功，说明我抢到了这个坑位！break 出去写数据。
                if (m_tail.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
                // 如果失败（返回 false），说明刚才的一瞬间有别的线程抢先抢走了这个位置。
                // compare_exchange_weak 会自动把最新的 tail 值更新到 pos 里。
                // 咱们也不气馁，拿着新的 pos 再次进入循环重试。
            } 
            else if (dif < 0) {
                // 情况B：seq < pos。说明 sequence 还是老的值，还没绕回来。
                // 这意味着队列满了。
                return false;
            } 
            else {
                // 情况C：diff > 0 (即 seq > pos)。
                // 这通常意味着 pos 已经过期了（被别的线程推得更远了），或者我的 pos 读错了。
                // 重新加载最新的 m_tail 试试。
                pos = m_tail.load(std::memory_order_relaxed);
            }
        }

        // 3. 写入数据
        // 因为我已经独占了这个 Cell (通过 CAS 抢到了 pos)，所以这里不需要锁
        cell->data = std::move(item);

        // 4. 更新 sequence。
        // 把 sequence 设为 pos + 1。
        // 比如在位置 0 写入了第 0 个任务，sequence 变为 1。
        // 消费者看到 sequence 是 1，就知道：“哦，第 0 个任务写完了，我可以读了（因为 1 == 0 + 1）”。
        // 使用 release 序，保证我的 data 写入一定在 sequence 更新之前完成。
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    // 无锁 Pop 操作
    // 返回 true 表示成功读取，false 表示队列已空
    bool pop(T& item) {
        Cell* cell;
        size_t pos = m_head.load(std::memory_order_relaxed); // 获取当前的读位置

        while (true) {
            cell = &m_buffer[pos & m_buffer_mask];
            
            // 1. 获取门票
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            
            // 2. 计算 diff。
            // 读操作要求的条件是：sequence == pos + 1
            // 比如我要读第 0 个任务，我要看 sequence 是不是 1。如果是 1，说明生产者写完了。
            intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);

            if (dif == 0) {
                // 情况A：数据已就绪。尝试原子地把 m_head 加 1。
                if (m_head.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break; // 抢到了读权！
                }
                // 失败了，被别的消费者抢了，重试。
            } 
            else if (dif < 0) {
                // 情况B：seq < pos + 1。
                // 说明 sequence 还是 pos (生产者正在写，还没改成 pos+1) 或者更旧。
                // 意味着队列是空的，或者数据还没准备好。
                return false;
            } 
            else {
                // 情况C：seq > pos + 1。
                // 说明 pos 过期了，重新加载 m_head。
                pos = m_head.load(std::memory_order_relaxed);
            }
        }

        // 3. 读取数据
        item = std::move(cell->data);

        // 4. 更新 sequence。
        // 关键点：把 sequence 设为 pos + capacity。
        // 比如 capacity=10。我在位置 0 读完了第 0 个任务。
        // 现在的 sequence 是 1。我要把它改成 10 (0 + 10)。
        // 这样，下一轮当 pos 绕回来变成 10 的时候，生产者检查 seq(10) == pos(10)，就又可以写了！
        cell->sequence.store(pos + m_buffer_mask + 1, std::memory_order_release);
        return true;
    }

    // 辅助函数 (兼容旧接口)
    bool isEmpty() const {
         // 注意：在无锁并发下，这个判断只能作为参考，不能完全准确
         return m_head.load(std::memory_order_relaxed) == m_tail.load(std::memory_order_relaxed);
    }
};

#endif
