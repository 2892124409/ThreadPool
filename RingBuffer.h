#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#include <vector>
#include <cstddef>

template <typename T>
class RingBuffer
{
public:
    // 构造函数：capacity 是你希望实际存储的任务最大数量
    explicit RingBuffer(size_t capacity)
        : m_capacity(capacity + 1), // 实际分配加 1，用于区分空和满
          m_head(0),
          m_tail(0)
    {
        m_data.resize(m_capacity);
    }

    // 1. 判断是否为空
    bool isEmpty() const
    {
        if (m_head == m_tail)
            return true;
        return false;
    }

    // 2. 判断是否已满
    bool isFull() const
    {
        if ((m_tail + 1) % m_capacity == m_head)
            return true;
        return false;
    }

    // 3. 写入数据
    bool push(T item)
    {
        if (isFull())
        {
            return false;
        }
        m_data[m_tail] = std::move(item);
        m_tail = (m_tail + 1) % m_capacity;
        return true;
    }

    // 4. 读取数据
    bool pop(T &item)
    {
        if (isEmpty())
        {
            return false;
        }
        item = std::move(m_data[m_head]);
        m_head = (m_head + 1) % m_capacity;
        return true;
    }

private:
    std::vector<T> m_data;
    size_t m_head;
    size_t m_tail;
    size_t m_capacity; // 内部实际数组的长度
};

#endif
