#ifndef HEAP_TIMER_H
#define HEAP_TIMER_H

#include <time.h>

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <vector>

typedef std::function<void()> TimeoutCallBack;

struct TimerNode {
    int id;              // 对应的文件描述符 fd
    time_t expires;      // 绝对过期时间点
    TimeoutCallBack cb;  // 任务回调函数
    // 重载比较运算符
    bool operator<(const TimerNode& t) const { return expires < t.expires; }
    bool operator>(const TimerNode& t) const { return expires > t.expires; }
};

class HeapTimer {
   public:
    HeapTimer() { m_heap.reserve(64); }
    ~HeapTimer() { clear(); }

    void add(int id, int timeout, const TimeoutCallBack& cb);  // 添加/更新
    void adjust(int id, int timeout);                          // 续期
    void del(int id);                                          // 物理删除
    void tick();                                               // 检查过期项
    void clear();
    int get_next_tick();  // 获取下次心跳间隔

   private:
    void del_(size_t i);
    void sift_up(size_t i);
    bool sift_down(size_t i, size_t n);
    void swap_node(size_t i, size_t j);

    std::vector<TimerNode> m_heap;
    // 映射 fd 到向量索引 i
    std::unordered_map<int, size_t> m_ref;
};

#endif