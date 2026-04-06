#include "heaptimer.h"

void HeapTimer::swap_node(size_t i, size_t j) {
    std::swap(m_heap[i], m_heap[j]);
    m_ref[m_heap[i].id] = i;
    m_ref[m_heap[j].id] = j;
}

void HeapTimer::sift_up(size_t i) {
    while (i > 0) {
        size_t parent = (i - 1) / 2;
        if (m_heap[parent] < m_heap[i]) break;
        swap_node(i, parent);
        i = parent;
    }
}

bool HeapTimer::sift_down(size_t i, size_t n) {
    size_t index = i;
    size_t child = i * 2 + 1;
    while (child < n) {
        if (child + 1 < n && m_heap[child + 1] < m_heap[child]) child++;
        if (m_heap[child] > m_heap[index]) break;
        swap_node(index, child);
        index = child;
        child = index * 2 + 1;
    }
    return index > i;
}

void HeapTimer::add(int id, int timeout, const TimeoutCallBack& cb) {
    if (m_ref.count(id)) {  // 已存在则调整
        size_t i = m_ref[id];
        m_heap[i].expires = time(NULL) + timeout;
        m_heap[i].cb = cb;
        if (!sift_down(i, m_heap.size())) sift_up(i);
    } else {  // 新增
        size_t i = m_heap.size();
        m_ref[id] = i;
        m_heap.push_back({id, time(NULL) + timeout, cb});
        sift_up(i);
    }
}

void HeapTimer::adjust(int id, int timeout) {
    size_t i = m_ref[id];
    m_heap[i].expires = time(NULL) + timeout;
    sift_down(i, m_heap.size());
}

void HeapTimer::del(int id) {
    if (m_heap.empty() || !m_ref.count(id)) return;
    del_(m_ref[id]);
}

void HeapTimer::del_(size_t i) {
    size_t last = m_heap.size() - 1;
    if (i < last) {
        swap_node(i, last);
        if (!sift_down(i, last)) sift_up(i);
    }
    m_ref.erase(m_heap.back().id);
    m_heap.pop_back();
}

void HeapTimer::tick() {
    while (!m_heap.empty()) {
        TimerNode node = m_heap.front();
        if (node.expires > time(NULL)) break;
        node.cb();  // 执行关闭连接的回调
        del(node.id);
    }
}

void HeapTimer::clear() {
    m_ref.clear();
    m_heap.clear();
}
