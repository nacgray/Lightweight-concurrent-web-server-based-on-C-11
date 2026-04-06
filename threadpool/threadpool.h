#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <pthread.h>

#include <cstdio>
#include <exception>
#include <list>

#include "../CGImysql/sql_connection_pool.h"
#include "../lock/locker.h"

// 线程池类，8个工作线程
// pthread_t *m_threads; 工作线程

// 拉模式：工作线程主动获取任务
template <typename T>
class threadpool {
   public:
    // thread_number是线程池中线程的数量，设置thread_number = 8，即8个工作线程
    // max_requests是请求队列中最多允许的、等待处理的请求的数量max_request =
    // 10000
    threadpool(int actor_model, connection_pool* connPool,
               int thread_number = 8, int max_request = 10000);
    ~threadpool();

    // 添加任务到工作队列
    bool append(T* request, int state);
    bool append_p(T* request);

   private:
    // 工作线程运行的函数，它不断从工作队列中取出任务并执行之
    static void* worker(void* arg);
    // 因为底层 pthread_create 拒收带有 this 指针的 C++ 成员函数。
    // 我们被迫造了一个没有灵魂的临时工（static worker）。
    // 然后通过极其精妙的第四个参数 arg，把老板自己（this）偷渡进去，再强转回
    // threadpool *，最终成功唤醒真正的核心逻辑 pool->run()。
    void run();

   private:
    int m_thread_number;  // 线程池中的线程数
    int m_max_requests;  // 请求队列中允许的最大请求数，可防止黑客网络攻击
    pthread_t* m_threads;  // 描述线程池的数组，其大小为m_thread_number，底层就是用
                           // new pthread_t[8]

    std::list<T*>
        m_workqueue;  // 半同步/半反应堆 (Half-Sync/Half-Reactive)-2.请求队列
    // 主线程把任务对象（request）通过 append 或 append_p
    // 塞进这个队列，解耦了主线程和工作线程。

    locker m_queuelocker;  // 互斥锁 保护请求队列的互斥锁，钥匙
    sem m_queuestat;       // 信号量 是否有任务需要处理
    connection_pool* m_connPool;  // 数据库
    int m_actor_model;  // 模型切换，0 代表 Proactor 模式，1 代表 Reactor 模式
};

// 线程池构造函数
template <typename T>
threadpool<T>::threadpool(int actor_model, connection_pool* connPool,
                          int thread_number, int max_requests)
    : m_actor_model(actor_model),
      m_thread_number(thread_number),
      m_max_requests(max_requests),
      m_threads(NULL),
      m_connPool(connPool) {
    if (thread_number <= 0 || max_requests <= 0) throw std::exception();
    m_threads = new pthread_t[m_thread_number];
    if (!m_threads) throw std::exception();

    // 创建工作线程
    for (int i = 0; i < thread_number; ++i) {
        if (pthread_create(m_threads + i, NULL, worker, this) != 0) {
            delete[] m_threads;
            throw std::exception();
        }
        if (pthread_detach(m_threads[i])) {
            delete[] m_threads;
            throw std::exception();
        }
    }
}

// 线程池析构函数
template <typename T>
threadpool<T>::~threadpool() {
    delete[] m_threads;
}

// 添加任务到工作队列
// Reactor模式下的append
template <typename T>
bool threadpool<T>::append(T* request, int state)  // state 0读 1写
{
    m_queuelocker.lock();  // 醒了立刻抢锁！(互斥锁)
    if (m_workqueue.size() >= m_max_requests) {
        m_queuelocker.unlock();
        return false;
    }
    request->m_state = state;
    m_workqueue.push_back(request);  // 将请求压入工作队列
    m_queuelocker.unlock();          // 解锁，中间为临界区
    m_queuestat.post();
    return true;
}

// Proactor模式下的append
template <typename T>
bool threadpool<T>::append_p(T* request) {
    m_queuelocker.lock();  // 上锁
    if (m_workqueue.size() >= m_max_requests) {
        m_queuelocker.unlock();
        return false;
    }
    m_workqueue.push_back(request);
    m_queuelocker.unlock();  // 解锁
    m_queuestat.post();
    return true;
}

template <typename T>
void* threadpool<T>::worker(void* arg) {
    threadpool* pool = (threadpool*)arg;
    pool->run();
    return pool;
}

// 半同步/半反应堆 (Half-Sync/Half-Reactive)-3.同步层（打工人 / 工作线程池）
template <typename T>
void threadpool<T>::run() {
    while (true) {
        m_queuestat.wait();    // 1.如果没有任务，工作线程休眠在请求队列上
        m_queuelocker.lock();  // 2.任务来临，上锁
        if (m_workqueue.empty()) {
            m_queuelocker.unlock();  // 任务队列空
            continue;
        }
        T* request = m_workqueue.front();  // 3.抢夺任务
        m_workqueue.pop_front();           // 4.pop出被抢的任务
        m_queuelocker.unlock();            // 5.解锁，否则线程将停摆
        // process()为6.
        if (!request) continue;
        if (1 == m_actor_model) {
            // Reactor第四步：工作线程被唤醒，读取数据，处理请求，注册写就绪事件
            if (0 == request->m_state)  // 0读
            {
                if (request->read_once())  // 工作线程去读网卡数据
                {
                    request->improv = 1;
                    connectionRAII mysqlcon(
                        &request->mysql,
                        m_connPool);  // 向数据库连接池申请一个可用的连接
                    // 依旧是第四步内容，执行 HTTP 解析
                    //  拿到任务后，同步执行极其耗时的业务逻辑
                    request->process();  // 去执行 HTTP 解析
                } else {
                    request->improv = 1;
                    request->timer_flag = 1;
                }
            }
            // Reactor第七步：工作线程被唤醒，往 socket 上写入处理结果
            else  // 1写
            {
                if (request->write()) {
                    request->improv = 1;
                } else {
                    request->improv = 1;
                    request->timer_flag = 1;
                }
            }
        } else {
            connectionRAII mysqlcon(
                &request->mysql,
                m_connPool);  // 向数据库连接池申请一个可用的连接
            // 3.工作线程享受现成数据，直接处理
            request->process();
        }
    }
}
#endif
