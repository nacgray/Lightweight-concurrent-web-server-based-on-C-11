#ifndef LOCKER_H
#define LOCKER_H

#include <pthread.h>
#include <semaphore.h>

#include <exception>

/*
三个类，锁locker、信号量sem、条件变量cond

锁一定适合信号量搭配一起使用的
*/

// 信号量
class sem {
   public:
    sem() {
        if (sem_init(&m_sem, 0, 0) != 0) {
            throw std::exception();
        }
    }
    sem(int num) {
        if (sem_init(&m_sem, 0, num) != 0) {
            throw std::exception();
        }
    }
    ~sem() { sem_destroy(&m_sem); }
    bool wait() { return sem_wait(&m_sem) == 0; }
    bool post() { return sem_post(&m_sem) == 0; }

   private:
    sem_t m_sem;  // POSIX 信号量
};

// 互斥锁
class locker {
   public:
    locker() {
        if (pthread_mutex_init(&m_mutex, NULL) != 0) {
            throw std::exception();
        }
    }
    ~locker() { pthread_mutex_destroy(&m_mutex); }
    // 构造与析构函数，体现了RAII
    // 上锁
    bool lock() {
        return pthread_mutex_lock(&m_mutex) == 0;
        // 向操作系统申请加锁，如果别人拿着锁，调用这个函数的线程就会被内核直接挂起休眠
    }

    // 解锁
    bool unlock() {
        return pthread_mutex_unlock(&m_mutex) == 0;
        // 释放锁，内核会负责叫醒下一个排队的人
    }

    // lock()与unlock()中间叫做临界区，保证同一个任务或者说数据只被一个线程操作
    pthread_mutex_t* get() { return &m_mutex; }

   private:
    pthread_mutex_t m_mutex;  // 互斥锁结构体
};

// 异步日志系统（log/block_queue.h 阻塞队列）中起到了极其核心的作用
// 条件变量
class cond {
   public:
    cond() {
        if (pthread_cond_init(&m_cond, NULL) != 0) {
            // pthread_mutex_destroy(&m_mutex);
            throw std::exception();
        }
    }
    ~cond() { pthread_cond_destroy(&m_cond); }
    bool wait(pthread_mutex_t* m_mutex) {
        int ret = 0;
        // pthread_mutex_lock(&m_mutex);
        ret = pthread_cond_wait(&m_cond, m_mutex);
        // pthread_mutex_unlock(&m_mutex);
        return ret == 0;
    }
    bool timewait(pthread_mutex_t* m_mutex, struct timespec t) {
        int ret = 0;
        // pthread_mutex_lock(&m_mutex);
        ret = pthread_cond_timedwait(&m_cond, m_mutex, &t);
        // pthread_mutex_unlock(&m_mutex);
        return ret == 0;
    }
    bool signal() { return pthread_cond_signal(&m_cond) == 0; }
    bool broadcast() { return pthread_cond_broadcast(&m_cond) == 0; }

   private:
    // static pthread_mutex_t m_mutex;
    pthread_cond_t m_cond;
};
#endif
