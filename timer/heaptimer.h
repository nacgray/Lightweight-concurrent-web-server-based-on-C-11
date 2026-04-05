#ifndef HEAP_TIMER_H
#define HEAP_TIMER_H

#include <vector>
#include <unordered_map>
#include <time.h>
#include <algorithm>
#include <functional>
#include <sys/epoll.h>    // epoll_ctl, epoll_event
#include <fcntl.h>        // fcntl, F_GETFL, F_SETFL, O_NONBLOCK
#include <unistd.h>       // close
#include <sys/socket.h>   // send
#include <signal.h>       // sigaction, sigfillset
#include <cstring>        // memset, strlen
#include <cerrno>         // errno
#include <cassert>        // assert
typedef std::function<void()> TimeoutCallBack;

struct TimerNode {
    int id;                // 对应的文件描述符 fd
    time_t expires;        // 绝对过期时间点
    TimeoutCallBack cb;    // 任务回调函数
    // 重载比较运算符
    bool operator<(const TimerNode& t) const { return expires < t.expires; }
    bool operator>(const TimerNode& t) const { return expires > t.expires; }
};

class HeapTimer {
public:
    HeapTimer() { m_heap.reserve(64); }
    ~HeapTimer() { clear(); }

    void add(int id, int timeout, const TimeoutCallBack& cb); // 添加/更新
    void adjust(int id, int timeout);                        // 续期
    void del(int id);                                         // 物理删除
    void tick();                                              // 检查过期项
    void clear();
    int get_next_tick();                                      // 获取下次心跳间隔

private:
    void del_(size_t i);
    void sift_up(size_t i);
    bool sift_down(size_t i, size_t n);
    void swap_node(size_t i, size_t j);

    std::vector<TimerNode> m_heap;
    // 映射 fd 到向量索引 i
    std::unordered_map<int, size_t> m_ref; 
};

class Utils
{
public:
    Utils() {}
    ~Utils() {}

    void init(int timeslot);

    //对文件描述符设置非阻塞
    int setnonblocking(int fd);

    //将内核事件表注册读事件，ET模式，选择开启EPOLLONESHOT
    void addfd(int epollfd, int fd, bool one_shot, int TRIGMode);

    //信号处理函数
    static void sig_handler(int sig);// 捕捉操作系统发来的信号

    //设置信号函数
    void addsig(int sig, void(handler)(int), bool restart = true);

    void show_error(int connfd, const char *info);

public:
    static int *u_pipefd;
    // sort_timer_lst m_timer_lst;
    static int u_epollfd;//epoll
    int m_TIMESLOT;
};

#endif