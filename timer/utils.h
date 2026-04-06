#ifndef UTILS_H
#define UTILS_H
#include <fcntl.h>       // fcntl, F_GETFL, F_SETFL, O_NONBLOCK
#include <signal.h>      // sigaction, sigfillset
#include <sys/epoll.h>   // epoll_ctl, epoll_event
#include <sys/socket.h>  // send
#include <unistd.h>      // close

#include <cassert>  // assert
#include <cerrno>   // errno
#include <cstring>  // memset, strlen

class Utils {
   public:
    Utils() {}
    ~Utils() {}

    void init(int timeslot);

    // 对文件描述符设置非阻塞
    int setnonblocking(int fd);

    // 将内核事件表注册读事件，ET模式，选择开启EPOLLONESHOT
    void addfd(int epollfd, int fd, bool one_shot, int TRIGMode);

    // 信号处理函数
    static void sig_handler(int sig);  // 捕捉操作系统发来的信号

    // 设置信号函数
    void addsig(int sig, void(handler)(int), bool restart = true);

    void show_error(int connfd, const char* info);

   public:
    static int* u_pipefd;
    static int u_epollfd;  // epoll
    int m_TIMESLOT;
};

#endif