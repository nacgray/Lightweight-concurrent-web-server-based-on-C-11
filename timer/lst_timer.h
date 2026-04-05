#ifndef LST_TIMER
#define LST_TIMER

#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <sys/stat.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/uio.h>

#include <time.h>
#include "../log/log.h"

// util_timer：定义了到期时间、回调函数、前后指针、client_data
// sort_timer_lst：维护双向升序链表
// Utils：系统级通信中枢


class util_timer;

struct client_data
{
    sockaddr_in address;// TCP连接的IP和端口
    int sockfd;
    util_timer *timer;// 指向该连接fd的倒计时器
};

class util_timer
{
public:
    util_timer() : prev(NULL), next(NULL) {}

public:
    time_t expire;//到期时间
    
    void (* cb_func)(client_data *);//回调函数
    client_data *user_data;// 绑定TCP连接
    util_timer *prev;
    util_timer *next;
};

class sort_timer_lst
{
public:
    sort_timer_lst();
    ~sort_timer_lst();

    void add_timer(util_timer *timer);//按时间顺序插进链表
    void adjust_timer(util_timer *timer);//请求活动后，往后面挪
    void del_timer(util_timer *timer);
    void tick();//清扫僵尸连接

private:
    void add_timer(util_timer *timer, util_timer *lst_head);

    util_timer *head;
    util_timer *tail;
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

    //定时处理任务，重新定时以不断触发SIGALRM信号
    void timer_handler();

    void show_error(int connfd, const char *info);

public:
    static int *u_pipefd;
    sort_timer_lst m_timer_lst;
    static int u_epollfd;//epoll
    int m_TIMESLOT;
};

void cb_func(client_data *user_data);

#endif
