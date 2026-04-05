#include "heaptimer.h"
#include <cstdio>
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
    if (m_ref.count(id)) { // 已存在则调整
        size_t i = m_ref[id];
        m_heap[i].expires = time(NULL) + timeout;
        m_heap[i].cb = cb;
        if (!sift_down(i, m_heap.size())) sift_up(i);
    } else { // 新增
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
    // printf("1\n");
    while (!m_heap.empty()) {
        TimerNode node = m_heap.front();
        if (node.expires > time(NULL)) break;
        // 【新增监视线】
        // printf("[Timer Radar] Detected expired fd: %d, executing callback...\n", node.id);
        node.cb(); // 执行关闭连接的回调
        del(node.id);
    }
}

void HeapTimer::clear() {
    m_ref.clear();
    m_heap.clear();
}


void Utils::init(int timeslot)
{
    m_TIMESLOT = timeslot;
}

//对文件描述符设置非阻塞
//调用Linux底层函数fcntl()函数，将fd的原本属性取出，强行换成O_NONBLOCK
int Utils::setnonblocking(int fd)
{
    int old_option = fcntl(fd, F_GETFL);
    int new_option = old_option | O_NONBLOCK;
    fcntl(fd, F_SETFL, new_option);
    return old_option;
}

//将内核事件表注册读事件，ET模式，选择开启EPOLLONESHOT
void Utils::addfd(int epollfd, int fd, bool one_shot, int TRIGMode)
{
    epoll_event event;
    event.data.fd = fd;

    if (1 == TRIGMode)
        event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    else
        event.events = EPOLLIN | EPOLLRDHUP;

    if (one_shot)
        event.events |= EPOLLONESHOT;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &event);
    setnonblocking(fd);//只要fd注册进了epoll，就一定是非阻塞的
}

//信号处理函数
void Utils::sig_handler(int sig)
{
    //为保证函数的可重入性，保留原来的errno
    int save_errno = errno;
    int msg = sig;
    send(u_pipefd[1], (char *)&msg, 1, 0);
    errno = save_errno;
}

//设置信号函数
void Utils::addsig(int sig, void(handler)(int), bool restart)
{
    struct sigaction sa;
    memset(&sa, '\0', sizeof(sa));
    sa.sa_handler = handler;
    if (restart)
        sa.sa_flags |= SA_RESTART;
    sigfillset(&sa.sa_mask);
    assert(sigaction(sig, &sa, NULL) != -1);
}



void Utils::show_error(int connfd, const char *info)
{
    send(connfd, info, strlen(info), 0);
    close(connfd);
}

int *Utils::u_pipefd = 0;
int Utils::u_epollfd = 0;
