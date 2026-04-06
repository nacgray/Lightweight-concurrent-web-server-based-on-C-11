#include "webserver.h"

// 主线程
// 半同步/半反应堆 (Half-Sync/Half-Reactive)

// 构造函数
WebServer::WebServer() {
    // http_conn类对象
    users = new http_conn[MAX_FD];

    // root文件夹路径
    char server_path[200];
    getcwd(server_path, 200);
    char root[6] = "/root";
    m_root = (char*)malloc(strlen(server_path) + strlen(root) + 1);
    strcpy(m_root, server_path);
    strcat(m_root, root);
}

// 析构函数
WebServer::~WebServer() {
    close(m_epollfd);
    close(m_listenfd);
    close(m_pipefd[1]);
    close(m_pipefd[0]);
    delete[] users;
    delete m_pool;
}

void WebServer::init(int port, string user, string passWord,
                     string databaseName, int log_write, int opt_linger,
                     int trigmode, int sql_num, int thread_num, int close_log,
                     int actor_model) {
    m_port = port;                  // 端口号
    m_user = user;                  // 数据库用户名
    m_passWord = passWord;          // 数据库密码
    m_databaseName = databaseName;  // 数据库名
    m_sql_num = sql_num;            // 数据库连接池数量,默认8
    m_thread_num = thread_num;      // thread_num = 8
    m_log_write = log_write;        // 日志写入方式，默认同步
    m_OPT_LINGER = opt_linger;  // 优雅关闭链接，默认不使用，==0不使用
    m_TRIGMode =
        trigmode;  // 触发组合模式,默认listenfd LT + connfd LT？？？？？？
    m_close_log = close_log;  // 关闭日志,默认不关闭
    m_actormodel =
        actor_model;  // 并发模型,默认是proactor，==0是proactor，==1是reactor
}

// 1.日志
void WebServer::log_write() {
    if (0 == m_close_log)  // 判断日志是否关闭
    {
        // 初始化日志
        if (1 == m_log_write)  // 写日志方式 0同步 1异步
            Log::get_instance()->init("./ServerLog", m_close_log, 2000, 800000,
                                      800);
        else
            Log::get_instance()->init("./ServerLog", m_close_log, 2000, 800000,
                                      0);
    }
}

// 2.数据库
void WebServer::sql_pool() {
    // 初始化数据库连接池
    m_connPool = connection_pool::GetInstance();
    m_connPool->init("localhost", m_user, m_passWord, m_databaseName, 3306,
                     m_sql_num, m_close_log);

    // 初始化数据库读取表
    users->initmysql_result(m_connPool);
}

// 3.线程池
void WebServer::thread_pool() {
    // 线程池
    m_pool = new threadpool<http_conn>(m_actormodel, m_connPool, m_thread_num);
}

// 4.触发模式 LT 与 ET
void WebServer::trig_mode() {
    // 0 LT / 1 ET
    // LT + LT
    if (0 == m_TRIGMode) {
        m_LISTENTrigmode = 0;
        m_CONNTrigmode = 0;
    }
    // LT + ET
    else if (1 == m_TRIGMode) {
        m_LISTENTrigmode = 0;
        m_CONNTrigmode = 1;
    }
    // ET + LT
    else if (2 == m_TRIGMode) {
        m_LISTENTrigmode = 1;
        m_CONNTrigmode = 0;
    }
    // ET + ET
    else if (3 == m_TRIGMode) {
        m_LISTENTrigmode = 1;
        m_CONNTrigmode = 1;
    }
}

// 5.监听
void WebServer::eventListen() {
    // 网络编程基础步骤
    m_listenfd = socket(PF_INET, SOCK_STREAM, 0);  // IPv4 TCP
    assert(m_listenfd >= 0);                       // 防御性编程

    // 优雅关闭连接
    if (0 == m_OPT_LINGER) {
        struct linger tmp = {0, 1};
        setsockopt(m_listenfd, SOL_SOCKET, SO_LINGER, &tmp, sizeof(tmp));
    } else if (1 == m_OPT_LINGER) {
        struct linger tmp = {1, 1};
        setsockopt(m_listenfd, SOL_SOCKET, SO_LINGER, &tmp, sizeof(tmp));
    }

    int ret = 0;
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);  // 字节序转换
    address.sin_port = htons(m_port);             // 字节序转换

    int flag = 1;
    setsockopt(m_listenfd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
    ret = bind(m_listenfd, (struct sockaddr*)&address, sizeof(address));  // 绑定
    assert(ret >= 0);
    ret = listen(m_listenfd, 5);
    assert(ret >= 0);

    utils.init(TIMESLOT);

    // epoll创建内核事件表
    epoll_event events[MAX_EVENT_NUMBER];
    m_epollfd = epoll_create(5);
    assert(m_epollfd != -1);

    utils.addfd(m_epollfd, m_listenfd, false, m_LISTENTrigmode);
    http_conn::m_epollfd = m_epollfd;

    ret = socketpair(PF_UNIX, SOCK_STREAM, 0, m_pipefd);
    assert(ret != -1);
    utils.setnonblocking(m_pipefd[1]);  // 仅设置m_pipefd[1]非阻塞？
    utils.addfd(m_epollfd, m_pipefd[0], false, 0);

    utils.addsig(SIGPIPE, SIG_IGN);
    utils.addsig(SIGALRM, utils.sig_handler, false);
    utils.addsig(SIGTERM, utils.sig_handler, false);

    alarm(TIMESLOT);

    // 工具类,信号和描述符基础操作
    Utils::u_pipefd = m_pipefd;
    Utils::u_epollfd = m_epollfd;
}

// 定时器相关函数
void WebServer::timer(int connfd, struct sockaddr_in client_address) {
    users[connfd].init(connfd, client_address, m_root, m_CONNTrigmode,
                       m_close_log, m_user, m_passWord, m_databaseName);

    // 直接 add，传入 lambda 表达式作为回调
    m_timer.add(connfd, 3 * TIMESLOT,
                std::bind(&WebServer::deal_timer, this, connfd));
}
void WebServer::adjust_timer(int fd) {  // 注意：参数直接传 fd 即可
    // 修改后：
    m_timer.adjust(fd, 3 * TIMESLOT);
    LOG_INFO("%s", "adjust timer once");
}
void WebServer::deal_timer(int fd) {
    // 这里的逻辑依然是关闭连接、移除 epoll 监听
    users[fd].close_conn();
    // 注意：堆定时器的 tick() 内部会自动调用这个回调并 del
    // 节点，所以这里不用重复写 del
    LOG_INFO("close fd %d", fd);
}

// 处理客户连接
bool WebServer::dealclientdata() {
    struct sockaddr_in client_address;  // sockaddr_in, ipv4专用地址结构体
    // 包含地址族 AF_INET，端口号（网络字节序表示），IPv4地址结构体
    /*
    Iv4地址结构体struct
    in_addr {
        u_int32_t s_addr;IPv4地址，要用网络字节序表示
    };
    */
    socklen_t client_addrlength = sizeof(client_address);  // socket地址长度
    // m_LISTENTrigmode LT水平触发模式 0关1开
    if (0 == m_LISTENTrigmode) {
        // accept();socket建立连接函数，返回一个全新的fd，connfd，对应建立连接的客户端fd
        int connfd = accept(m_listenfd, (struct sockaddr*)&client_address,
                            &client_addrlength);
        if (connfd < 0)  // connfd一定是>=0的，否则就是建立连接失败
        {
            LOG_ERROR("%s:errno is:%d", "accept error", errno);
            return false;
        }
        if (http_conn::m_user_count >= MAX_FD) {
            utils.show_error(connfd, "Internal server busy");
            LOG_ERROR("%s", "Internal server busy");
            return false;
        }
        // 初始化连接
        timer(connfd, client_address);
        // timer(connfd);
    }

    else {
        while (1) {
            int connfd = accept(m_listenfd, (struct sockaddr*)&client_address,
                                &client_addrlength);  // 接收socket
            if (connfd < 0) {
                LOG_ERROR("%s:errno is:%d", "accept error", errno);
                break;
            }
            if (http_conn::m_user_count >= MAX_FD) {
                utils.show_error(connfd, "Internal server busy");
                LOG_ERROR("%s", "Internal server busy");
                break;
            }
            timer(connfd, client_address);
            // timer(connfd);
        }
        return false;
    }
    return true;
}

bool WebServer::dealwithsignal(bool& timeout, bool& stop_server) {
    int ret = 0;
    int sig;
    char signals[1024];
    ret = recv(m_pipefd[0], signals, sizeof(signals), 0);
    if (ret == -1) {
        return false;
    } else if (ret == 0) {
        return false;
    } else {
        for (int i = 0; i < ret; ++i) {
            switch (signals[i]) {
                case SIGALRM: {
                    timeout = true;
                    break;
                }
                case SIGTERM: {
                    stop_server = true;
                    break;
                }
            }
        }
    }
    return true;
}

void WebServer::dealwithread(int sockfd) {
    if (1 == m_actormodel) {  // Reactor
        adjust_timer(sockfd);
        m_pool->append(users + sockfd, 0);
    } else {  // Proactor
        if (users[sockfd].read_once()) {
            LOG_INFO("deal with the client(%s)",
                     inet_ntoa(users[sockfd].get_address()->sin_addr));
            m_pool->append_p(users + sockfd);
            adjust_timer(sockfd);
        } else {
            deal_timer(sockfd);
        }
    }
}
void WebServer::dealwithwrite(int sockfd) {
    if (1 == m_actormodel) {  // Reactor
        adjust_timer(sockfd);
        m_pool->append(users + sockfd, 1);
    } else {  // Proactor
        if (users[sockfd].write()) {
            LOG_INFO("send data to the client(%s)",
                     inet_ntoa(users[sockfd].get_address()->sin_addr));
            adjust_timer(sockfd);
        } else {
            deal_timer(sockfd);
        }
    }
}

void WebServer::eventLoop() {
    bool timeout = false;
    bool stop_server = false;

    while (!stop_server) {
        int number = epoll_wait(m_epollfd, events, MAX_EVENT_NUMBER, -1);
        if (number < 0 && errno != EINTR) {
            LOG_ERROR("%s", "epoll failure");
            break;
        }

        for (int i = 0; i < number; i++) {
            int sockfd = events[i].data.fd;

            if (sockfd == m_listenfd) {
                if (!dealclientdata()) continue;
            } else if (events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
                deal_timer(sockfd);  // 链路断开，直接关闭
            } else if ((sockfd == m_pipefd[0]) &&
                       (events[i].events & EPOLLIN)) {
                if (!dealwithsignal(timeout, stop_server))
                    LOG_ERROR("%s", "signal handle failure");
            } else if (events[i].events & EPOLLIN) {
                dealwithread(sockfd);
            } else if (events[i].events & EPOLLOUT) {
                dealwithwrite(sockfd);
            }
        }
        if (timeout) {
            m_timer.tick();  // 触发堆定时器检查
            alarm(
                TIMESLOT);  // Linux的alarm定时器是“一次性”的，一定在使用完后要再次调用
            LOG_INFO("%s", "timer tick");
            timeout = false;
        }
    }
}
