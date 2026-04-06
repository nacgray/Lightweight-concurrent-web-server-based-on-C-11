#include "http_conn.h"
#include <mysql/mysql.h>

#include <fstream>

// 定义http响应的一些状态信息
const char* ok_200_title = "OK";
const char* error_400_title = "Bad Request";
const char* error_400_form =
    "Your request has bad syntax or is inherently impossible to staisfy.\n";
const char* error_403_title = "Forbidden";
const char* error_403_form =
    "You do not have permission to get file form this server.\n";
const char* error_404_title = "Not Found";
const char* error_404_form =
    "The requested file was not found on this server.\n";
const char* error_500_title = "Internal Error";
const char* error_500_form =
    "There was an unusual problem serving the request file.\n";

int http_conn::m_user_count = 0;
int http_conn::m_epollfd = -1;

locker m_lock;
map<string, string> users;  // 内存缓存（Cache）存放的用户名和密码

// 数据库
void http_conn::initmysql_result(connection_pool* connPool) {
    // 先从连接池中取一个连接
    MYSQL* mysql = NULL;
    connectionRAII mysqlcon(&mysql, connPool);

    // 在user表中检索username，passwd数据，浏览器端输入
    if (mysql_query(mysql, "SELECT username,passwd FROM user")) {
        LOG_ERROR("SELECT error:%s\n", mysql_error(mysql));
    }

    // 从表中检索完整的结果集
    MYSQL_RES* result = mysql_store_result(mysql);

    // 返回结果集中的列数
    int num_fields = mysql_num_fields(result);

    // 返回所有字段结构的数组
    MYSQL_FIELD* fields = mysql_fetch_fields(result);

    // 从结果集中获取下一行，将对应的用户名和密码，存入map中
    while (MYSQL_ROW row = mysql_fetch_row(result)) {
        string temp1(row[0]);
        string temp2(row[1]);
        users[temp1] = temp2;
    }
}

// 对文件描述符设置非阻塞
int setnonblocking(int fd) {
    int old_option = fcntl(fd, F_GETFL);
    int new_option = old_option | O_NONBLOCK;
    fcntl(fd, F_SETFL, new_option);
    return old_option;
}

// 将内核事件表注册读事件，ET模式，选择开启EPOLLONESHOT
// Reactor第一步：主线程往 epoll 内核事件表中注册 socket 上的读就绪事件
void addfd(int epollfd, int fd, bool one_shot, int TRIGMode) {
    epoll_event event;
    event.data.fd = fd;

    if (1 == TRIGMode)
        event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    else
        event.events = EPOLLIN | EPOLLRDHUP;

    if (one_shot) event.events |= EPOLLONESHOT;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &event);
    setnonblocking(fd);  // 将fd设置为SOCK_NONBLOCK
}

// 从内核时间表删除描述符
void removefd(int epollfd, int fd) {
    epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, 0);
    close(fd);
}

// 关闭连接，关闭一个连接，客户总量减一
void http_conn::close_conn(bool real_close) {
    if (real_close && (m_sockfd != -1)) {
        printf("close %d\n", m_sockfd);
        removefd(m_epollfd, m_sockfd);
        m_sockfd = -1;
        m_user_count--;
    }
}

// 阶段一：建立与初始化。初始化连接,外部调用初始化套接字地址
// 用户在客户端输入网址（./server启动项目后），发起TCP握手，调用accpet()建立连接
// 随后调用http_conn::init()初始化连接，为用户分配一个http_conn对象
void http_conn::init(int sockfd, const sockaddr_in& addr, char* root,
                     int TRIGMode, int close_log, string user, string passwd,
                     string sqlname) {
    m_sockfd = sockfd;
    m_address = addr;

    // 把客人的 sockfd 注册进 Epoll 管家的监听名单，并开启
    // EPOLLONESHOT（防并发冲突）。
    addfd(m_epollfd, sockfd, true, m_TRIGMode);
    m_user_count++;

    // 当浏览器出现连接重置时，可能是网站根目录出错或http响应格式出错或者访问的文件中内容完全为空
    doc_root = root;
    m_TRIGMode = TRIGMode;
    m_close_log = close_log;

    strcpy(sql_user, user.c_str());
    strcpy(sql_passwd, passwd.c_str());
    strcpy(sql_name, sqlname.c_str());

    init();  // 调用下方init()，应对HTTP Keep-Alive
}

// 初始化新接受的连接，无参数
// check_state默认为分析请求行状态
void http_conn::init() {
    // 无参的 init()
    // 操纵的是频繁变化的请求状态，避免了为一个长连接上的多次请求反复去 new 和
    // delete 内存块
    // 下一次同一个请求来临，发来的报文里带有 Connection:
    // keep-alive，不能调用带参数的init(),会关闭sockfd，将状态机状态拨回
    // CHECK_STATE_REQUESTLINE，清空读写缓冲区数据即可
    mysql = NULL;
    bytes_to_send = 0;
    bytes_have_send = 0;
    // 把主状态机拨回零点：m_check_state = CHECK_STATE_REQUESTLINE。
    m_check_state = CHECK_STATE_REQUESTLINE;
    m_linger = false;
    m_method = GET;
    m_url = 0;
    m_version = 0;
    m_content_length = 0;
    m_host = 0;
    m_start_line = 0;
    m_checked_idx = 0;
    m_read_idx = 0;
    m_write_idx = 0;
    cgi = 0;
    m_state = 0;
    timer_flag = 0;
    improv = 0;

    // 清空读/写缓冲区。
    memset(m_read_buf, '\0', READ_BUFFER_SIZE);
    memset(m_write_buf, '\0', WRITE_BUFFER_SIZE);
    memset(m_real_file, '\0', FILENAME_LEN);
}

// 循环读取客户数据，直到无数据可读或对方关闭连接
// 非阻塞ET工作模式下，需要一次性将数据读完
// Proactor模式下

// 第一步：当 epoll
// 提示网卡有数据时，不管是Proactor，还是Reactor，都会执行这个函数
// 阶段二：读取请求报文。
//  客户端把包含 GET /index.html... 的 HTTP 报文通过网卡发了过来，触发 Epoll 的
//  EPOLLIN 读事件
bool http_conn::read_once() {
    if (m_read_idx >= READ_BUFFER_SIZE)  // 文本数量超过读缓冲区大小，不允许！
    {
        return false;
    }
    int bytes_read = 0;

    // LT读取数据
    if (0 == m_TRIGMode) {
        // LT模式下不需要死循环，因为只要网卡数据没有读完，epoll_wait()出发时就会过来读数据
        // recv()读网卡数据,byte_read是这一次从网卡里读了多少字节数据
        bytes_read = recv(m_sockfd, m_read_buf + m_read_idx,
                          READ_BUFFER_SIZE - m_read_idx, 0);
        m_read_idx += bytes_read;

        if (bytes_read <= 0) {
            return false;
        }

        return true;
    }
    // ET读数据
    else {
        // ET模式下，因为就警告一次，所以要保证一次读完，加入while死循环
        while (true) {
            bytes_read = recv(m_sockfd, m_read_buf + m_read_idx,
                              READ_BUFFER_SIZE - m_read_idx, 0);
            // sockfd在init()调用addfd的时候，被修改成了非阻塞(O_NONBLOCK)
            // 当网卡被抽干时，非阻塞的 recv() 不会傻等，而是立刻返回
            // -1，并甩给经理一个 EAGAIN 错误码。
            // 这时候就表示已经读完数据，可以退出死循环了
            if (bytes_read == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                return false;
            } else if (bytes_read == 0) {
                return false;
            }
            m_read_idx += bytes_read;
        }
        return true;
    }
}

// 将事件重置为EPOLLONESHOT
void modfd(int epollfd, int fd, int ev, int TRIGMode) {
    epoll_event event;
    event.data.fd = fd;

    if (1 == TRIGMode)
        event.events = ev | EPOLLET | EPOLLONESHOT | EPOLLRDHUP;
    else
        event.events = ev | EPOLLONESHOT | EPOLLRDHUP;

    epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &event);
}
// 阶段六1
void http_conn::unmap() {
    if (m_file_address) {
        munmap(m_file_address, m_file_stat.st_size);
        // 发送完成，立刻调用 munmap 把这块内存还给操作系统，防止内存泄露
        m_file_address = 0;
    }
}
// 阶段六：回复响应与客户端接收
bool http_conn::write() {
    int temp = 0;

    if (bytes_to_send == 0) {
        modfd(m_epollfd, m_sockfd, EPOLLIN, m_TRIGMode);
        init();
        return true;
    }

    while (1) {
        // 响应头（m_iv[0]）和 网页/图片数据（m_iv[1]）
        temp = writev(m_sockfd, m_iv, m_iv_count);

        if (temp < 0) {
            if (errno == EAGAIN) {
                modfd(m_epollfd, m_sockfd, EPOLLOUT, m_TRIGMode);
                return true;
            }
            unmap();
            return false;
        }

        // 如果一次没发完，或者只发了一半，下次接着发的时候，绝不能从头重发
        bytes_have_send += temp;
        bytes_to_send -= temp;
        // if-else计算发到哪儿了
        //  情况A：响应头（m_iv[0]）已经全部发完了！现在剩网页内容（m_iv[1]）还没发完。
        if (bytes_have_send >= m_iv[0].iov_len) {
            m_iv[0].iov_len = 0;  // 响应头长度清零，下次不用发了
            // 精准偏移网页数据的指针
            m_iv[1].iov_base = m_file_address + (bytes_have_send - m_write_idx);
            m_iv[1].iov_len = bytes_to_send;
        }
        // 情况B：响应头（m_iv[0]）都没发完，网卡就满了。
        else {
            m_iv[0].iov_base = m_write_buf + bytes_have_send;
            m_iv[0].iov_len = m_iv[0].iov_len - bytes_have_send;
        }

        // 全部发送完毕
        if (bytes_to_send <= 0) {
            unmap();
            // 如果所有数据（网页图片、HTML等）都发送完毕了，事件重新改回
            // EPOLLIN
            modfd(m_epollfd, m_sockfd, EPOLLIN, m_TRIGMode);

            if (m_linger)  // 长连接，继续调用init，初始化数据，清空读写缓冲区等
            {
                init();
                return true;
            } else {
                return false;
            }
        }
    }
}

// 回复报文
bool http_conn::add_response(const char* format, ...) {
    if (m_write_idx >= WRITE_BUFFER_SIZE) return false;
    va_list arg_list;
    va_start(arg_list, format);
    int len = vsnprintf(m_write_buf + m_write_idx,
                        WRITE_BUFFER_SIZE - 1 - m_write_idx, format, arg_list);
    if (len >= (WRITE_BUFFER_SIZE - 1 - m_write_idx)) {
        va_end(arg_list);
        return false;
    }
    m_write_idx += len;
    va_end(arg_list);

    LOG_INFO("request:%s", m_write_buf);  // 回复报文，写入日志系统

    return true;
}
bool http_conn::add_content_type() {
    return add_response("Content-Type:%s\r\n", "text/html");
}
// 阶段五1：状态行
bool http_conn::add_status_line(int status, const char* title) {
    return add_response("%s %d %s\r\n", "HTTP/1.1", status, title);
}
// 阶段五2.1：告诉浏览器后面有几个字节的数据
bool http_conn::add_content_length(int content_len) {
    return add_response("Content-Length:%d\r\n", content_len);
}
// 阶段五2.2：告诉浏览器等会儿要不要断开连接
bool http_conn::add_linger() {
    return add_response("Connection:%s\r\n",
                        (m_linger == true) ? "keep-alive" : "close");
}
// 阶段五2.3：单独打印一个 \r\n。HTTP协议硬性要求，
bool http_conn::add_blank_line() { return add_response("%s", "\r\n"); }
// 阶段五2：内部打包调用 add_content_length  和 add_linger
bool http_conn::add_headers(int content_len) {
    return add_content_length(content_len) && add_linger() && add_blank_line();
}
// 阶段五3：
bool http_conn::add_content(const char* content) {
    return add_response("%s", content);
}

// 阶段五：服务端生成响应报文
bool http_conn::process_write(HTTP_CODE ret) {
    switch (ret) {
        case INTERNAL_ERROR: {
            add_status_line(500, error_500_title);
            add_headers(strlen(error_500_form));
            if (!add_content(error_500_form)) return false;
            break;
        }
        case BAD_REQUEST: {
            add_status_line(404, error_404_title);
            add_headers(strlen(error_404_form));
            if (!add_content(error_404_form)) return false;
            break;
        }
        case FORBIDDEN_REQUEST: {
            add_status_line(403, error_403_title);
            add_headers(strlen(error_403_form));
            if (!add_content(error_403_form)) return false;
            break;
        }
        case FILE_REQUEST: {
            add_status_line(200, ok_200_title);
            if (m_file_stat.st_size != 0) {
                add_headers(m_file_stat.st_size);
                m_iv[0].iov_base = m_write_buf;
                m_iv[0].iov_len = m_write_idx;
                m_iv[1].iov_base = m_file_address;
                m_iv[1].iov_len = m_file_stat.st_size;
                m_iv_count = 2;
                bytes_to_send = m_write_idx + m_file_stat.st_size;
                return true;
            } else {
                const char* ok_string = "<html><body></body></html>";
                add_headers(strlen(ok_string));
                if (!add_content(ok_string)) return false;
            }
        }
        default:
            return false;
    }
    m_iv[0].iov_base = m_write_buf;
    m_iv[0].iov_len = m_write_idx;
    m_iv_count = 1;
    bytes_to_send = m_write_idx;
    return true;
}

http_conn::HTTP_CODE http_conn::do_request() {
    strcpy(m_real_file, doc_root);
    int len = strlen(doc_root);
    const char* p = strrchr(m_url, '/');

    // ==========================================
    // 动态业务 API 路由：判断是否请求了 login.cgi 或 register.cgi
    // ==========================================
    if (cgi == 1 && (strcmp(p + 1, "login.cgi") == 0 ||
                     strcmp(p + 1, "register.cgi") == 0)) {
        // 提取账号密码的逻辑保持不变...
        char name[100], password[100];
        int i;
        for (i = 5; m_string[i] != '&'; ++i) name[i - 5] = m_string[i];
        name[i - 5] = '\0';

        int j = 0;
        for (i = i + 10; m_string[i] != '\0'; ++i, ++j)
            password[j] = m_string[i];
        password[j] = '\0';

        std::string hashed_pwd = get_salted_hash(name, password);

        if (strcmp(p + 1, "register.cgi") == 0) {
            char* sql_insert = (char*)malloc(sizeof(char) * 200);
            strcpy(sql_insert, "INSERT INTO user(username, passwd) VALUES(");
            strcat(sql_insert, "'");
            strcat(sql_insert, name);
            strcat(sql_insert, "', '");
            // 将存入数据库的明文 password 替换为 hashed_pwd.c_str()
            strcat(sql_insert, hashed_pwd.c_str());
            strcat(sql_insert, "')");

            if (users.find(name) == users.end()) {
                m_lock.lock();
                int res = mysql_query(mysql, sql_insert);
                // 内存字典里存的也是 Hash 值
                users.insert(pair<string, string>(name, hashed_pwd));
                m_lock.unlock();
                if (!res)
                    strcpy(m_url, "/log.html");
                else
                    strcpy(m_url, "/registerError.html");
            } else
                strcpy(m_url, "/registerError.html");

            free(sql_insert);
        } else if (strcmp(p + 1, "login.cgi") == 0) {
            // 将用户的明文密码加密后，去和数据库/内存里存的 Hash 值进行比对
            if (users.find(name) != users.end() && users[name] == hashed_pwd)
                strcpy(m_url, "/welcome.html");
            else
                strcpy(m_url, "/logError.html");
        }
    }

    // 静态资源路由（前面已经清理干净，直接拼接下发）
    strncpy(m_real_file + len, m_url, FILENAME_LEN - len - 1);

    if (stat(m_real_file, &m_file_stat) < 0) return NO_RESOURCE;
    if (!(m_file_stat.st_mode & S_IROTH)) return FORBIDDEN_REQUEST;
    if (S_ISDIR(m_file_stat.st_mode)) return BAD_REQUEST;

    int fd = open(m_real_file, O_RDONLY);
    m_file_address =
        (char*)mmap(0, m_file_stat.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return FILE_REQUEST;
}
// 阶段三2.1：parse_line()是从状态机去解析请求报文，找\r\n,切下干净一行后替换为\0\0
// 返回值为行的读取状态，有LINE_OK,LINE_BAD,LINE_OPEN
http_conn::LINE_STATUS http_conn::parse_line() {
    char temp;
    for (; m_checked_idx < m_read_idx; ++m_checked_idx) {
        // 一个字符一个字符的解析
        temp =
            m_read_buf[m_checked_idx];  // recv()读取网卡数据后存入m_read_buf内
        if (temp == '\r') {
            if ((m_checked_idx + 1) == m_read_idx)
                return LINE_OPEN;  // 还没有切到\n,数据没收全
            else if (m_read_buf[m_checked_idx + 1] == '\n') {
                m_read_buf[m_checked_idx++] = '\0';
                m_read_buf[m_checked_idx++] = '\0';
                return LINE_OK;  // 切到\r\n
            }
            return LINE_BAD;  // 切到了\r，但是后面还有数据，且不是\n，不符合HTTP请求报文
        } else if (temp == '\n') {
            if (m_checked_idx > 1 && m_read_buf[m_checked_idx - 1] == '\r') {
                m_read_buf[m_checked_idx - 1] = '\0';
                m_read_buf[m_checked_idx++] = '\0';
                return LINE_OK;  // 切到了\r\n
            }
            return LINE_BAD;  // 有\n但前者不是\r，不符合HTTP请求报文
        }
    }
    return LINE_OPEN;  // 没有切到\r\n，报文不完整
}

// 阶段三2.2：get_line()获取当前解析行首地址，定义在http_conn.h中

// 阶段三2.3：解析http请求行，获得请求方法，目标url及http版本号
http_conn::HTTP_CODE http_conn::parse_request_line(char* text) {
    // 请求行含有三个部分，method、url、version，parse_request_line()就是根据这三部分去解析
    m_url = strpbrk(
        text,
        " \t");  // strpbrk()在参数1(string)里找参数2(char)，text里找\t（空格或者tab），找到则返回指针
    if (!m_url)  // 没找到，m_url = nullptr
    {
        return BAD_REQUEST;  // 语法错误
    }
    *m_url++ = '\0';  // 把那个空格\t强行篡改成了字符串结束符\0,*m_url++先改后移
    char* method = text;  // 此时text就是GET\0a.jpg……
    if (strcasecmp(method, "GET") == 0)
        m_method = GET;
    else if (strcasecmp(method, "POST") == 0) {
        m_method = POST;
        cgi = 1;  // 开启动态处理模式
        // 不仅要把报文最后的账号密码抠出来，还要跟数据库进行交互验证，
        // 最后根据验证的对错，决定返回 /welcome.html（成功）还是
        // /logError.html（失败）
    } else
        return BAD_REQUEST;  // 语法错误
    m_url += strspn(
        m_url,
        " \t");  // strspn()计算参数1开头连续包含参数2中字符的长度并且跳过
    m_version = strpbrk(m_url, " \t");
    if (!m_version) return BAD_REQUEST;
    *m_version++ = '\0';
    m_version += strspn(m_version, " \t");
    // 协议解析
    if (strcasecmp(m_version, "HTTP/1.1") !=
        0)  // strcasecmp()比较两个字符串是否相等，忽略大小写，返回0代表相同
        return BAD_REQUEST;
    if (strncasecmp(m_url, "http://", 7) == 0) {
        m_url +=
            7;  // 跳过 http://
                // 直接定位后面的'/'因为'/'后面的才是有用的，"http://127.0.0.1/index.html"，跳过http://127.0.0.1
        m_url =
            strchr(m_url, '/');  // strchr()在字符串中寻找某一个特定的单个字符
    }

    if (strncasecmp(m_url, "https://", 8) == 0) {
        m_url += 8;
        m_url = strchr(m_url, '/');
    }

    if (!m_url || m_url[0] != '/') return BAD_REQUEST;
    // 当url为/时，显示判断界面
    if (strlen(m_url) ==
        1)  // 缺省，比如只有127.0.0.1/，则在后面加上judge.html登录界面
        strcat(m_url, "index.html");  // strcat()把参数2拼接到参数1的末尾
    m_check_state = CHECK_STATE_HEADER;  // 接下来解析请求头
    return NO_REQUEST;                   // 还没有解析完
}

// 阶段三2.4：解析http请求的一个头部信息
// 解析细节与2.3则大相径庭
http_conn::HTTP_CODE http_conn::parse_headers(char* text) {
    if (text[0] == '\0') {
        if (m_content_length != 0) {
            m_check_state = CHECK_STATE_CONTENT;
            return NO_REQUEST;
        }
        return GET_REQUEST;  // 没有请求头，直接请求成功
    } else if (strncasecmp(text, "Connection:", 11) == 0) {
        text += 11;
        text += strspn(text, " \t");
        if (strcasecmp(text, "keep-alive") == 0)  // 判断是否长连接keep-alive
        {
            m_linger = true;
        }
    } else if (strncasecmp(text, "Content-length:", 15) == 0) {
        text += 15;
        text += strspn(text, " \t");
        m_content_length = atol(text);
    } else if (strncasecmp(text, "Host:", 5) == 0) {
        text += 5;
        text += strspn(text, " \t");
        m_host = text;
    } else {
        LOG_INFO("oop!unknow header: %s", text);
    }
    return NO_REQUEST;
}

// 阶段三2.5判断http请求是否被完整读入
// 与2.3解析大相径庭
http_conn::HTTP_CODE http_conn::parse_content(char* text) {
    if (m_read_idx >= (m_content_length + m_checked_idx)) {
        text[m_content_length] = '\0';
        // POST请求中最后为输入的用户名和密码
        m_string = text;
        return GET_REQUEST;  // 请求完成
    }
    return NO_REQUEST;  // 请求还不完整
}

// 阶段三2：process()调用process_read()来解析请求报文，并返回最终解析状态
http_conn::HTTP_CODE http_conn::process_read() {
    LINE_STATUS line_status = LINE_OK;  // 从状态机状态，初始化为LINE_OK
    HTTP_CODE ret =
        NO_REQUEST;  // 主状态机状态，最终的返回值，NO_REQUEST表示请求还不完整，通过下面的操作最终判断主状态机状态
    char* text = 0;

    while ((m_check_state == CHECK_STATE_CONTENT && line_status == LINE_OK) ||
           ((line_status = parse_line()) == LINE_OK)) {
        text = get_line();             // 解析当前行的首地址
        m_start_line = m_checked_idx;  // 更新下一行首地址
        LOG_INFO("%s", text);  // 在日志里打印该行内容，因为将\r\n修改为\0\0了
        switch (m_check_state) {
            case CHECK_STATE_REQUESTLINE:  // m_check_state的默认状态
            {
                ret = parse_request_line(text);              // 切第一行
                if (ret == BAD_REQUEST) return BAD_REQUEST;  // 语法错误
                break;
            }
            case CHECK_STATE_HEADER: {
                ret = parse_headers(text);  // 切请求头
                if (ret == BAD_REQUEST)
                    return BAD_REQUEST;
                else if (ret == GET_REQUEST)  // 请求完成后继续操作
                {
                    return do_request();
                }
                break;
            }
            case CHECK_STATE_CONTENT:  // 切请求体
            {
                ret = parse_content(text);
                if (ret == GET_REQUEST)  // 请求完成后继续操作
                    return do_request();
                line_status = LINE_OPEN;  // 数据不全
                break;
            }
            default:
                return INTERNAL_ERROR;
        }
    }
    return NO_REQUEST;  // 请求不完整
}

// 阶段三1：服务端解析请求报文
void http_conn::process() {
    HTTP_CODE read_ret =
        process_read();  // 真正的请求报文解析函数，返回解析状态，来决定下一步操作
    if (read_ret == NO_REQUEST) {
        modfd(m_epollfd, m_sockfd, EPOLLIN, m_TRIGMode);
        return;
    }
    bool write_ret = process_write(read_ret);  // 读完就去写
    if (!write_ret) {
        close_conn();
    }
    // 进行完Http解析后，即read完成，可进行write操作，将EPOOLIN设置为EPOLLOUT
    modfd(m_epollfd, m_sockfd, EPOLLOUT, m_TRIGMode);
}
