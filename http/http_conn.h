#ifndef HTTPCONNECTION_H
#define HTTPCONNECTION_H
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

#include <map>
// 密码哈希加密
#include <openssl/sha.h>

#include <iomanip>
#include <sstream>

#include "../CGImysql/sql_connection_pool.h"
#include "../lock/locker.h"
#include "../log/log.h"

class http_conn {
   public:
    static const int FILENAME_LEN = 200;
    static const int READ_BUFFER_SIZE = 2048;
    static const int WRITE_BUFFER_SIZE = 1024;
    // HTTP请求方法
    enum METHOD {
        GET = 0,
        POST,
        HEAD,
        PUT,
        DELETE,
        TRACE,
        OPTIONS,
        CONNECT,
        PATH
    };
    // 主状态机（process_read）
    enum CHECK_STATE {
        CHECK_STATE_REQUESTLINE = 0,  // 一开始默认的状态，解析请求行
        CHECK_STATE_HEADER,           // 第一行结束后，变为此状态，解析请求头
        CHECK_STATE_CONTENT           // 切到了一个“空行”，头部结束了！
        // 如果这是一个 POST
        // 请求，状态就会变成CHECK_STATE_CONTENT，准备去读最后的请求体（账号密码）
    };
    // 整个 主状态机process_read() 函数解析完毕后，向上层汇报的最终处理结果
    enum HTTP_CODE {
        NO_REQUEST,   // 请求还不完整
        GET_REQUEST,  // 完美
        BAD_REQUEST,  // 语法错误
        NO_RESOURCE,
        FORBIDDEN_REQUEST,
        FILE_REQUEST,  // 客人请求的网页文件不仅存在，而且有权限访问。这是
                       // do_request() 函数在本地找完文件后返回的极佳状态。
        INTERNAL_ERROR,
        CLOSED_CONNECTION
    };
    // 从状态机
    enum LINE_STATUS {
        LINE_OK =
            0,  // 找到了 \r\n，成功切下了完整的一行。主状态机可以拿这一行去分析
        LINE_BAD,  // 出现了不符合 HTTP 协议规定的奇葩字符
        LINE_OPEN  // 数据还没收全，还得等下一次 epoll 触发，继续从网卡里读
    };

   public:
    http_conn() {}
    ~http_conn() {}

   public:
    void init(int sockfd, const sockaddr_in& addr, char*, int, int, string user,
              string passwd, string sqlname);
    void close_conn(bool real_close = true);  // 关闭连接
    bool read_once();  // read_once() 负责把网卡里的数据死循环抽干，塞进读缓冲区
    bool write();      // write() 负责把做好的响应报文，塞进网卡发给浏览器
    void process();    // 线程池唯一指定的干活入口,工作线程就是调用该接口
    void initmysql_result(
        connection_pool*
            connPool);  // 把数据库里的所有用户名和密码提前拉取到本地的
                        // map 里，方便极速校验
    sockaddr_in* get_address() { return &m_address; }
    int timer_flag;
    int improv;

   private:
    void init();
    // 核心流程控制
    HTTP_CODE process_read();  // 主状态机，负责统筹安排，一步步解析HTTP请求
    bool process_write(HTTP_CODE ret);
    // 根据process_read()分析出的结果（比如是200 OK还是404 Not
    // Found），负责生成对应的HTTP响应报文

    // 解析请求，拆解HTTP文本
    LINE_STATUS parse_line();  // 从状态机，找 \r\n，切出一行
    // 阶段三2.2：把刚刚切下来的那行文本的首地址返回来
    char* get_line() { return m_read_buf + m_start_line; };
    HTTP_CODE parse_request_line(
        char*
            text);  // 专切第一行。提取出 GET、目标路径 /potato.jpg、HTTP版本号
    HTTP_CODE parse_headers(
        char* text);  // 专切请求头。提取出 Host、Content-Length 等信息
    HTTP_CODE parse_content(
        char* text);  // 专切请求体。提取出 POST 请求附带的账号密码

    HTTP_CODE
    do_request();  // 业务逻辑函数，根据上面函数解析的文本来决定后续操作
    void unmap();  // 用完图片后，把 mmap 映射的内存释放掉

    // 生成响应头
    bool add_response(
        const char* format,
        ...);  // 底层调用C语言极其高级的vsnprintf（可变参数打印），把字符串写进m_write_buf（写缓冲区）
    bool add_status_line(
        int status, const char* title);  // 贴上最外面的大标签：HTTP/1.1 200 OK
    bool add_headers(
        int content_length);  // 一个打包函数，内部一口气调用下面几个函数
    bool add_content_type();  // 告诉浏览器这是HTML还是图片（文本类型）
    bool add_content_length(int content_length);  // 告诉浏览器字节数
    bool add_linger();  // 告诉浏览器咱们是保持长连接还是断开
    bool
    add_blank_line();  // 加上一个单独的
                       // \r\n。这代表着：响应头到此结束，下面就是真正的文件数据
    bool add_content(
        const char* content);  // 如果出错（比如
                               // 404），就直接把错误提示网页的文本拼在后面

   public:
    // 两个static全局变量
    static int m_epollfd;
    static int m_user_count;
    MYSQL* mysql;
    int m_state;  // 读为0, 写为1

   private:
    int m_sockfd;
    sockaddr_in m_address;

    // 状态机的核心
    char m_read_buf[READ_BUFFER_SIZE];  // 从网卡里读出来的 HTTP 纯文本存放位置
    long m_read_idx;  // 记录网卡里的数据一共读到了哪个位置，有效数据的末尾边界
    long m_checked_idx;  // 前正在分析哪一个字符。它永远在追赶 m_read_idx
    int m_start_line;    // 记录当前正在分析的这一行

    // 解析出来的 HTTP 字段
    CHECK_STATE m_check_state;  // 主状态机现在的状态
    METHOD m_method;            // GET 还是 POST 还是什么状态
    int cgi;                    // 是否启用的POST
    char* m_string;             // 存储请求头数据
    char* m_url;                // 要访问的文件路径
    char* m_version;            // HTTP/1.1
    char* m_host;               // 域名
    long m_content_length;      // 报文体长
    bool m_linger;              // 是否长连接

    // 写缓冲区与内存映射
    char m_real_file[FILENAME_LEN];  // 要找的网页文件，在服务器硬盘上的绝对路径
    char* m_file_address;
    struct stat m_file_stat;
    struct iovec m_iv[2];
    // m_iv[0] 指向响应头（m_write_buf），
    // m_iv[1]
    // 指向文件内容（m_file_address），调用一次函数就能把它们拼接在一起发给网卡
    int m_iv_count;
    int bytes_to_send;
    int bytes_have_send;
    char m_write_buf[WRITE_BUFFER_SIZE];  // HTTP 响应头（比如 HTTP/1.1 200
                                          // OK），全写在这个数组里
    int m_write_idx;

    // 数据库与路径
    char* doc_root;  // 网站的根目录路径
    map<string, string> m_users;
    int m_TRIGMode;
    int m_close_log;
    char sql_user[100];
    char sql_passwd[100];
    char sql_name[100];

    // 哈希加密防御
    // 核心战术组件：获取加盐后的 SHA-256 哈希字符串
    std::string get_salted_hash(const std::string& username,
                                const std::string& password) {
        // 1. 混合材料：用户名(动态盐) + 密码 + 固定的私盐(Pepper)
        std::string salted_pwd =
            username + password + "wangzaiyi_wangzhiyong_xiaojia_lala_20260220";

        // 2. 准备接收 SHA-256 的二进制结果
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256_CTX sha256;
        SHA256_Init(&sha256);
        SHA256_Update(&sha256, salted_pwd.c_str(), salted_pwd.size());
        SHA256_Final(hash, &sha256);

        // 3. 将不可读的二进制数据转化为 64 位的 16 进制字符串
        std::stringstream ss;
        for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
            ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
        }
        return ss.str();
    }
};

#endif
