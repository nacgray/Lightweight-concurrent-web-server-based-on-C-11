#include "config.h"
#include "webserver.h"

int main(int argc, char* argv[]) {
    // 需要修改的数据库信息,登录名,密码,库名
    string user = "root";
    string passwd = "root";
    string databasename = "yourdb";

    // 命令行解析   ./server -p 9006
    Config config;
    config.parse_arg(argc, argv);

    WebServer server;

    // 初始化
    // 通过config.cpp获取命令行信息后，对下列数据进行修改，否则就是否认值
    server.init(config.PORT, user, passwd, databasename, config.LOGWrite,
                config.OPT_LINGER, config.TRIGMode, config.sql_num,
                config.thread_num, config.close_log, config.actor_model);

    // 1.日志
    server.log_write();  // 初始化日志

    // 2.数据库
    server.sql_pool();  // 初始化数据库连接池

    // 3.线程池
    server.thread_pool();  // 初始化线程池

    // 4.触发模式
    server.trig_mode();  // 设置ET/LT模式

    // 5.监听
    server.eventListen();  // 创建listenfd和epollfd

    // 6.运行
    server.eventLoop();  // 陷入死循环

    return 0;
}
