#include "sql_connection_pool.h"

#include <mysql/mysql.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>
#include <list>
#include <string>

using namespace std;

connection_pool::connection_pool() {
    m_CurConn = 0;
    m_FreeConn = 0;
}

connection_pool* connection_pool::GetInstance() {
    static connection_pool connPool;  // 静态局部变量，全程序只有这一份
    return &connPool;
}

// 构造初始化
void connection_pool::init(string url, string User, string PassWord,
                           string DBName, int Port, int MaxConn,
                           int close_log) {
    m_url = url;
    m_Port = Port;
    m_User = User;
    m_PassWord = PassWord;
    m_DatabaseName = DBName;
    m_close_log = close_log;

    // 建立MaxConn次数据库TCP长连接
    for (int i = 0; i < MaxConn; i++) {
        MYSQL* con = NULL;
        con = mysql_init(con);  // 负责在本地内存里划出地盘

        if (con == NULL) {
            LOG_ERROR("MySQL Error");
            exit(1);  // 数据库连接失败，立马退出整个webserver进程
        }
        // 负责利用这块地盘，发起真实的 TCP
        // 网络通信，去和远端的数据库完成握手。建立数据库TCP长连接
        con =
            mysql_real_connect(con, url.c_str(), User.c_str(), PassWord.c_str(),
                               DBName.c_str(), Port, NULL, 0);

        if (con == NULL) {
            LOG_ERROR("MySQL Error");
            exit(1);
        }
        connList.push_back(
            con);  // 把所有成功的连接指针塞进C++ list（双向链表）里
        ++m_FreeConn;
    }

    reserve = sem(
        m_FreeConn);  // reserve 是一个信号量，它的值代表有几个可以连接的数据库

    m_MaxConn = m_FreeConn;  // 建立连接的数量
}

// 当有请求时，从数据库连接池中返回一个可用连接，更新使用和空闲连接数
MYSQL* connection_pool::GetConnection() {
    MYSQL* con = NULL;

    if (0 == connList.size()) return NULL;

    reserve.wait();  //>0苏醒 =0睡眠

    lock.lock();  // 上锁，建立临界区

    con = connList.front();  // 从存储数据库连接池的双向链表中返回一个可用连接
    connList.pop_front();

    // 更新使用和空闲连接数
    --m_FreeConn;
    ++m_CurConn;

    lock.unlock();  // 解锁
    return con;
}

// 释放当前使用的连接
bool connection_pool::ReleaseConnection(MYSQL* con) {
    if (NULL == con) return false;

    lock.lock();

    connList.push_back(con);  // 将数据库返回到数据库连接池中
    ++m_FreeConn;
    --m_CurConn;

    lock.unlock();

    reserve.post();  // 更新信号量
    return true;
}

// 销毁数据库连接池
void connection_pool::DestroyPool() {
    lock.lock();
    if (connList.size() > 0) {
        list<MYSQL*>::iterator it;
        for (it = connList.begin(); it != connList.end(); ++it) {
            MYSQL* con = *it;
            mysql_close(con);
        }
        m_CurConn = 0;
        m_FreeConn = 0;
        connList.clear();
    }

    lock.unlock();
}

// 当前空闲的连接数
int connection_pool::GetFreeConn() { return this->m_FreeConn; }

connection_pool::~connection_pool() { DestroyPool(); }

connectionRAII::connectionRAII(MYSQL** SQL, connection_pool* connPool) {
    *SQL = connPool->GetConnection();

    conRAII = *SQL;
    poolRAII = connPool;
}

connectionRAII::~connectionRAII() { poolRAII->ReleaseConnection(conRAII); }