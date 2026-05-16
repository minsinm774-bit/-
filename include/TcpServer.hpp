#ifndef TCP_SERVER_H
#define TCP_SERVER_H

#include "thread_pool.hpp"
#include "Connection.hpp"
#include <string_view>

#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <atomic>
#include <unordered_map>
#include <memory>
#include <vector>

#define MAX_EVENTS 1024
#define BACKLOG 1024

class TcpServer {
private:
    int port_{};
    int epoll_fd_{-1};
    int wake_up_fd_{-1};
    int listen_fd_{-1};
    std::atomic<bool> running_{false};

    std::unique_ptr<ThreadPool> thread_pool_;
    std::vector<epoll_event> events_;
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;

private:
    void init_socket();
    void set_non_blocking(int fd);

    void add_event(int fd, uint32_t events);
    void mod_event(int fd, uint32_t events);
    /** 结合写缓冲是否为空，选择合适的 EPOLL 掩码并 CTL_MOD（ET+ONESHOT） */
    void rearm_client(int fd);
    void remove_event(int fd);

    void drain_wake_fd();
    void try_echo_and_rearm(Connection* conn, int client_fd);

    void handle_new_connection();
    void handle_read_event(int client_fd);
    void handle_write_event(int client_fd);
    void handle_close(int client_fd);
    void handle_wake_up_event();

public:
    explicit TcpServer(int port);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void epoll_loop();
    void stop();
};

#endif
