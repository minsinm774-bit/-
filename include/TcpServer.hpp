#ifndef TCP_SERVER_H
#define TCP_SERVER_H

#include "protocol.hpp"
#include "thread_pool.hpp"
#include "Connection.hpp"
#include <string>
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

/** Java UDS 回包单片载荷上限，防止畸形长度拖垮网关 */
constexpr std::size_t kGatewayMaxPayload = 64U * 1024U * 1024U;

class TcpServer {
private:
    int port_{};
    std::string backend_uds_path_;

    int epoll_fd_{-1};
    int wake_up_fd_{-1};
    int listen_fd_{-1};
    /** 连接到 Java Unix 域服务端（SOCK_STREAM） */
    int backend_fd_{-1};

    std::atomic<bool> running_{false};

    std::unique_ptr<ThreadPool> thread_pool_;
    std::vector<epoll_event> events_;
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;

    /** 发往 Java 侧尚未写完的字节（可能含半个 Envelope+payload） */
    std::string backend_outbuf_;
    /** 从 Java 侧读入的原始流，按 Envelope 组帧解析 */
    std::string backend_inbuf_;

private:
    void init_socket();
    void set_non_blocking(int fd);
    void connect_backend_uds();

    void add_event(int fd, uint32_t events);
    void mod_event(int fd, uint32_t events);
    void rearm_client(int fd);
    void remove_event(int fd);

    void drain_wake_fd();
    void rearm_backend();

    /** 将一帧 [Envelope+payload] 追加到 backend 发送队列并尝试写出 */
    void queue_frame_to_java(int tcp_client_fd, std::string&& payload);
    void flush_backend_out();
    void handle_backend_read();
    /** 解析 backend_inbuf_ 中的完整帧并写往 TCP */
    void drain_java_incoming();

    /** 尽力把 Connection 的发送缓冲写到 TCP，必要时 MOD EPOLL */
    void flush_client_outgoing(Connection* conn, int client_fd);

    void forward_payload_to_tcp(int tcp_client_fd, std::string&& payload);

    void handle_new_connection();
    void handle_read_event(int client_fd);
    void handle_write_event(int client_fd);
    void handle_close(int client_fd);
    void handle_wake_up_event();
    void close_backend_fd();

public:
    /**
     * @param port 监听端口
     * @param backend_uds_path Java 监听的 SOCK_STREAM Unix 路径（网关作为 client connect）
     */
    explicit TcpServer(int port, std::string backend_uds_path);

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    ~TcpServer();

    void epoll_loop();
    void stop();
};

#endif
