#include "TcpServer.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <limits>

#include <iostream>
#include <vector>

#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace {

const uint32_t kEvIn = static_cast<uint32_t>(EPOLLIN | EPOLLET | EPOLLONESHOT);
const uint32_t kEvRW = static_cast<uint32_t>(EPOLLIN | EPOLLOUT | EPOLLET | EPOLLONESHOT);

/** 后端 UDS：不用 ONESHOT，便于在连续 ET 事件中读/写排空 */
const uint32_t kEvBackIn = static_cast<uint32_t>(EPOLLIN | EPOLLET);
const uint32_t kEvBackRW = static_cast<uint32_t>(EPOLLIN | EPOLLOUT | EPOLLET);

}  // namespace

TcpServer::TcpServer(int port, std::string backend_uds_path) : port_(port), backend_uds_path_(std::move(backend_uds_path)), running_(true), events_(MAX_EVENTS) {
    listen_fd_ = -1;
    epoll_fd_ = -1;
    wake_up_fd_ = -1;
    backend_fd_ = -1;

    // 创建线程池
    thread_pool_ = std::make_unique<ThreadPool>(MIN_THREADS);
    // 初始化套接字
    init_socket();

    // 创建epoll实例
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        throw std::runtime_error("epoll_create1 error");
    }

    // 创建wake_up_fd_实例
    wake_up_fd_ = static_cast<int>(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    if (wake_up_fd_ < 0) {
        throw std::runtime_error("eventfd error");
    }

    // 连接后端UDS
    connect_backend_uds();

    // 添加监听套接字事件
    add_event(listen_fd_, kEvIn);
    // 添加wake_up_fd_事件
    add_event(wake_up_fd_, kEvIn);
    // 添加后端UDS事件
    add_event(backend_fd_, kEvBackIn);
}

TcpServer::~TcpServer() {
    stop();
}

/** 连接后端UDS */
void TcpServer::connect_backend_uds() {
    // 如果后端UDS路径为空，则抛出异常
    if (backend_uds_path_.empty()) {
        throw std::runtime_error("gateway: empty backend unix socket path");
    }

    // 创建后端UDS套接字
    backend_fd_ = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (backend_fd_ < 0) {
        throw std::runtime_error("gateway: socket(AF_UNIX) failed");
    }
    set_non_blocking(backend_fd_);

    // 创建后端UDS地址
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    const std::string& p = backend_uds_path_;
    if (p.size() >= sizeof(addr.sun_path)) {
        ::close(backend_fd_);
        backend_fd_ = -1;
        throw std::runtime_error("gateway: unix path too long");
    }
    std::memcpy(addr.sun_path, p.c_str(), p.size() + 1U);

    // 计算后端UDS地址长度
    const socklen_t sulen =
        static_cast<socklen_t>(offsetof(sockaddr_un, sun_path)) +
        static_cast<socklen_t>(p.size() + 1U);

    if (::connect(backend_fd_, reinterpret_cast<sockaddr*>(&addr), sulen) != 0) {
        ::close(backend_fd_);
        backend_fd_ = -1;
        throw std::runtime_error(std::string("gateway: connect uds failed: ") + strerror(errno));
    }

    std::cout << "gateway: connected backend UDS \"" << backend_uds_path_ << "\"\n";
}

void TcpServer::init_socket() {
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(static_cast<uint16_t>(port_));

    // 创建套接字,SOCK_STREAM | SOCK_CLOEXEC表示创建一个TCP套接字，并设置为close-on-exec
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error("socket error");
    }

    /** 设置套接字选项，允许重用地址 */
    constexpr int reuse = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    set_non_blocking(listen_fd_);

    if (bind(listen_fd_, reinterpret_cast<const sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        throw std::runtime_error("bind error");
    }
    if (listen(listen_fd_, BACKLOG) < 0) {
        throw std::runtime_error("listen error");
    }
}

void TcpServer::set_non_blocking(int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        throw std::runtime_error("fcntl F_GETFL error");
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::runtime_error("fcntl F_SETFL error");
    }
}

void TcpServer::add_event(int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        throw std::runtime_error("epoll_ctl EPOLL_CTL_ADD error");
    }
}

void TcpServer::mod_event(int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        throw std::runtime_error("epoll_ctl EPOLL_CTL_MOD error");
    }
}

void TcpServer::remove_event(int fd) {
    if (fd < 0 || epoll_fd_ < 0) {
        return;
    }
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
}

void TcpServer::drain_wake_fd() {
    for (;;) {
        uint64_t u = 0;
        const ssize_t r = ::read(wake_up_fd_, &u, sizeof(u));
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            std::cerr << "read wake_up_fd_: " << strerror(errno) << '\n';
            break;
        }
        if (r == 0) {
            break;
        }
    }
}

/** 关闭后端UDS */
void TcpServer::close_backend_fd() {
    if (backend_fd_ < 0) {
        return;
    }
    // 移除后端UDS事件
    remove_event(backend_fd_);
    // 关闭后端UDS
    ::shutdown(backend_fd_, SHUT_RDWR);
    // 关闭后端UDS套接字
    ::close(backend_fd_);
    // 清空后端UDS输出缓冲区和输入缓冲区
    backend_fd_ = -1;
    // 清空后端UDS输出缓冲区和输入缓冲区
    backend_outbuf_.clear();
    // 清空后端UDS输入缓冲区
    backend_inbuf_.clear();
    // 打印后端UDS断开连接信息
    std::cerr << "gateway: backend UDS disconnected\n";
}

/** 重新注册后端UDS事件 */
void TcpServer::rearm_backend() {
    // 如果后端UDS套接字小于0，则返回
    if (backend_fd_ < 0) {
        return;
    }
    // 重新注册后端UDS事件
    mod_event(backend_fd_, backend_outbuf_.empty() ? kEvBackIn : kEvBackRW);
}

/*————————————————————————处理后tcp客户端输入数据包*————————————————————————*/
/** 2.刷新后端UDS输出缓冲区 */
void TcpServer::flush_backend_out() {
    // 如果后端UDS套接字小于0，则清空后端UDS输出缓冲区并返回
    if (backend_fd_ < 0) {
        backend_outbuf_.clear();
        return;
    }
    // 循环发送后端UDS输出缓冲区数据
    while (!backend_outbuf_.empty()) {
        // 发送后端UDS输出缓冲区数据
        const ssize_t s = ::send(
            backend_fd_, backend_outbuf_.data(), backend_outbuf_.size(), MSG_NOSIGNAL);
        if (s > 0) {
            // 删除已发送的数据
            backend_outbuf_.erase(0, static_cast<std::size_t>(s));
            continue;
        }
        if (s < 0 && errno == EINTR) {
            // 如果发送失败，则继续发送
            continue;
        }
        if (s < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // 如果发送失败，则继续发送
            break;
        }
        std::cerr << "gateway: send(backend) error: " << strerror(errno) << '\n';
        close_backend_fd();
        break;
    }
    // 重新注册后端UDS事件
    rearm_backend();
}

/** 1.封装TCP客户端帧数据，并刷新数据给后端UDS输出缓冲区 */
void TcpServer::queue_frame_to_java(int tcp_client_fd, std::string&& payload) {
    // 如果后端UDS套接字小于0，则关闭客户端连接并返回
    if (backend_fd_ < 0) {
        handle_close(tcp_client_fd);
        return;
    }
    // 如果帧数据大小大于kGatewayMaxPayload，则打印错误信息并关闭客户端连接并返回
    if (payload.size() > kGatewayMaxPayload) {
        std::cerr << "gateway: tcp payload too large, closing client fd=" << tcp_client_fd << '\n';
        handle_close(tcp_client_fd);
        return;
    }
    // 创建Envelope头
    Envelope hdr{};
    hdr.payload_len =
        htonl(static_cast<uint32_t>(std::min(payload.size(),
                                              static_cast<std::size_t>(std::numeric_limits<uint32_t>::max()))));
    hdr.client_fd = htonl(static_cast<uint32_t>(tcp_client_fd));

    // 将Envelope头添加到后端UDS输出缓冲区
    backend_outbuf_.append(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
    // 将帧数据添加到后端UDS输出缓冲区
    backend_outbuf_.append(payload);

    // 刷新后端UDS输出缓冲区
    flush_backend_out();
}

/*————————————————————————处理后端UDS输入数据包*————————————————————————*/
/** 2.拆解后端UDS输入数据包 并转发到Connection输出缓冲区*/
void TcpServer::drain_java_incoming() {
    // 循环处理后端UDS输入缓冲区
    for (;;) {
        // 如果后端UDS输入缓冲区大小小于Envelope头大小，则返回
        if (backend_inbuf_.size() < sizeof(Envelope)) {
            return;
        }
        // 创建Envelope头
        Envelope wire{};
        // 复制Envelope头到wire
        std::memcpy(&wire, backend_inbuf_.data(), sizeof(Envelope));
        // 将payload长度转换为网络字节序
        const uint32_t plen = ntohl(wire.payload_len);
        // 将client_fd转换为网络字节序
        const uint32_t cfd_u32 = ntohl(wire.client_fd);

        // 如果payload长度大于kGatewayMaxPayload，则打印错误信息并关闭后端UDS并返回
        if (plen > static_cast<uint32_t>(kGatewayMaxPayload)) {
            std::cerr << "gateway: insane payload_len from Java, closing backend\n";
            close_backend_fd();
            return;
        }
        // 计算帧数据大小
        const std::size_t frame = sizeof(Envelope) + static_cast<std::size_t>(plen);
        // 如果后端UDS输入缓冲区大小小于帧数据大小，则返回
        if (backend_inbuf_.size() < frame) {
            return;
        }
        // 创建帧数据
        std::string body(
            backend_inbuf_.data() + sizeof(Envelope),
            backend_inbuf_.data() + sizeof(Envelope) + static_cast<std::size_t>(plen));
        backend_inbuf_.erase(0, frame);
        // 将帧数据转发到TCP客户端
        forward_payload_to_tcp(static_cast<int>(cfd_u32), std::move(body));
    }
}

/** 1.读取后端UDS输入数据包，并拆解后端UDS数据包*/
void TcpServer::handle_backend_read() {
    // 如果后端UDS套接字小于0，则返回
    if (backend_fd_ < 0) {
        return;
    }

    // 循环读取后端UDS输入缓冲区
    for (;;) {
        // 创建缓冲区
        std::array<char, 8192> buf{};
        const ssize_t n = ::read(backend_fd_, buf.data(), buf.size());
        // 如果读取到0字节，则关闭后端UDS并返回
        if (n == 0) {
            close_backend_fd();
            return;
        }
        // 如果读取失败，则继续读取
        if (n < 0) {
            // 如果读取失败，则继续读取
            if (errno == EINTR) {
                continue;
            }
            // 如果读取失败，则继续读取
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            std::cerr << "gateway: read(backend) error: " << strerror(errno) << '\n';
            close_backend_fd();
            return;
        }
        // 将读取到的数据添加到后端UDS输入缓冲区
        backend_inbuf_.append(buf.data(), static_cast<std::size_t>(n));
        // 处理后端UDS输入缓冲区
        drain_java_incoming();
    }
}

/** 4.刷新TCP客户端输出缓冲区 */
void TcpServer::flush_client_outgoing(Connection* conn, int client_fd) {
    // 如果连接为空或客户端套接字小于0，则返回
    if (conn == nullptr || client_fd < 0) {
        return;
    }
    // 循环刷新TCP客户端输出缓冲区
    for (;;) {
        // 获取TCP客户端输出缓冲区
        const std::string_view pending = conn->get_write_buffer();
        // 如果输出缓冲区为空，则返回
        if (pending.empty()) {
            break;
        }
        // 发送TCP客户端输出缓冲区数据
        const ssize_t s =
            ::send(client_fd, pending.data(), pending.size(), MSG_NOSIGNAL);
        if (s > 0) {
            // 消耗TCP客户端输出缓冲区数据
            conn->consume_write_buffer(static_cast<std::size_t>(s));
            continue;
        }
        if (s < 0 && errno == EINTR) {
            // 如果发送失败，则继续发送
            continue;
        }
        if (s < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // 如果发送失败，则继续发送
            break;
        }
        // 打印错误信息并关闭客户端连接
        std::cerr << "gateway: send(tcp client) fd=" << client_fd << " err=" << strerror(errno) << '\n';
        handle_close(client_fd);
        return;
    }

    rearm_client(client_fd);
}

/** 3.转发后端UDS数据包给Connection输出缓冲区，并刷新TCP客户端输出缓冲区 */
void TcpServer::forward_payload_to_tcp(int tcp_client_fd, std::string&& payload) {
    // 如果TCP客户端套接字小于0，则返回
    auto it = connections_.find(tcp_client_fd);
    if (it == connections_.end()) {
        return;
    }
    // 获取Connection
    Connection* conn = it->second.get();
    // 如果帧数据不为空，则添加到Connection输出缓冲区
    if (!payload.empty()) {
        conn->append_write_buffer(std::string_view(payload.data(), payload.size()));
    }
    // 刷新TCP客户端输出缓冲区
    flush_client_outgoing(conn, tcp_client_fd);
}

/** 重新注册TCP客户端事件 */
void TcpServer::rearm_client(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        return;
    }
    const uint32_t ev =
        it->second->get_write_buffer().empty() ? kEvIn : kEvRW;
    mod_event(fd, ev);
}

/** 停止服务器 */
void TcpServer::stop() {
    // 如果服务器不运行，则返回
    if (!running_.load()) {
        return;
    }
    running_.store(false);

    // 写唤醒文件描述符
    const uint64_t wake_up_val = 1;
    const ssize_t w = ::write(wake_up_fd_, &wake_up_val, sizeof(wake_up_val));
    // 如果写唤醒文件描述符失败，则打印错误信息
    if (w < 0) {
        std::cerr << "write wake_up_fd_: " << strerror(errno) << '\n';
    }

    // 关闭线程池
    if (thread_pool_) {
        thread_pool_->shutdown();
    }

    // 关闭所有TCP客户端连接
    {
        std::vector<int> fds;
        fds.reserve(connections_.size());
        for (const auto& kv : connections_) {
            fds.push_back(kv.first);
        }
        for (int fd : fds) {
            handle_close(fd);
        }
    }

    // 关闭后端UDS连接
    close_backend_fd();

    // 关闭唤醒文件描述符
    if (wake_up_fd_ >= 0) {
        drain_wake_fd();
        remove_event(wake_up_fd_);
        ::close(wake_up_fd_);
        wake_up_fd_ = -1;
    }

    // 关闭监听文件描述符
    if (listen_fd_ >= 0) {
        remove_event(listen_fd_);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    // 关闭epoll文件描述符
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }

    // 清空事件
    events_.clear();
}

/** 处理新TCP连接 */
void TcpServer::handle_new_connection() {
    // 循环处理新TCP连接
    while (true) {
        // 初始化客户端
        sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);
        // 接受TCP连接
        const int client_fd =
            ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            std::cerr << "accept: " << strerror(errno) << '\n';
            break;
        }

        // 设置非阻塞
        set_non_blocking(client_fd);
        // 创建Connection
        connections_[client_fd] = std::make_unique<Connection>(client_fd);
        // 添加事件
        add_event(client_fd, kEvIn);
        // 打印新TCP连接信息
        std::cout << "new tcp connection fd=" << client_fd << " from "
                  << inet_ntoa(client_addr.sin_addr) << ':'
                  << ntohs(client_addr.sin_port) << '\n';
    }
    // 重新注册监听文件描述符事件
    mod_event(listen_fd_, kEvIn);
}

/** 处理TCP客户端读事件 */
void TcpServer::handle_read_event(int client_fd) {
    // 如果客户端套接字小于0，则返回
    if (client_fd < 0) {
        return;
    }
    auto it = connections_.find(client_fd);
    if (it == connections_.end()) {
        return;
    }

    // 获取Connection
    Connection* conn = it->second.get();

    // 循环读取TCP客户端输入缓冲区
    while (true) {
        // 创建缓冲区
        std::array<char, MAX_BUFFER_SIZE> buf{};
        const ssize_t n = ::read(client_fd, buf.data(), buf.size());
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            std::cerr << "read tcp error: " << strerror(errno) << '\n';
            handle_close(client_fd);
            return;
        }
        if (n == 0) {
            handle_close(client_fd);
            return;
        }
        // 添加到Connection输入缓冲区
        conn->append_read_buffer(std::string_view(buf.data(), static_cast<std::size_t>(n)));
    }

    // 获取Connection输入缓冲区数据
    std::string payload;
    payload.assign(conn->get_read_buffer().data(), conn->get_read_buffer().size());
    // 清空Connection输入缓冲区
    conn->clear_read_buffer();
    if (payload.empty()) {
        rearm_client(client_fd);
        return;
    }

    // 封装TCP客户端帧数据，并刷新数据给后端UDS输出缓冲区
    queue_frame_to_java(client_fd, std::move(payload));
    // 重新注册TCP客户端事件
    rearm_client(client_fd);
}

/** 处理TCP客户端写事件 */
void TcpServer::handle_write_event(int client_fd) {
    // 如果客户端套接字小于0，则返回
    if (client_fd < 0) {
        return;
    }
    auto it = connections_.find(client_fd);
    if (it == connections_.end()) {
        return;
    }
    // 刷新TCP客户端输出缓冲区
    flush_client_outgoing(it->second.get(), client_fd);
}

void TcpServer::handle_close(int client_fd) {
    if (client_fd < 0) {
        return;
    }

    auto it = connections_.find(client_fd);
    if (it == connections_.end()) {
        return;
    }

    // 移除TCP客户端事件
    remove_event(client_fd);

    // 创建客户端地址， 并获取客户端地址信息
    sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);
    std::string log_ip = "?";
    std::string log_port = "?";
    if (getpeername(client_fd, reinterpret_cast<sockaddr*>(&client_addr),
                    &client_addr_len) == 0) {
        log_ip = inet_ntoa(client_addr.sin_addr);
        log_port = std::to_string(ntohs(client_addr.sin_port));
    } else {
        std::cerr << "getpeername: " << strerror(errno) << '\n';
    }

    // 关闭Connection
    it->second->close_connection();
    connections_.erase(it);

    // 打印TCP连接关闭信息
    std::cout << "tcp connection fd=" << client_fd << " from " << log_ip << ':' << log_port << " closed\n";
}

/** 处理唤醒事件 */
void TcpServer::handle_wake_up_event() {
    drain_wake_fd();
    if (wake_up_fd_ >= 0) {
        mod_event(wake_up_fd_, kEvIn);
    }
}

/** epoll循环 */
void TcpServer::epoll_loop() {
    while (running_.load()) {
        const int nfds = epoll_wait(epoll_fd_, events_.data(), MAX_EVENTS, -1);
        if (nfds < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "epoll_wait error: " << strerror(errno) << '\n';
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            if (!running_.load()) {
                break;
            }
            const int fd = events_[i].data.fd;
            if (fd == listen_fd_) {
                handle_new_connection();
                continue;
            }
            if (fd == wake_up_fd_) {
                handle_wake_up_event();
                continue;
            }
            if (fd == backend_fd_) {
                const uint32_t ev = events_[i].events;
                if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    close_backend_fd();
                    continue;
                }
                if (ev & EPOLLIN) {
                    handle_backend_read();
                }
                if ((ev & EPOLLOUT) && backend_fd_ >= 0) {
                    flush_backend_out();
                }
                continue;
            }
            if (events_[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                handle_close(fd);
                continue;
            }
            if (events_[i].events & EPOLLIN) {
                handle_read_event(fd);
                continue;
            }
            if (events_[i].events & EPOLLOUT) {
                handle_write_event(fd);
                continue;
            }
        }
    }
}
