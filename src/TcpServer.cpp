#include "TcpServer.hpp"

#include <array>
#include <cerrno>
#include <cstring>

#include <iostream>
#include <string>
#include <vector>

#include <sys/eventfd.h>
#include <sys/socket.h>

namespace {

const uint32_t kEvIn = static_cast<uint32_t>(EPOLLIN | EPOLLET | EPOLLONESHOT);
const uint32_t kEvRW = static_cast<uint32_t>(EPOLLIN | EPOLLOUT | EPOLLET | EPOLLONESHOT);

}  // namespace

TcpServer::TcpServer(int port) : port_(port), running_(true), events_(MAX_EVENTS) {
    listen_fd_ = -1;
    epoll_fd_ = -1;
    wake_up_fd_ = -1;

    thread_pool_ = std::make_unique<ThreadPool>(MIN_THREADS);
    init_socket();

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        throw std::runtime_error("epoll_create1 error");
    }

    wake_up_fd_ = static_cast<int>(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    if (wake_up_fd_ < 0) {
        throw std::runtime_error("eventfd error");
    }

    add_event(listen_fd_, kEvIn);
    add_event(wake_up_fd_, kEvIn);
}

TcpServer::~TcpServer() {
    stop();
}

void TcpServer::init_socket() {
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(static_cast<uint16_t>(port_));

    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error("socket error");
    }

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

/* 添加事件 */
void TcpServer::add_event(int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        throw std::runtime_error("epoll_ctl EPOLL_CTL_ADD error");
    }
}

/* 修改事件 */
void TcpServer::mod_event(int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        throw std::runtime_error("epoll_ctl EPOLL_CTL_MOD error");
    }
}

void TcpServer::remove_event(int fd) {
    if (fd < 0) {
        return;
    }
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
}

/* 清空wake_up_fd */
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

/* 尝试回显并重新设置事件 */
void TcpServer::try_echo_and_rearm(Connection* conn, int client_fd) {
    if (conn == nullptr || client_fd < 0) {
        return;
    }

    std::string out;
    out.append(conn->get_read_buffer());
    conn->clear_read_buffer();

    if (out.empty()) {
        rearm_client(client_fd);
        return;
    }

    std::size_t sent = 0;
    while (sent < out.size()) {
        const ssize_t s = ::send(client_fd, out.data() + sent, out.size() - sent, MSG_NOSIGNAL);
        if (s > 0) {
            sent += static_cast<std::size_t>(s);
            continue;
        }
        if (s < 0 && errno == EINTR) {
            continue;
        }
        if (s < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            conn->append_write_buffer(std::string_view(out.data() + sent, out.size() - sent));
            mod_event(client_fd, kEvRW);
            return;
        }
        handle_close(client_fd);
        return;
    }

    rearm_client(client_fd);
}

void TcpServer::rearm_client(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        return;
    }
    const uint32_t ev =
        it->second->get_write_buffer().empty() ? kEvIn : kEvRW;
    mod_event(fd, ev);
}

void TcpServer::stop() {
    if (!running_.load()) {
        return;
    }
    running_.store(false);

    const uint64_t wake_up_val = 1;
    const ssize_t w = ::write(wake_up_fd_, &wake_up_val, sizeof(wake_up_val));
    if (w < 0) {
        std::cerr << "write wake_up_fd_: " << strerror(errno) << '\n';
    }

    if (thread_pool_) {
        thread_pool_->shutdown();
    }

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

    if (wake_up_fd_ >= 0) {
        drain_wake_fd();
        remove_event(wake_up_fd_);
        ::close(wake_up_fd_);
        wake_up_fd_ = -1;
    }

    if (listen_fd_ >= 0) {
        remove_event(listen_fd_);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }

    events_.clear();
}

void TcpServer::handle_new_connection() {
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);
        const int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len);
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

        set_non_blocking(client_fd);
        connections_[client_fd] = std::make_unique<Connection>(client_fd);
        add_event(client_fd, kEvIn);

        std::cout << "new connection from " << inet_ntoa(client_addr.sin_addr) << ':'
                  << ntohs(client_addr.sin_port) << '\n';
    }
    mod_event(listen_fd_, kEvIn);
}

void TcpServer::handle_read_event(int client_fd) {
    if (client_fd < 0) {
        return;
    }
    auto it = connections_.find(client_fd);
    if (it == connections_.end()) {
        return;
    }

    Connection* conn = it->second.get();

    while (true) {
        std::array<char, MAX_BUFFER_SIZE> buf{};
        const ssize_t n = ::read(client_fd, buf.data(), buf.size());
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            std::cerr << "read error: " << strerror(errno) << '\n';
            handle_close(client_fd);
            return;
        }
        if (n == 0) {
            handle_close(client_fd);
            return;
        }
        conn->append_read_buffer(std::string_view(buf.data(), static_cast<std::size_t>(n)));
    }

    if (conn->get_read_buffer().empty()) {
        rearm_client(client_fd);
        return;
    }

    try_echo_and_rearm(conn, client_fd);
}

void TcpServer::handle_write_event(int client_fd) {
    if (client_fd < 0) {
        return;
    }
    auto it = connections_.find(client_fd);
    if (it == connections_.end()) {
        return;
    }
    Connection* conn = it->second.get();

    for (;;) {
        const std::string_view pending = conn->get_write_buffer();
        if (pending.empty()) {
            break;
        }
        const ssize_t s = ::send(client_fd, pending.data(), pending.size(), MSG_NOSIGNAL);
        if (s > 0) {
            conn->consume_write_buffer(static_cast<std::size_t>(s));
            continue;
        }
        if (s < 0 && errno == EINTR) {
            continue;
        }
        if (s < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        }
        std::cerr << "send error: " << strerror(errno) << '\n';
        handle_close(client_fd);
        return;
    }

    rearm_client(client_fd);
}

void TcpServer::handle_close(int client_fd) {
    if (client_fd < 0) {
        return;
    }

    auto it = connections_.find(client_fd);
    if (it == connections_.end()) {
        return;
    }

    remove_event(client_fd);

    sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);
    std::string log_ip = "?";
    std::string log_port = "?";
    if (getpeername(client_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len) == 0) {
        log_ip = inet_ntoa(client_addr.sin_addr);
        log_port = std::to_string(ntohs(client_addr.sin_port));
    } else {
        std::cerr << "getpeername: " << strerror(errno) << '\n';
    }

    it->second->close_connection();
    connections_.erase(it);

    std::cout << "connection from " << log_ip << ':' << log_port << " closed\n";
}

void TcpServer::handle_wake_up_event() {
    drain_wake_fd();
    if (wake_up_fd_ >= 0) {
        mod_event(wake_up_fd_, kEvIn);
    }
}

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
            if (events_[i].events &
                (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
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
