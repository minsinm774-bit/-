#include "Connection.hpp"

#include <cerrno>
#include <cstring>

#include <sys/socket.h>

Connection::Connection(int fd) : fd_(fd), is_closed_(false) {}

Connection::~Connection() { close_connection(); }

void Connection::append_read_buffer(std::string_view data) {
    read_buffer_.append(data.data(), data.size());
}

void Connection::append_write_buffer(std::string_view data) {
    write_buffer_.append(data.data(), data.size());
}

void Connection::consume_write_buffer(std::size_t n) {
    if (n >= write_buffer_.size()) {
        write_buffer_.clear();
        return;
    }
    write_buffer_.erase(0, n);
}

void Connection::close_connection() {
    if (fd_ < 0) {
        is_closed_ = true;
        return;
    }
    read_buffer_.clear();
    write_buffer_.clear();
    if (::close(fd_) < 0) {
        // 析构路径避免抛异常
    }
    fd_ = -1;
    is_closed_ = true;
}
