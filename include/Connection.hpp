#ifndef CONNECTION_H
#define CONNECTION_H

#include <string>
#include <string_view>
#include <unistd.h>

#define MAX_BUFFER_SIZE 1024

class Connection {
private:
    int fd_{-1};
    std::string read_buffer_;
    std::string write_buffer_;
    bool is_closed_{true};

public:
    explicit Connection(int fd);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    int get_fd() const { return fd_; }

    void append_read_buffer(std::string_view data);
    std::string_view get_read_buffer() const { return read_buffer_; }
    void clear_read_buffer() { read_buffer_.clear(); }

    void append_write_buffer(std::string_view data);
    std::string_view get_write_buffer() const { return write_buffer_; }
    void clear_write_buffer() { write_buffer_.clear(); }
    /** 对已发送的数据按字节数前移写缓冲（非阻塞写出后调用） */
    void consume_write_buffer(std::size_t n);

    bool is_closed() const { return is_closed_; }

    void close_connection();
};

#endif
