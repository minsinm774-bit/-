#include <iostream>
#include <string>

#include "TcpServer.hpp"

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <tcp_listen_port> <backend_unix_socket_path>\n";
        std::cerr
            << "  Example: ./epoll_server 9000 /tmp/gateway_java.sock\n"
            << "  Java 侧需先在本机 listen 该 SOCK_STREAM UDS，再启动本进程。\n";
        return 1;
    }
    const int port = std::stoi(argv[1]);
    const std::string uds = argv[2];

    try {
        TcpServer server(port, uds);
        server.epoll_loop();
    } catch (const std::exception& ex) {
        std::cerr << "fatal: " << ex.what() << '\n';
        return 1;
    }
    return 0;
}
