#include <iostream>
#include <string>
#include "TcpServer.hpp"

int main(int argc, char* argv[]){
    if(argc != 2){
        std::cerr << "Usage: " << argv[0] << " <port>" << std::endl;
        return 1;
    }
    int port = std::stoi(argv[1]);
    TcpServer server(port);
    server.epoll_loop();
    return 0;
}
