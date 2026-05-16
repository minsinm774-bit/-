# Multi-Threaded Epoll TCP Proxy (C++ Server)

这是一个基于 C++ 开发的、多线程、高并发、非阻塞的网络通信中间件。项目采用 **Reactor** 架构，利用 Linux 内核的 `epoll` 机制（边缘触发 ET 模式）与 `EPOLLONESHOT` 特性，实现了安全、高效的事件分发网络。

为了实现业务的高度解耦，该服务主要作为**网络通信层（Proxy/Sidecar）**运行，专注于维持海量公网长连接，并通过 **Unix Domain Socket (UDS)** 将解析出的原始数据透明转发至本地的业务服务（如 Java、Python），完美实现多语言混合架构。

---

## 🛠 核心技术栈与架构设计

* **Reactor 核心事件循环**：单线程 `epoll_wait` 负责高效的连接监听与事件收发分发，避免了传统的多线程阻塞式 I/O 带来的上下文切换开销。
* **边缘触发 (Edge Triggered, ET)**：全网口与套接字均采用非阻塞模式（`SOCK_NONBLOCK`），配合 `while` 循环读写，榨干单核网卡性能。
* **线程安全与 `EPOLLONESHOT`**：通过 `EPOLLONESHOT` 锁死正在被处理的套接字状态，配合任务线程池（Thread Pool）并发处理数据读取与业务打包，彻底杜绝多线程竞争读取同一个 FD 的数据错乱。
* **原子防泄露安全 (`SOCK_CLOEXEC`)**：所有套接字创建（包括 `accept4`）原子级内嵌 `SOCK_CLOEXEC` 标志，防止在多线程环境下发生子进程 FD 泄露导致 TCP 僵死。
* **跨线程唤醒 (`eventfd`)**：主事件循环注册 `EFD_NONBLOCK` 的 `eventfd`。当工作线程或管理层需要优雅关闭服务器或注入定时任务时，一键唤醒阻塞中的 `epoll_wait`。
* **多语言业务桥接 (Unix Domain Socket)**：C++ 进程与本地业务进程通过极速 UDS 管道进行进程间通信（IPC）。C++ 仅对数据包包裹一层携带 `client_fd` 标识的透明信封，业务协议完全由后端的 Java/Python 动态控制。

---

## 📐 通信架构数据流向

整个系统的网络 I/O 与业务处理采用分层架构，数据在各模块间的流向如下：

1. **公网入站**：[外部客户端群] ──(TCP 长连接)──> [C++ 主线程 epoll_wait]
2. **内部并发**：[C++ 主线程] ──(非阻塞事件分发)──> [线程池工作线程]
3. **协议打包**：[工作线程] ──(注入 client_fd 信封头)──> [BusinessBridge (UDS)]
4. **业务转发**：[BusinessBridge] ──(Unix 域套接字 IPC)──> [本地业务进程 (Java/Python)]
5. **异步唤醒**：[退出/控制信号] ──(write 8字节)──> [eventfd] ──> 强制唤醒 [epoll_wait]

---

## 📋 内部透明通信协议格式

为了让 C++ 代理在完全不理解 Java/Python 具体业务内容的前提下，能够精准地把响应回传给特定客户端，UDS 管道传输时采用了自定义的“最简信封协议”：

| 字段名 | 类型/长度 | 作用说明 |
| :--- | :--- | :--- |
| **payload_len** | `uint32_t` (4 字节) | 后面紧跟的原始业务数据（Payload）的实际字节长度 |
| **client_fd** | `uint32_t` (4 字节) | 该数据所属的客户端 TCP 套接字标识，用于上行回传路由 |
| **Business Data** | 变长字节流 | 真实的业务数据（如 JSON、Protobuf 或自定义协议） |

* **下行流**：C++ 收到 TCP 数据 -> 头部拼接 8 字节信封 -> 塞入 UDS -> Java 业务层。
* **上行流**：Java 处理完毕 -> 保持 8 字节信封头不变（内含原 `client_fd`） -> 塞回 UDS -> C++ 解析出 `client_fd` -> 发送给对应的真实公网客户端。

---

## 🚀 编译与运行

### 环境依赖
* Linux Kernel 3.9+ (需要支持 `SO_REUSEPORT`, `accept4`, `eventfd`)
* 支持 C++11 及以上标准的编译器 (如 GCC 4.8+)

### 编译步骤
```bash
mkdir build && cd build
cmake ..
make
