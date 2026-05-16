#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#define MAX_THREADS 8
#define MIN_THREADS 4
#define MAX_TASKS 1000

// shutdown 阶段在「全部 worker 已退出循环」上前等待的最长时间；超时则 detach 仍卡住的线程（见 shutdown 注释）。
#define SHUTDOWN_JOIN_TIMEOUT_MS 5000

class ThreadPool {
public:
    explicit ThreadPool(std::size_t num_threads = MIN_THREADS);
    ~ThreadPool();

    bool submit(std::function<void()> task);

    ThreadPool(const ThreadPool& other) = delete;
    ThreadPool& operator=(const ThreadPool& other) = delete;
    ThreadPool(ThreadPool&& other) noexcept = delete;
    ThreadPool& operator=(ThreadPool&& other) noexcept = delete;

    void shutdown();

private:
    std::mutex mutex;
    std::condition_variable condition;
    std::queue<std::function<void()>> tasks;
    std::vector<std::thread> threads;
    std::atomic<std::size_t> active_threads;
    std::atomic<bool> stop;
    std::atomic<std::size_t> exited_workers;

    void worker();
    bool expand_threads(std::size_t new_threads);
};

#endif
