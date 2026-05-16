#include "thread_pool.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>

void ThreadPool::worker() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex);
            condition.wait(lock, [this]() { return !tasks.empty() || stop; });
            if (stop && tasks.empty()) {
                exited_workers.fetch_add(1);
                return;
            }
            if (!tasks.empty()) {
                task = tasks.front();
                tasks.pop();
            } else {
                continue;
            }
            if (active_threads.load() >= threads.size()) {
                expand_threads(threads.size() * 2);
            }
            active_threads.fetch_add(1);
        }
        try {
            if (task) {
                task();
            }
        } catch (const std::exception& e) {
            std::cerr << "ThreadPool: exception in task: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "ThreadPool: unknown exception in task" << std::endl;
        }
        active_threads.fetch_sub(1);
    }
}

void ThreadPool::shutdown() {
    // TcpServer::stop() 会先 shutdown；析构 ThreadPool 时可能再调用一次，需幂等。
    if (threads.empty()) {
        return;
    }

    const std::size_t expected_exits = threads.size();

    {
        std::unique_lock<std::mutex> lock(mutex);
        stop.store(true);
    }
    condition.notify_all();

    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(SHUTDOWN_JOIN_TIMEOUT_MS);

    while (std::chrono::steady_clock::now() < deadline) {
        if (exited_workers.load(std::memory_order_acquire) >= expected_exits) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (exited_workers.load(std::memory_order_acquire) >= expected_exits) {
        for (std::thread& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    } else {
        std::cerr << "ThreadPool: shutdown join timed out after " << SHUTDOWN_JOIN_TIMEOUT_MS
                  << " ms, detaching remaining joinable workers\n";
        for (std::thread& t : threads) {
            if (t.joinable()) {
                t.detach();
            }
        }
    }

    threads.clear();
    active_threads = 0;
    exited_workers.store(0);
    stop.store(false);
}

bool ThreadPool::expand_threads(std::size_t new_threads) {
    if (new_threads <= threads.size()) {
        return false;
    }
    if (threads.size() >= static_cast<std::size_t>(MAX_THREADS)) {
        std::cerr << "ThreadPool: max_threads is already at MAX_THREADS" << std::endl;
        return false;
    }
    const std::size_t target = std::min(new_threads, static_cast<std::size_t>(MAX_THREADS));
    const std::size_t old_size = threads.size();
    threads.resize(target);
    for (std::size_t i = old_size; i < target; ++i) {
        threads[i] = std::thread(&ThreadPool::worker, this);
    }
    return true;
}

ThreadPool::ThreadPool(std::size_t num_threads) {
    if (num_threads > MAX_THREADS) {
        throw std::runtime_error("ThreadPool: num_threads > MAX_THREADS");
    }
    stop.store(false);
    threads.resize(std::max(num_threads, static_cast<std::size_t>(MIN_THREADS)));
    for (std::size_t i = 0; i < threads.size(); i++) {
        threads[i] = std::thread(&ThreadPool::worker, this);
    }
    active_threads = 0;
    exited_workers = 0;
}

ThreadPool::~ThreadPool() {
    shutdown();
}

bool ThreadPool::submit(std::function<void()> task) {
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (stop) {
            throw std::runtime_error("ThreadPool is stopped");
        }
        if (tasks.size() > MAX_TASKS) {
            while (tasks.size() > MAX_TASKS) {
                condition.wait(lock, [this]() { return tasks.size() < MAX_TASKS || stop; });
            }
            if (stop) {
                std::cerr << "ThreadPool is stopped" << std::endl;
                return false;
            }
        }
        tasks.emplace(std::move(task));
    }
    condition.notify_one();
    return true;
}
