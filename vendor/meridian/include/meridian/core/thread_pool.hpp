#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

namespace meridian {

// A bounded thread pool for parallel task execution
// - Fixed number of worker threads
// - Task queue with configurable max size
// - Graceful shutdown with optional task completion
class ThreadPool {
public:
    // Create a thread pool with specified number of threads
    // max_queue_size: 0 = unlimited, otherwise limits pending tasks
    explicit ThreadPool(size_t num_threads = std::thread::hardware_concurrency(),
                        size_t max_queue_size = 0)
        : max_queue_size_(max_queue_size)
        , stopping_(false)
        , stopped_(false) {
        if (num_threads == 0) {
            num_threads = 1;
        }

        workers_.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] {
                worker_loop();
            });
        }
    }

    ~ThreadPool() {
        shutdown();
    }

    // Non-copyable
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // Move-only
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    // Submit a task and get a future for the result
    template<typename F, typename... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {

        using ReturnType = std::invoke_result_t<F, Args...>;

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

        std::future<ReturnType> result = task->get_future();

        {
            std::unique_lock<std::mutex> lock(mutex_);

            if (stopping_) {
                throw std::runtime_error("Cannot submit to stopped thread pool");
            }

            // Wait if queue is full
            if (max_queue_size_ > 0) {
                queue_not_full_.wait(lock, [this] {
                    return tasks_.size() < max_queue_size_ || stopping_;
                });
            }

            if (stopping_) {
                throw std::runtime_error("Cannot submit to stopped thread pool");
            }

            tasks_.emplace([task]() { (*task)(); });
        }

        queue_not_empty_.notify_one();
        return result;
    }

    // Submit a task without waiting for result (fire and forget)
    template<typename F, typename... Args>
    void execute(F&& f, Args&&... args) {
        auto bound = std::bind(std::forward<F>(f), std::forward<Args>(args)...);

        {
            std::unique_lock<std::mutex> lock(mutex_);

            if (stopping_) {
                return; // Silently drop
            }

            // Wait if queue is full
            if (max_queue_size_ > 0) {
                queue_not_full_.wait(lock, [this] {
                    return tasks_.size() < max_queue_size_ || stopping_;
                });
            }

            if (stopping_) {
                return;
            }

            tasks_.emplace([bound = std::move(bound)]() mutable { bound(); });
        }

        queue_not_empty_.notify_one();
    }

    // Wait for all pending tasks to complete
    void wait_all() {
        std::unique_lock<std::mutex> lock(mutex_);
        all_done_.wait(lock, [this] {
            return tasks_.empty() && active_tasks_ == 0;
        });
    }

    // Shutdown the thread pool
    // wait_for_tasks: if true, complete pending tasks before stopping
    void shutdown(bool wait_for_tasks = true) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_) return;
            stopping_ = true;

            if (!wait_for_tasks) {
                // Clear pending tasks
                std::queue<std::function<void()>> empty;
                std::swap(tasks_, empty);
            }
        }

        queue_not_empty_.notify_all();
        queue_not_full_.notify_all();

        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }

        stopped_ = true;
    }

    // Force-shutdown the thread pool with a timeout.
    // Clears pending tasks, waits up to `timeout` for active tasks to finish,
    // then detaches any workers still stuck in long-running blocking calls
    // (e.g., safe_exec subprocess I/O). This prevents the caller from hanging
    // indefinitely when workers are stuck in uninterruptible operations.
    void force_shutdown(std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_) return;
            stopping_ = true;

            // Clear pending tasks
            std::queue<std::function<void()>> empty;
            std::swap(tasks_, empty);
        }

        queue_not_empty_.notify_all();
        queue_not_full_.notify_all();

        // Wait for currently-active tasks to finish (with timeout)
        {
            std::unique_lock<std::mutex> lock(mutex_);
            bool finished = all_done_.wait_for(lock, timeout, [this] {
                return active_tasks_ == 0;
            });

            if (!finished) {
                // Workers are still stuck in tasks — detach them to avoid
                // blocking the caller. The detached threads will be cleaned
                // up when the process exits.
                for (auto& w : workers_) {
                    if (w.joinable()) w.detach();
                }
                stopped_ = true;
                return;
            }
        }

        // All tasks completed; workers will see stopping_+empty queue and exit
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
        stopped_ = true;
    }

    // Get number of worker threads
    size_t size() const { return workers_.size(); }

    // Get number of pending tasks
    size_t pending() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tasks_.size();
    }

    // Check if shutdown was requested
    bool is_stopping() const { return stopping_; }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> task;

            {
                std::unique_lock<std::mutex> lock(mutex_);

                queue_not_empty_.wait(lock, [this] {
                    return stopping_ || !tasks_.empty();
                });

                if (stopping_ && tasks_.empty()) {
                    return;
                }

                task = std::move(tasks_.front());
                tasks_.pop();
                ++active_tasks_;
            }

            queue_not_full_.notify_one();

            try {
                task();
            } catch (...) {
                // Swallow exceptions to prevent thread termination
            }

            {
                std::lock_guard<std::mutex> lock(mutex_);
                --active_tasks_;
            }
            all_done_.notify_all();
        }
    }

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;

    mutable std::mutex mutex_;
    std::condition_variable queue_not_empty_;
    std::condition_variable queue_not_full_;
    std::condition_variable all_done_;

    size_t max_queue_size_;
    size_t active_tasks_ = 0;
    std::atomic<bool> stopping_;
    bool stopped_;
};

// Execute a function for each element in a range using a thread pool
// Returns a vector of results (or void if F returns void)
template<typename Iterator, typename F>
auto parallel_for_each(ThreadPool& pool, Iterator begin, Iterator end, F&& func)
    -> std::vector<std::invoke_result_t<F, decltype(*begin)>> {

    using ReturnType = std::invoke_result_t<F, decltype(*begin)>;

    if constexpr (std::is_void_v<ReturnType>) {
        std::vector<std::future<void>> futures;
        for (auto it = begin; it != end; ++it) {
            futures.push_back(pool.submit([&func, it]() { func(*it); }));
        }
        for (auto& f : futures) {
            f.get(); // Propagate exceptions
        }
        return {};
    } else {
        std::vector<std::future<ReturnType>> futures;
        for (auto it = begin; it != end; ++it) {
            futures.push_back(pool.submit([&func, it]() { return func(*it); }));
        }
        std::vector<ReturnType> results;
        results.reserve(futures.size());
        for (auto& f : futures) {
            results.push_back(f.get());
        }
        return results;
    }
}

// Execute a function for each element in a range using a thread pool
// with progress reporting callback
template<typename Iterator, typename F, typename ProgressFn>
auto parallel_for_each_with_progress(
    ThreadPool& pool,
    Iterator begin, Iterator end,
    F&& func,
    ProgressFn&& progress_fn)
    -> std::vector<std::invoke_result_t<F, decltype(*begin)>> {

    using ReturnType = std::invoke_result_t<F, decltype(*begin)>;

    size_t total = std::distance(begin, end);
    std::atomic<size_t> completed{0};
    std::mutex progress_mutex;

    auto wrapped_func = [&](auto&& item) {
        if constexpr (std::is_void_v<ReturnType>) {
            func(std::forward<decltype(item)>(item));
        } else {
            auto result = func(std::forward<decltype(item)>(item));
            size_t done = ++completed;
            {
                std::lock_guard<std::mutex> lock(progress_mutex);
                progress_fn(done, total);
            }
            return result;
        }
    };

    if constexpr (std::is_void_v<ReturnType>) {
        std::vector<std::future<void>> futures;
        for (auto it = begin; it != end; ++it) {
            futures.push_back(pool.submit([&wrapped_func, it]() {
                wrapped_func(*it);
            }));
        }
        for (auto& f : futures) {
            f.get();
            size_t done = ++completed;
            {
                std::lock_guard<std::mutex> lock(progress_mutex);
                progress_fn(done, total);
            }
        }
        return {};
    } else {
        std::vector<std::future<ReturnType>> futures;
        for (auto it = begin; it != end; ++it) {
            futures.push_back(pool.submit([&wrapped_func, it]() {
                return wrapped_func(*it);
            }));
        }
        std::vector<ReturnType> results;
        results.reserve(futures.size());
        for (auto& f : futures) {
            results.push_back(f.get());
        }
        return results;
    }
}

// Simple parallel map function - creates temporary pool
template<typename Container, typename F>
auto parallel_map(const Container& items, F&& func, size_t num_threads = 0)
    -> std::vector<std::invoke_result_t<F, typename Container::value_type>> {

    if (num_threads == 0) {
        num_threads = std::min(items.size(),
            static_cast<size_t>(std::thread::hardware_concurrency()));
    }
    if (num_threads == 0) num_threads = 1;

    ThreadPool pool(num_threads);
    return parallel_for_each(pool, items.begin(), items.end(),
                             std::forward<F>(func));
}

// Async delivery queue for fire-and-forget operations with bounded concurrency
template<typename T>
class AsyncDeliveryQueue {
public:
    using Handler = std::function<void(T)>;

    AsyncDeliveryQueue(size_t num_workers, size_t max_queue_size, Handler handler)
        : pool_(num_workers, max_queue_size)
        , handler_(std::move(handler)) {}

    // Enqueue an item for async delivery
    // Returns false if queue is full and stopping
    bool enqueue(T item) {
        try {
            pool_.execute([this, item = std::move(item)]() mutable {
                try {
                    handler_(std::move(item));
                } catch (...) {
                    // Swallow delivery errors
                }
            });
            return true;
        } catch (...) {
            return false;
        }
    }

    // Wait for all pending deliveries
    void flush() {
        pool_.wait_all();
    }

    // Shutdown the queue
    void shutdown(bool wait = true) {
        pool_.shutdown(wait);
    }

    size_t pending() const { return pool_.pending(); }

private:
    ThreadPool pool_;
    Handler handler_;
};

} // namespace meridian
