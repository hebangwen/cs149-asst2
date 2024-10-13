#include "tasksys.h"
#include <chrono>
#include <future>
#include <iostream>
#include <sstream>
#include <thread>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), m_num_threads(num_threads) {

}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<std::thread> threads;
    int n = num_total_tasks - num_total_tasks % m_num_threads;
    for (int i = 0; i < n; i += m_num_threads) {
        for (int j = 0; j < m_num_threads; j++) {
            threads.emplace_back([=] () {
                runnable->runTask(i + j, num_total_tasks);
            });
        }

        for (int j = 0; j < m_num_threads; j++) {
            threads[j].join();
        }

        threads.clear();
    }

    {
        int m = num_total_tasks - n;
        for (int j = 0; j < m; j++) {
            threads.emplace_back([=] () {
                runnable->runTask(n + j, num_total_tasks);
            });
        }

        for (int j = 0; j < m; j++) {
            threads[j].join();
        }

        threads.clear();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    m_is_finished = false;

    for (int i = 0; i < num_threads; i++) {
        m_threads.emplace_back([&] () {
            while (!m_is_finished) {
                TaskTypeInternal task;
                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    if (!m_task_pool.empty()) {
                        task = std::move(m_task_pool.front());
                        m_task_pool.pop();
                    }
                }

                if (task) {
                    task();
                }
            }
        });
    }

}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    m_is_finished = true;
    for (auto& thread : m_threads) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<std::promise<int>> promises(num_total_tasks);
    std::vector<std::future<int>>  futures;

    for (int i = 0; i < num_total_tasks; i++) {
        auto& promise = promises[i];
        futures.push_back(promise.get_future());

        std::lock_guard<std::mutex> lock(m_mutex);
        m_task_pool.push([i, &runnable, &num_total_tasks, &promise] () {
            runnable->runTask(i, num_total_tasks);
            promise.set_value(0);
        });
    }

    for (int i = 0; i < num_total_tasks; i++) {
        futures[i].wait();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    m_is_finished = false;

    for (int i = 0; i < num_threads; i++) {
        m_threads.emplace_back([&] () {
            while (true) {
                TaskTypeInternal task;
                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    m_cond_var.wait(lock, [&] () { return !m_task_pool.empty() || m_is_finished; });
                    if (!m_task_pool.empty()) {
                        task = std::move(m_task_pool.front());
                        m_task_pool.pop();
                    }

                    if (m_is_finished) break;
                }

                if (task) {
                    task();
                }
            }
        });
    }


}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    m_is_finished = true;
    m_cond_var.notify_all();
    for (auto& thread : m_threads) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::vector<std::promise<int>> promises(num_total_tasks);
    std::vector<std::future<int>>  futures;

    for (int i = 0; i < num_total_tasks; i++) {
        auto& promise = promises[i];
        futures.push_back(promise.get_future());

        std::lock_guard<std::mutex> lock(m_mutex);
        m_task_pool.push([i, &runnable, &num_total_tasks, &promise] () {
            runnable->runTask(i, num_total_tasks);
            promise.set_value(0);
        });

        m_cond_var.notify_one();
    }

    for (int i = 0; i < num_total_tasks; i++) {
        futures[i].wait();
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
