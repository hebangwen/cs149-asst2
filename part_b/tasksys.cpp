#include "tasksys.h"
#include <chrono>
#include <mutex>
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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    m_num_threads = num_threads;
    m_task_counter = 0;

    for (int i = 0; i < m_num_threads; i++) {
        m_threads.emplace_back([&, i] () {
            while (!m_has_finished) {
                TaskTypeInternal task;
                {
                    std::unique_lock<std::mutex> lock(m_nodep_mutex);
                    m_nodep_cv.wait(lock, [&] () { return !m_nodep_tasks.empty() || m_has_finished; });
                    if (!m_nodep_tasks.empty()) {
                        task = std::move(m_nodep_tasks.front());
                        m_nodep_tasks.pop();
                    }
                }

                if (task) {
                    printf("%s::%d\t\trun task!\n", __func__, __LINE__);
                    task();
                    printf("%s::%d\t\tfinish task!\n", __func__, __LINE__);
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    m_has_finished = true;
    m_nodep_cv.notify_all();
    for (auto& t : m_threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    for (auto* ptr : m_all_tasks) {
        delete ptr;
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    // std::this_thread::sleep_for(std::chrono::seconds(1));
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    int task_id = m_task_counter++;
    auto* task_record = new TaskRecord<int>{};
    task_record->task_id = task_id;
    task_record->dep_tasks = deps;
    task_record->promises.resize(num_total_tasks);

    m_all_tasks.push_back(task_record);

    printf("%s::%d\t\tsubmit task, record id %d, total task %d!\n", 
            __func__, __LINE__, task_id, num_total_tasks);

    if (deps.empty()) {
        std::unique_lock<std::mutex> lock(m_nodep_mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            auto& promise = task_record->promises[i];
            task_record->futures.push_back(
                promise.get_future());

            m_nodep_tasks.push([&runnable, &promise, i, num_total_tasks] () {
                printf("%s::%d\t\trunning task %d/%d\n",
                        __func__, __LINE__, i, num_total_tasks);
                runnable->runTask(i, num_total_tasks);
                promise.set_value(0);
                printf("%s::%d\t\tfinish task %d/%d\n",
                        __func__, __LINE__, i, num_total_tasks);
            });
            m_nodep_cv.notify_one();
        }
        lock.unlock();

        m_running_tasks.push_back(task_record);
    } else {

        for (int i = 0; i < num_total_tasks; i++) {
            auto& promise = task_record->promises[i];
            task_record->futures.push_back(
                promise.get_future());
            task_record->tasks.emplace_back(
                [&runnable, &promise, i, num_total_tasks] () {
                    printf("%s::%d\t\trunning task %d/%d\n",
                            __func__, __LINE__, i, num_total_tasks);
                    runnable->runTask(i, num_total_tasks);
                    promise.set_value(0);
                }
            );
        }

        m_dep_tasks.push_back(task_record);
    }

    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    printf("%s::%d\t\tsync, task size: %zu!\n", 
            __func__, __LINE__, m_running_tasks.size());

    while (!m_running_tasks.empty()) {
        for (auto* ptr : m_running_tasks) {
            printf("%s::%d\t\tfuture size: %zu\n", 
                    __func__, __LINE__, ptr->futures.size());
            for (auto& future : ptr->futures) {
                future.wait();
            }
        }
        m_running_tasks.clear();
    printf("%s::%d\t\twaited!\n", __func__, __LINE__);

        std::deque<TaskRecord<int> *> nodep_tasks_ptrs;

        for (auto it = m_dep_tasks.begin(); it != m_dep_tasks.end(); ) {
            auto& deps = (*it)->dep_tasks;
            bool is_deps_in_nodep_queue = true;
            for (auto& task_id : deps) {
                if (!m_all_tasks[task_id]->dep_tasks.empty()) {
                    is_deps_in_nodep_queue = false;
                    break;
                }
            }

            if (is_deps_in_nodep_queue) {
                auto* ptr = *it;
                ptr->dep_tasks.clear();
                nodep_tasks_ptrs.push_back(ptr);

                it = m_dep_tasks.erase(it);
            } else {
                it++;
            }
        }

        std::unique_lock<std::mutex> lock(m_nodep_mutex);
        while (!nodep_tasks_ptrs.empty()) {
            auto* task_record_ptr = nodep_tasks_ptrs.front();
            for (auto& task : task_record_ptr->tasks) {
                m_nodep_tasks.push(std::move(task));
                m_nodep_cv.notify_one();
            }

            task_record_ptr->dep_tasks.clear();
            task_record_ptr->tasks.clear();
            m_running_tasks.push_back(task_record_ptr);

            nodep_tasks_ptrs.pop_front();
        }
        lock.unlock();
    }

    return;
}
