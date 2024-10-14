#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <queue>
#include <mutex>
#include <thread>
#include <functional>



template<typename T>
class threadsafe_queue {
    private:
        mutable std::mutex mut; 
        std::queue<T> data_queue;
        std::condition_variable data_cond;

    public:
        threadsafe_queue () {}

        threadsafe_queue(threadsafe_queue const& other) {
            std::lock_guard<std::mutex> lk(other.mut);
            data_queue = other.data_queue;
        }

        void push(const T& new_value) {
            std::lock_guard<std::mutex> lk(mut);
            data_queue.push(new_value);
            data_cond.notify_one();
        }

        void push(T&& value) {
            std::lock_guard<std::mutex> lk(mut);
            data_queue.push(value);
            data_cond.notify_one();
        }

        void wait_and_pop(T& value) {
            std::unique_lock<std::mutex> lk(mut);
            data_cond.wait(lk, [this] { return !data_queue.empty(); });
            value = std::move(data_queue.front());
            data_queue.pop();
        }

        std::shared_ptr<T> wait_and_pop() {
            std::unique_lock<std::mutex> lk(mut);
            data_cond.wait(lk, [this] { return !data_queue.empty(); });
            std::shared_ptr<T> res(std::make_shared<T>(data_queue.front()));
            data_queue.pop();
            return res;
        }

        bool try_pop(T& value) {
            std::lock_guard<std::mutex> lk(mut);
            if (data_queue.empty()) {
                return false;
            }
            value = data_queue.front();
            data_queue.pop();
            return true;
        }

        std::shared_ptr<T> try_pop() {
            std::lock_guard<std::mutex> lk(mut);
            if(data_queue.empty()) {
                return std::shared_ptr<T>();
            }

            std::shared_ptr<T> res(std::make_shared<T>(data_queue.front()));
            data_queue.pop();
            return res;
        }

        bool empty() const {
            std::lock_guard<std::mutex> lk(mut);
            return data_queue.empty();
        }
};


/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        using TaskTypeInternal = std::function<void (void)>;
        std::vector<std::thread> m_threads;
        std::vector<double> m_threads_time_cost;
        std::vector<int> m_workloads;
        int m_num_threads;
        double m_time_thres;
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        using TaskTypeInternal = std::function<void (void)>;
        std::vector<std::thread> m_threads;
        std::vector<threadsafe_queue<TaskTypeInternal>> m_thread_task_pool;
        int m_num_threads;
        bool m_is_finished;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        using TaskTypeInternal = std::function<void (void)>;
        std::vector<std::thread> m_threads;
        std::queue<TaskTypeInternal> m_task_pool;
        std::mutex m_mutex;
        std::condition_variable m_cond_var;
        bool m_is_finished;
};

#endif
