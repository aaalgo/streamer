#pragma once
#include <atomic>
#include <queue>
#include <thread>
#include <condition_variable>
#include <boost/thread.hpp>
#include <boost/python.hpp>
#include <boost/python/raw_function.hpp>

namespace streamer {

    namespace py = boost::python;

    namespace impl {

        template<typename T>
        class TaskQueue
        {
            std::condition_variable enqueue_cond;
            std::condition_variable dequeue_cond;
            std::mutex mutex;
            std::queue<T> queue;
            size_t max;
            int producers;
            std::atomic<int> done;
        public:
            TaskQueue (size_t max_, int producers_ = 1) : max(max_), producers(producers_), done(0) {
            }

            void finish () {
                ++done;
                if (done >= producers)
                    dequeue_cond.notify_all();
            }

            void enqueue (T item)
            {
                std::unique_lock<std::mutex> lock(mutex);
                enqueue_cond.wait(lock, [this](){ return queue.size() < max; });
                queue.push(item);
                dequeue_cond.notify_all();
            }

            bool dequeue (T *item)
            {
                std::unique_lock<std::mutex> lock(mutex);
                dequeue_cond.wait(lock, [this]() { return queue.size() > 0 || done >= producers; });
                if (queue.size() > 0) {
                    std::swap(*item, queue.front());
                    queue.pop();
                    enqueue_cond.notify_all();
                    return true;
                }
                return false;
            }

            bool dequeue_python (T *item)
            {
                std::unique_lock<std::mutex> lock(mutex);
                Py_BEGIN_ALLOW_THREADS
                dequeue_cond.wait(lock, [this]() { return queue.size() > 0 || done >= producers; });
                Py_END_ALLOW_THREADS
                if (queue.size() > 0) {
                    std::swap(*item, queue.front());
                    queue.pop();
                    enqueue_cond.notify_all();
                    return true;
                }
                return false;
            }
        };
    }

    class ScopedGState {
        bool lock;
        PyGILState_STATE gstate;
    public:
        ScopedGState (bool l = true): lock(l), gstate(PyGILState_UNLOCKED) {
            if (lock) {
                gstate = PyGILState_Ensure();
            }
        }
        ~ScopedGState () {
            if (lock) {
                PyGILState_Release(gstate);
            }
        }
    };

    template <typename Task>
    class Streamer {

        impl::TaskQueue<Task *> input_queue;
        impl::TaskQueue<py::object *> output_queue;
        boost::thread_group producer_threads, worker_threads;

        virtual Task *stage1 (py::object *) = 0;
        virtual py::object *stage2 (Task *task) = 0;

        void producer (py::object *next) {
            for (;;) {
                py::object *obj = nullptr;
                try {
                    ScopedGState _;
                    obj = new py::object((*next)());
                }
                catch (...) {
                    ScopedGState _;
                    delete next;
                    break;
                }
                input_queue.enqueue(stage1(obj));
            }
            input_queue.finish();
        }

        void worker (void) {
            for (;;) {
                Task *task;
                if (!input_queue.dequeue(&task)) break;
                output_queue.enqueue(stage2(task));
            }
            output_queue.finish();
        }

    public:
        Streamer (py::object gen, int workers, int depth=128)
            : input_queue(depth, 1),
            output_queue(depth, workers) {

            py::object *next = new py::object(gen.attr("__next__"));
            producer_threads.create_thread([this, &next](){producer(next);});
            for (int i = 0; i < workers; ++i) {
                worker_threads.create_thread([this](){worker();});
            }
        }

        ~Streamer () {
            producer_threads.join_all();
            worker_threads.join_all();
        }

        py::object next () {
            py::object *item;
            if (!output_queue.dequeue_python(&item)) {
                return py::object();
            }
            py::object x = *item;
            delete item;
            return x;
        }
    };

    template <typename T>
    void register_streamer (char const *name = "Streamer") {
        PyEval_InitThreads();
        py::class_<Streamer<T>, boost::noncopyable>(name, py::init<py::object, int>())
            .def("__next__", &Streamer<T>::next)
        ;
    }
}
