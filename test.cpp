#include "streamer.h"


using namespace streamer;

struct Loader {

    struct Task {
        int value;
    };

    Task *stage1 (py::object *obj) {
        Task *task = new Task;
        {
            ScopedGState _;
            task->value = py::extract<int>(*obj);
            delete obj;
        }
        return task;
    }

    py::object *stage2 (Task *task) {
        ScopedGState _;
        py::object *obj = new py::object(task->value);
        delete task;
        return obj;
    }
};

BOOST_PYTHON_MODULE(cpp)
{
    register_streamer<Loader>();
}

