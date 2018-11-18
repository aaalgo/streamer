#include "streamer.h"


using namespace streamer;

struct DummyLoader {
    typedef int Task;

    Task *stage1 (py::object *obj) {
        return new int(py::extract<int>(obj));
    }

    py::object *stage2 (Task *task) {
        PyGILState_STATE gstate;
        gstate = PyGILState_Ensure();
        py::object *obj = new py::object(*task);
        PyGILState_Release(gstate);
        delete task;
        return obj;
    }
};

BOOST_PYTHON_MODULE(cpp)
{
    register_streamer<DummyLoader>();
}

