
from locust import User, TaskSet, events

from client.common.common_type import concurrent_global_params
from client.parameters.params import ConcurrentTasksParams


def event_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            exception = None if result[1] is True else "False"
            rt = result[0][1] * 1000
            events.request.fire(request_type=concurrent_global_params.request_type, name=func.__name__,
                                response_time=rt, response_length=0, exception=exception, context=User.context)
        return inner_wrapper
    return wrapper


class ClientTask:
    def __init__(self, obj: callable, tasks_params: ConcurrentTasksParams, request_type: str = "grpc"):
        self.obj = obj
        self.tasks_params = tasks_params
        concurrent_global_params.request_type = request_type

    def __getattr__(self, name):
        func = getattr(self.obj, "concurrent_{0}".format(name))

        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            exception = None if result[1] is True else "False"
            rt = result[0][1] * 1000
            events.request.fire(request_type=concurrent_global_params.request_type, name=name,
                                response_time=rt, response_length=0, exception=exception, context=User.context)

        return wrapper

    # @event_catch()
    # def debug(self):
    #     return self.obj.concurrent_debug(self.tasks_params.debug.params)
    #
    # @event_catch()
    # def search(self):
    #     return self.obj.concurrent_search(self.tasks_params.search.params)
    #
    # @event_catch()
    # def query(self):
    #     return self.obj.concurrent_query(self.tasks_params.query.params)
    #
    # @event_catch()
    # def flush(self):
    #     return self.obj.concurrent_flush(self.tasks_params.flush.params)
    #
    # @event_catch()
    # def load(self):
    #     return self.obj.concurrent_load(self.tasks_params.load.params)
    #
    # @event_catch()
    # def release(self):
    #     return self.obj.concurrent_release(self.tasks_params.release.params)
    #
    # @event_catch()
    # def insert(self):
    #     return self.obj.concurrent_insert(self.tasks_params.insert.params)
    #
    # @event_catch()
    # def delete(self):
    #     return self.obj.concurrent_delete(self.tasks_params.delete.params)
    #
    # @event_catch()
    # def scene_test(self):
    #     return self.obj.concurrent_scene_test(self.tasks_params.scene_test.params)
    #
    # @event_catch()
    # def scene_insert_delete_flush(self):
    #     return self.obj.concurrent_scene_insert_delete_flush(self.tasks_params.scene_insert_delete_flush.params)


class MyTaskSet(TaskSet):

    def debug(self):
        self.client.debug(self.tasks_params.debug.params)

    def search(self):
        self.client.search(self.tasks_params.search.params)

    def query(self):
        self.client.query(self.tasks_params.query.params)

    def flush(self):
        self.client.flush(self.tasks_params.flush.params)

    def load(self):
        self.client.load(self.tasks_params.load.params)

    def release(self):
        self.client.release(self.tasks_params.release.params)

    def insert(self):
        self.client.insert(self.tasks_params.insert.params)

    def delete(self):
        self.client.delete(self.tasks_params.delete.params)

    def scene_test(self):
        self.client.scene_test(self.tasks_params.scene_test.params)

    def scene_insert_delete_flush(self):
        self.client.scene_insert_delete_flush(self.tasks_params.scene_insert_delete_flush.params)
