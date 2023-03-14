

class RequestResponseParser:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @property
    def code(self):
        for k, v in self.kwargs.items():
            if str(k).lower() == "code":
                return int(v)
        return float("inf")

    @property
    def data(self):
        for k, v in self.kwargs.items():
            if str(k).lower() == "data":
                return v
        return None

    @property
    def message(self):
        for k, v in self.kwargs.items():
            if str(k).lower() == "message":
                return v
        return None

    @property
    def request_id(self):
        return self.kwargs.get("RequestId", None)

    @property
    def to_dict(self):
        return self.kwargs


def request_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            check_result = kwargs.pop("check_result", True)
            res = func(*args, **kwargs)

            response = RequestResponseParser(**res)
            if check_result and response.code != 0:
                raise Exception(f"[request_catch] Request response check failed, code != 0, {res}")
            return response
        return inner_wrapper
    return wrapper
