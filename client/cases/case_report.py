from client.common.common_func import update_dict_value


class CasesReport:
    def __init__(self):
        pass

    def add_attr(self, update=False, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)
            else:
                self.__dict__[k] = v if not update else update_dict_value(v, self.__dict__[k])

    def to_dict(self):
        return self.__dict__
