
class CasesReport:
    def __init__(self):
        pass

    def add_attr(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                setattr(self, k, v)
            else:
                self.__dict__[k] = v

    def to_dict(self):
        return self.__dict__
