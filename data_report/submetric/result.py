

class ResultMetric:

    def __init__(self, test_result: dict = {}):
        self.test_result = test_result

    def update(self, test_result: dict = {}):
        self.test_result = test_result or self.test_result

    def clear_property(self):
        self.test_result = {}
