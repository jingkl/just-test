import sys
import numpy as np

from client.cases.base import Base
from client.common.common_func import get_source_file, read_ann_hdf5_file, normalize_data, get_acc_metric_type


class AccCases(Base):
    """
    neighbors: used to compare with search results, topk <= columns(100), nq <= rows(10000)
    test: vector argument for search
    train: vector to insert into database
    distances: dis between neighbors and test
    """

    dataset_neighbors = []
    dataset_test = []
    dataset_train = []
    dataset_distances = []

    def sift_128_euclidean(self):
        # file parsing
        self.parsing_file(sys._getframe().f_code.co_name)

        # params check: single instance
        self.parsing_params()

        # test steps

        # result check and return

    def sift_256_hamming(self):
        pass

    def glove_200_angular(self):
        pass

    def glove_100_angular(self):
        pass

    def glove_25_angular(self):
        pass

    def kosarak_27983_jaccard(self):
        pass

    def parsing_params(self):
        pass

    def parsing_file(self, key):
        src_file = get_source_file(key)
        data_set = read_ann_hdf5_file(src_file)
        metric_type = get_acc_metric_type(src_file)

        self.dataset_neighbors = np.array(data_set["neighbors"])
        self.dataset_test = normalize_data(metric_type, np.array(data_set["test"]))
        self.dataset_train = normalize_data(metric_type, np.array(data_set["train"]))


if __name__ == '__main__':
    t = AccCases()
    t.sift_128_euclidean()