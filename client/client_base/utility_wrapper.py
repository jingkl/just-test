import sys
import time
from datetime import datetime

from pymilvus import utility

from client.util.api_request import api_request
from client.check.func_check import ResponseChecker
from client.common.common_param import InterfaceResponse


TIMEOUT = 600


class ApiUtilityWrapper:
    """ Method of encapsulating utility files """

    ut = utility

    def bulk_load(self, collection_name, partition_name="", channels="", row_based=True, files="", timeout=None,
                  using="default", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.bulk_load, collection_name, partition_name, channels, row_based, files,
                                       timeout, using], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       collection_name=collection_name, using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def get_bulk_load_state(self, task_id, timeout=None, using="default", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.get_bulk_load_state, task_id, timeout, using], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, task_id=task_id,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def wait_for_bulk_load_tasks_completed(self, task_ids, timeout=None, using="default", **kwargs):
        start = time.time()
        successes = {}
        fails = {}
        if timeout is not None:
            task_timeout = timeout / len(task_ids)
        else:
            task_timeout = TIMEOUT
        while True and (len(successes) + len(fails)) < len(task_ids):
            in_progress = {}
            time.sleep(0.5)
            for task_id in task_ids:
                if successes.get(task_id, None) is not None or fails.get(task_id, None) is not None:
                    continue
                else:
                    state, _ = self.get_bulk_load_state(task_id, task_timeout, using, **kwargs)
                    if state.state_name == "BulkLoadPersisted":  # "BulkLoadCompleted"
                        successes[task_id] = state
                    elif state.state_name == "BulkLoadFailed":
                        fails[task_id] = state
                    else:
                        in_progress[task_id] = state
            end = time.time()
            if timeout is not None:
                if end - start > timeout:
                    in_progress.update(fails)
                    in_progress.update(successes)
                    return False, in_progress

        if len(fails) == 0:
            return True, successes
        else:
            fails.update(successes)
            return False, fails

    def get_query_segment_info(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.get_query_segment_info, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       collection_name=collection_name, timeout=timeout, using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def loading_progress(self, collection_name, partition_names=None, using="default", check_task=None,
                         check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.loading_progress, collection_name, partition_names, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       collection_name=collection_name, partition_names=partition_names,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def wait_for_loading_complete(self, collection_name, partition_names=None, timeout=None, using="default",
                                  check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.wait_for_loading_complete, collection_name, partition_names, timeout,
                                       using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       collection_name=collection_name, partition_names=partition_names,
                                       timeout=timeout, using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def index_building_progress(self, collection_name, index_name="", using="default", check_task=None,
                                check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.index_building_progress, collection_name, index_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       collection_name=collection_name, index_name=index_name,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def wait_for_index_building_complete(self, collection_name, index_name="", timeout=None, using="default",
                                         check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.wait_for_index_building_complete, collection_name,
                                    index_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       collection_name=collection_name, index_name=index_name,
                                       timeout=timeout, using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def has_collection(self, collection_name, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.has_collection, collection_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       collection_name=collection_name, using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def has_partition(self, collection_name, partition_name, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.has_partition, collection_name, partition_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       collection_name=collection_name, partition_name=partition_name,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop_collection(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.drop_collection, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result,
                                       collection_name=collection_name, timeout=timeout, using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def list_collections(self, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.list_collections, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def calc_distance(self, vectors_left, vectors_right, params=None, timeout=None, using="default", check_task=None,
                      check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.calc_distance, vectors_left, vectors_right, params, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def load_balance(self, collection_name, src_node_id, dst_node_ids, sealed_segment_ids, timeout=None,
                     using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.load_balance, collection_name, src_node_id, dst_node_ids,
                                       sealed_segment_ids, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def create_alias(self, collection_name, alias, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.create_alias, collection_name, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop_alias(self, alias, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.drop_alias, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def alter_alias(self, collection_name, alias, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.alter_alias, collection_name, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def list_aliases(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.list_aliases, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def mkts_from_datetime(self, d_time=None, milliseconds=0., delta=None):
        d_time = datetime.now() if d_time is None else d_time
        res, res_result = api_request([self.ut.mkts_from_datetime, d_time, milliseconds, delta])
        return InterfaceResponse(*res, res_result, res_result)

    def mkts_from_hybridts(self, hybridts, milliseconds=0., delta=None):
        res, res_result = api_request([self.ut.mkts_from_hybridts, hybridts, milliseconds, delta])
        return InterfaceResponse(*res, res_result, res_result)

    def create_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.create_resource_group, name, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def drop_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.drop_resource_group, name, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def describe_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.describe_resource_group, name, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def list_resource_groups(self, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.list_resource_groups, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def transfer_node(self, source, target, num_node, using="default", timeout=None, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.transfer_node, source, target, num_node, using, timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)

    def transfer_replica(self, source, target, collection_name, num_replica, using="default", timeout=None,
                         check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, res_result = api_request([self.ut.transfer_replica, source, target, collection_name, num_replica, using,
                                       timeout])
        check_result = ResponseChecker(res, func_name, check_task, check_items, res_result, timeout=timeout,
                                       using=using).run()
        return InterfaceResponse(*res, res_result, check_result)
