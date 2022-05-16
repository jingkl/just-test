import os
import random
import json
import math
import string
import numpy as np
import pandas as pd
from yaml import full_load
import copy
from sklearn import preprocessing
from typing import List

from utils.util_log import log
from utils.util_catch import func_request


""" common func """


def dict_recursive_key(_dict, key=None):
    if isinstance(_dict, dict):
        key_list = list(_dict.keys())

        for k in key_list:
            if isinstance(_dict[k], dict):
                dict_recursive_key(_dict[k], key)

            if key is None:
                if _dict[k] is key:
                    del _dict[k]
            else:
                if _dict[k] == key:
                    del _dict[k]
    return _dict


def modify_file(file_path_list, is_modify=False, input_content=""):
    """
    file_path_list : file list -> list[<file_path>]
    is_modify : does the file need to be reset
    input_content ：the content that need to insert to the file
    """
    if not isinstance(file_path_list, list):
        log.error("[modify_file] file is not a list.")

    for file_path in file_path_list:
        if not file_path:
            continue
        folder_path, file_name = os.path.split(file_path)
        if not os.path.isdir(folder_path):
            log.debug("[modify_file] folder(%s) is not exist." % folder_path)
            os.makedirs(folder_path)

        if not os.path.isfile(file_path):
            log.error("[modify_file] file(%s) is not exist." % file_path)
        else:
            if is_modify is True:
                log.debug("[modify_file] start modifying file(%s)..." % file_path)
                with open(file_path, "r+") as f:
                    f.seek(0)
                    f.truncate()
                    f.write(input_content)
                    f.close()
                log.info("[modify_file] file(%s) modification is complete." % file_path)


def read_json_file(file_path):
    if not isinstance(file_path, str):
        log.error("[read_json_file] Param of file_path({}) is not a str.".format(type(file_path)))
        return {}

    if not os.path.isfile(file_path):
        log.error("[read_json_file] file(%s) is not exist." % file_path)
        return {}

    try:
        with open(file_path) as f:
            file_dict = json.load(f)
            f.close()
    except Exception as e:
        file_dict = {}
        log.error("[read_json_file] Can not open json file({0}), error: {1}".format(file_path, e))
    finally:
        if f:
            f.close()
    return file_dict


def read_yaml_file(file_path):
    if not isinstance(file_path, str):
        log.error("[read_yaml_file] Param of file_path({}) is not a str.".format(type(file_path)))
        return {}

    if not os.path.isfile(file_path):
        log.error("[read_yaml_file] file(%s) is not exist." % file_path)
        return {}

    try:
        with open(file_path) as f:
            file_dict = full_load(f)
            f.close()
    except Exception as e:
        file_dict = {}
        log.error("[read_yaml_file] Can not open yaml file({0}), error: {1}".format(file_path, e))
    finally:
        if f:
            f.close()
    return file_dict


def parser_input_config(input_content):
    msg = "[parser_input_config] Can not parser input config: {0}, type: {1}".format(input_content, type(input_content))
    if input_content in ["", None, {}]:
        _content = {}
    elif isinstance(input_content, str):
        if input_content.endswith('json'):
            _content = read_json_file(input_content)
        elif input_content.endswith('yaml'):
            _content = read_yaml_file(input_content)
        else:
            try:
                _content = eval(input_content)
            except Exception:
                raise Exception(msg)
    elif isinstance(input_content, dict):
        _content = input_content
    else:
        raise Exception(msg)
    return _content


def dict_update(source, target):
    for key, value in source.items():
        if isinstance(value, dict) and key in target:
            dict_update(source[key], target[key])
        else:
            target[key] = value
    return target


def update_dict_value(server_resource, values_dict):
    if not isinstance(server_resource, dict) or not isinstance(values_dict, dict):
        return values_dict

    _source = copy.deepcopy(server_resource)
    _target = copy.deepcopy(values_dict)

    target = dict_update(_source, _target)

    return target


def execute_funcs(funcs: List[tuple]):
    for func in funcs:
        if len(func) == 1:
            args = func[0]
            kwargs = {}
            func_request(args, **kwargs)
        elif len(func) == 2:
            args = func[0]
            kwargs = func[1]
            func_request(args, **kwargs)
        else:
            log.error("[execute_funcs] Parameter error: {0}".format(func))


def truncated_output(context, row_length=300):
    _str = str(context)
    return _str[0:row_length] + '......' if len(_str) > row_length else _str
