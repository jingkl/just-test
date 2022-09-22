# -*- coding: utf-8 -*-
import time
import datetime
import logging
import string
import random
import os
import requests
import json
import copy
import pytz
from yaml import full_load, dump
import tableprint as tp
from pprint import pprint

from deploy.commons.common_params import CLUSTER, STANDALONE, dataNode, queryNode, indexNode, proxy, APIVERSION, \
    DefaultApiVersion, ClassID
from utils.util_log import log


def get_api_version(kind):
    if kind not in APIVERSION.keys():
        return APIVERSION[DefaultApiVersion]
    return APIVERSION[kind]


def dict_to_str(source, target, target_list):
    for key, value in source.items():
        if isinstance(value, dict):
            _t = ".%s" % key if target != "" else key
            dict_to_str(source[key], target + _t, target_list)
        else:
            _t = ".%s=%s," % (key, str(value))
            target_list.append(target + _t)

    return target_list


def dict_to_set_str(source_dict):
    """
    :param source_dict: {'dataNode': {'replicas': 1}, 'queryNode': {'replicas': 1}, 'test': 'str'}
    :return: "dataNode.replicas=1,queryNode.replicas=1,test=str"
    """

    if not isinstance(source_dict, dict):
        return ""

    _target_list = dict_to_str(source_dict, "", [])

    _target = ""
    for i in _target_list:
        _target += str(i)

    return _target[:-1]


def dict_update(source, target):
    for key, value in source.items():
        if isinstance(value, dict) and key in target:
            dict_update(source[key], target[key])
        else:
            target[key] = value
    return target


def update_dict_value(server_resource, values_dict):
    """
    from benedict import benedict
    target = benedict(server_resource).deepcopy()
    update_dict = [values_dict, {}, {"a": 1}]
    target.merge(*update_dict, overwrite=True, concat=False)
    """
    if not isinstance(server_resource, dict) or not isinstance(values_dict, dict):
        return values_dict

    _source = copy.deepcopy(server_resource)
    _target = copy.deepcopy(values_dict)

    target = dict_update(_source, _target)

    return target


def read_file(file_path):
    if not isinstance(file_path, str):
        log.error("[read_file] Param of file_path({}) is not a str.".format(type(file_path)))
        return {}

    if not os.path.isfile(file_path):
        log.error("[read_file] file(%s) is not exist." % file_path)
        return {}

    try:
        with open(file_path) as f:
            file_content = f.read()
            f.close()
    except Exception as e:
        file_content = ""
        log.error("[read_file] Can not open file({0}), error: {1}".format(file_path, e))
    finally:
        if f:
            f.close()
    return file_content


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


def write_yaml_file(file_path, values_dict):
    if not isinstance(file_path, str):
        log.error("[write_yaml_file] Param of file_path({}) is not a str.".format(type(file_path)))
        return {}

    if not os.path.isfile(file_path):
        log.error("[write_yaml_file] file(%s) is not exist." % file_path)
        return {}

    try:
        with open(file_path, 'w') as f:
            dump(values_dict, f, default_flow_style=False)
        f.close()
    except Exception as e:
        log.error("[write_yaml_file] Can not open yaml file({0}), error: {1}".format(file_path, e))


def get_token(url):
    rep = requests.get(url)
    data = json.loads(rep.text)
    if 'token' in data:
        token = data['token']
    else:
        token = ''
        log.error("[get_token] Can not get token.")
    return token


def get_tags(url, token):
    headers = {'Content-type': "application/json",
               "charset": "UTF-8",
               "Accept": "application/vnd.docker.distribution.manifest.v2+json",
               "Authorization": "Bearer %s" % token}
    try:
        rep = requests.get(url, headers=headers)
        data = json.loads(rep.text)

        tags = []
        if 'tags' in data:
            tags = data["tags"]
        else:
            log.error("[get_tags] Can not get the tag list")
        return tags
    except:
        log.error("[get_tags] Can not get the tag list")
        return []


def get_master_tags(tags_list, prefix):
    _list = []

    if not isinstance(tags_list, list):
        log.error("[get_master_tags] tags_list is not a list.")
        return _list

    for tag in tags_list:
        if prefix in tag and tag != prefix + "-latest":
            _list.append(tag)
    return _list


def get_config_digest(url, token):
    headers = {'Content-type': "application/json",
               "charset": "UTF-8",
               "Accept": "application/vnd.docker.distribution.manifest.v2+json",
               "Authorization": "Bearer %s" % token}
    try:
        rep = requests.get(url, headers=headers)
        data = json.loads(rep.text)

        digest = ''
        if 'config' in data and 'digest' in data["config"]:
            digest = data["config"]["digest"]
        else:
            log.error("[get_config_digest] Can not get the digest")
        return digest
    except:
        log.error("[get_config_digest] Can not get the digest")
        return ""


def get_latest_tag(limit=100, prefix="master", repository="milvusdb/milvus-dev"):
    service = "registry.docker.io"

    auth_url = "https://auth.docker.io/token?service=%s&scope=repository:%s:pull" % (service, repository)
    tags_url = "https://index.docker.io/v2/%s/tags/list" % repository
    tag_url = "https://index.docker.io/v2/milvusdb/milvus-dev/manifests/"

    master_latest_digest = get_config_digest(tag_url + prefix + "-latest", get_token(auth_url))
    tags = get_tags(tags_url, get_token(auth_url))
    tag_list = get_master_tags(tags, prefix)

    latest_tag = ""
    for i in range(1, len(tag_list) + 1):
        tag_name = str(tag_list[-i])
        tag_digest = get_config_digest(tag_url + tag_name, get_token(auth_url))
        if tag_digest == master_latest_digest:
            latest_tag = tag_name
            break
        if i > limit:
            break

    if latest_tag == "":
        latest_tag = prefix + "-latest"
        log.error("[get_latest_tag] Can't find the latest image name")
    log.info("[get_latest_tag] The image name used is %s" % str(latest_tag))
    return latest_tag


def get_image_tag():
    url = "https://harbor.zilliz.cc/api/v2.0/projects/milvus/repositories/milvus/artifacts?page=1&page_size=1&with_" + \
          "tag=true&with_label=false&with_scan_overview=false&with_signature=false&with_immutable_status=false"
    headers = {"accept": "application/json",
               "X-Accept-Vulnerabilities": "application/vnd.scanner.adapter.vuln.report.harbor+json; version=1.0"}
    try:
        rep = requests.get(url, headers=headers)
        data = json.loads(rep.text)
        tag_name = data[0]["tags"][0]["name"]
        log.info("[get_image_tag] The image name used is %s" % str(tag_name))
        return tag_name
    except:
        log.error("[get_image_tag] Can not get the tag list")
        return "master-latest"


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


def gen_release_name(prefix=''):
    return prefix + '-' + str(random.randint(1, 100)) + '-' + str(random.randint(1000, 10000))


def server_resource_check(other_configs):
    if not isinstance(other_configs, list):
        log.error("[server_resource_check] the type of other_config is not list: {}".format(other_configs))
        return []

    for other_config in other_configs:
        if not isinstance(other_config, dict):
            log.error("[DefaultConfigs] The elements of the list are not dictionaries: {}".format(other_config))
            log.error("[DefaultConfigs] The param of other_configs: {}".format(other_configs))
            return []

    return other_configs


def gen_server_config_name(cpu, mem, cluster=True, **kwargs):
    _name = CLUSTER if cluster else STANDALONE

    _name += "_{0}c{1}m".format(cpu, mem)

    keys = kwargs.keys()
    check_list = [dataNode, queryNode, indexNode, proxy]
    for i in keys:
        if i in check_list:
            _name += "_{0}{1}".format(i, kwargs[i])
    log.debug("[gen_server_config_name] server config name: {}".format(_name))
    return _name


def utc_conversion(utc_str=""):
    utc_format = "%Y-%m-%dT%H:%M:%SZ"
    utc_time = datetime.datetime.strptime(utc_str, utc_format)
    today = datetime.datetime.utcnow()

    _delta = today - utc_time
    delta = _delta.total_seconds()
    _hour = int(delta / 3600)
    _minute = int((delta - _hour * 3600) / 60)
    delta_time = str(_delta)
    if _hour <= 0 and _minute <= 0:
        delta_time = "{0}s".format(delta)
    elif _hour > 0 and _minute > 0:
        delta_time = "{0}h{1}m".format(_hour, _minute)
    elif _hour > 0 and _minute == 0:
        delta_time = "{0}h".format(_hour)
    elif _hour == 0 and _minute > 0:
        delta_time = "{0}m".format(_minute)

    return delta_time


def format_dict_output(data_keys: tuple, data_list: list):
    output_str = ""
    len_dict = {}
    for k in data_keys:
        len_dict.update({k: len(k)})

    for _dict in data_list:
        if not isinstance(_dict, dict):
            return data_list
        for k in data_keys:
            if len_dict[k] < len(str(_dict[k])):
                len_dict.update({k: len(str(_dict[k]))})

    title = ""
    for k in data_keys:
        title += str(k).ljust(len_dict[k] + 5)
    # log.info(title)
    output_str += title + '\n'

    values = []
    for _dict in data_list:
        _value = ""
        for k in data_keys:
            _value += str(_dict[k]).ljust(len_dict[k] + 5)
        values.append(_value)

    for v in values:
        # log.info(v)
        output_str += v + '\n'
    log.info("\n" + output_str)
    return output_str
    # return {"title": data_keys,
    #         "values": values}


def check_multi_keys_exist(target: dict, keys: list):
    """
    :return key's value
    """
    t = target
    for k in keys:
        if k in t:
            t = t[k]
        else:
            raise ValueError("[check_multi_keys_exist] Keys:{0} not in target dict:{1}".format(keys, target))
    return t


def parser_op_item(item: dict):
    # _tt = utc_conversion(item["metadata"]["creationTimestamp"])
    # return {"NAME": item["metadata"]["name"],
    #         "STATUS": item["status"]["phase"],
    #         "RESTARTS": item["status"]["containerStatuses"][0]["restartCount"],
    #         "AGE": _tt,
    #         "IP": item["status"]["podIP"],
    #         "NODE": item["spec"]["nodeName"]}

    container_status = check_multi_keys_exist(item, ["status", "containerStatuses"])
    max_count = 0
    for c in container_status:
        if "restartCount" in c:
            max_count = c["restartCount"] if c["restartCount"] > max_count else max_count
    _tt = utc_conversion(check_multi_keys_exist(item, ["metadata", "creationTimestamp"]))
    return {"NAME": check_multi_keys_exist(item, ["metadata", "name"]),
            "STATUS": check_multi_keys_exist(item, ["status", "phase"]),
            "RESTARTS": max_count,
            "AGE": _tt,
            "IP": check_multi_keys_exist(item, ["status", "podIP"]),
            "NODE": check_multi_keys_exist(item, ["spec", "nodeName"])}


def hide_value(source, keys):
    for key, value in source.items():
        if isinstance(value, dict) and key not in keys:
            hide_value(source[key], keys)
        if key in keys and not isinstance(value, dict):
            source[key] = "***"
    return source


def hide_dict_value(source, keys):
    if not isinstance(source, dict) or not isinstance(keys, list):
        return source
    _s = copy.deepcopy(source)
    target = hide_value(_s, keys)
    return target


def find_key(resource: dict, key: str, values: list):
    for k, v in resource.items():
        if isinstance(v, dict):
            find_key(v, key, values)
        if key == k and str(v).isdigit():
            _v = int(resource["replicas"]) * v if "replicas" in resource else v
            values.append(int(_v))
    return values


def add_resource(resource_dict, key):
    res = find_key(resource_dict, key, [])
    return sum(res)


def get_class_mode(deploy_mode, deploy_class):
    """
    :param deploy_mode: cluster or standalone
    :param deploy_class: str
    :return: tuple
    """
    if hasattr(ClassID, deploy_class):
        if deploy_class in ["XLarge", "XXLarge"]:
            return eval("ClassID.{0}".format(deploy_class)), CLUSTER
        else:
            return eval("ClassID.{0}".format(deploy_class)), STANDALONE
    else:
        class_mode = "XLarge" if deploy_mode == CLUSTER else "Free"
        return eval("ClassID.{0}".format(class_mode)), deploy_mode


def get_vdc_server_resource(resource):
    template = {
        "imageTag": -1,
        "rootCoord": {
            "replicas": -1,
            "cpu": -1,
            "memory": -1
        },
        "dataCoord": {
            "replicas": -1,
            "cpu": -1,
            "memory": -1
        },
        "indexCoord": {
            "replicas": -1,
            "cpu": -1,
            "memory": -1
        },
        "dataNode": {
            "replicas": -1,
            "cpu": -1,
            "memory": -1
        },
        "indexNode": {
            "replicas": -1,
            "cpu": -1,
            "memory": -1
        },
        "queryNode": {
            "replicas": -1,
            "cpu": -1,
            "memory": -1
        },
        "proxy": {
            "replicas": -1,
            "cpu": -1,
            "memory": -1
        },
        "standalone": {
            "replicas": -1,
            "cpu": -1,
            "memory": -1
        }
    }

