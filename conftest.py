import pytest
from utils.util_log import log
from configs.log_config import log_config
from commons.common_func import modify_file
from parameters.input_params import param_info, InputParamsBase
from deploy.commons.common_params import Helm, Operator, STANDALONE


def pytest_addoption(parser):
    parser.addoption("--client_version", action="store", default="2.2", help="client version")
    parser.addoption("--host", action="store", default="localhost", help="service's ip")
    parser.addoption("--port", action="store", default=19530, help="service's port")
    parser.addoption("--tag", action="store", default="all", help="only run tests matching the tag.")
    parser.addoption('--clean_log', action='store_true', default=False, help="clean log before testing")
    parser.addoption('--secure', action='store_true', default=False, help="using secure when connection server")
    parser.addoption("--user", action="store", default="", help="enable secure and set user name")
    parser.addoption("--password", action="store", default="", help="enable secure and set user password")
    parser.addoption("--run_id", action="store", default=None, help="run id for client test")
    parser.addoption('--err_msg', action='store', default="err_msg", help="error message of test")
    parser.addoption("--milvus_tag", action="store", default=None, help="Milvus container tag")
    parser.addoption("--milvus_tag_prefix", action="store", default="", help="Milvus container tag prefix")
    parser.addoption("--tag_repository", action="store", default=None, help="tag repository")
    parser.addoption("--update_helm_file", action="store_true", default=False, help="update helm file values.yaml")
    parser.addoption("--release_name_prefix", action="store", default="", help="release name prefix")
    parser.addoption("--release_name", action="store", default="", help="release name")
    parser.addoption('--sync_report', action='store_true', default=False, help="sync report result")
    parser.addoption('--async_report', action='store_true', default=False, help="async report result")
    # case input params
    parser.addoption("--deploy_skip", action="store_true", default=False,
                     help="skip deploy, use the incoming address or the default address")
    parser.addoption("--client_test_skip", action="store_true", default=False,
                     help="skip client test, only deploy or other test")
    parser.addoption("--deploy_tool", action="store", default=Helm, help="helm or operator")
    parser.addoption("--deploy_mode", action="store", default="", help="standalone or cluster")
    parser.addoption("--deploy_config", action="store", default="", help="str or dict")
    parser.addoption("--upgrade_config", action="store", default="", help="str or dict")
    parser.addoption('--deploy_retain', action='store_true', default=False, help="retain Milvus")
    parser.addoption("--case_params", action="store", default="", help="str or dict")
    parser.addoption('--case_skip_prepare', action='store_true', default=False, help="skip prepare collection")
    parser.addoption('--case_skip_prepare_clean', action='store_true', default=False,
                     help="skip clean collection before test")
    parser.addoption('--case_skip_build_index', action='store_true', default=False, help="skip rebuild index")
    parser.addoption('--case_skip_clean_collection', action='store_true', default=False, help="skip remove collection")


@pytest.fixture
def host(request):
    return request.config.getoption("--host")


@pytest.fixture
def port(request):
    return request.config.getoption("--port")


""" fixture func """


@pytest.fixture(scope="session", autouse=True)
def initialize_env(request):
    """ clean log before testing """
    client_version = request.config.getoption("--client_version")
    host = request.config.getoption("--host")
    port = request.config.getoption("--port")
    clean_log = request.config.getoption("--clean_log")
    secure = request.config.getoption("--secure")
    user = request.config.getoption("--user")
    password = request.config.getoption("--password")
    run_id = request.config.getoption("--run_id")
    milvus_tag = request.config.getoption("--milvus_tag")
    milvus_tag_prefix = request.config.getoption("--milvus_tag_prefix")
    tag_repository = request.config.getoption("--tag_repository")
    update_helm_file = request.config.getoption("--update_helm_file")
    deploy_skip = request.config.getoption("--deploy_skip")
    deploy_retain = request.config.getoption("--deploy_retain")
    client_test_skip = request.config.getoption("--client_test_skip")
    release_name_prefix = request.config.getoption("--release_name_prefix")
    release_name = request.config.getoption("--release_name")
    sync_report = request.config.getoption("--sync_report")
    async_report = request.config.getoption("--async_report")
    # release_name_prefix = getattr(request.config.option, "release_name_prefix")

    """ params check """
    # assert ip_check(host) and number_check(port)

    # increase the capacity of the stack
    # sys.setrecursionlimit(10000)

    """ modify log files """
    file_path_list = [log_config.log_debug, log_config.log_info, log_config.log_err]
    modify_file(file_path_list=file_path_list, is_modify=clean_log)

    log.info("#" * 80)
    log.info("[initialize_milvus] Log cleaned up, start testing...")
    param_info.prepare_param_info(client_version, host, port, secure=secure, milvus_tag=milvus_tag,
                                  milvus_tag_prefix=milvus_tag_prefix, tag_repository=tag_repository,
                                  deploy_skip=deploy_skip, deploy_retain=deploy_retain, run_id=run_id,
                                  client_test_skip=client_test_skip, update_helm_file=update_helm_file,
                                  release_name_prefix=release_name_prefix, release_name=release_name,
                                  sync_report=sync_report, async_report=async_report, param_user=user,
                                  param_password=password)
    log.info("[initialize_milvus] Global parameters: {0}".format(param_info.to_dict()))
    # yield
    # if param_info.test_status is False:
    #     msg = "Test result is False, please check!!!"
    #     log.error(msg)
    #     assert False
    #     sys.exit(-1)
    #     pytest.exit(msg)


@pytest.fixture(scope="session")
def input_params(request) -> InputParamsBase:
    return InputParamsBase(**{
        "deploy_tool": str(request.config.getoption("--deploy_tool")).lower(),
        "deploy_mode": str(request.config.getoption("--deploy_mode")).lower(),
        "deploy_config": request.config.getoption("--deploy_config"),
        "upgrade_config": request.config.getoption("--upgrade_config"),
        "case_params": request.config.getoption("--case_params"),
        "case_skip_prepare": request.config.getoption("--case_skip_prepare"),
        "case_skip_prepare_clean": request.config.getoption("--case_skip_prepare_clean"),
        "case_skip_build_index": request.config.getoption("--case_skip_build_index"),
        "case_skip_clean_collection": request.config.getoption("--case_skip_clean_collection")
    })


@pytest.fixture(scope="function")
def clear_env(request):
    log.info("[clear_env] init clear env")

    def fin():
        try:
            log.info("[clear_env] clear env")
            pass
        except Exception as e:
            log.error(e)

    request.addfinalizer(fin)


# for test exit in the future
# @pytest.hookimpl(hookwrapper=True, tryfirst=True)
# def pytest_runtest_makereport():
#     result = yield
#     report = result.get_result()
#     if report.outcome == "failed":
#         msg = "The execution of the test case fails and the test exits..."
#         log.error(msg)
#         pytest.exit(msg)
