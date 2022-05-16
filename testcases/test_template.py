import pytest
import time
import os

from client.client_base.client_base import TestcaseBase
from utils.util_log import log

from db_client.client_db import DBClient


class TestTemplate(TestcaseBase):
    """classdef    ::=  [decorators] "class" classname [inheritance] ":" suite
    inheritance ::=  "(" [parameter_list] ")"
    classname   ::=  identifier
    """

    def test_1(self):
        log.debug(" test debug ")
        log.info(" test info   ")
        log.error(" test info   ")
        res = DBClient().mongo_db_client.query({"run_id": 1600884560})
        log.error(res)

    def test_2(self):
        log.debug(" test debug2 ")
        log.info(" test info2   ")
        log.error(" test info2   ")
        # res = DBClient().mongo_db_client.query({"run_id": 1600884560})
        # log.error(res)
        os.environ["jingjing"] = "jingjing"
        log.error(os.environ.keys())
        log.error(os.getenv('jingjing'))

