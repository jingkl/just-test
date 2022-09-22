import logging
import sys
import copy
from collections import Counter

from configs.log_config import log_config
from commons.common_type import LogLevel
from commons.common_params import EnvVariable


class TestLogConfig:
    def __init__(self, logger, log_debug, log_info, log_err, log_level, use_stream=True):
        self.logger = logger
        self.log_debug = log_debug
        self.log_info = log_info
        self.log_err = log_err
        self.log_level = eval("logging.{0}".format(log_level)) if hasattr(LogLevel, log_level) else logging.DEBUG
        self.handlers = []

        self.log = logging.getLogger(self.logger)
        self.log.setLevel(logging.DEBUG)
        self.log.makeRecord = self.makeRecord
        self.set_log_level(use_stream=use_stream)

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
                   func=None, extra=None, sinfo=None):
        """
        A factory method which can be overridden in subclasses to create
        specialized LogRecords.
        """
        # print("[TestLogConfig] makeRecord has been called.")
        rv = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)
        if extra is not None:
            for key in extra:
                if key in ["filename", "lineno"]:
                    continue
                if (key in ["message", "asctime"]) or (key in rv.__dict__):
                    raise KeyError("Attempt to overwrite %r in LogRecord" % key)
                rv.__dict__[key] = extra[key]
        return rv

    def set_log_level(self, use_stream=True):
        try:
            _format = "[%(asctime)s - %(levelname)5s - %(name)s]: %(message)s (%(filename)s:%(lineno)s)"
            formatter = logging.Formatter(_format)

            dh = logging.FileHandler(self.log_debug)
            dh.setLevel(logging.DEBUG)
            dh.setFormatter(formatter)
            self.log.addHandler(dh)
            self.handlers.append(dh)

            fh = logging.FileHandler(self.log_info)
            fh.setLevel(logging.INFO)
            fh.setFormatter(formatter)
            self.log.addHandler(fh)
            self.handlers.append(fh)

            eh = logging.FileHandler(self.log_err)
            eh.setLevel(logging.ERROR)
            eh.setFormatter(formatter)
            self.log.addHandler(eh)
            self.handlers.append(eh)

            if use_stream:
                sh = logging.StreamHandler(sys.stdout)
                sh.setLevel(self.log_level)
                sh.setFormatter(formatter)
                self.log.addHandler(sh)
                self.handlers.append(sh)

        except Exception as e:
            print("Can not use %s or %s or %s to log : %s" % (self.log_debug, self.log_info, self.log_err, str(e)))


class TestLog:
    log_level = EnvVariable.LOG_LEVEL

    def __init__(self, logger="fouram", log_debug=log_config.log_debug, log_info=log_config.log_info,
                 log_err=log_config.log_err):
        self.logger = logger
        self.log_debug = log_debug
        self.log_info = log_info
        self.log_err = log_err
        self.log_handlers_parents = []
        self.log_handlers = []

        self.test_log = TestLogConfig(logger, self.log_debug, self.log_info, self.log_err, self.log_level).log
        print("[TestLog] Test log level:{0}, log file:{1},{2},{3}".format(self.log_level, self.log_debug, self.log_info,
                                                                          self.log_err))

        self.log_handlers_parents.extend(self.test_log.handlers)

    @property
    def log_msg(self):
        return "[TestLog] Test log level:{0}, log file:{1},{2},{3}".format(self.log_level, self.log_debug,
                                                                           self.log_info, self.log_err)

    def debug(self, msg, *args, **kwargs):
        # if self.log_level == LogLevel.DEBUG:
        self.test_log.debug(msg, extra={"filename": str(sys._getframe().f_back.f_code.co_filename).split('/')[-1],
                                        "lineno": sys._getframe().f_back.f_lineno}, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.test_log.info(msg, extra={"filename": str(sys._getframe().f_back.f_code.co_filename).split('/')[-1],
                                       "lineno": sys._getframe().f_back.f_lineno}, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.test_log.warning(msg, extra={"filename": str(sys._getframe().f_back.f_code.co_filename).split('/')[-1],
                                          "lineno": sys._getframe().f_back.f_lineno}, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.test_log.error(msg, extra={"filename": str(sys._getframe().f_back.f_code.co_filename).split('/')[-1],
                                        "lineno": sys._getframe().f_back.f_lineno}, *args, **kwargs)

    def print(self, *args, sep=' ', end='\n', file=None):
        print(*args, sep=sep, end=end, file=file)

    def reset_log_file_path(self, subfolder="", use_stream=False):
        self.remove_log_handlers()

        log_config.get_default_config(subfolder=subfolder)
        self.log_debug = log_config.log_debug
        self.log_info = log_config.log_info
        self.log_err = log_config.log_err

        self.test_log = TestLogConfig(self.logger, self.log_debug, self.log_info, self.log_err, self.log_level,
                                      use_stream=use_stream).log
        print("[TestLog] Test log level:{0}, log file:{1},{2},{3}".format(self.log_level, self.log_debug, self.log_info,
                                                                          self.log_err))
        self.log_handlers.extend(self.test_log.handlers)

    def remove_log_handlers(self):
        for i in self.log_handlers:
            if i not in self.log_handlers_parents:
                self.test_log.removeHandler(i)
        self.log_handlers = []


"""All modules share this unified log"""
log = TestLog()
