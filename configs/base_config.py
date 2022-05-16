import os


class BaseConfig:

    @staticmethod
    def get_env_variable(default_var='', default_value=''):
        """ get env variable for testing """
        try:
            return str(os.environ[default_var])
        except Exception as e:
            print("[get_env_variable] %s failed to get environment variables : %s, use default value : %s" %
                  (str(default_var), str(e), default_value))
            return default_value

    @staticmethod
    def create_path(file_path):
        if not os.path.isdir(str(file_path)):
            print("[create_path] folder(%s) is not exist." % file_path)
            print("[create_path] create path now...")
            os.makedirs(file_path)
