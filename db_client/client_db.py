from configs.config_info import config_info
from db_client.client_influx_db_v1 import ClientInfluxDBV1
from db_client.client_influx_db import ClientInfluxDB
from db_client.client_mongo_db import ClientMongoDB
from utils.util_log import log


class DBClient:
    def __init__(self):
        self.config = config_info

        self.influx_db_clients = []
        self.influx_db_v1_clients = []
        self.mongo_db_clients = []
        self.init_clients()

    def influx_insert(self, tags=None, fields=None, time=None, measurement=None):
        for c in self.influx_db_clients + self.influx_db_v1_clients:
            c.insert(measurement=measurement, tags=tags, fields=fields, time=time)

    def mongo_insert(self, data):
        for c in self.mongo_db_clients:
            c.insert(data)

    def _influx_v2(self):
        for k, v in self.config.influx_db_params.items():
            if isinstance(v, dict) and sorted(["bucket", "token", "org", "url"]) == sorted(v.keys()):
                v.update({"bucket": "fouram"})
                self.influx_db_clients.append(ClientInfluxDB(**v))
            else:
                log.error("[DBClient] Failed to initialize influxDB client : {}".format(k))

    def _influx_v1(self):
        for k, v in self.config.influx_db_v1_params.items():
            if isinstance(v, dict) and sorted(["host", "port", "username", "password", "database"]) == sorted(v.keys()):
                v.update({"database": "fouram"})
                self.influx_db_v1_clients.append(ClientInfluxDBV1(**v))
            else:
                log.error("[DBClient] Failed to initialize influxDBV1 client : {}".format(k))

    def _mongo_db(self):
        for k, v in self.config.mongo_db_servers.items():
            if isinstance(v, str) and v != "":
                self.mongo_db_clients.append(ClientMongoDB(v, dbname="fouram", collection_name="configs"))
            else:
                log.error("[DBClient] Failed to initialize MongoDB client : {}".format(k))

    def init_clients(self):
        self._influx_v1()
        self._influx_v2()
        self._mongo_db()


Database_Client = DBClient()

