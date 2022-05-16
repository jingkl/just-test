import uuid

from deploy.commons.common_params import ClassID
from deploy.vdc_rest_api.request_handler import Request


class CloudRMApi:

    def __init__(self, host: str, user_id: str, region_id="", source_app="Cloud-Meta"):
        self.host = host
        self.UserId = user_id
        self.region_id = region_id
        self.SourceApp = source_app
        self.req = Request()

        self.headers = {
            "UserId": self.UserId,
            "RequestId": str(uuid.uuid1()),
            "SourceApp": self.SourceApp,
        }

    def create(self, class_id=ClassID.Free, db_version="v2.0.1", instance_name="benchmark-vdc", mock_tag=False,
               white_list_address="0.0.0.0/0", instance_type=1, project_id="000000000", region_id="",
               instance_description="benchmark test instance", root_pwd="Milvus123"):
        url = self.host + "/resource/v1/instance/milvus/create"
        region_id = region_id if region_id != "" else self.region_id
        body = {
            "classId": class_id,
            "dbVersion": db_version,
            "defaultCharacterset": "UTF-8",
            "defaultTimeZone": "UTC",
            "instanceDescription": instance_description,
            "instanceName": instance_name,
            "instanceType": instance_type,
            "mockTag": mock_tag,
            "projectId": project_id,
            "regionId": region_id,
            "rootPwd": root_pwd,
            "whitelistAddress": white_list_address
        }
        return self.req.post(url=url, body=body, headers=self.headers)

    def delete(self, instance_id="in01-bdee1b970e4b2a20b3ab0659937"):
        url = self.host + "/resource/v1/instance/milvus/delete"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers)

    def describe(self, instance_id: str):
        url = self.host + "/resource/v1/instance/milvus/describe?InstanceId=" + instance_id
        return self.req.get(url=url, headers=self.headers)

    def list(self, class_id=None, db_version=None, instance_description=None, page_num=None, per_page_num=50,
             region_id=None):
        url = self.host + "/resource/v1/instance/milvus/list?perPageNum={0}".format(per_page_num)
        if page_num is not None:
            url += "&pageNum={0}".format(page_num)

        if class_id is not None:
            url += "&classId={0}".format(class_id)

        if db_version is not None:
            url += "&dbVersion={0}".format(db_version)

        if instance_description is not None:
            url += "&instanceDescription={0}".format(instance_description)

        if region_id is not None:
            url += "&regionId={0}".format(region_id)
        return self.req.get(url=url, headers=self.headers)

    def modify(self):
        pass

    def resume(self, instance_id: str):
        url = self.host + "/resource/v1/instance/milvus/resume"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers)

    def stop(self, instance_id: str):
        url = self.host + "/resource/v1/instance/milvus/stop"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers)
