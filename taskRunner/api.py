import boto3
import requests
import json
import logging

log = logging.getLogger()

class AuthenticationClient:
    def __init__(self, api_host):
        self.api_host = api_host

    def authenticate(self, api_key, api_secret):
        url = f"{self.api_host}/authentication/cognito-config"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = json.loads(response.content)

            cognito_app_client_id = data["tokenPool"]["appClientId"]
            cognito_region = data["region"]

            cognito_idp_client = boto3.client(
                "cognito-idp",
                region_name=cognito_region,
                aws_access_key_id="",
                aws_secret_access_key="",
            )

            login_response = cognito_idp_client.initiate_auth(
              AuthFlow="USER_PASSWORD_AUTH",
              AuthParameters={"USERNAME": api_key, "PASSWORD": api_secret},
              ClientId=cognito_app_client_id,
            )

            access_token = login_response["AuthenticationResult"]["AccessToken"]
            return access_token
        except requests.HTTPError as e:
            log.error(f"failed to reach authentication server with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode authentication response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to authenticate with error: {e}")
            raise e

class WorkflowInstanceClient:
    def __init__(self, api_host):
        self.api_host = api_host

    def get_workflow_instance(self, workflow_instance_id, session_token):
        url = f"{self.api_host}/workflows/instances/{workflow_instance_id}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {session_token}"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            workflow_instance = response.json()

            return workflow_instance
        except requests.HTTPError as e:
            log.error(f"failed to fetch workflow instance with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode workflow instance response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to get workflow instance with error: {e}")
            raise e

    def put_workflow_instance(self, workflow_instance_id, completed_at, session_token):
        url = f"{self.api_host}/workflows/instances"

        headers = {
            "Content-Type": 'application/json',
            "Authorization": f"Bearer {session_token}"
        }

        request_body = {
            "uuid": f"{workflow_instance_id}",
            "completedAt": f"{completed_at}"
        }

        try:
            response = requests.put(url, json=request_body, headers=headers)
            response.raise_for_status()
            response_body = response.json()

            return response_body
        except requests.HTTPError as e:
            log.error(f"failed to update workflow instance with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode update workflow instance response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to update workflow instance with error: {e}")
            raise e
