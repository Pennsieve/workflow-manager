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

    def refresh_token(self, refresh_token):
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

            refresh_response = cognito_idp_client.initiate_auth(
                AuthFlow="REFRESH_TOKEN_AUTH",
                AuthParameters={"REFRESH_TOKEN": refresh_token},
                ClientId=cognito_app_client_id,
            )

            access_token = refresh_response["AuthenticationResult"]["AccessToken"]
            return access_token
        except requests.HTTPError as e:
            log.error(f"failed to reach authentication server with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode authentication response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to refresh token with error: {e}")
            raise e

    def revoke_token(self, refresh_token):
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

            cognito_idp_client.revoke_token(
                Token=refresh_token,
                ClientId=cognito_app_client_id,
            )

            log.info("successfully revoked refresh token")
            return True
        except requests.HTTPError as e:
            log.error(f"failed to reach authentication server with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode authentication response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to revoke token with error: {e}")
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

    def put_workflow_instance_status(self, workflow_instance_id, status, timestamp, session_token):
        url = f"{self.api_host}/workflows/instances/{workflow_instance_id}/status"

        headers = {
            "Content-Type": 'application/json',
            "Authorization": f"Bearer {session_token}"
        }

        request_body = {
            "status": f"{status}",
            "timestamp": int(timestamp),
        }

        try:
            response = requests.put(url, json=request_body, headers=headers)
            response.raise_for_status()
            response_body = response.json()

            return response_body
        except requests.HTTPError as e:
            log.error(f"request update workflow instance status failed with error: {e}")
        except json.JSONDecodeError as e:
            log.error(f"failed to decode update workflow instance status response with error: {e}")
        except Exception as e:
            log.error(f"failed to update workflow instance status with error: {e}")

    def put_workflow_instance_processor_status(self, workflow_instance_id, processor_id, status, timestamp, session_token):
        url = f"{self.api_host}/workflows/instances/{workflow_instance_id}/processor/{processor_id}/status"

        headers = {
            "Content-Type": 'application/json',
            "Authorization": f"Bearer {session_token}"
        }

        request_body = {
            "status": f"{status}",
            "timestamp": int(timestamp),
        }

        try:
            response = requests.put(url, json=request_body, headers=headers)
            response.raise_for_status()
            response_body = response.json()

            return response_body
        except requests.HTTPError as e:
            log.error(f"request to update workflow instance processor status failed with error: {e}")
        except json.JSONDecodeError as e:
            log.error(f"failed to decode update workflow instance processor status response with error: {e}")
        except Exception as e:
            log.error(f"failed to update workflow instance processor status with error: {e}")


class WorkflowClient:
    def __init__(self, api_host):
        self.api_host = api_host

    def get_workflow(self, workflowUuid, session_token):
        url = f"{self.api_host}/workflows/{workflowUuid}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {session_token}"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            workflow = response.json()

            return workflow
        except requests.HTTPError as e:
            log.error(f"failed to fetch workflow with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode workflow response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to get workflow with error: {e}")
            raise e
        
class ApplicationClient:
    def __init__(self, api_host):
        self.api_host = api_host

    def get_application(self, source_url, session_token, organization_id):
        url = f"{self.api_host}/applications?organization_id={organization_id}&sourceUrl={source_url}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {session_token}"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            workflow = response.json()

            return workflow
        except requests.HTTPError as e:
            log.error(f"failed to fetch workflow with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode workflow response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to get workflow with error: {e}")
            raise e        