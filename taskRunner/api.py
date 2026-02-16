import boto3
import requests
import json
import logging
import base64

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

            cognito_app_client_id = data["userPool"]["appClientId"]
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
            refresh_token = login_response["AuthenticationResult"]["RefreshToken"]
            return access_token, refresh_token
        except requests.HTTPError as e:
            log.error(f"failed to reach authentication server with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode authentication response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to authenticate with error: {e}")
            raise e

    def refresh_token(self, refresh_token, device_key=None, session_token=None):
        url = f"{self.api_host}/authentication/cognito-config"

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = json.loads(response.content)

            cognito_app_client_id = data["userPool"]["appClientId"]
            cognito_region = data["region"]

            cognito_idp_client = boto3.client(
                "cognito-idp",
                region_name=cognito_region,
                aws_access_key_id="",
                aws_secret_access_key="",
            )

            auth_parameters = {"REFRESH_TOKEN": refresh_token}

            # If device_key not provided but session_token is, extract device_key from token
            if not device_key and session_token:
                try:
                    decoded = self.decode_token(session_token)
                    device_key = decoded.get("device_key")
                    if device_key:
                        log.info(f"extracted device_key from session_token: {device_key}")
                except Exception as e:
                    log.warning(f"failed to extract device_key from session_token: {e}")

            if device_key:
                auth_parameters["DEVICE_KEY"] = device_key

            refresh_response = cognito_idp_client.initiate_auth(
                AuthFlow="REFRESH_TOKEN_AUTH",
                AuthParameters=auth_parameters,
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

            cognito_app_client_id = data["userPool"]["appClientId"]
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

    def decode_token(self, session_token):
        """
        Decode JWT token to extract user information without verification.
        Note: This does not verify the token signature, only decodes the payload.

        Returns dict with user info like:
        {
            "sub": "user-uuid",
            "cognito:username": "username",
            "email": "user@example.com",
            "cognito:groups": ["group1", "group2"],
            ...
        }
        """
        try:
            # JWT format: header.payload.signature
            parts = session_token.split('.')
            if len(parts) != 3:
                raise ValueError("Invalid JWT token format")

            # Decode the payload (second part)
            payload = parts[1]
            # Add padding if needed
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding

            decoded_bytes = base64.urlsafe_b64decode(payload)
            decoded_json = json.loads(decoded_bytes)

            return decoded_json
        except Exception as e:
            log.error(f"failed to decode token with error: {e}")
            raise e

class WorkflowInstanceClient:
    def __init__(self, api_host):
        self.api_host = api_host

    def get_workflow_instance(self, workflow_instance_id, session_token):
        url = f"{self.api_host}/compute/workflows/instances/{workflow_instance_id}"

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
        url = f"{self.api_host}/compute/workflows/instances/{workflow_instance_id}/status"

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
        url = f"{self.api_host}/compute/workflows/instances/{workflow_instance_id}/processor/{processor_id}/status"

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
        url = f"{self.api_host}/compute/workflows/definitions/{workflowUuid}"

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

class UserClient:
    def __init__(self, api_host):
        self.api_host = api_host

    def get_user_profile(self, session_token):
        """
        Get the authenticated user's profile information from Pennsieve API.
        This makes an API call to get full user details.
        """
        url = f"{self.api_host}/user"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {session_token}"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            user_profile = response.json()

            return user_profile
        except requests.HTTPError as e:
            log.error(f"failed to fetch user profile with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode user profile response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to get user profile with error: {e}")
            raise e

    def create_api_key(self, session_token, name="workflow-manager"):
        """
        Create a new API key/secret pair for the authenticated user.
        This can be used with the pennsieve-agent.
        """
        url = f"{self.api_host}/token?api_key"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {session_token}"
        }

        try:
            response = requests.post(url, json={"name": name}, headers=headers)
            response.raise_for_status()
            data = response.json()

            return data["key"], data["secret"]
        except requests.HTTPError as e:
            log.error(f"failed to create API key with error: {e}")
            raise e
        except json.JSONDecodeError as e:
            log.error(f"failed to decode API key response with error: {e}")
            raise e
        except Exception as e:
            log.error(f"failed to create API key with error: {e}")
            raise e