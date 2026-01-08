import json
import requests
import sys
from boto3 import client as boto3_client
import shutil
import logging

from api import AuthenticationClient, WorkflowInstanceClient, UserClient
from config import Config
from datetime import datetime, timezone

logger = logging.getLogger('WorkflowManager')

def main():
    # Setup logging
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    config = Config()

    workflow_instance_id = sys.argv[1]
    session_token = sys.argv[2]
    refresh_token = sys.argv[3]
    api_key = sys.argv[4]

    auth_client = AuthenticationClient(config.API_HOST)
    workflow_instance_client = WorkflowInstanceClient(config.API_HOST2)
    user_client = UserClient(config.API_HOST)

    session_token = auth_client.refresh_token(refresh_token, session_token=session_token)
    workflow_instance_client.put_workflow_instance_status(
        workflow_instance_id,
        'SUCCEEDED',
        datetime.now(timezone.utc).timestamp(),
        session_token
    )

    # Delete the API key created for this workflow
    try:
        user_client.delete_api_key(session_token, api_key)
        logger.info(f"API key deleted successfully: {api_key}")
    except Exception as e:
        # Don't fail the workflow if API key deletion fails
        logger.error(f"failed to delete API key: {e}")

    # Revoke the refresh token after successful completion
    try:
        auth_client.revoke_token(refresh_token)
        logger.info("refresh token revoked successfully")
    except Exception as e:
        # Don't fail the workflow if token revocation fails
        logger.error(f"failed to revoke refresh token: {e}")

if __name__ == '__main__':
    main()
