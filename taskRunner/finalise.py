import json
import requests
import sys

from api import AuthenticationClient, WorkflowInstanceClient
from config import Config
from datetime import datetime, timezone

def main():
    config = Config()

    workflow_instance_id = sys.argv[1]
    api_key = sys.argv[2]
    api_secret = sys.argv[3] 

    auth_client = AuthenticationClient(config.API_HOST)
    workflow_instance_client = WorkflowInstanceClient(config.API_HOST2)

    session_token = auth_client.authenticate(api_key, api_secret)
    response_body = workflow_instance_client.put_workflow_instance_status(
        workflow_instance_id,
        'SUCCEEDED',
        datetime.now(timezone.utc).timestamp(),
        session_token
    )

    print(json.dumps(response_body))

if __name__ == '__main__':
    main()
