import json
import requests
import sys

from api import AuthenticationClient, WorkflowInstanceClient
from config import Config

def main():
    config = Config()

    workflow_instance_id = sys.argv[1]
    api_key = sys.argv[2]
    api_secret = sys.argv[3] 

    auth_client = AuthenticationClient(config.API_HOST)
    session_token = auth_client.authenticate(api_key, api_secret)

    workflow_instance_client = WorkflowInstanceClient(config.API_HOST2)
    workflow_instance = workflow_instance_client.get_workflow_instance(workflow_instance_id, session_token)

    print(json.dumps(workflow_instance["workflow"]))

if __name__ == '__main__':
    main()
