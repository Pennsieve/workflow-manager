import json
import requests
import sys

from api import AuthenticationClient, WorkflowInstanceClient, WorkflowClient
from config import Config
from datetime import datetime, timezone

def main():
    config = Config()

    workflow_instance_id = sys.argv[1]
    api_key = sys.argv[2]
    api_secret = sys.argv[3] 

    auth_client = AuthenticationClient(config.API_HOST)
    session_token = auth_client.authenticate(api_key, api_secret)

    workflow_instance_client = WorkflowInstanceClient(config.API_HOST2)
    workflow_instance = workflow_instance_client.get_workflow_instance(workflow_instance_id, session_token)

    # now = datetime.now(timezone.utc).timestamp()
    # workflow_instance_client.put_workflow_instance_status(workflow_instance_id, 'STARTED', now, session_token)

    workflowMapperObject = {}
    workflowMapperObject["v1"] = workflow_instance.get("workflow")

    workflowUuid = workflow_instance.get("workflowUuid")
    if workflowUuid is not None:
        workflow_client = WorkflowClient(config.API_HOST2)
        workflow_v2 = workflow_client.get_workflow(workflowUuid, session_token)
        workflowMapperObject["v2"] = workflow_v2
    else:
        workflowMapperObject["v2"] = None   
    
    print(json.dumps(workflowMapperObject), end="")


if __name__ == '__main__':
    main()
