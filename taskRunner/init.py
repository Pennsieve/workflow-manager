import json
import requests
import sys

from api import AuthenticationClient, WorkflowInstanceClient, WorkflowClient
from config import Config
from datetime import datetime, timezone

def main():
    config = Config()

    workflow_instance_id = sys.argv[1]
    session_token = sys.argv[2]

    workflow_instance_client = WorkflowInstanceClient(config.API_HOST2)
    workflow_instance = workflow_instance_client.get_workflow_instance(workflow_instance_id, session_token)

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
