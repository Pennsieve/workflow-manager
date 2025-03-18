import json
import requests
import sys
from boto3 import client as boto3_client
import shutil

from api import AuthenticationClient, WorkflowInstanceClient
from config import Config
from datetime import datetime, timezone

def main():
    config = Config()

    workflow_instance_id = sys.argv[1]
    api_key = sys.argv[2]
    api_secret = sys.argv[3]
    output_directory = sys.argv[4]

    auth_client = AuthenticationClient(config.API_HOST)
    workflow_instance_client = WorkflowInstanceClient(config.API_HOST2)

    session_token = auth_client.authenticate(api_key, api_secret)
    workflow_instance_client.put_workflow_instance_status(
        workflow_instance_id,
        'SUCCEEDED',
        datetime.now(timezone.utc).timestamp(),
        session_token
    )

    # start visualization task 
    workflow_instance = workflow_instance_client.get_workflow_instance(workflow_instance_id, session_token)
    workflow_instance_params = workflow_instance["params"]
   
    if config.IS_LOCAL:
        return
    
    ecs_client = boto3_client("ecs", region_name=config.REGION)
    if workflow_instance_params["visualize"] == "true":
        response = start_visualization_task(ecs_client, config)
        print(json.dumps(response))
    else:
        # Clear output directory
        try:
            shutil.rmtree(f'{output_directory}/{workflow_instance_id}')
        except Exception as e:
            print(f"Failed to delete directory: {e}")

def start_visualization_task(ecs_client, config):
    if config.IS_LOCAL:
        return "local-task-arn","container/task-arn/local"

    if not serving_requests:
        return ecs_client.update_service(
            cluster=config.CLUSTER_NAME,
            service=config.VIZ_CONTAINER_NAME,
            desiredCount=1
        )

    return {}

def serving_requests(ecs_client, cluster_name, service_name):    
    # Get service details
    response = ecs_client.describe_services(
        cluster=cluster_name,
        services=[service_name]
    )
    
    # Check if service exists
    if not response['services']:
        return False
    
    # Get running count
    running_count = response['services'][0]['runningCount']
    
    # Return whether service has running tasks
    return running_count > 0

if __name__ == '__main__':
    main()
