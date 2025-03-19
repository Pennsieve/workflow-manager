import json
import requests
import sys
from boto3 import client as boto3_client
import shutil
import logging

from api import AuthenticationClient, WorkflowInstanceClient
from config import Config
from logger import WorkflowManagerLogger
from datetime import datetime, timezone

logger = logging.getLogger('WorkflowManager')

def main():
    # Setup logging
    logger = WorkflowManagerLogger()
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
    workflow_instance_params = workflow_instance['params']

    logger.LOGGER.info(workflow_instance_params)
    if 'visualize' in workflow_instance_params:
        if workflow_instance_params['visualize'] == "true":
            logger.LOGGER.info("visualize set to true")
            if config.IS_LOCAL:
                return
            
            logger.LOGGER.info("starting visualization")
            ecs_client = boto3_client("ecs", region_name=config.REGION)
            response = start_visualization_task(ecs_client, config)
            print(json.dumps(response))
    else:
        logger.LOGGER.info("visualize set to false")
        # Clear output directory
        try:
            shutil.rmtree(output_directory)
            print(f'dir: {output_directory} deleted')
        except Exception as e:
            print(f"Failed to delete directory: {e}")

def start_visualization_task(ecs_client, config):
    if config.IS_LOCAL:
        return "local-task-arn","container/task-arn/local"

    if not serving_requests(ecs_client, config.CLUSTER_NAME, config.VIZ_CONTAINER_NAME):
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
