#!/usr/bin/python3

from boto3 import client as boto3_client
import sys
import os
import requests
import json
import logging

logger = logging.getLogger('WorkflowManager')

ecs_client = boto3_client("ecs", region_name=os.environ['REGION'])

# Gather our code in a main() function
def main():
    workspaceDir=sys.argv[7]
    filename=f'{workspaceDir}/events.log'
    # Setup logging
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(filename)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    print('running task runner for integrationID', sys.argv[1])
    integration_id = sys.argv[1]
    api_key = sys.argv[2]
    api_secret = sys.argv[3]
    workflow = json.loads(sys.argv[4])
    inputDir = sys.argv[5]
    outputDir = sys.argv[6]

    # compute node / workflow manager specific
    subnet_ids = os.environ['SUBNET_IDS']
    cluster_name = os.environ['CLUSTER_NAME']
    security_group = os.environ['SECURITY_GROUP_ID']
    base_dir = os.environ['BASE_DIR']
    env = os.environ['ENVIRONMENT']
    
    # App specific - params? - defaults on app creation, then overriden on run
    # session token retrieval should be on the processor(s)
    pennsieve_host = ""
    pennsieve_host2 = ""
    pennsieve_upload_bucket = "" # agent specific
    pennsieve_agent_home = "/tmp" # agent specific

    if env == "dev":
        pennsieve_host = "https://api.pennsieve.net"
        pennsieve_host2 = "https://api2.pennsieve.net"
        pennsieve_upload_bucket = "pennsieve-dev-uploads-v2-use1"
    else:
        pennsieve_host = "https://api.pennsieve.io"
        pennsieve_host2 = "https://api2.pennsieve.io"

    container_name = ""
    task_definition_name = ""
    for app in workflow:
        # get session_token
        r = requests.get(f"{pennsieve_host}/authentication/cognito-config")
        r.raise_for_status()

        cognito_app_client_id = r.json()["tokenPool"]["appClientId"]
        cognito_region = r.json()["region"]

        cognito_idp_client = boto3_client(
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
        session_token = login_response["AuthenticationResult"]["AccessToken"]

        container_name = app['applicationContainerName']
        task_definition_name = app['applicationId']
        application_type = app['applicationType']

        environment = [
            {
                'name': 'INTEGRATION_ID',
                'value': integration_id
            },
            {
                'name': 'PENNSIEVE_API_KEY',
                'value': api_key
            },
            {
                'name': 'PENNSIEVE_API_SECRET',
                'value': api_secret
            },
            {
                'name': 'BASE_DIR',
                'value': base_dir
            },
            {
                'name': 'PENNSIEVE_API_HOST',
                'value': pennsieve_host
            },
                                    {
                'name': 'PENNSIEVE_API_HOST2',
                'value': pennsieve_host2
            },
            {
                'name': 'SESSION_TOKEN',
                'value': session_token
            },
            {
                'name': 'ENVIRONMENT',
                'value': env
            },
            {
                'name': 'REGION',
                'value': os.environ['REGION']
            },
            {
                'name': 'INPUT_DIR',
                'value': inputDir
            },
            {
                'name': 'OUTPUT_DIR',
                'value': outputDir
            },
            {
                'name': 'PENNSIEVE_AGENT_HOME',
                'value': pennsieve_agent_home
            },
            {
                'name': 'PENNSIEVE_UPLOAD_BUCKET',
                'value': pennsieve_upload_bucket
            }, 
        ]
                
        if 'params' in app:
            application_params = app['params']        
            for key, value in application_params.items():
                new_param = {
                                'name': f'{key}'.upper(),
                                'value': f'{value}'
                }
                environment.append(new_param)

            print(environment) 

        command = []
        if 'commandArguments' in app:
            command = app['commandArguments']
    
        logger.info("starting: container_name={0},application_type={1}".format(container_name, application_type))
        # start Fargate task
        if cluster_name != "":
            print("Starting Fargate task")
            response = ecs_client.run_task(
                cluster = cluster_name,
                launchType = 'FARGATE',
                taskDefinition=task_definition_name,
                count = 1,
                platformVersion='LATEST',
                networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': subnet_ids.split(","),
                    'assignPublicIp': 'ENABLED',
                    'securityGroups': [security_group]
                    }   
                },
                overrides={
                'containerOverrides': [
                    {
                        'name': container_name,
                        'environment': environment,
                        'command': command,
                    },
                ],
            })
            task_arn = response['tasks'][0]['taskArn']
            logger.info("started: container_name={0},application_type={1}".format(container_name, application_type))
            
            waiter = ecs_client.get_waiter('tasks_stopped')
            waiter.wait(
                cluster=cluster_name,
                tasks=[task_arn],
                WaiterConfig={
                    'Delay': 30,
                    'MaxAttempts': 2000
                }
            )

            response = ecs_client.describe_tasks(
                cluster=cluster_name,
                tasks=[task_arn]
            )

            print(response)
            exit_code = response['tasks'][0]['containers'][0]['exitCode']

            if exit_code == 0:
                logger.info("success: container_name={0},application_type={1}".format(container_name, application_type))
            else:
                logger.error("error: container_name={0},application_type={1}".format(container_name, application_type))
                sys.exit(1)

            print("Fargate Task has stopped: " + task_definition_name)

# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()