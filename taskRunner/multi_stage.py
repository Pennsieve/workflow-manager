#!/usr/bin/python3

import csv
import json
import logging
import os
import requests
import subprocess
import sys
import hashlib
import shutil

from api import AuthenticationClient, WorkflowInstanceClient, ApplicationClient, UserClient
from boto3 import client as boto3_client
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

    workflowInstanceId = sys.argv[1]
    session_token = sys.argv[2]
    refresh_token = sys.argv[3]
    workflowVersionMappingObject = json.loads(sys.argv[4])
    input_directory = sys.argv[5]
    output_directory = sys.argv[6]
    workspace_directory = sys.argv[7]
    resources_directory = sys.argv[8]
    work_directory = sys.argv[9]

    version = 'v1'
    workflow = []
    organization_id = ""

    if workflowVersionMappingObject['v1'] is not None:
        logger.info("running v1 workflow")
        workflow = workflowVersionMappingObject['v1']

    if workflowVersionMappingObject['v2'] is not None:
        version = 'v2'
        logger.info("running v2 workflow")
        workflow = workflowVersionMappingObject['v2']

    logger.info("running task runner for workflow instance ID={0}, version={1}".format(workflowInstanceId, version))

    config = Config()
    auth_client = AuthenticationClient(config.API_HOST)
    workflow_instance_client = WorkflowInstanceClient(config.API_HOST2)
    user_client = UserClient(config.API_HOST)

    # Generate API key/secret for pennsieve-agent
    api_key, api_secret = user_client.create_api_key(session_token, name=f"workflow-{workflowInstanceId}")
    logger.info(f"created API key for workflow instance: {workflowInstanceId}")

    ecs_client = boto3_client("ecs", region_name=config.REGION)
    sts_client = boto3_client("sts")

    with open("{0}/processors.csv".format(workspace_directory), 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        header = ['integration_id', 'task_id', 'log_group_name', 'log_stream_name', 'application_uuid', 'container_name', 'application_type']
        writer.writerow(header)
        csvfile.close()

    # v2: # loop through executionOrder and use DAG to determine INPUT_DIR and OUTPUT_DIR
    if version == 'v2':
        workflow = workflowVersionMappingObject['v2']['executionOrder']
        organization_id = workflowVersionMappingObject['v2']['organizationId']
    else:
        workflow = workflowVersionMappingObject['v1']
 
    for app in workflow:
        input_directory, output_directory = setupDirectories(version, app, workflowVersionMappingObject, input_directory, output_directory, work_directory)
        logger.info("input_directory: {0}, output_directory: {1}".format(input_directory, output_directory)) # TODO: remove

        environment = [
            {
                'name': 'INTEGRATION_ID',
                'value': workflowInstanceId
            },
            {
                'name': 'BASE_DIR',
                'value': config.BASE_DIR
            },
            {
                'name': 'PENNSIEVE_API_HOST',
                'value': config.API_HOST
            },
            {
                'name': 'PENNSIEVE_API_HOST2',
                'value': config.API_HOST2
            },
            {
                'name': 'SESSION_TOKEN',
                'value': session_token
            },
            {
                'name': 'ENVIRONMENT',
                'value': config.ENVIRONMENT
            },
            {
                'name': 'REGION',
                'value': config.REGION
            },
            {
                'name': 'INPUT_DIR',
                'value': input_directory
            },
            {
                'name': 'OUTPUT_DIR',
                'value': output_directory
            },
            {
                'name': 'RESOURCES_DIR',
                'value': resources_directory
            },
            {
                'name': 'PENNSIEVE_AGENT_HOME',
                'value': "/tmp"
            },
            {
                'name': 'PENNSIEVE_UPLOAD_BUCKET',
                'value': config.UPLOAD_BUCKET
            },
            {
                'name': 'CLUSTER_NAME',
                'value': config.CLUSTER_NAME
            },
            {
                'name': 'SUBNET_IDS',
                'value': config.SUBNET_IDS
            },
            {
                'name': 'PENNSIEVE_API_KEY',
                'value': api_key
            },
            {
                'name': 'PENNSIEVE_API_SECRET',
                'value': api_secret
            },
        ]

        # TODO: determine params for v2
        if 'params' in app:
            application_params = app['params']
            for key, value in application_params.items():
                new_param = {
                    'name': f'{key}'.upper(),
                    'value': f'{value}'
                }
                environment.append(new_param)

        # currently not used
        if version == 'v1':
            command = app.get('commandArguments', [])
        else:
            command = [] 

        apps = getRuntimeVariables(version, app, config, session_token, organization_id) # apps to run in parallel for v2
        # currently supporting one task at a time, not parallel tasks
        if version == 'v1':
            container_name, task_definition_name, application_type, application_uuid, requires_gpu = apps['applicationContainerName'], apps['applicationId'], apps['applicationType'], apps['uuid'], apps.get('runOnGpu', False)
        else:
            container_name, task_definition_name, application_type, application_uuid, requires_gpu = apps[0]['applicationContainerName'], apps[0]['applicationId'], apps[0]['applicationType'], apps[0]['uuid'], apps[0].get('runOnGpu', False)
            
        logger.info("starting: container_name={0}, application_type={1}, task_definition_name={2}".format(container_name, application_type, task_definition_name))

        if config.IS_LOCAL or config.CLUSTER_NAME != "":
            logger.info("starting fargate task"  + task_definition_name)

            now = datetime.now(timezone.utc).timestamp()
            task_arn = start_task(ecs_client, config, task_definition_name, container_name, environment, command, workflowInstanceId, input_directory, output_directory, version, workflowVersionMappingObject, app, requires_gpu)
            workflow_instance_client.put_workflow_instance_processor_status(workflowInstanceId, application_uuid, 'STARTED', now, session_token)

            logger.info("started: container_name={0},application_type={1}".format(container_name, application_type))

            # gather log related info
            task_id = task_arn.split("/")[2]
            log_stream_name = "ecs/{0}/{1}".format(container_name, task_id)
            log_group_name = get_log_group_name(ecs_client, config, task_definition_name)

            with open("{0}/processors.csv".format(workspace_directory), 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                data = [[workflowInstanceId, task_id, log_group_name, log_stream_name, application_uuid, container_name, application_type]]

                for row in data:
                    writer.writerow(row)
                csvfile.close()

            sync_logs(sts_client, config, workflowInstanceId, workspace_directory)
            exit_code = poll_task(ecs_client, config, task_arn)

            now = datetime.now(timezone.utc).timestamp()
            session_token = auth_client.refresh_token(refresh_token, session_token=session_token)  # refresh token
            if exit_code == 0:
                workflow_instance_client.put_workflow_instance_processor_status(workflowInstanceId, application_uuid, 'SUCCEEDED', now, session_token)
                logger.info("success: container_name={0}, application_type={1}".format(container_name, application_type))
            else:
                workflow_instance_client.put_workflow_instance_processor_status(workflowInstanceId, application_uuid, 'FAILED', now, session_token)
                logger.error("error: container_name={0}, application_type={1}".format(container_name, application_type))
                sys.exit(1)

    logger.info("fargate task has stopped: " + task_definition_name)

def start_task(ecs_client, config, task_definition_name, container_name, environment, command, integration_id, input_dir, output_dir, version, workflowVersionMappingObject, app, requires_gpu):
    if config.IS_LOCAL:
        if version == 'v2':
            print(f"copying files from {input_dir} to {output_dir}")
            dag = workflowVersionMappingObject['v2']['dag']

            logger.info("initialise data") 
            for a in app:
                dependencies = dag[a]
                if len(dependencies) == 0:
                    with open(f'{output_dir}/test-file.txt', "w") as file:
                        file.write(f'Initialisation started by application: {a}')
                else:
                    shutil.copytree(input_dir, output_dir, dirs_exist_ok=True)
                    with open(f'{output_dir}/test-file.txt', "w") as file:
                        file.write(f'Processed by application: {a}')
            
        return "arn:aws:ecs:local:000000000000:task/local-cluster/local-task-id"
        

    # Base run_task parameters
    run_task_params = {
        'cluster': config.CLUSTER_NAME,
        'taskDefinition': container_name,
        'count': 1,
        'networkConfiguration': {
            'awsvpcConfiguration': {
                'subnets': config.SUBNET_IDS.split(","),
                'securityGroups': [config.SECURITY_GROUP]
            }
        },
        'overrides': {
            'containerOverrides': [
                {
                    'name': container_name,
                    'environment': environment,
                    'command': command,
                },
            ],
        }
    }

    # Use GPU capacity provider or Fargate based on requires_gpu
    if requires_gpu:
        run_task_params['capacityProviderStrategy'] = [
            {
                'capacityProvider': config.GPU_CAPACITY_PROVIDER,
                'weight': 1,
                'base': 0
            }
        ]
    else:
        # assignPublicIp is only supported for Fargate launch type
        run_task_params['networkConfiguration']['awsvpcConfiguration']['assignPublicIp'] = 'ENABLED'
        run_task_params['launchType'] = 'FARGATE'
        run_task_params['platformVersion'] = 'LATEST'

    # Add tags for dev environment
    if config.ENVIRONMENT == 'dev':
        run_task_params['tags'] = [
            {
                'key': 'WorkflowInstanceId',
                'value': integration_id
            },
            {
                'key': 'ComputeNode',
                'value': config.CLUSTER_NAME
            },
            {
                'key': 'Environment',
                'value': config.ENVIRONMENT
            },
            {
                'key': 'Project',
                'value': config.CLUSTER_NAME
            },
        ]

    response = ecs_client.run_task(**run_task_params)

    if not response.get('tasks'):
        failure_reason = response.get('failures', [{}])[0].get('reason', 'Unknown')
        raise Exception(f"Failed to start ECS task: {failure_reason}")

    task_arn = response['tasks'][0]['taskArn']

    return task_arn

def poll_task(ecs_client, config, task_arn):
    if config.IS_LOCAL:
        return 0

    waiter = ecs_client.get_waiter('tasks_stopped')
    waiter.wait(
        cluster=config.CLUSTER_NAME,
        tasks=[task_arn],
        WaiterConfig={
            'Delay': 30,
            'MaxAttempts': 2000
        }
    )

    response = ecs_client.describe_tasks(
        cluster=config.CLUSTER_NAME,
        tasks=[task_arn]
    )

    container = response['tasks'][0]['containers'][0]

    if 'exitCode' in container:
        exit_code = container['exitCode']
    else:
        logger.info(container)
        return -1 # error
        
    return exit_code

def get_log_group_name(ecs_client, config, task_definition_name):
    if config.IS_LOCAL:
        return "local-log-group-name"

    log_response = ecs_client.describe_task_definition(taskDefinition=task_definition_name)
    log_configuration = log_response['taskDefinition']['containerDefinitions'][0]['logConfiguration']
    log_group_name = log_configuration['options']['awslogs-group']

    return log_group_name

def sync_logs(sts_client, config, integration_id, workspace_directory):
    if config.IS_LOCAL:
        return

    account_id = sts_client.get_caller_identity()["Account"]
    bucket_name = "tfstate-{0}".format(account_id)
    prefix = "{0}/logs/{1}".format(config.ENVIRONMENT, integration_id)

    try:
        output = subprocess.run(["aws", "s3", "sync", workspace_directory, "s3://{0}/{1}/".format(bucket_name, prefix)]) 
        logger.info(output)
    except subprocess.CalledProcessError as e:
        logger.info(f"command failed with return code {e.returncode}")

def setupDirectories(version, app, workflowVersionMappingObject, input_dir, output_dir, work_dir):
    if version == 'v1':
        return input_dir, output_dir
    
    logger.info(f"setting up directories for version: {version}, app: {app}")

    v2_input_dir = ""
    v2_output_dir = ""
    dag = workflowVersionMappingObject['v2']['dag']
    logger.info("determining input directory") 
    for a in app:
        dependencies = dag[a]
        # for now, only support one dependency
        # applications with no dependencies will have input_dir as ""
        if len(dependencies) > 0:
            if len(dependencies) == 1:
                for dependency in dependencies:
                    input_dir_hash = hashlib.sha256(dependency.encode()).hexdigest()
                    v2_input_dir = os.path.join(work_dir, input_dir_hash[:12])
                    os.makedirs(v2_input_dir, exist_ok=True)
            else:
                logger.error("multiple dependencies not supported yet")
                sys.exit(1)

        output_dir_hash = hashlib.sha256(a.encode()).hexdigest()    
        v2_output_dir = os.path.join(work_dir, output_dir_hash[:12])
        os.makedirs(v2_output_dir, exist_ok=True)

    return v2_input_dir, v2_output_dir             

def getRuntimeVariables(version, app, config, session_token, organization_id):
    # Placeholder for actual logic to determine runtime variables
    if version == 'v1':
        return app
    
    applications = []
    # for v2 app is a list of applications to run in parallel
    for a in app:
        logger.info(f"fetching application details for: {a}")
        application_client = ApplicationClient(config.API_HOST2)
        application = application_client.get_application(a, session_token, organization_id)
        applications.append(application[0])

    return applications


if __name__ == '__main__':
    main()
