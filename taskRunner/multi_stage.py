#!/usr/bin/python3

import csv
import json
import logging
import os
import requests
import subprocess
import sys

from api import AuthenticationClient, WorkflowInstanceClient
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

    integration_id = sys.argv[1]
    api_key = sys.argv[2]
    api_secret = sys.argv[3]
    workflow = json.loads(sys.argv[4])
    input_directory = sys.argv[5]
    output_directory = sys.argv[6]
    workspace_directory = sys.argv[7]

    logger.info("running task runner for workflow instance ID={0}".format(integration_id))

    config = Config()
    auth_client = AuthenticationClient(config.API_HOST)
    workflow_instance_client = WorkflowInstanceClient(config.API_HOST2)

    ecs_client = boto3_client("ecs", region_name=config.REGION)
    sts_client = boto3_client("sts")

    with open("{0}/processors.csv".format(workspace_directory), 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        header = ['integration_id', 'task_id', 'log_group_name', 'log_stream_name', 'application_uuid', 'container_name', 'application_type']
        writer.writerow(header)
        csvfile.close()

    for app in workflow:
        session_token = auth_client.authenticate(api_key, api_secret)

        container_name = app['applicationContainerName']
        task_definition_name = app['applicationId']
        application_type = app['applicationType']
        application_uuid = app['uuid']

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
        ]

        if 'params' in app:
            application_params = app['params']
            for key, value in application_params.items():
                new_param = {
                    'name': f'{key}'.upper(),
                    'value': f'{value}'
                }
                environment.append(new_param)

        command = app.get('commandArguments', [])

        logger.info("starting: container_name={0}, application_type={1}, task_definition_name={2}".format(container_name, application_type, task_definition_name))

        if config.IS_LOCAL or config.CLUSTER_NAME != "":
            logger.info("starting fargate task"  + task_definition_name)

            now = datetime.now(timezone.utc).timestamp()
            task_arn, container_task_arn = start_task(ecs_client, config, task_definition_name, container_name, environment, command, integration_id)
            workflow_instance_client.put_workflow_instance_processor_status(integration_id, application_uuid, 'STARTED', now, session_token)

            logger.info("started: container_name={0},application_type={1}".format(container_name, application_type))

            # gather log related info
            task_id = container_task_arn.split("/")[2]
            log_stream_name = "ecs/{0}/{1}".format(container_name, task_id)
            log_group_name = get_log_group_name(ecs_client, config, task_definition_name)

            with open("{0}/processors.csv".format(workspace_directory), 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                data = [[integration_id, task_id, log_group_name, log_stream_name, application_uuid, container_name, application_type]]

                for row in data:
                    writer.writerow(row)
                csvfile.close()

            sync_logs(sts_client, config, integration_id, workspace_directory)
            exit_code = poll_task(ecs_client, config, task_arn)

            now = datetime.now(timezone.utc).timestamp()
            session_token = auth_client.authenticate(api_key, api_secret)  # refresh token
            if exit_code == 0:
                workflow_instance_client.put_workflow_instance_processor_status(integration_id, application_uuid, 'SUCCEEDED', now, session_token)
                logger.info("success: container_name={0}, application_type={1}".format(container_name, application_type))
            else:
                workflow_instance_client.put_workflow_instance_processor_status(integration_id, application_uuid, 'FAILED', now, session_token)
                logger.error("error: container_name={0}, application_type={1}".format(container_name, application_type))
                sys.exit(1)

    logger.info("fargate task has stopped: " + task_definition_name)

def start_task(ecs_client, config, task_definition_name, container_name, environment, command, integration_id):
    if config.IS_LOCAL:
        return "local-task-arn","container/task-arn/local"

    response = ecs_client.run_task(
        cluster = config.CLUSTER_NAME,
        launchType = 'FARGATE',
        taskDefinition=task_definition_name,
        count = 1,
        platformVersion='LATEST',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': config.SUBNET_IDS.split(","),
                'assignPublicIp': 'ENABLED',
                'securityGroups': [config.SECURITY_GROUP]
                }   
        },
        tags=[
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
                'value': environment
            }
        ],
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
    container_task_arn = response['tasks'][0]['containers'][0]['taskArn']

    return task_arn, container_task_arn

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

if __name__ == '__main__':
    main()
