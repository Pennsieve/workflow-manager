#!/usr/bin/python3

import csv
import json
import logging
import os
import subprocess
import sys
import hashlib
import shutil

from api import AuthenticationClient, WorkflowInstanceClient, ApplicationClient
from boto3 import client as boto3_client
from config import Config
from datetime import datetime, timezone

logger = logging.getLogger('WorkflowManager')

def validate_linear_chain(nodes):
    """Validate DAG is a linear chain and return nodes in execution order."""
    if not nodes:
        raise ValueError("workflow has no nodes")

    by_id = {n["id"]: n for n in nodes}

    # Find root(s)
    roots = [n for n in nodes if not n.get("dependsOn")]
    if len(roots) != 1:
        raise ValueError(f"expected 1 root node, found {len(roots)}")

    # Build child map: parentId → childId
    children = {}
    for n in nodes:
        for dep_id in (n.get("dependsOn") or []):
            if dep_id in children:
                raise ValueError(f"node {dep_id} has multiple dependents — not a linear chain")
            children[dep_id] = n["id"]

    # Validate each node has at most 1 dependency
    for n in nodes:
        if len(n.get("dependsOn") or []) > 1:
            raise ValueError(f"node {n['id']} has multiple dependencies — not a linear chain")

    # Walk the chain
    ordered = [roots[0]]
    current = roots[0]["id"]
    while current in children:
        next_id = children[current]
        ordered.append(by_id[next_id])
        current = next_id

    if len(ordered) != len(nodes):
        raise ValueError(f"chain has {len(ordered)} nodes but workflow has {len(nodes)}")

    return ordered

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
    execution_run = json.loads(sys.argv[4])
    input_directory = sys.argv[5]
    output_directory = sys.argv[6]
    workspace_directory = sys.argv[7]
    resources_directory = sys.argv[8]
    work_directory = sys.argv[9]

    nodes = execution_run["nodes"]
    organization_id = execution_run["organizationId"]

    ordered_nodes = validate_linear_chain(nodes)

    logger.info("running task runner for workflow instance ID={0}".format(workflowInstanceId))

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

    # Build nodeId → sourceUrl lookup for dependency resolution
    node_source_urls = {n["id"]: n["sourceUrl"] for n in ordered_nodes}

    for node in ordered_nodes:
        input_directory, output_directory = setupDirectories(node, node_source_urls, input_directory, output_directory, work_directory)
        logger.info("input_directory: {0}, output_directory: {1}".format(input_directory, output_directory))

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
        ]

        command = []

        application = getRuntimeVariables(node["sourceUrl"], config, session_token, organization_id)
        container_name = application['applicationContainerName']
        task_definition_name = application['applicationId']
        application_type = application['applicationType']
        application_uuid = application['uuid']
        requires_gpu = application.get('runOnGpu', False)

        logger.info("starting: container_name={0}, application_type={1}, task_definition_name={2}".format(container_name, application_type, task_definition_name))

        if config.IS_LOCAL or config.CLUSTER_NAME != "":
            logger.info("starting fargate task"  + task_definition_name)

            now = datetime.now(timezone.utc).timestamp()
            task_arn = start_task(ecs_client, config, task_definition_name, container_name, environment, command, workflowInstanceId, input_directory, output_directory, node, requires_gpu)
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

def start_task(ecs_client, config, task_definition_name, container_name, environment, command, integration_id, input_dir, output_dir, node, requires_gpu):
    if config.IS_LOCAL:
        deps = node.get("dependsOn") or []
        if len(deps) == 0:
            with open(f'{output_dir}/test-file.txt', "w") as file:
                file.write(f'Initialisation started by application: {node["sourceUrl"]}')
        else:
            print(f"copying files from {input_dir} to {output_dir}")
            shutil.copytree(input_dir, output_dir, dirs_exist_ok=True)
            with open(f'{output_dir}/test-file.txt', "w") as file:
                file.write(f'Processed by application: {node["sourceUrl"]}')

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

def setupDirectories(node, node_source_urls, input_dir, output_dir, work_dir):
    logger.info(f"setting up directories for node: {node['id']}")

    v2_input_dir = ""
    deps = node.get("dependsOn") or []

    if len(deps) > 0:
        dep_source_url = node_source_urls[deps[0]]
        input_dir_hash = hashlib.sha256(dep_source_url.encode()).hexdigest()
        v2_input_dir = os.path.join(work_dir, input_dir_hash[:12])
        os.makedirs(v2_input_dir, exist_ok=True)

    output_dir_hash = hashlib.sha256(node["sourceUrl"].encode()).hexdigest()
    v2_output_dir = os.path.join(work_dir, output_dir_hash[:12])
    os.makedirs(v2_output_dir, exist_ok=True)

    return v2_input_dir, v2_output_dir

def getRuntimeVariables(source_url, config, session_token, organization_id):
    logger.info(f"fetching application details for: {source_url}")
    application_client = ApplicationClient(config.API_HOST2)
    application = application_client.get_application(source_url, session_token, organization_id)
    return application[0]


if __name__ == '__main__':
    main()
