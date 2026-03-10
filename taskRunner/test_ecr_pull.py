#!/usr/bin/python3

"""
Smoke test for cross-account ECR image pull.

Calls the appstore authorize endpoint to resolve the ECR image URI from a
sourceUrl, then registers a temporary ECS task definition pointing at that
image, runs it on the cluster, and reports whether the image pull succeeded.

Required env vars:
  SOURCE_URL                - Application source URL (e.g. https://github.com/org/repo)
  SOURCE_VERSION            - Application version (e.g. v1.0.8)
  SESSION_TOKEN             - Valid Pennsieve session token
  TASK_EXECUTION_ROLE_ARN   - ECS task execution role ARN with ECR pull permissions

Uses existing env vars from Config: CLUSTER_NAME, SUBNET_IDS, SECURITY_GROUP_ID, REGION
"""

import logging
import os
import sys

import requests
from boto3 import client as boto3_client
from config import Config

TASK_FAMILY = "test-ecr-cross-account-pull"

logger = logging.getLogger("TestEcrPull")


def main():
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    config = Config()

    source_url = os.getenv("SOURCE_URL")
    source_version = os.getenv("SOURCE_VERSION")
    session_token = os.getenv("SESSION_TOKEN")
    execution_role_arn = os.getenv("TASK_EXECUTION_ROLE_ARN")

    if not source_url:
        logger.error("SOURCE_URL env var is required")
        sys.exit(1)
    if not source_version:
        logger.error("SOURCE_VERSION env var is required")
        sys.exit(1)
    if not session_token:
        logger.error("SESSION_TOKEN env var is required")
        sys.exit(1)
    if not execution_role_arn:
        logger.error("TASK_EXECUTION_ROLE_ARN env var is required (set in workflow manager environment)")
        sys.exit(1)

    # 1. Resolve the ECR image URI via the appstore authorize endpoint
    image_uri = authorize_image(config, source_url, source_version, session_token)
    logger.info(f"Resolved image URI: {image_uri}")
    logger.info(f"Execution role: {execution_role_arn}")
    logger.info(f"Cluster: {config.CLUSTER_NAME}")

    ecs_client = boto3_client("ecs", region_name=config.REGION)
    task_def_arn = None

    try:
        # 2. Register a temporary task definition
        task_def_arn = register_task_definition(ecs_client, config, image_uri, execution_role_arn)
        logger.info(f"Registered task definition: {task_def_arn}")

        # 3. Run the task
        task_arn = run_test_task(ecs_client, config, TASK_FAMILY)
        logger.info(f"Started task: {task_arn}")

        # 4. Poll until the task stops
        result = poll_task(ecs_client, config, task_arn)

        # 5. Report result
        if result["success"]:
            logger.info("PASS: Cross-account ECR pull succeeded")
            logger.info(f"Container exit code: {result['exit_code']}")
        else:
            logger.error(f"FAIL: {result['reason']}")
            if "CannotPullContainerError" in result.get("reason", ""):
                logger.error("The ECR resource policy on the source account likely does not allow this account to pull.")
            sys.exit(1)

    finally:
        # 6. Cleanup - deregister the temp task definition
        if task_def_arn:
            try:
                ecs_client.deregister_task_definition(taskDefinition=task_def_arn)
                logger.info(f"Deregistered task definition: {task_def_arn}")
            except Exception as e:
                logger.warning(f"Failed to deregister task definition: {e}")


def authorize_image(config, source_url, version, session_token):
    url = f"{config.API_HOST2}/applications/store/authorize"
    params = {
        "sourceUrl": source_url,
        "version": version,
        "userId": "ecr-pull-test",
    }
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {session_token}",
    }

    logger.info(f"Calling appstore authorize: sourceUrl={source_url}, version={version}")
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    data = response.json()

    if not data.get("authorized"):
        message = data.get("message", "Unknown authorization failure")
        logger.error(f"Appstore authorize denied: {message}")
        sys.exit(1)

    image_url = data.get("imageUrl")
    if not image_url:
        logger.error("Appstore authorize response missing imageUrl")
        sys.exit(1)

    return image_url


def register_task_definition(ecs_client, config, image_uri, execution_role_arn):
    response = ecs_client.register_task_definition(
        family=TASK_FAMILY,
        executionRoleArn=execution_role_arn,
        networkMode="awsvpc",
        requiresCompatibilities=["FARGATE"],
        cpu="256",
        memory="512",
        containerDefinitions=[
            {
                "name": TASK_FAMILY,
                "image": image_uri,
                "essential": True,
                "command": ["echo", "cross-account ECR pull succeeded"],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": f"/ecs/{TASK_FAMILY}",
                        "awslogs-region": config.REGION,
                        "awslogs-stream-prefix": "ecs",
                        "awslogs-create-group": "true",
                    },
                },
            }
        ],
    )
    return response["taskDefinition"]["taskDefinitionArn"]


def run_test_task(ecs_client, config, task_family):
    run_task_params = {
        "cluster": config.CLUSTER_NAME,
        "taskDefinition": task_family,
        "count": 1,
        "launchType": "FARGATE",
        "platformVersion": "LATEST",
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": config.SUBNET_IDS.split(","),
                "securityGroups": [config.SECURITY_GROUP],
                "assignPublicIp": "ENABLED",
            }
        },
        "overrides": {
            "containerOverrides": [
                {
                    "name": task_family,
                    "command": ["echo", "cross-account ECR pull succeeded"],
                }
            ]
        },
    }

    response = ecs_client.run_task(**run_task_params)

    if not response.get("tasks"):
        failures = response.get("failures", [])
        reason = failures[0].get("reason", "Unknown") if failures else "Unknown"
        raise Exception(f"Failed to start ECS task: {reason}")

    return response["tasks"][0]["taskArn"]


def poll_task(ecs_client, config, task_arn):
    logger.info("Waiting for task to stop...")

    waiter = ecs_client.get_waiter("tasks_stopped")
    waiter.wait(
        cluster=config.CLUSTER_NAME,
        tasks=[task_arn],
        WaiterConfig={"Delay": 10, "MaxAttempts": 60},  # up to 10 minutes
    )

    response = ecs_client.describe_tasks(cluster=config.CLUSTER_NAME, tasks=[task_arn])
    task = response["tasks"][0]
    container = task["containers"][0]

    # Check for pull failure
    stopped_reason = task.get("stoppedReason", "")
    if "CannotPullContainerError" in stopped_reason:
        return {"success": False, "reason": stopped_reason, "exit_code": None}

    # Check container exit
    if "exitCode" in container:
        exit_code = container["exitCode"]
        if exit_code == 0:
            return {"success": True, "reason": "OK", "exit_code": 0}
        else:
            return {
                "success": False,
                "reason": f"Container exited with code {exit_code}",
                "exit_code": exit_code,
            }

    return {
        "success": False,
        "reason": f"Task stopped without exit code: {stopped_reason}",
        "exit_code": None,
    }


if __name__ == "__main__":
    main()
