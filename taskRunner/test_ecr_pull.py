#!/usr/bin/python3

"""
Smoke test for cross-account ECR image pull.

Registers a temporary ECS task definition pointing at Account A's private ECR,
runs it on Account B's cluster, and reports whether the image pull succeeded.

Required env vars:
  TEST_ECR_IMAGE      - Full image URI from Account A
                        e.g. 740463337177.dkr.ecr.us-east-1.amazonaws.com/repo:tag

Uses existing env vars from Config: CLUSTER_NAME, SUBNET_IDS, SECURITY_GROUP_ID, REGION
"""

import logging
import os
import sys

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

    test_image = os.getenv("TEST_ECR_IMAGE")
    execution_role_arn = os.getenv("TASK_EXECUTION_ROLE_ARN")

    if not test_image:
        logger.error("TEST_ECR_IMAGE env var is required")
        sys.exit(1)
    if not execution_role_arn:
        logger.error("TASK_EXECUTION_ROLE_ARN env var is required (set in workflow manager environment)")
        sys.exit(1)

    logger.info(f"Testing cross-account ECR pull: {test_image}")
    logger.info(f"Execution role: {execution_role_arn}")
    logger.info(f"Cluster: {config.CLUSTER_NAME}")

    ecs_client = boto3_client("ecs", region_name=config.REGION)
    task_def_arn = None

    try:
        # 1. Register a temporary task definition
        task_def_arn = register_task_definition(ecs_client, config, test_image, execution_role_arn)
        logger.info(f"Registered task definition: {task_def_arn}")

        # 2. Run the task
        task_arn = run_test_task(ecs_client, config, TASK_FAMILY)
        logger.info(f"Started task: {task_arn}")

        # 3. Poll until the task stops
        result = poll_task(ecs_client, config, task_arn)

        # 4. Report result
        if result["success"]:
            logger.info("PASS: Cross-account ECR pull succeeded")
            logger.info(f"Container exit code: {result['exit_code']}")
        else:
            logger.error(f"FAIL: {result['reason']}")
            if "CannotPullContainerError" in result.get("reason", ""):
                logger.error("The ECR resource policy on Account A likely does not allow Account B to pull.")
            sys.exit(1)

    finally:
        # 5. Cleanup - deregister the temp task definition
        if task_def_arn:
            try:
                ecs_client.deregister_task_definition(taskDefinition=task_def_arn)
                logger.info(f"Deregistered task definition: {task_def_arn}")
            except Exception as e:
                logger.warning(f"Failed to deregister task definition: {e}")


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
