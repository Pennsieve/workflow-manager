#!/usr/bin/python3

from boto3 import client as boto3_client
# import modules used here -- sys is a very standard one
import sys
import os
import json

ecs_client = boto3_client("ecs", region_name=os.environ['REGION'])

# Gather our code in a main() function
def main():
    subnet_ids = os.environ['SUBNET_IDS']
    cluster_name = os.environ['CLUSTER_NAME']
    security_group = os.environ['SECURITY_GROUP_ID']
    inputDir = sys.argv[1]
    outputDir = sys.argv[2]
    workflow = json.loads(sys.argv[3])

    container_name = ""
    task_definition_name = ""
    for app in workflow:
        if app['applicationType'] == 'processor':
            container_name = app['applicationContainerName']
            task_definition_name = app['applicationId']

    environment = [
        {
            'name': 'INPUT_DIR',
            'value': inputDir
        },
        {
            'name': 'OUTPUT_DIR',
            'value': outputDir
        },                       
    ]

    # params_file = f'{inputDir}/params.json'
    # if (os.path.isfile(params_file)):
    #     with open(params_file) as f:
    #         params = json.load(f)
            
    #     for key, value in params.items():
    #         new_param = {
    #                         'name': f'{key}'.upper(),
    #                         'value': value
    #         }
    #         environment.append(new_param)

    # print(environment)
    
    task_definition_name = r.json()["applicationId"]
    container_name = r.json()["applicationContainerName"]

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
		        },
	        ],
        })
        task_arn = response['tasks'][0]['taskArn']

        waiter = ecs_client.get_waiter('tasks_stopped')
        waiter.wait(
            cluster=cluster_name,
            tasks=[task_arn],
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 300
            }
        )

        print("Fargate Task has stopped: " + task_definition_name)


# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()