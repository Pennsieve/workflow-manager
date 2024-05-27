#!/usr/bin/python3

from boto3 import client as boto3_client
# import modules used here -- sys is a very standard one
import sys
import os
import json
import requests

ecs_client = boto3_client("ecs", region_name=os.environ['REGION'])

# Gather our code in a main() function
def main():
    subnet_ids = os.environ['SUBNET_IDS']
    cluster_name = os.environ['CLUSTER_NAME']
    security_group = os.environ['SECURITY_GROUP_ID']
    inputDir = sys.argv[1]
    outputDir = sys.argv[2]
    app_uuid = sys.argv[3]
    api_key = sys.argv[4]
    api_secret = sys.argv[5]

    env = os.environ['ENVIRONMENT']

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

    # App specific - params? - defaults on app creation, then overriden on run
    pennsieve_host = ""
    pennsieve_host2 = ""

    if env == "dev":
        pennsieve_host = "https://api.pennsieve.net"
        pennsieve_host2 = "https://api2.pennsieve.net"
    else:
        pennsieve_host = "https://api.pennsieve.io"
        pennsieve_host2 = "https://api2.pennsieve.io"

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

    # APP specific - in db
    r = requests.get(f"{pennsieve_host2}/applications/{app_uuid}", headers={"Authorization": f"Bearer {session_token}"})
    r.raise_for_status()
    print(r.json())
    
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
        )

        print("Fargate Task has stopped" + task_definition_name)


# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()