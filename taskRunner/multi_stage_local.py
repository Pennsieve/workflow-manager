#!/usr/bin/python3

import sys
import os
import json

# Gather our code in a main() function
def main():
    workflow=json.loads(sys.argv[1])
    # print(workflow)
    container_name = ""
    task_definition_name = ""
    for app in workflow:
        container_name = app['applicationContainerName']
        task_definition_name = app['applicationId']
        
        print(container_name, task_definition_name)

        environment = [
        {
            'name': 'INTEGRATION_ID',
            'value': "some_id"
        }, 
        ]

        # command = []

        if 'params' in app:
            application_params = app['params']        
            for key, value in application_params.items():
                new_param = {
                                'name': f'{key}'.upper(),
                                'value': value
                }
                environment.append(new_param)

            print(environment)   


    


# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()