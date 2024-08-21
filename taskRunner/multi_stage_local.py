#!/usr/bin/python3

import sys
import os
import json
import logging
import csv

logger = logging.getLogger('WorkflowManager')

# Gather our code in a main() function
def main():
    workflow=json.loads(sys.argv[1])
    workspaceDir=sys.argv[2]
    filename=f'{workspaceDir}/events.log'
    # print(workflow)

    # Set a log level for the logger
    logger.setLevel(logging.INFO)
    # Create a console handler 
    handler = logging.FileHandler(filename)
    # Set INFO level for handler
    handler.setLevel(logging.INFO)
    # Create a message format that matches earlier example
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # Add our format to our handler
    handler.setFormatter(formatter)
    # Add our handler to our logger
    logger.addHandler(handler)

    container_name = ""
    task_definition_name = ""

    # create csv file
    with open("{0}/processors.csv".format(workspaceDir), 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        data = [['integration_id', 'log_group_name', 'log_stream_name', 'container_name', 'application_type']]

        for row in data:
            writer.writerow(row)

        csvfile.close()  


    for app in workflow:
        container_name = app['applicationContainerName']
        task_definition_name = app['applicationId']
        logger.info("started container_name={0},task_definition_name={1}".format(container_name, task_definition_name))
        application_type = app['applicationType']
        
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

        with open("{0}/processors.csv".format(workspaceDir), 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            data = [['someId', 'someLogGroupName', 'someLogStreamName', container_name, application_type]]

            for row in data:
                writer.writerow(row)
            csvfile.close()
        
  

# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()