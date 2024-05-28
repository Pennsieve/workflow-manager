#!/usr/bin/python3

import sys
import os
import json

# Gather our code in a main() function
def main():
    workflow=json.loads(sys.argv[1])
    container_name = ""
    task_definition_name = ""
    for app in workflow:
        if app['applicationType'] == 'preprocessor':
            container_name = app['applicationContainerName']
            task_definition_name = app['applicationId']
    print(container_name, task_definition_name)     


    


# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()