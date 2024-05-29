#!/usr/bin/python3

import sys
import requests
import json
from boto3 import client as boto3_client
import os

# Gather our code in a main() function
def main():
    env = os.environ['ENVIRONMENT']
    integrationId = sys.argv[1]
    api_key = sys.argv[2]
    api_secret = sys.argv[3] 

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
    
    r = requests.get(f"{pennsieve_host2}/integrations/{integrationId}", headers={"Authorization": f"Bearer {session_token}"})
    r.raise_for_status()
    print(r.json()["workflow"])

# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()