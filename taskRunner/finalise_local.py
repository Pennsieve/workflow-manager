#!/usr/bin/python3

import sys
import requests
import json
from boto3 import client as boto3_client
import os
import csv
from datetime import datetime, timezone


# Gather our code in a main() function
def main():
    env = os.environ['ENVIRONMENT']
    integrationId = os.environ['INTEGRATION_ID']
    session_token = os.environ['SESSION_TOKEN']

    pennsieve_host2 = "https://api2.pennsieve.net"

    completedAt = datetime.now(timezone.utc)
    payload = {"uuid": f"{integrationId}", "completedAt": f"{completedAt}"}
    r = requests.put(f"{pennsieve_host2}/integrations", json=payload, headers={"Authorization": f"Bearer {session_token}", "Content-Type": 'application/json'})
    r.raise_for_status()
    print(json.dumps(r.json()))

# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()