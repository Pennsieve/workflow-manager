import json
import sys

from api import WorkflowInstanceClient
from config import Config


def main():
    config = Config()

    workflow_instance_id = sys.argv[1]
    session_token = sys.argv[2]

    workflow_instance_client = WorkflowInstanceClient(config.API_HOST2)
    execution_run = workflow_instance_client.get_workflow_instance(workflow_instance_id, session_token)

    result = {
        "nodes": execution_run.get("nodes", []),
        "organizationId": execution_run.get("organizationId", ""),
    }

    print(json.dumps(result), end="")


if __name__ == '__main__':
    main()
