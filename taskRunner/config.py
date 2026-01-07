import os
import uuid

def getenv(key, required):
    value = os.getenv(key)
    if required and not value:
        raise ValueError(f"Environment variable '{key}' is required but not set.")
    return value

class Config:
    def __init__(self):
        self.ENVIRONMENT        = os.getenv('ENVIRONMENT', 'local').lower()
        self.IS_LOCAL           = self.ENVIRONMENT == 'local'

        self.SUBNET_IDS                     = getenv('SUBNET_IDS', not self.IS_LOCAL)
        self.CLUSTER_NAME                   = getenv('CLUSTER_NAME', not self.IS_LOCAL)
        self.SECURITY_GROUP                 = getenv('SECURITY_GROUP_ID', not self.IS_LOCAL)
        self.BASE_DIR                       = getenv('BASE_DIR', not self.IS_LOCAL)
        self.REGION                         = getenv('REGION', not self.IS_LOCAL)
        self.GPU_CAPACITY_PROVIDER          = getenv('GPU_CAPACITY_PROVIDER', not self.IS_LOCAL)

        if self.ENVIRONMENT == 'local' or self.ENVIRONMENT == 'dev':
            self.API_HOST       = 'https://api.pennsieve.net'
            self.API_HOST2      = 'https://api2.pennsieve.net'
            self.UPLOAD_BUCKET  = "pennsieve-dev-uploads-v2-use1"
        else:
            self.API_HOST       = 'https://api.pennsieve.io'
            self.API_HOST2      = 'https://api2.pennsieve.io'
            self.UPLOAD_BUCKET  = "" # set by the agent
