import logging
import os

import boto3
from airflow.models import Connection

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class SecretsManager:
    def __init__(self, variables_prefix=None, connections_prefix=None):
        self.secretsmanager_client = boto3.client('secretsmanager')
        self.variables_prefix = variables_prefix if variables_prefix is not None else os.getenv('VARIABLES_PREFIX')
        self.connections_prefix = connections_prefix if connections_prefix is not None else os.getenv(
            'CONNECTIONS_PREFIX')

    def get_secret(self, name):
        if name is None:
            raise ValueError

        try:
            kwargs = {'SecretId': name}
            response = self.secretsmanager_client.get_secret_value(**kwargs)
        except ClientError:
            logger.exception("Couldn't get value for secret %s.", name)
            raise
        else:
            return response

    def get_variable(self, name):
        return self.get_secret(f"{self.variables_prefix}/{name}")['SecretString']

    def get_connection(self, name):
        return Connection(uri=self.get_secret(f"{self.connections_prefix}/{name}")['SecretString'])
