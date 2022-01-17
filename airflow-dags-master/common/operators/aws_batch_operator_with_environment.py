from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator
from airflow.models import Variable


class AwsBatchOperatorWithEnvironment(AwsBatchOperator):
    def __init__(self, *, job_name: str, job_definition: str, job_queue: str, overrides: dict, environment: str = None,
                 **kwargs):
        if environment is None:
            environment = Variable.get("ENVIRONMENT")

        super().__init__(job_name=job_name, job_definition=f"{job_definition}-{environment}",
                         job_queue=f"{job_queue}-{environment}", overrides=overrides, **kwargs)
