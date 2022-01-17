import datetime


def date_to_datetime(date):
    try:
        date = date.replace(' ', 'T')
        return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S%z')
    except ValueError:
        return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f%z')


class Config:
    def __init__(self, dag, execution_date, dag_run_start_date, s3_connection, snowflake_connection,
                 snowflake_output_connection, stages_bucket, models_bucket):
        self.dag = dag
        self.execution_date = execution_date
        self.dag_run_start_date = dag_run_start_date
        self.s3_connection = s3_connection
        self.snowflake_connection = snowflake_connection
        self.snowflake_output_connection = snowflake_output_connection
        self.stages_bucket = stages_bucket
        self.models_bucket = models_bucket

    def get_features_sets_key(self):
        return f"{self.dag}/features_set/{self.execution_date}.csv"

    def get_prediction_key(self):
        return f"{self.dag}/prediction/{self.execution_date}.csv"

    def get_post_prediction_key(self):
        return f"{self.dag}/post_prediction/{self.execution_date}.csv"

    def get_post_prediction_features_key(self):
        return f"{self.dag}/post_prediction_features/{self.execution_date}.csv"

    def get_model_key(self, model):
        return f"models/{model}"

    def get_execution_date_datetime(self):
        return date_to_datetime(self.execution_date)

    def get_dag_run_start_date_datetime(self):
        return date_to_datetime(self.dag_run_start_date)

    def get_execution_date_datetime_plus_delta(self, delta):
        return (self.get_execution_date_datetime() + delta).isoformat()

    def get_execution_date_date(self):
        return self.get_execution_date_datetime().date()

    def get_dag_run_start_date_date(self):
        return self.get_dag_run_start_date_datetime().date()

    def get_aws_batch_args(self, action_args):
        return action_args + [self.dag, self.execution_date, self.dag_run_start_date]
