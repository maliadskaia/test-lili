"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from common import slack
from common.configuration import Config
from common.operators.python_operator_with_config import PythonOperatorWithConfig
from overdraft_prediction import overdraft_prediction

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 12, 6, 0, 0),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_retry_callback": slack.slack_retry_notification,
    "on_failure_callback": slack.slack_failed_notification
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def check_prediction_file_exists(config, backend_bucket, **context):
    s3_hook = S3Hook(aws_conn_id="s3")
    backend_key_prefix = f'overdraft_prediction_{overdraft_prediction.MODEL_KEY}_{config.get_execution_date_datetime_plus_delta(timedelta(hours=-1))}'
    matched_keys = s3_hook.list_keys(bucket_name=backend_bucket, prefix=backend_key_prefix)

    if matched_keys:
        logging.info("Found the following files in bucket %s: %s", backend_key_prefix, matched_keys)
    else:
        slack_msg = """
            <!channel>
           :red_circle: Cannot find overdraft prediction file. 
           *Key prefix*: {key_prefix}  
           *Bucket*: {bucket}  
           *Execution Time*: {exec_date}  
                   """.format(
            key_prefix=backend_key_prefix,
            bucket=backend_bucket,
            exec_date=config.execution_date
        )
        slack.slack_notification(context, slack_msg)


with DAG(
        "monitor_overdraft_prediction", default_args=default_args, schedule_interval="0 7 * * *"
) as dag:
    config = Config(dag=dag.dag_id,
                    execution_date="{{ execution_date }}",
                    dag_run_start_date="{{ dag_run.start_date }}",
                    snowflake_connection=BaseHook.get_connection('snowflake'),
                    snowflake_output_connection=BaseHook.get_connection('snowflake_output'),
                    s3_connection=BaseHook.get_connection('s3'),
                    stages_bucket=Variable.get("BUCKET_STAGES"),
                    models_bucket=Variable.get("BUCKET_MODELS"),
                    )

    op_kwargs = {
        "dag": dag.dag_id,
        "run_id": "{{ run_id }}",
        "execution_date": "{{ execution_date }}",
        "dag_run_start_date": "{{ dag_run.start_date }}",
        "backend_bucket": Variable.get("BUCKET_BACKEND_OVERDRAFT_PREDICTION"),
        "config": config
    }

    monitor_prediction_file_exists = PythonOperatorWithConfig(
        provide_context=True,
        task_id='monitor_prediction_file_exists',
        python_callable=check_prediction_file_exists,
        op_kwargs=op_kwargs
    )
