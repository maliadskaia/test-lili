"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from common import slack
from common.configuration import Config
from common.operators.aws_batch_operator_with_environment import AwsBatchOperatorWithEnvironment
from common.operators.python_operator_with_config import PythonOperatorWithConfig
from overdraft_prediction import overdraft_prediction
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 12, 4, 0, 0),
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

with DAG(
        "overdraft_prediction", default_args=default_args, schedule_interval="0 7 * * 0-4"
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

    # create_feature_set = PythonOperatorWithConfig(
    #     task_id='create_feature_set',
    #     python_callable=overdraft_prediction.create_features_sets,
    #     op_kwargs=op_kwargs
    # )

    create_feature_set = AwsBatchOperatorWithEnvironment(
        dag=dag,
        task_id='create_feature_set',
        aws_conn_id='s3',
        job_name='create_feature_set',
        job_definition='airflow-job-definition',
        job_queue='airflow-job-queue',
        overrides={'command': config.get_aws_batch_args(["overdraft_prediction", "create_features_sets"])},
    )

    predict = PythonOperatorWithConfig(
        task_id='predict',
        python_callable=overdraft_prediction.predict,
        op_kwargs=op_kwargs
    )

    load_prediction_to_snowflake = PythonOperatorWithConfig(
        task_id='load_prediction_to_snowflake',
        python_callable=overdraft_prediction.load_prediction_to_snowflake,
        op_kwargs=op_kwargs
    )

    create_post_predication_features_sets = PythonOperatorWithConfig(
        task_id='create_post_predication_features_sets',
        python_callable=overdraft_prediction.create_post_predication_features_sets,
        op_kwargs=op_kwargs
    )

    post_predict_process = PythonOperatorWithConfig(
        task_id='post_predict_process',
        python_callable=overdraft_prediction.post_predict_process,
        op_kwargs=op_kwargs
    )

    load_post_prediction_to_snowflake = PythonOperatorWithConfig(
        task_id='load_post_prediction_to_snowflake',
        python_callable=overdraft_prediction.load_post_prediction_to_snowflake,
        op_kwargs=op_kwargs
    )

    copy_prediction_to_backend_bucket = PythonOperatorWithConfig(
        provide_context=True,
        task_id='copy_prediction_to_backend_bucket',
        python_callable=overdraft_prediction.copy_prediction_to_backend_bucket,
        op_kwargs=op_kwargs
    )

    statistics_tests = PythonOperatorWithConfig(
        provide_context=True,
        task_id='statistics_tests',
        python_callable=overdraft_prediction.statistics_tests,
        op_kwargs=op_kwargs
    )

    slack_revoke_reasons = PythonOperatorWithConfig(
        provide_context=True,
        task_id='slack_revoke_reasons',
        python_callable=overdraft_prediction.slack_revoke_reasons,
        op_kwargs=op_kwargs
    )

create_feature_set >> predict
predict >> create_post_predication_features_sets
predict >> load_prediction_to_snowflake
create_post_predication_features_sets >> post_predict_process
post_predict_process >> copy_prediction_to_backend_bucket
post_predict_process >> load_post_prediction_to_snowflake
load_post_prediction_to_snowflake >> statistics_tests
statistics_tests >> slack_revoke_reasons
