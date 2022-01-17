import os

from common.configuration import Config
from tests.integration.common import connections

BUCKET = 'lili-ml-tests'
DAG = 'test-dag'
EXECUTION_DATE = '2021-10-12T06:00:00+00:00'
DAG_RUN_START_DATE = '2021-10-12 15:17:00.625664+00:00'


config = Config(dag=DAG,
                execution_date=EXECUTION_DATE,
                dag_run_start_date=DAG_RUN_START_DATE,
                snowflake_connection=connections.snowflake_connection,
                snowflake_output_connection=connections.snowflake_out_connection,
                s3_connection=connections.s3_connection,
                stages_bucket=BUCKET,
                models_bucket=os.environ['BUCKET_MODELS'],
                )
