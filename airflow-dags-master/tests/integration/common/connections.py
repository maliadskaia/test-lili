import json
import os
from airflow.models.connection import Connection

s3_connection = Connection(
    conn_id='s3_connection',
    conn_type='S3',
    login=os.environ['AWS_ACCESS_KEY_ID'],
    password=os.environ['AWS_SECRET_ACCESS_KEY'],
    extra=json.dumps(dict(region_name=os.environ['AWS_REGION'])),
)

snowflake_connection = Connection(
    conn_id='snowflake_conn',
    conn_type='Snowflake',
    host=os.environ['SNOWFLAKE_HOST'],
    login=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    extra=json.dumps(dict(schema=os.environ['SNOWFLAKE_SCHEMA'],
                          warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
                          database=os.environ['SNOWFLAKE_DATABASE'])),
)

snowflake_out_connection = Connection(
    conn_id='snowflake_conn',
    conn_type='Snowflake',
    host=os.environ['SNOWFLAKE_OUTPUT_HOST'],
    login=os.environ['SNOWFLAKE_OUTPUT_USER'],
    password=os.environ['SNOWFLAKE_OUTPUT_PASSWORD'],
    extra=json.dumps(dict(schema=os.environ['SNOWFLAKE_OUTPUT_SCHEMA'],
                          warehouse=os.environ['SNOWFLAKE_OUTPUT_WAREHOUSE'],
                          database=os.environ['SNOWFLAKE_OUTPUT_DATABASE'])),
)