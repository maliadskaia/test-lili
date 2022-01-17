import json
import os
from airflow.models.connection import Connection

s3_connection = Connection(
    conn_id='s3_connection',
    conn_type='S3',
    login='AAAAAAAAAAAAAAAAAAAA',
    password='ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ',
    extra=json.dumps(dict(region_name='aaaa')),
)

snowflake_connection = Connection(
    conn_id='snowflake_conn',
    conn_type='Snowflake',
    host='some_host',
    login='some_user',
    password='some password',
    extra=json.dumps(dict(schema='some_schema', warehouse='some_warehouse', database='some_database')),
)
