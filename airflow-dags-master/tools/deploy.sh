#!/bin/sh

aws s3 sync . s3://"$BUCKET_AIRFLOW_DAGS"/dags \
            --exclude 'stuff/*' \
            --exclude 'tests/*' \
            --exclude 'experiments/*' \
            --exclude 'venv/*' \
            --exclude '*__pycache__*' \
            --exclude '.**' \
            --exclude 'notebooks/*' \
            --exclude 'model_dumps/*' \
            --exclude '*.env'

