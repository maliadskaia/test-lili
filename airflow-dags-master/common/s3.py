import json
import pickle

import boto3
import pandas as pd
import smart_open
from smart_open import open


def createS3Client(s3_connection):
    return boto3.client(service_name='s3',
                        region_name=json.loads(s3_connection.extra)["region_name"],
                        aws_access_key_id=s3_connection.login,
                        aws_secret_access_key=s3_connection.password)


def createS3Resource(s3_connection):
    return boto3.resource('s3',
                          region_name=json.loads(s3_connection.extra)["region_name"],
                          aws_access_key_id=s3_connection.login,
                          aws_secret_access_key=s3_connection.password)


def smart_open(s3_connection, bucket, key, mode):
    params = {'client': createS3Client(s3_connection)}
    return open(f"s3://{bucket}/{key}", mode=mode, transport_params=params)


def smart_read(s3_connection, bucket, key):
    return smart_open(s3_connection, bucket, key, mode='r')


def smart_write(s3_connection, bucket, key, mode='w'):
    return smart_open(s3_connection, bucket, key, mode=mode)


def store_dataframe_to_s3(df, s3_connection, bucket, key, header=False):
    with smart_write(s3_connection, bucket, key) as fout:
        df.to_csv(fout, index=False, header=header)


def get_object(s3_connection, bucket, key):
    s3_client = createS3Client(s3_connection)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj


def delete_object(s3_connection, bucket, key):
    s3_client = createS3Client(s3_connection)
    obj = s3_client.delete_object(Bucket=bucket, Key=key)
    return obj


def copy_object(s3_connection, src_bucket, src_key, dest_bucket, dest_key):
    s3_resource = createS3Resource(s3_connection)
    copy_source = {
        'Bucket': src_bucket,
        'Key': src_key
    }
    bucket = s3_resource.Bucket(dest_bucket)
    bucket.copy(copy_source, dest_key)


def load_csv(s3_connection, bucket, key, header, names):
    with smart_open(s3_connection, bucket, key, mode='rb') as fin:
        return pd.read_csv(fin, header=header, names=names)


def load_pickle(s3_connection, bucket, key):
    class MyCustomUnpickler(pickle.Unpickler):
        def find_class(self, module, name):
            if module == "__main__":
                module = "overdraft_prediction.model"

            return super().find_class(module, name)

    with smart_open(s3_connection, bucket, key, mode='rb') as fin:
        unpickler = MyCustomUnpickler(fin)
        obj = unpickler.load()
        return obj


def dump_pickle(s3_connection, bucket, key, model):
    with smart_write(s3_connection, bucket, key, mode='wb') as fout:
        pickle.dump(model, fout)
