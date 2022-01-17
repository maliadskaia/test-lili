from unittest import TestCase

import boto3
import pandas as pd

from moto import mock_s3
from common import s3
from tests.unit.common.connections import s3_connection
from tests.common.simple_model import SimpleModel
from tests.unit.common.consts import BUCKET
from botocore.exceptions import ClientError


class Test(TestCase):
    mock_s3 = mock_s3()

    def setUp(self):
        self.mock_s3.start()
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=BUCKET)

    def tearDown(self):
        self.mock_s3.stop()

    def store_dataframe_to_s3_setup(self):
        data = []

        for i in range(0, 10):
            data.append(['a', 1, 2.1, i])

        self.df = pd.DataFrame(data, columns=['ID', 'FEATURE1', 'FEATURE2', 'FEATURE3'])

        self.key = f"test_store_dataframe_to_s3.csv"

    def store_dataframe_to_s3_clean_up(self):
        s3.delete_object(s3_connection, BUCKET, self.key)

    def test_store_dataframe_to_s3_no_header(self):
        self.store_dataframe_to_s3_setup()
        s3.store_dataframe_to_s3(self.df, s3_connection, BUCKET, self.key)

        with s3.smart_read(s3_connection, BUCKET, self.key) as fin:
            for i, line in enumerate(fin):
                self.assertEqual(f'a,1,2.1,{i}\n', line)

            self.assertEqual(i, 9)

        self.store_dataframe_to_s3_clean_up()

    def test_store_dataframe_to_s3_with_header(self):
        self.store_dataframe_to_s3_setup()
        s3.store_dataframe_to_s3(self.df, s3_connection, BUCKET, self.key, header=True)

        with s3.smart_read(s3_connection, BUCKET, self.key) as fin:
            for i, line in enumerate(fin):
                if i == 0:
                    self.assertEqual('ID,FEATURE1,FEATURE2,FEATURE3\n', line)
                else:
                    self.assertEqual(f'a,1,2.1,{i - 1}\n', line)

            self.assertEqual(i, 10)

        self.store_dataframe_to_s3_clean_up()

    def test_copy_object(self):
        src_key = 'test_copy_object'
        dest_key = 'copy/test_copy_object'
        file_content = "test_text"

        with s3.smart_write(s3_connection, BUCKET, src_key) as fout:
            fout.write(file_content)

        s3.copy_object(s3_connection, BUCKET, src_key, BUCKET, dest_key)

        with s3.smart_read(s3_connection, BUCKET, dest_key) as fin:
            for i, line in enumerate(fin):
                self.assertEqual(line, file_content)
            self.assertEqual(i, 0)

        s3.delete_object(s3_connection, BUCKET, src_key)
        s3.delete_object(s3_connection, BUCKET, dest_key)

    def test_get_object(self):
        key = 'test_object'
        file_content = "test_text"

        with s3.smart_write(s3_connection, BUCKET, key) as fout:
            fout.write(file_content)

        self.assertEqual(s3.get_object(s3_connection, BUCKET, key)['Body'].read().decode('utf-8'), file_content)

        s3.delete_object(s3_connection, BUCKET, key)

    def test_delete_object(self):
        key = 'test_object'
        file_content = "test_text"

        with s3.smart_write(s3_connection, BUCKET, key) as fout:
            fout.write(file_content)

        self.assertEqual(s3.get_object(s3_connection, BUCKET, key)['Body'].read().decode('utf-8'), file_content)

        s3.delete_object(s3_connection, BUCKET, key)

        try:
            s3.get_object(s3_connection, BUCKET, key)
            self.fail()
        except ClientError as ex:
            if ex.response['Error']['Code'] != 'NoSuchKey':
                self.fail()

    def test_dump_pickel(self):
        key = 'model'
        s3.dump_pickle(s3_connection, BUCKET, key, SimpleModel())
        model = s3.load_pickle(s3_connection, BUCKET, key)
        self.assertEqual(model.predict(None, [[1, 2, 3]]), [[6]])

    def test_load_pickel(self):
        key = 'model'
        s3.dump_pickle(s3_connection, BUCKET, key, SimpleModel())
        model = s3.load_pickle(s3_connection, BUCKET, key)
        self.assertEqual(model.predict(None, [[1, 2, 3]]), [[6]])
