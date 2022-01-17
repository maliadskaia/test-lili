from unittest import TestCase, mock

import boto3
import pandas as pd
from moto import mock_s3

from common import features_sets, s3
from tests.unit.common.consts import config


class Test(TestCase):
    mock_s3 = mock_s3()

    def setUp(self):
        self.mock_s3.start()
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=config.stages_bucket)

    def tearDown(self):
        self.mock_s3.stop()

    @mock.patch('snowflake.connector.connect')
    def test_create_and_store_to_s3(self, mock_snowflake_connector):
        def create_features_set_dataframe(config1, conn):
            self.assertEqual(config1, config)
            self.assertEqual(conn, mock_snowflake_connector.return_value)

            data = []

            for i in range(0, 10):
                data.append(['a', 1, 2.1, i])

            return pd.DataFrame(data, columns=['ID', 'FEATURE1', 'FEATURE2', 'FEATURE3'])

        features_sets.create_and_store_to_s3(config, config.get_features_sets_key(), create_features_set_dataframe)

        with s3.smart_read(config.s3_connection, config.stages_bucket, config.get_features_sets_key()) as fin:
            for i, row in enumerate(fin):
                self.assertEqual(row, f"a,1,2.1,{i}\n")

            self.assertEqual(i, 9)
