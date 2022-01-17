from unittest import TestCase

import boto3
import pandas as pd
from moto import mock_s3

from common import s3, snowflake
from overdraft_prediction import overdraft_prediction
from tests.integration.common.consts import config


class Test(TestCase):
    mock_s3 = mock_s3()

    def setUp(self):
        self.mock_s3.start()
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=config.stages_bucket)

    def tearDown(self):
        self.mock_s3.stop()

    def store_dataframe_to_s3_setup(self, key):
        data = []

        for i in range(0, 10):
            data.append([260101578607, 100, '2021-09-08', 'reason -2,2'])

        df = pd.DataFrame(data, columns=['ID', 'FEATURE1', 'FEATURE2', 'FEATURE3'])
        s3.store_dataframe_to_s3(df, config.s3_connection, config.stages_bucket, key)

    def test_load_prediction_to_snowflake(self):
        self.store_dataframe_to_s3_setup(config.get_prediction_key())
        overdraft_prediction.load_prediction_to_snowflake(config)
        conn = snowflake.connect(config.snowflake_output_connection)
        predictions_df = pd.read_sql(
            '''SELECT bank_account_id, prediction, expiration_date, reason FROM ml_overdraft_prediction''',
            conn)
        self.assertEqual(predictions_df.shape[0], 10)

        for i, row in enumerate(predictions_df.iterrows()):
            self.assertListEqual([260101578607, 100, '2021-09-08', 'reason -2,2'], list(row[1].values))

    def test_load_post_prediction_to_snowflake(self):
        self.store_dataframe_to_s3_setup(config.get_post_prediction_key())
        overdraft_prediction.load_post_prediction_to_snowflake(config)
        conn = snowflake.connect(config.snowflake_output_connection)
        predictions_df = pd.read_sql(
            '''SELECT bank_account_id, prediction, expiration_date, reason FROM ml_overdraft_post_prediction''',
            conn)
        self.assertEqual(predictions_df.shape[0], 10)

        for i, row in enumerate(predictions_df.iterrows()):
            self.assertListEqual([260101578607, 100, '2021-09-08', 'reason -2,2'], list(row[1].values))
