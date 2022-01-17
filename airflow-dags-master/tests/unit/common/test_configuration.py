from datetime import datetime, timezone, timedelta
from unittest import TestCase

from common import configuration
from common.configuration import Config


class Test(TestCase):
    config = Config(dag='dag1',
                    execution_date='2021-07-06T15:16:59.625664+00:00',
                    dag_run_start_date='2021-07-08 15:17:01+00:00',
                    s3_connection=None,
                    snowflake_connection=None,
                    snowflake_output_connection=None,
                    stages_bucket=None,
                    models_bucket=None
                    )

    def test_get_features_sets_key(self):
        self.assertEqual(self.config.get_features_sets_key(), 'dag1/features_set/2021-07-06T15:16:59.625664+00:00.csv')

    def test_get_prediction_key(self):
        self.assertEqual(self.config.get_prediction_key(), 'dag1/prediction/2021-07-06T15:16:59.625664+00:00.csv')

    def test_get_post_prediction_key(self):
        self.assertEqual(self.config.get_post_prediction_key(),
                         'dag1/post_prediction/2021-07-06T15:16:59.625664+00:00.csv')

    def test_get_model_key(self):
        self.assertEqual(self.config.get_model_key("some_model.pkl"), 'models/some_model.pkl')

    def test_date_to_datetime(self):
        self.assertEqual(configuration.date_to_datetime('2021-07-06T15:16:59+00:00'),
                         datetime(2021, 7, 6, 15, 16, 59, tzinfo=timezone.utc))
        self.assertEqual(configuration.date_to_datetime('2021-07-08T15:17:23.625664+00:00'),
                         datetime(2021, 7, 8, 15, 17, 23, 625664, tzinfo=timezone.utc))
        
    def test_get_execution_date_datetime(self):
        self.assertEqual(self.config.get_execution_date_datetime(),
                         datetime(2021, 7, 6, 15, 16, 59, 625664, tzinfo=timezone.utc))

    def test_get_dag_run_start_date_datetime(self):
        self.assertEqual(self.config.get_dag_run_start_date_datetime(),
                         datetime(2021, 7, 8, 15, 17, 1, tzinfo=timezone.utc))

    def test_get_execution_date_datetime_plus_delta(self):
        self.assertEqual(self.config.get_execution_date_datetime_plus_delta(timedelta(hours=1)),
                         '2021-07-06T16:16:59.625664+00:00')

    def test_get_execution_date_date(self):
        self.assertEqual(self.config.get_execution_date_date(), datetime(2021, 7, 6).date())

    def test_get_dag_run_start_date_date(self):
        self.assertEqual(self.config.get_dag_run_start_date_date(), datetime(2021, 7, 8).date())
