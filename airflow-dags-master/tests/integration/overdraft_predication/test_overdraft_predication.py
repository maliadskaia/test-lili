import datetime
from unittest import TestCase, mock

import pandas as pd

import overdraft_prediction.overdraft_prediction as overdraft_prediction
from common import snowflake, s3
from overdraft_prediction import model
from tests.integration.common.consts import config


class Test(TestCase):
    # mock_s3 = mock_s3()
    #
    # def setUp(self):
    #     self.mock_s3.start()
    #     conn = boto3.resource('s3', region_name='us-east-1')
    #     conn.create_bucket(Bucket=BUCKET)
    #
    # def tearDown(self):
    #     self.mock_s3.stop()

    def get_bank_account_id_count(self):
        conn = snowflake.connect(config.snowflake_connection)
        bank_account_id_count = pd.read_sql(
            '''SELECT COUNT(DISTINCT bank_account_id) AS COUNTER
            FROM   dwh.fact_mysql_customer_monthly
            WHERE bank_account_id NOT IN (SELECT bank_account_id FROM "LILI_ANALYTICS"."DWH"."FACT_MYSQL_ACCOUNT_BUCKET");''',
            conn)
        return bank_account_id_count

    # TODO: complete this
    def test_create_features_set_dataframe(self):
        conn = snowflake.connect(config.snowflake_connection)
        df = overdraft_prediction.create_features_set_dataframe(config, conn)
        print(df.shape)
        for i in range(0, df.shape[1]):
            print(str(i) + " - " + str(df.iloc[:, i].name))

    #        bank_account_id_count = self.get_bank_account_id_count()
    #        self.assertEqual(df.shape[0], bank_account_id_count['COUNTER'].iloc[0])
    #        self.assertListEqual(list(df.columns), ['BANK_ACCOUNT_NUMBER', 'ACCOUNT_ACTIVE', 'PRO_CUSTOMER',

    def test_predict_and_post_predict(self):
        overdraft_prediction.create_features_sets(config)
        overdraft_prediction.predict(config)
        overdraft_prediction.create_post_predication_features_sets(config)
        overdraft_prediction.post_predict_process(config)
        overdraft_prediction.load_post_prediction_to_snowflake(config)

        bank_account_id_count = self.get_bank_account_id_count()

        predictions = s3.load_csv(config.s3_connection, config.stages_bucket, config.get_prediction_key(), header=None,
                                  names=["bank_account_id", "prediction", "expiration_date", "reason"])
        for _, post_prediction_row in predictions.iterrows():
            self.assertTrue(isinstance(post_prediction_row[0], int))
            self.assertIn(post_prediction_row[1], list(model.predictions_to_overdraft.values()))
            self.assertIn(post_prediction_row[2],
                          (config.get_dag_run_start_date_date() + datetime.timedelta(days=30)).strftime('%Y-%m-%d'))

        self.assertEqual(predictions.shape[0], bank_account_id_count['COUNTER'].iloc[0])

        post_predictions = s3.load_csv(config.s3_connection, config.stages_bucket, config.get_post_prediction_key(),
                                       header=None,
                                       names=["bank_account_id", "prediction", "expiration_date", "reason"])
        overdraft_with_process = [50 if x == 100 or x == 60 else x for x in model.predictions_to_overdraft.values()]
        for _, post_prediction_row in post_predictions.iterrows():
            self.assertTrue(isinstance(post_prediction_row[0], int))
            self.assertIn(post_prediction_row[1], overdraft_with_process)
            self.assertIn(post_prediction_row[2],
                          (config.get_dag_run_start_date_date() + datetime.timedelta(days=30)).strftime('%Y-%m-%d'))

        self.assertEqual(post_predictions.shape[0], bank_account_id_count['COUNTER'].iloc[0])

        post_predictions_features = s3.load_csv(config.s3_connection, config.stages_bucket,
                                                config.get_post_prediction_features_key(),
                                                header=None,
                                                names=["bank_account_id", "is_new"])

        predictions = predictions.merge(post_predictions_features, on=['bank_account_id'])
        predictions.loc[predictions['prediction'] > 20 & predictions['is_new']] = 20
        predictions.loc[predictions['prediction'] > 50] = 50
        predictions.loc[predictions['bank_account_id'] == 260101000149, 'prediction'] = 50

        pre_and_post_prediction = post_predictions.merge(predictions, on=['bank_account_id'])
        pre_and_post_prediction = pre_and_post_prediction[
            pre_and_post_prediction.apply(lambda x: x.prediction_x != x.prediction_y, axis=1)]
        self.assertTrue(pre_and_post_prediction.empty)
        self.assertTrue(post_predictions[post_predictions['bank_account_id'] == 260101000149].reset_index().at[
                            0, 'reason'].endswith(' update manually Lilac'))

    def test_probability_vector(self):
        overdraft_prediction.create_features_sets(config)

    @mock.patch('airflow.providers.slack.operators.slack_webhook.SlackWebhookOperator.execute')
    def test_statistics_tests(self, mock_slack_webhook_operator):
        overdraft_prediction.statistics_tests(config)

    @mock.patch('airflow.providers.slack.operators.slack_webhook.SlackWebhookOperator.execute')
    def test_slack_revoke_reasons(self, mock_slack_webhook_operator):
        overdraft_prediction.slack_revoke_reasons(config)
