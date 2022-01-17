import csv
from unittest import TestCase

import boto3
from moto import mock_s3

from common import s3
from common.prediction import predict, BULK_SIZE
from tests.common.simple_model import SimpleModel
from tests.unit.common.consts import BUCKET, config

MODEL_KEY = 'simple_model.pkl'


class Test(TestCase):
    mock_s3 = mock_s3()

    def setUp(self):
        self.mock_s3.start()
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=BUCKET)
        s3.dump_pickle(config.s3_connection, config.stages_bucket, config.get_model_key(MODEL_KEY), SimpleModel())

    def tearDown(self):
        self.mock_s3.stop()

    def test_predict_small_feature_set(self):
        # create feature set
        with s3.smart_write(config.s3_connection, config.stages_bucket,
                            config.get_features_sets_key()) as fout:
            rows = [["a", 1, 1, 1, 1],
                    ["b", 1, 2, 3, 4]]
            csv_writer = csv.writer(fout)
            csv_writer.writerows(rows)

        # Test prediction
        predict(config, MODEL_KEY)

        with s3.smart_read(config.s3_connection, config.stages_bucket,
                           config.get_prediction_key()) as fin:
            for i, row in enumerate(fin):
                if i == 0:
                    self.assertEqual(row, "a,4.0\n")
                elif i == 1:
                    self.assertEqual(row, "b,10.0\n")

            self.assertEqual(i, 1)

    def test_predict_exactly_one_bulk_features_sets(self):
        # save model
        model = SimpleModel()
        model_key = 'simple_model.pkl'

        s3.dump_pickle(config.s3_connection, config.stages_bucket, config.get_model_key(model_key), model)

        # create feature set
        with s3.smart_write(config.s3_connection, config.stages_bucket, config.get_features_sets_key()) as fout:
            csv_writer = csv.writer(fout)
            for i in range(0, BULK_SIZE):
                row = [f"a{i}", 1, 1, 1, i]
                csv_writer.writerow(row)

        # Test prediction
        predict(config, model_key)

        with s3.smart_read(config.s3_connection, config.stages_bucket, config.get_prediction_key()) as fin:
            for i, row in enumerate(fin):
                self.assertEqual(row, f"a{i},{i + 3.0}\n")

            self.assertEqual(i, BULK_SIZE - 1)

        # clean up
        s3.delete_object(config.s3_connection, config.stages_bucket, config.get_features_sets_key())

    def test_predict_more_then_one_bulk_with_tail_features_sets(self):
        # save model
        model = SimpleModel()
        model_key = 'simple_model.pkl'

        s3.dump_pickle(config.s3_connection, config.stages_bucket, config.get_model_key(model_key), model)

        # create feature set
        with s3.smart_write(config.s3_connection, config.stages_bucket, config.get_features_sets_key()) as fout:
            csv_writer = csv.writer(fout)
            rows_count = int(BULK_SIZE + BULK_SIZE / 2)
            for i in range(0, rows_count):
                row = [f"a{i}", 1, 1, 1, i]
                csv_writer.writerow(row)

        # Test prediction
        predict(config, model_key)

        with s3.smart_read(config.s3_connection, config.stages_bucket, config.get_prediction_key()) as fin:
            for i, row in enumerate(fin):
                self.assertEqual(row, f"a{i},{i + 3.0}\n")

            self.assertEqual(i, rows_count - 1)

        # clean up
        s3.delete_object(config.s3_connection, config.stages_bucket, config.get_features_sets_key())

    def test_predict_very_large_features_sets_for_multipart_writing(self):
        # save model
        model = SimpleModel()
        model_key = 'simple_model.pkl'

        s3.dump_pickle(config.s3_connection, config.stages_bucket, config.get_model_key(model_key), model)

        # create feature set
        with s3.smart_write(config.s3_connection, config.stages_bucket, config.get_features_sets_key()) as fout:
            csv_writer = csv.writer(fout)
            rows_count = int(BULK_SIZE * 300 + BULK_SIZE / 2)
            for i in range(0, rows_count):
                row = [f"a{i}", 1, 1, 1, i]
                csv_writer.writerow(row)

        # Test prediction
        predict(config, model_key)

        with s3.smart_read(config.s3_connection, config.stages_bucket, config.get_prediction_key()) as fin:
            for i, row in enumerate(fin):
                self.assertEqual(row, f"a{i},{i + 3.0}\n")

            self.assertEqual(i, rows_count - 1)

        # clean up
        s3.delete_object(config.s3_connection, config.stages_bucket, config.get_features_sets_key())

    def test_predict_exactly_one_bulk_features_sets(self):
        # create feature set
        with s3.smart_write(config.s3_connection, config.stages_bucket, config.get_features_sets_key()) as fout:
            csv_writer = csv.writer(fout)
            for i in range(0, BULK_SIZE):
                row = [f"a{i}", 1, 1, 1, i]
                csv_writer.writerow(row)

        # Test prediction
        predict(config, MODEL_KEY)

        with s3.smart_read(config.s3_connection, config.stages_bucket, config.get_prediction_key()) as fin:
            for i, row in enumerate(fin):
                self.assertEqual(row, f"a{i},{i + 3.0}\n")

            self.assertEqual(i, BULK_SIZE - 1)

        # clean up
        s3.delete_object(config.s3_connection, config.stages_bucket, config.get_features_sets_key())

    def test_predict_more_then_one_bulk_with_tail_features_sets(self):
        # create feature set
        with s3.smart_write(config.s3_connection, config.stages_bucket, config.get_features_sets_key()) as fout:
            csv_writer = csv.writer(fout)
            rows_count = int(BULK_SIZE + BULK_SIZE / 2)
            for i in range(0, rows_count):
                row = [f"a{i}", 1, 1, 1, i]
                csv_writer.writerow(row)

        # Test prediction
        predict(config, MODEL_KEY)

        with s3.smart_read(config.s3_connection, config.stages_bucket, config.get_prediction_key()) as fin:
            for i, row in enumerate(fin):
                self.assertEqual(row, f"a{i},{i + 3.0}\n")

            self.assertEqual(i, rows_count - 1)

        # clean up
        s3.delete_object(config.s3_connection, config.stages_bucket, config.get_features_sets_key())

    def test_predict_very_large_features_sets_for_multipart_writing(self):
        # create feature set
        with s3.smart_write(config.s3_connection, config.stages_bucket, config.get_features_sets_key()) as fout:
            csv_writer = csv.writer(fout)
            rows_count = int(BULK_SIZE * 300 + BULK_SIZE / 2)
            for i in range(0, rows_count):
                row = [f"a{i}", 1, 1, 1, i]
                csv_writer.writerow(row)

        # Test prediction
        predict(config, MODEL_KEY)

        with s3.smart_read(config.s3_connection, config.stages_bucket, config.get_prediction_key()) as fin:
            for i, row in enumerate(fin):
                self.assertEqual(row, f"a{i},{i + 3.0}\n")

            self.assertEqual(i, rows_count - 1)

        # clean up
        s3.delete_object(config.s3_connection, config.stages_bucket, config.get_features_sets_key())
