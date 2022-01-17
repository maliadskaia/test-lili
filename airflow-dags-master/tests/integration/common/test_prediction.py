import csv
from unittest import TestCase

from common import s3
from common.prediction import predict, BULK_SIZE
from tests.common.simple_model import SimpleModel
from tests.integration.common.consts import  config

MODEL_KEY = 'simple_model.pkl'


class Test(TestCase):
    def setUp(self):
        s3.dump_pickle(config.s3_connection, config.stages_bucket, config.get_model_key(MODEL_KEY), SimpleModel())

    def tearDown(self):
        s3.delete_object(config.s3_connection, config.stages_bucket, config.get_model_key(MODEL_KEY))

    # this method actually goes to s3, we can delete it if we don't want to check it
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
        s3.delete_object(config.s3_connection, config.stages_bucket, config.get_prediction_key())
        s3.delete_object(config.s3_connection, config.stages_bucket, config.get_features_sets_key())
