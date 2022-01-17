import csv
import logging
import numpy as np

from contextlib import closing
from common import s3

BULK_SIZE = 10000


def predict(config, model_key):
    logging.info("loading model from s3://%s/%s", config.models_bucket, config.get_model_key(model_key))
    model = s3.load_pickle(config.s3_connection, config.models_bucket, config.get_model_key(model_key))

    with closing(s3.smart_read(config.s3_connection, config.stages_bucket, config.get_features_sets_key())) as fin:
        with s3.smart_write(config.s3_connection, config.stages_bucket, config.get_prediction_key()) as fout:
            i = 0
            keys_bulk = []
            features_sets_bulk = []

            for i, row in enumerate(csv.reader(fin)):
                keys_bulk.append(row[0])
                features_set = np.array(row[1:]).astype(np.float)
                features_sets_bulk.append(features_set)

                if (i + 1) % BULK_SIZE == 0:
                    logging.info(f"Going to predict {len(features_sets_bulk)} features sets. Current index: {i}")
                    predict_and_write(config, fout, keys_bulk, features_sets_bulk, model)
                    keys_bulk = []
                    features_sets_bulk = []

            if len(keys_bulk) != 0:
                logging.info(f"Going to predict {len(features_sets_bulk)} features sets. Current index: {i}")
                predict_and_write(config, fout, keys_bulk, features_sets_bulk, model)


def predict_and_write(config, fout, keys_bulk, features_sets_bulk, model):
    predictions = model.predict(config, np.array(features_sets_bulk))
    writer = csv.writer(fout, quoting=csv.QUOTE_MINIMAL, quotechar='"')

    for j, key in enumerate(keys_bulk):
        predictions[j].insert(0, key)
        writer.writerow(predictions[j])
