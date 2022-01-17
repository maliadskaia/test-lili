import logging
from contextlib import closing

from common import s3, snowflake


def create_and_store_to_s3(config, bucket_key, create_features_set_dataframe):
    conn = snowflake.connect(config.snowflake_connection)

    with closing(conn):
        df = create_features_set_dataframe(config, conn)
        logging.info("store features sets to s3://%s/%s", config.stages_bucket, bucket_key)
        s3.store_dataframe_to_s3(df, config.s3_connection, config.stages_bucket, bucket_key, False)
