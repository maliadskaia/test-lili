import importlib.resources as pkg_resources
import json
import logging
from contextlib import closing

import pandas as pd
import snowflake.connector as sf
from pandas import DataFrame
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine as ce

from common import snowflake


def connect(snowflake_connection):
    snowflake_extra = json.loads(snowflake_connection.extra)
    return sf.connect(user=snowflake_connection.login,
                      password=snowflake_connection.password,
                      account=snowflake_connection.host,
                      database=snowflake_extra["database"],
                      warehouse=snowflake_extra["warehouse"],
                      schema=snowflake_extra["schema"]
                      )


def create_engine(snowflake_connection):
    snowflake_extra = json.loads(snowflake_connection.extra)
    return ce(URL(
        user=snowflake_connection.login,
        password=snowflake_connection.password,
        account=snowflake_connection.host,
        database=snowflake_extra["database"],
        warehouse=snowflake_extra["warehouse"],
        schema=snowflake_extra["schema"],
    ))


class SnowflakeDataExtractor:
    def __init__(self, resource):
        self.resource = resource

    def extract(self, config, filename) -> DataFrame:
        conn = snowflake.connect(config.snowflake_connection)

        with closing(conn):
            return self.extract_with_conn(config, conn, filename)

    def extract_with_conn(self, config, conn, filename) -> DataFrame:
        params = {"dag_run_start_date": config.dag_run_start_date}
        logging.info("executing query %s with parmas %s", filename, params)
        query = pkg_resources.read_text(self.resource, filename)
        return pd.read_sql(query, conn, params=params)
