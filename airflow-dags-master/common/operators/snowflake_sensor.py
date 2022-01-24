import json

from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sensors.sql import SqlSensor


class SnowflakeSensor(SqlSensor):
    def __init__(self, **kwargs):
        self.template_fields = ('sql', 'parameters')
        super().__init__(**kwargs)

    def _get_hook(self):
        snowflake_connection = BaseHook.get_connection(self.conn_id)
        snowflake_extra = json.loads(snowflake_connection.extra)
        return SnowflakeHook(
            snowflake_conn_id=self.conn_id,
            account=snowflake_connection.host,
            warehouse=snowflake_extra["warehouse"],
            database=snowflake_extra["database"],
            schema=snowflake_extra["schema"],
        )
