from airflow.operators.python import PythonOperator
from typing import Dict


class PythonOperatorWithConfig(PythonOperator):
    def execute(self, context: Dict):
        context.update(self.op_kwargs)
        self.op_kwargs["config"].execution_date = self.op_kwargs["execution_date"]
        self.op_kwargs["config"].dag_run_start_date = self.op_kwargs["dag_run_start_date"]
        super().execute(context)
