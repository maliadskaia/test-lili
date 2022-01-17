import sys

from common.configuration import Config
from common.batch_secret_manager.secrets_manager import SecretsManager
from overdraft_prediction import overdraft_prediction

ACTIONS = {
    ("overdraft_prediction", "create_features_sets"): overdraft_prediction.create_features_sets,
}


def main(argv):
    dag = argv[2]
    execution_date = argv[3]
    dag_run_start_date = argv[4]
    secrets_manager = SecretsManager()
    config = Config(dag=dag,
                    execution_date=execution_date,
                    dag_run_start_date=dag_run_start_date,
                    snowflake_connection=secrets_manager.get_connection('snowflake'),
                    snowflake_output_connection=secrets_manager.get_connection('snowflake_output'),
                    s3_connection=secrets_manager.get_connection('s3'),
                    stages_bucket=secrets_manager.get_variable("BUCKET_STAGES"),
                    models_bucket=secrets_manager.get_variable("BUCKET_MODELS"),
                    )

    ACTIONS[argv[0], argv[1]](config)


if __name__ == "__main__":
    main(sys.argv[1:])
