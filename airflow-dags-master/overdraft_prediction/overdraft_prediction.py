import datetime
import logging
import numpy as np
import pandas as pd
import time
from tabulate import tabulate

from common import prediction, features_sets, s3, slack, snowflake
from overdraft_prediction import queries
from overdraft_prediction.statistics_tests import TESTS

MODEL_KEY = "ODmodel_2021-10-12T07:55:05.563440"

COLS_LIST = ['BANK_ACCOUNT_NUMBER', 'ACCOUNT_ACTIVE', 'PRO_CUSTOMER', 'TIME_FROM_SIGNUP_DATE',
             'IS_PRO_SINCE_SIGNUP', 'CURRENT_BALANCE', 'TOTAL_MONEY_IN',
             'TOTAL_MONEY_IN_COUNT', 'TOTAL_MONEY_IN_WEEK1', 'TOTAL_MONEY_IN_COUNT_WEEK1',
             'TOTAL_MONEY_IN_WEEK2', 'TOTAL_MONEY_IN_COUNT_WEEK2', 'TOTAL_MONEY_IN_WEEK3',
             'TOTAL_MONEY_IN_COUNT_WEEK3', 'TOTAL_MONEY_IN_WEEK4', 'TOTAL_MONEY_IN_COUNT_WEEK4',
             'OVERDRAFT_LIMIT', 'HAD_NEGATIVE_BALANCE', 'ATM_SUM', 'SWIPE_SUM', 'SPEND_SUM',
             'DIRECT_DEPOSIT_SUM', 'DIRECT_PAY_SUM', 'ACH_SUM', 'CHECK_SUM', 'GREENDOT_SUM', 'CARD_DEPOSIT_SUM',
             'AVERAGE_BALANCE', 'DID_PAYROLL', 'DID_MARKETPLACE', 'DID_FINANCIAL_INSTITUTION', 'DID_UNEMPLOYMENT',
             'DID_TAX_REFUND', 'DID_NONE', 'LOGIN_COUNT', 'PENDING_CHECK', 'DENIED_AUTH_NSF', 'DENIED_AUTH_NSF_CNT',
             'DENIED_AUTH', 'DENIED_AUTH_CNT', 'AUTH_EXP', 'AUTH_EXP_CNT', 'DENIED_AUTH_INACTIVE_CARD',
             'DENIED_AUTH_INACTIVE_CARD_CNT', 'DENIED_AUTH_INVALID_PIN', 'DENIED_AUTH_INVALID_PIN_CNT',
             'DENIED_AUTH_GAS', 'DENIED_AUTH_GAS_CNT', 'ACH_CREDIT_FAIL', 'ACH_CREDIT_FAIL_CNT',
             'ACH_CREDIT_RETURN', 'ACH_CREDIT_RETURN_CNT', 'CREATE_HOLD', 'CREATE_HOLD_CNT', 'EXPIRE_HOLD',
             'EXPIRE_HOLD_CNT', 'TIME_FROM_LAST_TRANSACTION', 'TIME_FROM_LAST_MONEY_IN', 'AVG_MONEY_IN',
             'IS_RECURRENT_DD', 'CLOSE_EXPIRATION']


def create_features_set_dataframe(config, conn):
    extractor = snowflake.SnowflakeDataExtractor(queries)
    customer_login_per_date_range = extractor.extract_with_conn(config, conn, "customers_logins.sql")
    money_in_week1 = extractor.extract_with_conn(config, conn, "money_in_week1.sql")
    money_in_week2 = extractor.extract_with_conn(config, conn, "money_in_week2.sql")
    money_in_week3 = extractor.extract_with_conn(config, conn, "money_in_week3.sql")
    money_in_week4 = extractor.extract_with_conn(config, conn, "money_in_week4.sql")
    overdraft_limits_table = extractor.extract_with_conn(config, conn, "overdraft_limits.sql")
    mcc_table = extractor.extract_with_conn(config, conn, "mcc.sql")
    pending_check = extractor.extract_with_conn(config, conn, "pending_check.sql")
    trans_rej = extractor.extract_with_conn(config, conn, "trans_rej.sql")
    current_balance = extractor.extract_with_conn(config, conn, "current_balance.sql")
    added_bank_account_num = extractor.extract_with_conn(config, conn, "added_bank_account.sql")
    financial_data = extractor.extract_with_conn(config, conn, "financial_data.sql")
    recurrent_dd_ids = extractor.extract_with_conn(config, conn, "recurrent_direct_deposit.sql")

    if (mcc_table['MONEY_AMOUNT'].min() < -1000000):
        raise Exception('Error! Currently we perform optimization of the values of MONEY_AMOUNT'
                        'in MCC_TABLE in order to reduce memory usage. It seems that there are'
                        'MONEY_AMOUNT values that exceed the limitations. This can cause rounding'
                        'problems and thus producing wrong amounts. In order to fix this error, you'
                        'need to take 2 actions. First one is to remove the np.float32 optimization to'
                        'MONEY_AMOUNT in mcc_table and delete this if statement.'
                        'Second one is to upgrade AWS MWAA instance to large.')

    mcc_table['MONEY_AMOUNT'] = mcc_table['MONEY_AMOUNT'].astype(np.float32)
    mcc_table = pd.crosstab(mcc_table.BANK_ACCOUNT_ID, mcc_table.MCC_CODE, values=mcc_table.MONEY_AMOUNT,
                            aggfunc='sum').fillna(0)
    table_all = financial_data.merge(customer_login_per_date_range, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(added_bank_account_num, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(current_balance, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(trans_rej, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(pending_check, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(mcc_table, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(money_in_week1, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(money_in_week2, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(money_in_week3, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(money_in_week4, on=['BANK_ACCOUNT_ID'], how='left')
    table_all = table_all.merge(recurrent_dd_ids, on=['BANK_ACCOUNT_ID'], how='left')
    table_all.BANK_ACCOUNT_NUMBER = table_all.BANK_ACCOUNT_NUMBER.astype(np.int64)
    table_all = table_all.merge(overdraft_limits_table[['BANK_ACCOUNT_NUMBER', 'OVERDRAFT_LIMIT', 'CLOSE_EXPIRATION']],
                                on=['BANK_ACCOUNT_NUMBER'],
                                how='left')

    table_all['DID_NONE'] = (1 - table_all[
        ['DID_PAYROLL', 'DID_MARKETPLACE', 'DID_FINANCIAL_INSTITUTION', 'DID_UNEMPLOYMENT', 'DID_TAX_REFUND']].max(
        axis=1))
    table_all['TIME_FROM_LAST_TRANSACTION'] = \
        (pd.to_datetime(table_all['PERIOD_END'], format="%Y%m") - table_all['LAST_TRANSACTION_IN_TIME']) \
            .astype('timedelta64[D]')
    table_all['TIME_FROM_LAST_MONEY_IN'] = \
        (pd.to_datetime(table_all['PERIOD_END'], format="%Y%m%") - table_all['LAST_TRANSACTION_MONEY_IN_IN_TIME']) \
            .astype('timedelta64[D]')
    table_all['TIME_FROM_SIGNUP_DATE'] = \
        (pd.to_datetime(table_all['PERIOD_END'], format="%Y%m") - table_all['SIGNUP_DATE']) \
            .astype('timedelta64[D]')
    table_all = table_all.drop(['SIGNUP_DATE', 'PERIOD_END', 'LAST_TRANSACTION_IN_TIME'], axis=1)
    table_all['AVG_MONEY_IN'] = np.where(table_all['TOTAL_MONEY_IN'] == 0.00, 0.00,
                                         table_all['TOTAL_MONEY_IN'] / table_all['TOTAL_MONEY_IN_COUNT'])
    table_all = table_all.fillna(0)
    table_all = table_all.replace(np.nan, 0)
    table_all = table_all.reset_index(drop=True)
    # table parameters should be in the following order:
    # -------------------------------------------------
    # 'BANK_ACCOUNT_NUMBER', 'ACCOUNT_ACTIVE', 'PRO_CUSTOMER', 'TIME_FROM_SIGNUP_DATE', 'IS_PRO_SINCE_SIGNUP',
    # 'CURRENT_BALANCE', 'TOTAL_MONEY_IN', 'TOTAL_MONEY_IN_COUNT', 'HAD_NEGATIVE_BALANCE', 'ATM_SUM', 'SWIPE_SUM',
    # 'SPEND_SUM', 'DIRECT_DEPOSIT_SUM', 'DIRECT_PAY_SUM', 'ACH_SUM', 'CHECK_SUM', 'GREENDOT_SUM',
    # 'CARD_DEPOSIT_SUM', 'AVERAGE_BALANCE', 'DID_PAYROLL', 'DID_MARKETPLACE', 'DID_FINANCIAL_INSTITUTION',
    # 'DID_UNEMPLOYMENT', 'DID_TAX_REFUND', 'DID_NONE', 'LOGIN_COUNT', 'PENDING_CHECK', 'DENIED_AUTH_NSF',
    # 'DENIED_AUTH_NSF_CNT', 'DENIED_AUTH', 'DENIED_AUTH_CNT', 'AUTH_EXP', 'AUTH_EXP_CNT',
    # 'DENIED_AUTH_INACTIVE_CARD', 'DENIED_AUTH_INACTIVE_CARD_CNT', 'DENIED_AUTH_INVALID_PIN',
    # 'DENIED_AUTH_INVALID_PIN_CNT', 'DENIED_AUTH_GAS', 'DENIED_AUTH_GAS_CNT', 'ACH_CREDIT_FAIL',
    # 'ACH_CREDIT_FAIL_CNT', 'ACH_CREDIT_RETURN', 'ACH_CREDIT_RETURN_CNT', 'CREATE_HOLD', 'CREATE_HOLD_CNT',
    # 'EXPIRE_HOLD', 'EXPIRE_HOLD_CNT', 'TIME_FROM_LAST_TRANSACTION', 'TIME_FROM_LAST_MONEY_IN', 'AVG_MONEY_IN',

    # (503 MCC codes in ascending order)

    mcc_list_for_overdraft = extractor.extract_with_conn(config, conn, "mcc_list_for_overdraft.sql")

    mcc_list = list(mcc_list_for_overdraft['MCC_CODE'])

    num_all = -len(COLS_LIST) - 1
    clmns = sorted(list(table_all.columns))
    missing_mcc = list(set(mcc_list) - set(clmns[:num_all]))
    for elt in missing_mcc:
        table_all[elt] = 0
    all_cols = COLS_LIST + mcc_list
    return table_all[all_cols]


def create_post_predication_features_set_dataframe(config, conn):
    extractor = snowflake.SnowflakeDataExtractor(queries)
    first_time_users = extractor.extract_with_conn(config, conn, "first_time_users.sql")
    return first_time_users


def create_features_sets(config):
    features_sets.create_and_store_to_s3(config, config.get_features_sets_key(), create_features_set_dataframe)


def predict(config):
    prediction.predict(config, MODEL_KEY + ".pkl")


def create_post_predication_features_sets(config):
    features_sets.create_and_store_to_s3(config, config.get_post_prediction_features_key(),
                                         create_post_predication_features_set_dataframe)


def post_predict_process(config):
    predictions = s3.load_csv(config.s3_connection, config.stages_bucket,
                              config.get_prediction_key(), header=None,
                              names=["bank_account_id", "prediction", "expiration_date", "reason"])
    post_prediction_features = s3.load_csv(config.s3_connection, config.stages_bucket,
                                           config.get_post_prediction_features_key(), header=None,
                                           names=["bank_account_id", "IS_NEW"])
    predictions = predictions.merge(post_prediction_features, on=['bank_account_id'])

    predictions.loc[(predictions['IS_NEW'] == 1) &
                    (predictions['prediction'] > 20), 'reason'] = predictions['reason'] + ' new customer'
    predictions.loc[(predictions['IS_NEW'] == 1) &
                    (predictions['prediction'] > 20), 'prediction'] = 20
    predictions.loc[predictions['prediction'] == 100, 'prediction'] = 50
    predictions.loc[predictions['prediction'] == 60, 'prediction'] = 50
    predictions.loc[predictions['bank_account_id'] == 260101000149, 'prediction'] = 50
    predictions.loc[(predictions['bank_account_id'] == 260101000149), 'reason'] = predictions[
                                                                                      'reason'] + ' update manually Lilac'
    predictions.drop(columns=['IS_NEW'], inplace=True)

    s3.store_dataframe_to_s3(predictions, config.s3_connection, config.stages_bucket,
                             config.get_post_prediction_key(), False)


def load_csv_to_snowflake(config, key, out_table):
    engine = snowflake.create_engine(config.snowflake_output_connection)

    with engine.connect() as conn:
        try:
            df = s3.load_csv(config.s3_connection, config.stages_bucket, key, header=None,
                             names=["bank_account_id", "prediction", "expiration_date", "reason"])
            df["execution_date"] = config.get_execution_date_datetime()
            df["dag_run"] = config.get_dag_run_start_date_datetime()
            df["created_at"] = datetime.datetime.now()
            df.to_sql(out_table, con=conn, index=False, if_exists='replace', chunksize=10000)
        finally:
            conn.close()
            engine.dispose()


def load_prediction_to_snowflake(config):
    load_csv_to_snowflake(config, config.get_prediction_key(), 'ml_overdraft_prediction')


def load_post_prediction_to_snowflake(config):
    load_csv_to_snowflake(config, config.get_post_prediction_key(), 'ml_overdraft_post_prediction')


def copy_prediction_to_backend_bucket(config, backend_bucket, **context):
    backend_key = f'overdraft_prediction_{MODEL_KEY}_{config.execution_date}_{int(time.time())}.csv'
    file_url = f's3://{backend_bucket}/{backend_key}'
    logging.info("copy prediction from bucket s3://%s/%s to bucket s3://%s/%s", config.stages_bucket,
                 config.get_post_prediction_key(), backend_bucket, backend_key)
    s3.copy_object(config.s3_connection, config.stages_bucket, config.get_post_prediction_key(),
                   backend_bucket, backend_key)
    slack.slack_file_successfully_upload(context, file_url)


def build_statistics_message(tests, df):
    row = df.iloc[0]
    msg = "*Prediction statistics tests result:* \n"

    for test in tests:
        msg += f"\t{':white_check_mark:' if test.test(row) else ':x:'} "
        msg += f"{test.field_prefix}: {test.format(row)}"
        msg += "\n"

    return msg


def statistics_tests(config, **context):
    # Since statistics_tests use `ml_overdraft_post_prediction` which created and filled by airflow, the query
    # must be executed in the same database, therefore we use snowflake_output_connection
    df = snowflake.SnowflakeDataExtractor(queries) \
        .extract_with_conn(config, snowflake.connect(config.snowflake_output_connection),
                           "overdraft_statistics_checks.sql")

    # Store result to snowfalke
    engine = snowflake.create_engine(config.snowflake_output_connection)
    df.insert(0, "dag_run_start_date", config.get_dag_run_start_date_datetime())
    df["execution_date"] = config.get_execution_date_datetime()
    df["created_at"] = datetime.datetime.now()
    df.to_sql('overdraft_statistics_tests_result', con=engine, index=False, if_exists='append')

    # Slack the result
    msg = build_statistics_message(TESTS, df)
    slack.slack_notification(context, msg)


def slack_revoke_reasons(config, **context):
    # Since statistics_tests use `ml_overdraft_post_prediction` which created and filled by airflow, the query
    # must be executed in the same database, therefore we use snowflake_output_connection
    df = snowflake.SnowflakeDataExtractor(queries) \
        .extract_with_conn(config, snowflake.connect(config.snowflake_output_connection),
                           "revoke_reasons.sql")
    msg = "*Revoke reasons* \n" \
          "```" + \
          tabulate(df, headers='keys', tablefmt='psql') + \
          "```"
    slack.slack_notification(context, msg)
