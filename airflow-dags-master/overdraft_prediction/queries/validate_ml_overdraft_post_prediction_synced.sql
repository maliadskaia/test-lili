SELECT TRUE
FROM "DWH"."ML_OVERDRAFT_POST_PREDICTION"
WHERE DAG_RUN = %(dag_run_start_date)s
HAVING count(1) = (select count(distinct bank_account_id) from LILI_ANALYTICS.DWH.fact_mysql_customer_monthly);
