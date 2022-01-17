SELECT bank_account_id
    ,SUM(CASE WHEN SUB_TYPE='denied_auth_nsf' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) denied_auth_nsf
    ,COUNT(CASE WHEN SUB_TYPE='denied_auth_nsf' THEN bank_account_id ELSE NULL END ) denied_auth_nsf_cnt
    ,SUM(CASE WHEN SUB_TYPE='denied_auth' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) denied_auth
    ,COUNT(CASE WHEN SUB_TYPE='denied_auth' THEN bank_account_id ELSE NULL END ) denied_auth_cnt
    ,SUM(CASE WHEN SUB_TYPE='auth_exp' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) auth_exp
    ,COUNT(CASE WHEN SUB_TYPE='auth_exp' THEN bank_account_id ELSE NULL END ) auth_exp_cnt
    ,SUM(CASE WHEN SUB_TYPE='denied_auth_inactive_card' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) denied_auth_inactive_card
    ,COUNT(CASE WHEN SUB_TYPE='denied_auth_inactive_card' THEN bank_account_id ELSE NULL END ) denied_auth_inactive_card_cnt
    ,SUM(CASE WHEN SUB_TYPE='denied_auth_invalid_pin' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) denied_auth_invalid_pin
    ,COUNT(CASE WHEN SUB_TYPE='denied_auth_invalid_pin' THEN bank_account_id ELSE NULL END ) denied_auth_invalid_pin_cnt
    ,SUM(CASE WHEN SUB_TYPE='denied_auth_gas' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) denied_auth_gas
    ,COUNT(CASE WHEN SUB_TYPE='denied_auth_gas' THEN bank_account_id ELSE NULL END ) denied_auth_gas_cnt
    ,SUM(CASE WHEN SUB_TYPE='ach_credit_fail' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) ach_credit_fail
    ,COUNT(CASE WHEN SUB_TYPE='ach_credit_fail' THEN bank_account_id ELSE NULL END ) ach_credit_fail_cnt
    ,SUM(CASE WHEN SUB_TYPE='ach_credit_return' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) ach_credit_return
    ,COUNT(CASE WHEN SUB_TYPE='ach_credit_return' THEN bank_account_id ELSE NULL END ) ach_credit_return_cnt
    ,SUM(CASE WHEN SUB_TYPE='create_hold' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) create_hold
    ,COUNT(CASE WHEN SUB_TYPE='create_hold' THEN bank_account_id ELSE NULL END ) create_hold_cnt
    ,SUM(CASE WHEN SUB_TYPE='expire_hold' THEN ABS(TRANSACTION_AMOUNT) ELSE NULL END ) expire_hold
    ,COUNT(CASE WHEN SUB_TYPE='expire_hold' THEN bank_account_id ELSE NULL END ) expire_hold_cnt
FROM "LILI_ANALYTICS"."ODS"."MYSQL_ACCOUNT_TRANSACTION_REJECT"
WHERE TRANSACTION_DATE >= DATEADD(MONTH, -1,  DATE(%(dag_run_start_date)s) ) AND TRANSACTION_DATE <= %(dag_run_start_date)s
GROUP BY 1