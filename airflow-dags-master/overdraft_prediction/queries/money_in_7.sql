SELECT
    bank_account_id,
    SUM(CASE WHEN (act_type='PM' AND type<>'C2') OR (act_type='AD' AND (details='Debit Card transfer' OR type='FM')) AND transaction_amount > 1 THEN transaction_amount ELSE NULL END) - SUM(CASE WHEN (type='FM' AND act_type='AD' AND details='Direct Deposit Return') THEN ABS(transaction_amount) ELSE 0 END) AS Total_money_in_last_7_days,
    COUNT(CASE WHEN (act_type='PM' AND type<>'C2') OR (act_type='AD' AND (details='Debit Card transfer' OR type='FM')) AND transaction_amount > 1 THEN transaction_amount  ELSE NULL END) - COUNT(CASE WHEN (type='FM' AND act_type='AD' AND details='Direct Deposit Return') THEN ABS(transaction_amount) ELSE NULL END) AS Total_money_in_Count_last_7_days
FROM DWH.fact_mysql_account_transaction_all
WHERE transaction_date <= %(dag_run_start_date)s AND transaction_date >= DATEADD(DAY, -24,  DATE(%(dag_run_start_date)s) )
GROUP BY 1
ORDER BY 1