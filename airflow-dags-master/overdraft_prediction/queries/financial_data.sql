SELECT
        dw.bank_account_id,
        MAX(dw.account_active) AS account_active,
        MIN(signup_date) AS signup_date,
        TO_TIMESTAMP(%(dag_run_start_date)s) AS period_end,
        MAX(settle_date) AS last_transaction_in_time,
        COUNT(CASE WHEN ata.rolling_balance < 0 THEN 1 ELSE NULL END) AS had_negative_balance,
        MAX(CASE WHEN act_type='PM' THEN transaction_date ELSE NULL END) AS last_transaction_money_in_in_time,
        ABS(SUM(CASE WHEN act_type IN ('ST','VS','IS','VI', 'DB', 'SD', 'MP') AND ata.type = 'W' THEN ata.transaction_amount ELSE 0 END)) AS ATM_sum,
        ABS(SUM(CASE WHEN act_type IN ('ST','VS','IS','VI', 'DB', 'SD', 'MP') AND ata.type <> 'W' THEN ata.transaction_amount ELSE 0 END)) AS Swipe_sum,
        ABS(SUM(CASE WHEN act_type IN ('ST','VS','IS','VI', 'DB', 'SD', 'MP') THEN ata.transaction_amount ELSE 0 END)) AS Spend_sum,
        SUM(CASE WHEN act_type='PM' AND ata.type='FM' THEN ABS(ata.transaction_amount) ELSE 0 END) - SUM(CASE WHEN (ata.type='FM' AND act_type='AD' AND details='Direct Deposit Return') THEN ABS(ata.transaction_amount) ELSE 0 END) AS direct_deposit_sum,
        SUM(CASE WHEN act_type='PM' AND (ata.type IN ('VT','VA','VH','MX')) THEN ABS(ata.transaction_amount) ELSE 0 END) AS direct_pay_sum,
        ABS(SUM(CASE WHEN (act_type='PM' AND ata.type='AC') AND ata.transaction_amount > 0 THEN ata.transaction_amount ELSE 0 END)) AS ACH_sum,
        SUM(CASE WHEN act_type='PM' AND (ata.type='OR') THEN ABS(ata.transaction_amount) ELSE 0 END) AS Check_sum,
        SUM(CASE WHEN act_type='PM' AND (ata.type IN ('GT', 'GO', 'CE')) THEN ABS(ata.transaction_amount) ELSE 0 END) AS Greendot_sum,
        SUM(CASE WHEN (act_type='AD' AND details='Debit Card transfer') THEN ABS(ata.transaction_amount) ELSE 0 END) AS Card_Deposit_sum,
        SUM(CASE WHEN (act_type='PM' AND ata.type<>'C2') OR (act_type='AD' AND (details='Debit Card transfer' OR ata.type='FM')) AND transaction_amount > 1 THEN transaction_amount ELSE NULL END) - SUM(CASE WHEN (ata.type='FM' AND act_type='AD' AND details='Direct Deposit Return') THEN ABS(ata.transaction_amount) ELSE 0 END) AS Total_money_in,
        COUNT(CASE WHEN (act_type='PM' AND ata.type<>'C2') OR (act_type='AD' AND (details='Debit Card transfer' OR ata.type='FM')) AND transaction_amount > 1 THEN transaction_amount  ELSE NULL END) - COUNT(CASE WHEN (ata.type='FM' AND act_type='AD' AND details='Direct Deposit Return') THEN ABS(ata.transaction_amount) ELSE NULL END) AS Total_money_in_Count,
        AVG(ata.rolling_balance) AS average_balance,
        COUNT(DISTINCT CASE WHEN dds.type='PAYROLL' THEN 1 ELSE NULL END) AS did_payroll,
        COUNT(DISTINCT CASE WHEN dds.type='Marketplace' THEN 1 ELSE NULL END) AS did_marketplace,
        COUNT(DISTINCT CASE WHEN dds.type='FINANCIAL INSTITUTION' THEN 1 ELSE NULL END) AS did_financial_institution,
        COUNT(DISTINCT CASE WHEN dds.type='Unemployment' THEN 1 ELSE NULL END) AS did_unemployment,
        COUNT(DISTINCT CASE WHEN dds.type='Tax Refund' THEN 1 ELSE NULL END) AS did_tax_refund,
        MAX(pro_customer) AS pro_customer
    FROM (SELECT bank_account_id, signup_date, account_active, MAX(pro_customer) AS pro_customer
            FROM DWH.fact_mysql_customer_monthly
            GROUP BY 1,2,3) dw
    LEFT JOIN DWH.fact_mysql_account_transaction_all ata
        ON dw.bank_account_id=ata.bank_account_id AND ata.settle_date <= %(dag_run_start_date)s AND ata.settle_date >= DATEADD(MONTH, -1,  DATE(%(dag_run_start_date)s) )
    LEFT JOIN ODS.mysql_direct_deposit_sources dds
        ON dds.merchant=ata.details
    GROUP BY 1
    ORDER BY 1