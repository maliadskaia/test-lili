select
    bank_account_id,
    sum(case when (act_type='PM' and type<>'C2') or (act_type='AD' and (details='Debit Card transfer' or type='FM')) and transaction_amount > 1 then transaction_amount else null end) - sum(case when (type='FM' and act_type='AD' and details='Direct Deposit Return') then abs(transaction_amount) else 0 end) as Total_money_in_week4,
    count(case when (act_type='PM' and type<>'C2') or (act_type='AD' and (details='Debit Card transfer' or type='FM')) and transaction_amount > 1 then transaction_amount  else null end) - count(case when (type='FM' and act_type='AD' and details='Direct Deposit Return') then abs(transaction_amount) else null end) as Total_money_in_Count_week4
from DWH.fact_mysql_account_transaction_all
where settle_date > dateadd(day, 21, dateadd(month, -1, %(dag_run_start_date)s)) and settle_date <= %(dag_run_start_date)s
group by 1
order by 1