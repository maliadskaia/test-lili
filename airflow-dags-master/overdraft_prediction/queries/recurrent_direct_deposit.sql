select bank_account_id,
(CASE WHEN count(bank_account_id)>0 THEN 1 END) as IS_RECURRENT_DD
from LILI_ANALYTICS.ods.MYSQL_DW_CUSTOMER_MONTHLY_NEW dcmn
where bank_account_id in (select bank_account_id
                           from (select c.bank_account_number,ata2.bank_account_id,max(total_direct_deposit) as total_direct_deposit,
                                SUM(CASE WHEN (ata2.act_type='PM' AND ata2.type<>'C2') OR (ata2.act_type='AD' AND (ata2.details='Debit Card transfer' OR ata2.type='FM'))
                                THEN ata2.transaction_amount ELSE NULL END) Total_money_in
                                from LILI_ANALYTICS.ods.MYSQL_ACCOUNT_TRANSACTION_ALL as ata2
                            inner join (
                            select bank_account_number,b.bank_account_id, sum(Total_Amount) as total_direct_deposit
                            from (select bank_account_id,bank_account_number
                                    from LILI_ANALYTICS.ods.MYSQL_DW_CUSTOMER_MONTHLY_NEW where account_legit =1 and pro_customer=1 group by 1,2) as a
                            inner join (select bank_account_id,
                                        case when dss.type is null then 'N/A' else upper(dss.type) end AS Category,
                                        sum(transaction_amount) AS Total_Amount
                                        from  LILI_ANALYTICS.ods.MYSQL_ACCOUNT_TRANSACTION_ALL AS ata
                                        left join LILI_ANALYTICS.ods.MYSQL_DIRECT_DEPOSIT_SOURCES as dss on dss.merchant=ata.details
                                        where ata.act_type='PM' and ata.type='FM'
                                        And settle_date >= DATEADD(MONTH, -1, DATE(%(dag_run_start_date)s))
                                        group by bank_account_id, dss.type
                                        having Category in ('PAYROLL', 'PAYMENTS', 'MARKETPLACE','SOCIAL SECURITY ADMINISTRATION','FEDERAL PAYROLL')
                                        ) as b
                            on a.bank_account_id=b.bank_account_id
                            group by 1,2
                            order by 3 desc) as c
                        on ata2.bank_account_id = c.bank_account_id
                        where settle_date >= DATEADD(MONTH, -1, DATE(%(dag_run_start_date)s))
                        group by 1,2) as d
where total_direct_deposit>=500 OR (total_money_in>=500 and total_direct_deposit>=200))
group by 1