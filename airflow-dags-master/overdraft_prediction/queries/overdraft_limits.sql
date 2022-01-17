SELECT *,
case when EXPIRATION_DATE >= DATE(%(dag_run_start_date)s)
AND DATE(EXPIRATION_DATE) <= DATEADD(DAY, 3, DATE(%(dag_run_start_date)s)) then 1 else 0 end as CLOSE_EXPIRATION
FROM "LILI_ANALYTICS"."ODS"."OVERDRAFT_LIMITS"