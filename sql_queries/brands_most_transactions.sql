with
-- getting max month to filter later
user_months as (
    select
        date_trunc('month', max("createdDate")) as max_month 
    from "fetch".users
)

select 
    b."name" as "Brand Name"
    , COUNT(distinct r."_id") as "Total Transactions" -- adding distinct here since we're duping receipt_ids from rewards_receipt_items
    , DENSE_RANK() over (order by COUNT(distinct r."_id") desc) as "Rank"
from 
    "fetch".receipts r 
join 
    "fetch".rewards_receipt_items rri on 
        rri."receipt_id" = r."_id"
join 
    "fetch".brands b on 
        b."brandCode" = rri."brandCode"
join 
    "fetch".users u ON 
        u."_id" = r."userId"
where 
    u."createdDate" >= (select max_month from user_months) - interval '6 months'
    AND u.role != 'fetch-staff' -- making sure we're not including any test data from staff
group by 
    b."name"
limit 15