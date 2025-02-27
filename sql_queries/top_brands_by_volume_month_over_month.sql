WITH 

source_data as (
    select
        date_trunc('month', r."createDate") as "Month"
        , b."name" as "Brand Name"
        , count(distinct rri."receipt_id") as "Total Receipts"
        , DENSE_RANK()  over (partition by date_trunc('month', r."createDate") order by count(distinct rri."receipt_id") desc) as "Rank"
    from 
        "fetch".rewards_receipt_items rri
    join 
        "fetch".brands b on 
            rri."brandCode" = b."brandCode"
    join 
        "fetch".receipts r on 
            rri.receipt_id = r."_id"
    group by 
        b."name", "Month"
    order by 
        "Month" desc, "Rank" asc
)

select 
    *
from 
    source_data
where 
    "Rank" <= 5
    AND "Month" between '2021-01-01' and '2021-02-28'