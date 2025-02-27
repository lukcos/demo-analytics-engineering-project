-- Used to generate report of top 5 brands by receipt volumes

select 
    b."name" as "Brand Name"
    , count(distinct rri."receipt_id") as "Total Receipts | Feb 2021"
from 
    "fetch".rewards_receipt_items rri
join 
    "fetch".brands b on 
        rri."brandCode" = b."brandCode"
join 
    "fetch".receipts r on 
        rri.receipt_id = r."_id"
where 
    r."createDate" between '2021-02-01' and '2021-03-01'
group by 
    b."name"
order by 
    "Total Receipts | Feb 2021" desc