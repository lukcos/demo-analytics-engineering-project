select 
    count(DISTINCT r."_id") "Total Receipts"
    , count(DISTINCT rri."receipt_id") "Total Rewards Receipt Items"
    , (count(DISTINCT rri."receipt_id")::NUMERIC / count(DISTINCT r."_id")::NUMERIC) AS "Percentage"
from 
    "fetch".receipts r
left join 
    "fetch".rewards_receipt_items rri ON 
        rri.receipt_id = r."_id"
        AND rri."brandCode" IS NOT NULL
where 
    r."createDate" BETWEEN '02-01-2021' AND '03-01-2021'