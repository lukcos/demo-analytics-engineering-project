select
    "rewardsReceiptStatus"
    , SUM("purchasedItemCount") as total_items
from 
    "fetch".receipts
where 
    "rewardsReceiptStatus" IN ('FINISHED', 'REJECTED')
group by 
    "rewardsReceiptStatus"