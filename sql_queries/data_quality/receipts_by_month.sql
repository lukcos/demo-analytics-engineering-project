select 
    date_trunc('month', "createDate") AS month
    , count("_id") AS total_receipts
from 
    "fetch".receipts
group by 
    1