select
    date_trunc('hour', "createDate") AS hour
    , count("_id") AS total_receipts
from 
    "fetch".receipts
where 
    date_trunc('day', "createDate") = (select date_trunc('day', max("createDate")) from "fetch".receipts)
group by 1