select 
    (select COUNT(distinct "_id") from "fetch".receipts) as "Total Receipts"
    , (select COUNT(distinct "receipt_id") from "fetch".rewards_receipt_items) as "Total Receipts with Rewards Receipt Items"
    , (select COUNT(distinct "receipt_id") from "fetch".rewards_receipt_items)::FLOAT / (select COUNT(distinct "_id") from "fetch".receipts)::FLOAT as "Percentage of Receipts with Items"