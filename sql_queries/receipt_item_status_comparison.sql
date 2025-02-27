with
-- cleaning items quantity data
clean_items as (
    select 
        "receipt_id"
        -- using userFlaggedQuantity as fallback for when quantityPurchased is null
        , case 
            when "quantityPurchased" is not null then "quantityPurchased"
            when "quantityPurchased" is null and "userFlaggedQuantity" is not null then "userFlaggedQuantity" 
            else NULL
            end as "clean_item_count"
    from 
        "fetch".rewards_receipt_items rri
)

select 
    r."rewardsReceiptStatus"
    , SUM(c."clean_item_count") as "Total Items"
from
    "fetch".receipts r
join 
    clean_items c ON 
        c."receipt_id" = r."_id"
where 
    r."rewardsReceiptStatus" IN ('FINISHED', 'REJECTED')
group by 
    r."rewardsReceiptStatus"