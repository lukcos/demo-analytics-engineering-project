select 
    "receipt_id"
    , "brandCode"
    -- using userFlaggedQuantity as fallback for when quantityPurchased is null
    , case 
        when "quantityPurchased" is not null then "quantityPurchased"
        when "quantityPurchased" is null and "userFlaggedQuantity" is not null then "userFlaggedQuantity" 
        else NULL
        end as "clean_item_count"
    , case 
        when "finalPrice"::NUMERIC is not null then "finalPrice"
        when "finalPrice"::NUMERIC is null and "userFlaggedPrice"::NUMERIC is not null then "userFlaggedPrice"::NUMERIC
        else NULL
        end as "clean_price"
from 
    "fetch".rewards_receipt_items