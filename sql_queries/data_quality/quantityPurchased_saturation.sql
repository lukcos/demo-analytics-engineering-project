select 
    count(*)
from 
    "fetch".rewards_receipt_items rri
where 
    ("quantityPurchased" IS NOT NULL and "userFlaggedQuantity" IS NOT NULL)
    AND "quantityPurchased" != "userFlaggedQuantity"