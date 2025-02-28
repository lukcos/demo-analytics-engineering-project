with
-- cleaning item quantity and prices
clean_items as (
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
),

-- total spends per receipt
spends as (
    select 
        "receipt_id"
        , "brandCode"
        , "clean_item_count" * "clean_price" as "total_spend"
    from 
        clean_items
),

-- getting max month to filter later
user_months as (
    select
        date_trunc('month', max("createdDate")) as max_month 
        -- , date_trunc('month', min("createdDate")) as min_month 
    from "fetch".users

)

select 
    b."name" as "Brand Name"
    , SUM(s.total_spend) as "Total Spend"
    , DENSE_RANK() over (order by SUM(s.total_spend) desc) as "Rank"
from 
    "fetch".receipts r 
join 
    spends s ON 
        s."receipt_id" = r."_id"
join 
    "fetch".brands b on 
        b."brandCode" = s."brandCode"
join 
    "fetch".users u ON 
        u."_id" = r."userId"
where 
    u."createdDate" >= (select max_month from user_months) - interval '6 months'
    AND u.role != 'fetch-staff' -- making sure we're not including any test data from staff
group by 
    b."name"