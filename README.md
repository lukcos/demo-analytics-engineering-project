# Lukas Garrison | Fetch Analytics Engineer Assessment

## Getting Started

You can load the sample data directly into a postgres database by running the following
``` 
python -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt
```
Define your `.env` file with a var `PG_URI` that contains your postgres URI. Once that's defined, you can simply load in the sample data by running: 
```
python scripts/run_scripts.py
```

You should be good to go and run the accompanying SQL scripts against your data now.

## 📚 Resources

https://github.com/lukcos/fetch-analytics-engineer/tree/main

https://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/data-modeling.html

## 🪨  Data Model

---

> Prompt
> 
> 
> ### *First: Review Existing Unstructured Data and Diagram a New Structured Relational Data Model*
> 
> *Review the 3 sample data files provided below. Develop a simplified, structured, relational diagram to represent how you would model the data in a data warehouse. The diagram should show each table’s fields and the joinable keys. You can use pencil and paper, readme, or any digital drawing or diagramming tool with which you are familiar. If you can upload the text, image, or diagram into a git repository and we can read it, we will review it!*
>

Here is the data model I came to after looking over the data and exploring it with python in Jupyter Notebook. After examing all of the possible fields that could be present on the JSON provided, I landed on a 4 table schema, breaking out `receipts.rewardsReceiptItemList` into its own table. There were too many fields nested within that object to feel confident that simply using postgres jsonb operator (IE `receipts.rewardsReceiptItemList::JSON->>'brandCode'`) would be robust enough to do more complex analysis on this data. After answering questions, I feel confident this was the right approach to keep the data model manageable. I also added the `receipt_id` as its own field on the exploded `rewardsReceiptItemList` table to be able to reference back to the original receipt.

![image.png](/imgs/data_model.png)

The following code can be used and plugged into dbdiagram.io to reproduce this model with relations: 

```sql
Table receipts {
  _id uuid [primary key]
  bonusPointsEarned bigint
  bonusPointsEarnedReason varchar
  createDate timestamp
  dateScanned timestamp
  finishedDate timestamp
  modifyDate timestamp
  pointsAwardedDate timestamp
  pointsEarned text
  purchaseDate timestamp
  purchasedItemCount bigint 
  rewardsReceiptStatus text
  totalSpent text 
  userId text
}

Table rewards_receipt_items {
    barcode text 
    description text 
    finalPrice doubleprecision
    itemPrice doubleprecision
    needsFetchReview boolean         
    partnerItemId text 
    preventTargetGapPoints boolean         
    quantityPurchased bigint          
    userFlaggedBarcode text 
    userFlaggedNewItem boolean         
    userFlaggedPrice text 
    userFlaggedQuantity bigint          
    receipt_id text
    needsFetchReviewReason text 
    pointsNotAwardedReason text 
    pointsPayerId text 
    rewardsGroup text 
    rewardsProductPartnerId text 
    userFlaggedDescription text 
    originalMetaBriteBarcode text 
    originalMetaBriteDescription text 
    brandCode text 
    competitorRewardsGroup text 
    discountedItemPrice doubleprecision
    originalReceiptItemText text 
    itemNumber text 
    originalMetaBriteQuantityPurchased bigint          
    pointsEarned doubleprecision
    targetPrice doubleprecision
    competitiveProduct boolean         
    originalFinalPrice text 
    originalMetaBriteItemPrice text 
    deleted boolean
}

Table users {
    _id text [primary key]
    active boolean
    createdDate timestamp
    lastLogin timestamp
    role text
    signUpSource text
    state text
}

Table brands {
    _id text [primary key]
    barcode bigint  
    category text    
    categoryCode text    
    name text    
    topBrand boolean 
    "cpg.ref" text    
    "cpg.id" text    
    brandCode text    

}

Ref: rewards_receipt_items.receipt_id > receipts._id
Ref: receipts.userId > users._id
Ref: rewards_receipt_items.brandCode > brands.brandCode
```

If curious, you can go through the `data_exploration.ipynb` file to get a really... really... raw look at my exploration process. 

I took this model and created some elt scripts that transform the JSON objects and load them into a postgres database.

## 🔍 Queries

---

> Prompt
> 
> 
> ### *Second: Write queries that directly answer predetermined questions from a business stakeholder*
> 
> *Write SQL queries against your new structured relational data model that answer at least two of the following bullet points below of your choosing. Commit them to the git repository along with the rest of the exercise.*
> 
> *Note: When creating your data model be mindful of the other requests being made by the business stakeholder. If you can capture more than two bullet points in your model while keeping it clean, efficient, and performant, that benefits you as well as your team.*
> 

### Question 1:  What are the top 5 brands by receipts scanned for most recent month?

**🧠 Thought Process**

My main concern when seeing this question is regarding what “most recent month” actually means for the stakeholder. Using the above data model loaded into postgres, we can do some light exploration of the quality of the data:

```sql
select 
    date_trunc('month', "createDate") AS month
    , count("_id") AS total_receipts
from 
    "fetch".receipts
group by 
    1
```

![image.png](/imgs/receipt_monthly_distribution.png)

Already, there’s potential for miscommunication in only 30 records falling into the “most recent month”. Now, the data could be valid and they could absolutely be asking for the brands out of these 30 records. So, one other assumption is if it’s a timezone issue, we would see that the majority of these records would have the most volume from 12:00AM - ~4:00AM. 

Let’s take a look: 

```sql
select
    date_trunc('hour', "createDate") AS hour
    , count("_id") AS total_receipts
from 
    "fetch".receipts
where 
    date_trunc('day', "createDate") = (select date_trunc('day', max("createDate")) from "fetch".receipts)
group by 1
```

![image.png](/imgs/receipt_daily_dist.png)

Well… that’s clearly not what’s going on here. Receipts are being recorded all throughout March 1st. 

**⚠️ NEEDS CLARIFICATION:** This would be where I’d reach out to both the stakeholder and whoever sourced this data to clarify what “most recent month” means to them. ALTHOUGH! If this were happening in real time rather than a historical analysis based on sample data, it would be MUCH easier to assume what “most recent month”  means. 

Now, I could also fall on this sword and just do an analysis for both March and Feb 2021 so that I can at least be prepared for either scenario. But, that would depend on how much of a time crunch there is for the stakeholder. Sometimes you just need answers without too much analysis.

<aside>
⚠️

Data Quality Issue

Given this sample data and looking at timestamps, it’s hard to determine how reliable the records are in terms of *all* data being available. We can see that there’s an ill-defined end date, no records in Dec 2020, and a few random records floating in from Oct and Nov. If the majority of this analysis is going to be based on timeseries, we need to establish criteria to clean this data further - or request the complete data set.

</aside>

Without stakeholder input - and the sake of moving on with this exercise - let’s focus on Feb since that will give us a more comprehensive analysis for top brands. 

Since we’ve already broken out `rewardsReceiptItemList` into its own table, we can use the `"fetch".rewards_receipt_items."brandCode"` and `"fetch".brands."brandCode"` as our primary key to examine receipt volumes by brand for Feb.

But… I already know that `rewardsReceiptItemList` is missing from quite a few of the `receipt` records. 

```sql
select 
    (count(DISTINCT rri."receipt_id")::NUMERIC / count(DISTINCT r."_id")::NUMERIC) AS rewards_items_saturation
from 
    "fetch".receipts r
left join 
    "fetch".rewards_receipt_items rri ON 
        rri.receipt_id = r."_id"
```

Using this, we can see that only `60.68%` of receipts have associated `rewardsReceiptItemList` records. Let’s filter that down to Feb 21:

```sql
select 
    (count(DISTINCT rri."receipt_id")::NUMERIC / count(DISTINCT r."_id")::NUMERIC) AS rewards_items_saturation
from 
    "fetch".receipts r
left join 
    "fetch".rewards_receipt_items rri ON 
        rri.receipt_id = r."_id"
where 
    r."createDate" BETWEEN '02-01-2021' AND '03-01-2021'
```

Quick note - in postgres, using the BETWEEN and dates like this means that the date is being calc’d as 2-1-2021 at 12AM and 3-1-2021 12AM. So effectively, all of Feb and no records from Mar are being taken into account here.

We can now see that just `26.8%` of the 444 receipts in Feb 21 have at least one associated `rewardsReceiptItemList`! Let’s see if there’s at least a `brandCode` we can reconcile against for these records.

```sql
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
```

![image.png](/imgs/total_feb.png)

We’re cooked. Only `1.4%` of February has a `brandCode` we can reconcile against. 

<aside>
⚠️

Data Quality Issue

There’s simply not enough data to produce a report that any stakeholder would have confidence to inform their decision making. Having just over 1% of viable records to reconcile for a report screams validation issues happening in the background. My first question here would be “what are the data validation scripts currently being used?”. And then having an audit of source-to-target pipelines to ensure high quality data is only making its way into data sources being used for analytics.

</aside>

Onward and upward 😌. Let’s just knock out this query since we already know there’s a litany of charges just waiting for whoever gave us this data. 

Here’s what I’d pass along to answer the question of top 5 brands by receipt volume for Feb 2021:

```sql
WITH 

source_data as (
    select
        date_trunc('month', r."createDate") as "Month"
        , b."name" as "Brand Name"
        , count(distinct rri."receipt_id") as "Total Receipts"
        , DENSE_RANK()  over (partition by date_trunc('month', r."createDate") order by count(distinct rri."receipt_id") desc) as "Rank"
    from 
        "fetch".rewards_receipt_items rri
    join 
        "fetch".brands b on 
            rri."brandCode" = b."brandCode"
    join 
        "fetch".receipts r on 
            rri.receipt_id = r."_id"
    group by 
        b."name", "Month"
    order by 
        "Month" desc, "Rank" asc
)

select 
    *
from 
    source_data
where 
    "Rank" <= 5
    AND "Month" between '2021-02-01' and '2021-02-28'
```

Given the schema of the json is still fresh, you could potentially add a `now()` function and `now() - interval '1 month'` to make this dynamic. Or, set a variable as `max(date_trunc('month', receipts."createDate")` and reference that in the `where` clause. A lot of opportunity for iteration there. 

Here, since we’re doing the `date_trunc` in `source_data` to create month, we’ll also need to adjust our `end_date` to the last day of the month we’re interested in examining.

We’re also leveraging the `DENSE_RANK()` function to get our actual rankings based on the month and volumes of receipts of that month per brand. Pushing that into a CTE, we can then filter in the where clause to ensure that `Rank` is within top 5 and then we can filter to a specific time frame.

Another note here is that we’re using `join` to not only join data from other tables, but filter to only when those records exist across tables. Depending on if the stakeholder would like to know the volumes of receipts without brands, that information might be helpful to them.

This script can be found in the github repository under `./sql_queries/top_brands_by_receipt_volume.sql`

### Question 2:  How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?

**🧠 Thought Process**

Well, we now have a SQL cript that cranks out top 5 brands from the “most previous” month - and we’ve done this in a way to the only update we really need to make is open up the month start date to Jan ‘21 instead of Feb ‘21. 

```sql
WITH 

source_data as (
select
    date_trunc('month', r."createDate") as "Month"
    , b."name" as "Brand Name"
    , count(distinct rri."receipt_id") as "Total Receipts | Feb 2021"
    , DENSE_RANK()  over (partition by date_trunc('month', r."createDate") order by count(distinct rri."receipt_id") desc) as "Rank"
from 
    "fetch".rewards_receipt_items rri
join 
    "fetch".brands b on 
        rri."brandCode" = b."brandCode"
join 
    "fetch".receipts r on 
        rri.receipt_id = r."_id"
group by 
    b."name", "Month"
order by 
    "Month" desc, "Rank" asc
)

select 
    *
from 
    source_data
where 
    "Rank" <= 5
    AND "Month" between '2021-01-01' and '2021-03-01' -- Only change
```

![image.png](/imgs/rank_by_volume_months.png)

And here’s the visualization of what those rankings look like month over month in a visualization and number of receipts. Nice! 

### Question 3:  When considering *total number of items purchased* from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

**🧠 Thought Process**

So, I need to know what statuses are actually available to reference in the `rewardsReceiptStatus` field. After doing some digging to find distinct values in `rewardsReceiptStatus`, I can see that `Accepted` is actually not a valid value: 

```sql
>>> select distinct "rewardsReceiptStatus" from "fetch".receipts r

rewardsReceiptStatus:
REJECTED
SUBMITTED
FLAGGED
PENDING
FINISHED
```

<aside>
⚠️

Data Quality Issue

Now, this could be a simple fumbling from the stakeholder in conflating “Accepted” with “Finished”, but it could be an actual value in this data that’s missing. “Finished” implies that a transaction has been completed, where “Accepted” could refer to multiple status states, but used internally as its own data definition. Whatever the case is, this is where we’d need to at least raise this issue to the stakeholder to clarify *their* understanding of what “Accepted” means in this context.

</aside>

But, for the sake of this exercise, let’s make an assumption that the stakeholder meant “Finished” when they communicated “Accepted”. We can easily create a `WHERE field IN ('val', 'val',...)` clause to filter to ‘Accepted’ and ‘Rejected’ status. 

One thing I want to check to see is how `receipts.purchasedItemCount` compares against `rewards_receipt_items.quantityPurchased`. I have a feeling that there might be some instances where `receipts.purchasedItemCount` might be `null` but we have a value on `rewards_receipt_items.quantityPurchased`. So let's check:
```sql
select
    r."rewardsReceiptStatus"
    , r."purchasedItemCount"
    , rri."quantityPurchased"
from 
    "fetch".receipts r
join 
    "fetch".rewards_receipt_items rri on
        r."_id" = rri."receipt_id"
where 
    r."purchasedItemCount" IS NULL
```

Sure enough, we have `49` records where `quantityPurchased` is showing `1` but `purchasedItemCount` is `null`. This means that choosing either of these to quantify total purchase count is going to result in an unreliable analysis. For the sake of only using `receipts` to produce a report, we can do the following: 

```sql
select
    "rewardsReceiptStatus"
    , SUM("purchasedItemCount") as total_items
from 
    "fetch".receipts
where 
    "rewardsReceiptStatus" IN ('FINISHED', 'REJECTED')
group by 
    "rewardsReceiptStatus"
```
![image.png](/imgs/finished_v_rejected_receipt_only.png)

We get a roughly `2%` of receipts being "REJECTED" when looking at the total number of "FINISHED" and "REJECTED" receipts. Note, this is not _all_ receipts being compared since other statuses have been left off.

Let's try to approach this from using `rewards_receipt_items`. We also need to be mindful that we’re not being asked to count the receipts themselves, but the number of items associated with that receipt. This poses another problem → not every receipt has a `rewardsReceiptItemList` value present! So, there’s going to be missing data for this analysis. 

Let’s see how much available data we have to work with where `rewardsReceiptItemsList` is present: 

```sql
select 
    (select COUNT(distinct "_id") from "fetch".receipts) as "Total Receipts"
    , (select COUNT(distinct "receipt_id") from "fetch".rewards_receipt_items) as "Total Receipts with Rewards Receipt Items"
    , (select COUNT(distinct "receipt_id") from "fetch".rewards_receipt_items)::FLOAT / (select COUNT(distinct "_id") from "fetch".receipts)::FLOAT as "Percentage of Receipts with Items"
```

**Total Receipts**

**1,119**

**Total Receipts with Rewards Receipt Items**

**679**

**Percentage of Receipts with Items**

**60.68%**

<aside>
⚠️

Data Quality Issue

With only ~61% of receipt records having `rewardsReceiptItemList` present, we’re missing quite a bit of data to do a comprehensive analysis. That means that for any analysis needing to understand individual items, 40% of data is unusable. My initial questions would be “why are we missing this field on nearly half of our records?”. I’d want to better understand the data flow and all the channels where receipt data is being created, what validation measures are in place, and if this should be a required field for all receipt records. If so, why are there paths in the data flow that item data isn’t being captured properly? (tldr; is this a bug or a feature)

</aside>

So we have about 60% of records we can do this analysis on. Let’s continue by doing some counts with making the assumption that the stakeholder mean “Finished” instead of “Accepted”.

```sql
select 
    r."rewardsReceiptStatus"
    , count(rri.*) as "Total Items"
from
    "fetch".receipts r
join 
    "fetch".rewards_receipt_items rri ON 
        rri."receipt_id" = r."_id"
where 
    r."rewardsReceiptStatus" IN ('FINISHED', 'REJECTED')
group by 
    r."rewardsReceiptStatus"
```

Now, this is good as a first glance at number of items, BUT what we actually need is the SUM of `quantityPurchased` to get *all* of the items from this query. 

Doing a quick analysis of `quantityPurchased`, unfortunately there are some `null` values in there. Luckily, there’s a fallback for `userFlaggedQuantity`. I have questions as to the validity of this field, but let’s take a closer look at the quality of this field:

```sql
select 
    count(*)
from 
    "fetch".rewards_receipt_items rri
where 
    ("quantityPurchased" IS NOT NULL and "userFlaggedQuantity" IS NOT NULL)
    AND "quantityPurchased" != "userFlaggedQuantity"
```

By squeazing out the count of for how many records of rewards_receipt_items have both `quantityPurchased` and `userFlaggedQuantity`, AND if they aren’t equal, we can see that there’s only `6` records that match this criteria. That’s roughly `0.09%` of our total records. We’ll keep with this method, and use `quantityPurchased` as our ideal field value, but fallback on `userFlaggedQuantity` when `quantityPurchased` is null.

```sql
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
```

![image.png](/imgs/finished_v_rejected.png)

This script can be found in `./sql_queries/receipt_item_status_comparison.sql` 

With these percentages, it makes sense to use a donut chart to get a little better visualization of what’s happening.

Nice! With the `8,367` items available, we see that only `2.28%` of the items had a receipt with `rewardsReceiptStatus` of “REJECTED”in the pool of "REJECTED" and "FINISHED" receipts.

Some ways this could be enhanced or further analysis could be done: 

- Having a better understanding of `quantityPurchased` v. `userFlaggedQuantity`
- Combine fallback methods for `quantityPurchased`, `userFlaggedQuantity`, and `purchasedItemCount` - again, reconciliation is an issue here.
- Filter to a time frame to look at accepted v. rejected over time to see if there are any trends
- Group by category, brand, rewards group, etc.
- Filter out any items → ie those with `description` ”ITEM NOT FOUND”

### Question 4:  Which brand has the most *spend* among users who were created within the past 6 months?

**🧠 Thought Process**

I know I need to focus on “within the past 6 months”. For this exercise, I know that we’re sometime in 2021, but I can’t leverage postgres’s `now()` function to have executable code for analysis. Let’s just focus on users for now. 

First, I just want to know if we have 6 months worth of user data to work with. Using max and min, I can get a quick answer: 

```sql
select
    date_trunc('month', max("createdDate")) as max_month 
    , date_trunc('month', min("createdDate")) as min_month 
from "fetch".users
```

**max_month: February 1, 2021, 12:00 AM**

**min_month: December 1, 2014, 12:00 AM**

Great, I can see that the “most recent” month for user creations we have is Feb ‘21 and goes back all the way to Dec ‘14.

I can use these to craft “variables” that we can call in our final query. 

```sql
with 
user_months as (
    select
        date_trunc('month', max("createdDate")) as max_month 
        -- , date_trunc('month', min("createdDate")) as min_month  -- if needed
    from "fetch".users
)

select 
    *
from 
    "fetch".users
WHERE
    "createdDate" >= (select max_month from user_months) - interval '6 months'
```

Here we can use that var to do some dynamic filtering later on. 

Next let me take a look at most spend. We can use the previous query in question 3 to help us out in determining spend per receipt.

```sql
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
```

So now we have a `clean_item_count` and a `clean_price` to caclulate total spend. 

The last piece here is joining all the data together to get our brands, users, and spends. Alright, this is going to be a chunky query:

```sql
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

-- bringing it all together
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
group by 
    b."name"
```

![image.png](/imgs/brand_by_vol.png)

We can now see that **Cracker Barrel Cheese** is the highest ranking brand in terms of most spend for users created in the most recent 6 months.

This query can be found `./sql_queries/brands_most_spend.sql` 🙂

### Question 5:  Which brand has the most *transactions* among users who were created within the past 6 months?

**🧠 Thought Process**

This is slightly easier than the previous query. Here, we can refer back to the receipt record and understand each record is essentially “a transaction”. Which is nice, since we won’t have to do data backfilling to get plain, old counts. We can repurpose a bit from the previous query to get us to our answer. 

The main thing we need is to ensure we’re not doubling up on any `receipt._ids` from joining on the `rewards_receipt_items`. We can throw a `distinct` in the count to ensure we’re only counting each receipt *once* as a transaction. Removing some of the cleaning CTEs, and updating the joins in our main `select`, we get the following:

```sql
with
-- getting max month to filter later
user_months as (
    select
        date_trunc('month', max("createdDate")) as max_month 
    from "fetch".users
)

select 
    b."name" as "Brand Name"
    , COUNT(distinct r."_id") as "Total Transactions" -- adding distinct here since we're duping receipt_ids from rewards_receipt_items
    , DENSE_RANK() over (order by COUNT(distinct r."_id") desc) as "Rank"
from 
    "fetch".receipts r 
join 
    "fetch".rewards_receipt_items rri on 
        rri."receipt_id" = r."_id"
join 
    "fetch".brands b on 
        b."brandCode" = rri."brandCode"
join 
    "fetch".users u ON 
        u."_id" = r."userId"
where 
    u."createdDate" >= (select max_month from user_months) - interval '6 months'
group by 
    b."name"
```

![image.png](/imgs/brand_by_transactions.png)

Great! We can see here that **Pepsi** was the brand with the most transactions for users created in the most recent 6 months! It’s also funny to notice that Cracker Barrel Cheese ranks 6th in transactions but 1st in spend. 

This query can be found `./sql_queries/brands_most_transactions.sql` 🙂

## **🤔 Data Quality Issues**

---

> *Prompt*
> 
> 
> *Using the programming language of your choice (SQL, Python, R, Bash, etc...) identify as many data quality issues as you can. We are not expecting a full blown review of all the data provided, but instead want to know how you explore and evaluate data of questionable provenance.*
> 
> *Commit your code and findings to the git repository along with the rest of the exercise.*
> 

Throughout the above exercise, I’ve called out exactly when I encountered specific data quality issues. I’m going to just summarize much of what I’ve identified while doing this exercise in a couple of categories.

All code can be found in `./sql_queries/data_quality`

### 🕥 Time/Date Data

- Ambiguity around "most recent month" definition
- Ill-defined start and end date in the dataset
- Missing data for specific time periods (no records for December 2020)
- Inconsistent record distribution across time periods

### 🫥 Data Completeness

- Only ~1% of records viable for certain reports
- Missing `rewardsReceiptItemList` in approximately 40% of receipt records
- Null values in critical fields like `quantityPurchased`

### 😵‍💫 Data Inconsistency

- Terminology confusion between "Accepted" vs "Finished" status values
- Discrepancies between `quantityPurchased` and `userFlaggedQuantity` (though only in 0.09% of records)

### ✅ Process and Validation Concerns

- I have many questions about existing data validation scripts
- There’s potentially need for audit of source-to-target pipelines
- Unclear data flow and validation measures across receipt data creation channels
- Uncertainty if missing fields are bugs or intentional features

### 💣 Analysis Impact

- Insufficient data reliability for stakeholder decision-making
- Need for additional data cleaning criteria for time-series analyses
- Approximately 40% of data unusable for item-level analysis
- Risk of double-counting transactions when joining tables

### Recommendations

1. Get clarification from stakeholders on ambiguous terms ("most recent month", "Accepted" status)
2. Investigate data validation processes currently in place
3. Determine root causes for missing `rewardsReceiptItemList` data
4. Work towards defining some criteria for cleaning timestamp data
5. Document any other existing fallback approaches (e.g., using `userFlaggedQuantity` when `quantityPurchased` is null)

## **👔 Stakeholder Communication**

---

> *Prompt*
> 
> 
> *Construct an email or slack message that is understandable to a product or business leader who isn’t familiar with your day to day work. This part of the exercise should show off how you communicate and reason about data with others. Commit your answers to the git repository along with the rest of your exercise.*
> 
> - *What questions do you have about the data?*
> - *How did you discover the data quality issues?*
> - *What do you need to know to resolve the data quality issues?*
> - *What other information would you need to help you optimize the data assets you're trying to create?*
> - *What performance and scaling concerns do you anticipate in production and how do you plan to address them?*

Slack message: 

Hey {name}! 👋 Hope all’s well and you’re having a good week! So, I’ve been working through the data request, and while I’ve been able to answer most of the questions positioned by you (and possibly group), I wanted you to be aware of some severe quality issues I’ve found while working with the provided data. 

One of the biggest concerns I have here is with the completness of the provided data. Working through answering “what are the top 5 brands by receipts scanned for most recent month?”, it became apparent when reconciling records that a large chunk of the data - **nearly 40% of receipt records** - didn’t have the necessary fields to confidently link receipt to brands. These missing fields mean that **we’re only able to fully use ~60% of our receipt data to gain real insights into the brand-to-user relationship and behavior**. What’s even worse, is that of that 60% of records, we can only reconcile a fraction of those receipts to brands. Of our 1,114 receipt records, only 46 (4.1%) had enough data to link back to individual brands! This isn’t enough data to have establish significance. 

While I was still able to perfrom an analysis with the data that I’ve received, a few more glaring issues popped up. 

- We don’t have any receipt data for December 2021
    - Looking at the distribution of receipt records being created for the data set, all of December is missing.
- “Most recent month” is a relative term with this analysis and since we a very small amount of data for Mar ‘21, we need to decide on an agreed definition for the timeframe.
- There’s consistently missing data within our receipt items that impacts our ability to track and measure spend across brands and users. IE:
    - There are 554 instances of item descriptions either being left blank or labled as “ITEM NOT FOUND”
    - There’s no standardization of the description field values
- There’s also miscommunication around a shared definiton of what “Accepted” status means on a receipt. In the system, no such status exists. It’s ambiguous since we could be referring to “Finished” or some combination of status(es) to define “Accepted”. We should try and rally around a shared definition here.

I feel like our first step in establishing confidence in our analytics program is to look at the root cause of these data quality issues. What I’d love to do is open up a dialog with product and engineering to see if I can get a better picture of the data pipeline, our points of ingestion, and data validation methods that are currently in use. From there, we can start to identify and document where the failure points are in our system. Until we do this, we run the risk of bad data continually making its way into our database. Ideating and implementing solutions to reduce erroneous and incomplete data from getting stored in our database would be in everyone’s (and the company’s) best interest. 

While that’s the long-term solution, one immediate actionable thing we can do to salvage this analysis is up the volume of sample records from Jan 2021 to Feb 2021. Or open that up to a 6 months timeframe. Ideally, getting a more robust sample set of data might allow us to use our existing scripts to perform the same analysis but gain a little more confidence in the significance of the results. That along with hammering out our data dictionary for ambiguous terminology should get us in “better” shape than where we sit right now. 

I honestly don’t feel comfortable scaling this data solution as it exists today. We’re just running the risk of shoving more bad data into our database, potentially compromising existing analytics solutions, and killing confidence in our data culture if we were to scale now. There’s just too much inconsistent, missing, and irreconcilable data currently. We should be focusing our efforts on the entry point of data into our data pipelines and implementing validation methods. I’d be more than happy to reach out to whoever I need to for getting that conversation started.

Once we have those items ironed out, I'd also love to explore productionizing a solution with solid data. Given solid data, a few tweaks, and fine-tuning the model, we can create automated pulls and transormations of this data so that we can have reliable, up-to-date, and accurate data. 

Lemme know if you have any questions or want to chat about this over a call!