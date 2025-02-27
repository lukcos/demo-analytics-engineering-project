# Lukas Garrison | Fetch Analytics Engineer Assessment

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

![image.png](Lukas%20Garrison%20Fetch%20Analytics%20Engineer%20Assessment%201a60860cd3528048b09bd5878c5ad12b/image.png)

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

![image.png](Lukas%20Garrison%20Fetch%20Analytics%20Engineer%20Assessment%201a60860cd3528048b09bd5878c5ad12b/image%201.png)

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

![image.png](Lukas%20Garrison%20Fetch%20Analytics%20Engineer%20Assessment%201a60860cd3528048b09bd5878c5ad12b/image%202.png)

We’re cooked. Only `1.4%` of February has a `brandCode` we can reconcile against. 

<aside>
⚠️

Data Quality Issue

There’s simply not enough data to produce a report that any stakeholder would have confidence to inform their decision making. Having just over 1% of viable records to reconcile for a report screams validation issues happening in the background. My first question here would be “what are the data validation scripts currently being used?”. And then having an audit of source-to-target pipelines to ensure high quality data is only making its way into data sources being used for analytics.

</aside>

Onward and upward 😌. Let’s just knock out this query since we already know there’s a litany of charges just waiting for whoever gave us this data. 

Here’s what I’d pass along to answer the question of top 5 brands by receipt volume for Feb 2021:

```sql
select 
    b."name" as "Brand Name"
    , count(distinct rri."receipt_id") as "Total Receipts | Feb 2021"
from 
    "fetch".rewards_receipt_items rri
join 
    "fetch".brands b on 
        rri."brandCode" = b."brandCode"
join 
    "fetch".receipts r on 
        rri.receipt_id = r."_id"
where 
    r."createDate" between '2021-02-01' and '2021-03-01'
group by 
    b."name"
order by 
    "Total Receipts | Feb 2021" desc
limit 
        5
```

Given the schema of the json is still fresh, you could potentially add a `now()` function and `now() - interval '1 month'` to make this dynamic. Or, set a variable as `max(date_trunc('month', receipts."createDate")` and reference that in the `where` clause. A lot of opportunity for iteration there. 

This script can be found in the github repository! 

### Question 2:  How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?

**🧠 Thought Process**