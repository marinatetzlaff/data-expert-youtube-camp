# HW7 - Data Pipeline Maintenance

Imagine you are in a group of 4 data engineers, you will be in charge of creating the following things:
You oversee managing these 5 pipelines that cover the following business areas:
-   Profit
-   Unit-level profit needed for experiments
-   Aggregate profit reported to investors
-   Growth
-   Aggregate growth reported to investors
-   Daily growth needed for experiments
-   Engagement
-   Aggregate engagement reported to investors

## Owners

The **primary and secondary owners** of these pipelines should be the data engineers.

### Primary Owners

For profit-related pipelines, we should have 2 primary owners, given the high importance of these pipelines. Likely those primary owners will also be the most senior data engineers, with the most expertise in their fields. For growth-related pipelines, there will be 1 primary owner, and for engagement we should have another primary owner. 

### Secondary Owners

Similarly, profit-related pipelines will have 2 secondary owners, with the other types of pipelines (growth and engagement) having 1 secondary owner.

We will have 1 primary owner of growth be the secondary owner for profit. Similarly, 1 primary owner of engagement will be the secondary owner for profit. Finally, the 2 primary owners for profit will be the secondary owner for engagement and growth.

##  On-call schedule

The **on-call schedule** may be such that every sprint a dedicated team member takes up to 10% of the teams’ story points in requests, which will translate to 1 total day of request work. Each sprint consists of 10 working days. Each sprint afterwards will be a rotation to a different team member. 

If requests exceed the allotted 10% team effort, then a request for new work will be submitted to the ticketing system, as this indicates that the business team has new priorities that go beyond a simple ad-hoc request.

###  On-call schedule

#### Flexibility

Additionally, holidays will be a joint effort among two to three engineers, which will rotate annually so that the same team of people is not responsible for being on call year after year.

#### Coverage Period

Each of the four data engineers will have at least 1 day of uninterrupted holiday time.

In addition, determine the coverage period, which is usually 24 hours per day and break it into manageable shifts such as 3 8-hour blocks assigned to the three engineers, with the fourth engineer having that day off.

#### Compensation

Consider compensating engineers extra for working on-call during holidays.

## Run books
Suppose that we have **run books** for all pipelines that report metrics to investors. 

Something that can go wrong in these pipelines is that we have a schema change from a third-party API or another team's data, or that we have a data quality issue leading to failed tests.

### Explain data quality checks
In this case, the run books can help explain what data quality checks we have in place and what happens when data quality checks fail. In particular:

- **SLAs**: The SLA for how long it will take to get a group of pipelines according to its business area will be specified.

- **On-call procedures**: We specify here that when an on-call incident arises:

- Retry failed transformations up to 3 times. Automatically re-ingest missing files from the source.
- Escalate for unresolved errors to the primary owner. Escalate to the Secondary Owner after a few hours of no resolution.
- Fallback: load partial data with a warning placeholder to relevant teams.

### Document the schema
They can also serve as documentation around schema so that when the data model design changes, we are able to determine quickly what factor in the data model has changed. In particular:

- **Pipeline descriptions**: each pipeline's purpose will be specified and a brief description of the types of data it processes.

- **Example pipeline**: Profit Calculation pipeline

- Description: Calculates the daily and monthly profit metrics but summarizing revenue and expense data for financial reporting.

- Source data: revenue data by order ID. Expense data such as operational costs, marketing expenses, and labor costs.

- Transformed data: Daily gross and net profit per product category. Monthly profit trends with an alert to determine when profits have dropped.

- Target data: tables such as daily_profit_summary and monthly_profit_trends.

- **Example Data quality checks**:

1. Schema validation. Ensure that revenue entries with null or negative amounts to be identified.
2. Reconciliation. Totals match between raw data and transform data.
3. Anomaly detection. Identifies outliers in profit trends.
4. Duplicates. Identify duplicate order IDs to remove them.
5. Data completeness. Ensure that data exists for all dates, if not trigger a re-ingestion.


