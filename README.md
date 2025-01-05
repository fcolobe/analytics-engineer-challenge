# Analytics Engineering Technical Test

## Context

We operate demand-responsive transit services across multiple territories and countries.
Our system tracks the entire user journey from initial search to completed trips, storing data about:
- Products and their territories
- Users and their search patterns
- Actual bookings and trip completions

The main goal of this test is to evaluate your ability to create a clean, efficient,
and queryable data model that helps analyze our transit operations,
with a focus on completed trips and user activity patterns.

It is highly prefered to answer the first question (the only required one) with a structured project than answer everything with a hacky setup.

## Data Structure

The source data model is documented in `schema.dbml`. You can visualize it using [dbdiagram.io](https://dbdiagram.io) by copying the content of the file.

For each table in the model, a parquet file has been provided in the `data/` directory:
- `data/products.parquet`
- `data/product_territories.parquet`
- `data/users.parquet`
- `data/history_search.parquet`
- `data/dispatch_bookings.parquet`

## Important note

Note that some identifiers might not be unique across all products. Your data model should take this into consideration.
- Example: A user with ID=1 in product "A" is different from a user with ID=1 in product "B"

To determine if a booking became an actual trip, consider both the `status` and `passenger_status` fields in the `dispatch_bookings` table.

## Technical Stack

- Python 3.9+ 
- DuckDB for data manipulation and querying (`pip install duckdb`)
- Raw SQL for data modeling (no frameworks required)

## Assignment

### Goal

Create an analytics-ready data model that makes it easy to analyze completed trips across our system.
Your model should facilitate answering questions about user engagement and operational performance.

### Steps

1. Study the provided data model
2. Design and implement your analytical data model using SQL views/tables
3. Answer the required questions
4. Document your approach and findings

### Questions to Answer

1. [REQUIRED] Stats on operations:
   - How many completed trips do we have per country?
   - What's the average number of searches, bookings, and completed trips per user?

2. [OPTIONAL] Operational insights (if time permits):
   - What's the conversion rate from searches to completed trips per territory?
   - What's the distribution of booking channels (booked_from) for completed trips?

3. [OPTIONAL] Advanced analysis (if time permits):
   - Identify patterns in cancellation rates across territories
   - Analyze peak booking hours per country
   - Your own insights about the data

### Evaluation Criteria

You will be evaluated on:
1. **Data Modeling**
   - Clear separation of concerns (raw/prepared/reporting layers)
   - Meaningful naming conventions for tables, views, and fields (don't hesitate to rename)
   - Efficient denormalization choices
   - Documentation of your models (you can update the DBML file with your new models)

2. **Code Quality**
   - SQL query efficiency
   - Code organization (add helpers, formatting, isolate sql queries in files...)
   - Reusability of created models

3. **Analysis**
   - Accuracy of results
   - Clarity of explanations
   - Additional insights (if provided)

## Submission

### Getting Started

We provide a skeleton for your solution in `main.py` (see the file for details). The structure includes functions for:
- `load_data`: Loading data from parquet files
- `prepare_dataset` / `create_reports`: Creating your analytical model
- Running the analysis queries

You should focus on implementing these functions while maintaining a clean and organized codebase.
The main goal is to evaluate your `SQL` / data modeling skills, so the python code should stay minimalist:
```python
conn = duckdb.connect()

sql = """
-- A first table creation
CREATE OR REPLACE VIEW table_1 as
    (select 'table_1' as "src", '42' as "content");

-- A second table creation
CREATE OR REPLACE VIEW table_2 as
    (select 'table_2' as "src", '42_again' as "content");

-- Some results
WITH
    results_table_1 as (select * from table_1),
    results_table_2 as (select * from table_2)
select * from results_table_1
UNION ALL
select * from results_table_2;
"""

conn.sql(sql).show()
```

### Time Limit

You should spend no more than 2-3 hours on this test. If you run out of time, document what you would have done with more time.

### Deliverables

Minimal deliverable in a zip archive:
1. Your completed Python script
2. Documentation of your models (You can update `schema.dbml` if you want)
3. `REPORT.md` containing:
   - Documentation of your data modeling choices
   - Answers to the questions
   - Any additional insights you discovered
   - Any assumptions you made

You can ignore the `data` folder and the `padam_data.duckdb` file created by the `main.py` in your submission.
The reviewer already has the parquet files, and will delete the `padam_data.duckdb` file before running the `main.py` script.
Don't hesitate to version your implementation to help the reviewer follow your progression.

While we provide a skeleton in `main.py`, you're welcome to structure your solution differently as long as it:
- Creates clean, reusable analytical datasets
- Documents your data models and transformations
- Answers the required questions
- Demonstrates analytics engineering best practices (clear layer separation, meaningful naming, efficient transformations)

Your approach and choices are as important as the final results - please explain your reasoning in the `REPORT.md`.
