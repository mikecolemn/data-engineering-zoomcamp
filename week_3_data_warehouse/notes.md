# Week 3 Data warehouse

* OLAP vs OLTP
* What is a data warehouse
* BigQuery
  * Cost
  * Partitions and clustering
  * Best Practices
  * Internals
  * ML in BQ

Slides available at _(https://docs.google.com/presentation/d/1a3ZoBAXFk8-EhUsd7rAZd-5p_HpltkzSeujjRGB2TAI/edit#slide=id.g10eebc44ce4_0_55)_

OLTP = Online Transaction Processing
OLAP = Online Analytical Processing

OLTP would be used in your backend processing, group a few queries together and rollback if something fails

Data warehouse is a OLAP solution.  Used for reporting an data analysis

BigQuery has publicly available data sets.  Can search for them, like `citibike_stations`.  Looks like they might be organized under a `bigquery-public-data` project.

From your query results, you can save them off to files or other BigQuery tables.
Can Explore Results with Data Studio, or looks like there are other options in current system

BigQuery Costs:

* On demand pricing - 1 TB of data processed is $5
* Flat rate pricing
  * Bazsed on nubmer of pre requested slots
  * 100 slots is $2,000/month = 400 TB data processed based on on demand pricing


Start processing some more files through Prefect for the work leading in to creating an external table, but ran into lack of space on server.  Slack had some tips.  Ran below to see where it was being used up, from my ~/ location

`sudo du -h --block-size=G | sort -n -r | head -n 30`

Couple things jumped out at me:
```
6G      ./.prefect/storage
6G      ./.prefect
...
2G      ./git/data-engineering-zoomcamp
2G      ./git
```

Cleared out the the ./.prefect/storage folder, which seems to store flow run results


Used terraform to create the `nytaxi` database in my BQ

Can create `external table` from data from buckets.  Looks like a SQL script, can tell the format of the external data, and uris (?) pointing to the files in this case, with wildcard strings

In BigQuery, if you look at the details of an External Table, it won't be able to show size or rows, becuase the data is really still stored in your GCS bucket

CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_extended-signal-376421/data/yellow/yellow_tripdata_2019-*.parquet', 'gs://dtc_data_lake_extended-signal-376421/data/yellow/yellow_tripdata_2020-*.parquet']
);

Had issues creating the external table above at first.  Issues was with data types varying in the parquet files loaded.  Had to re-process them, specifying specific dtypes for the pandas dataframes when I processed them.  Once all the data types were consistent across files, the external table creation process succeeded

# Partitioning

Partitioning can greatly improve performance.  Once partitioned, if you're looking for only the data for a particular partition, the other partitions are not even factored into any querying

```
CREATE OR REPLACE TABLE extended-signal-376421.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM extended-signal-376421.nytaxi.external_yellow_tripdata;
```
After this ran, the job information shows 13.54 GB processed and 13.54 GB billed.  Assuming this adds up to the flat pricing of 1TB of processing for $5


```
CREATE OR REPLACE TABLE extended-signal-376421.nytaxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM extended-signal-376421.nytaxi.external_yellow_tripdata;
```
The job information on this one also shows 13.54 GB processed and 13.54 GB billed.

Should look into how to see history of jobs run and total billed

Can look at the Details tab of the table information to see what the table type is (Partitioned) and what it's partitioned by

Partitioned vs non-partitioned tables show a different icon

Below will show the difference of querying against partitioned vs non-partitioned data.  If you highlight one of the scripts, or all of them, and hold a second, some information will be displayed on the right, in the menu bar above the script, telling you how much data the script will use when processing.
```
-- Impact of partition
-- Scanning 1.11GB of data
SELECT DISTINCT(VendorID)
FROM nytaxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~72.81 MB of DATA
SELECT DISTINCT(VendorID)
FROM nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';
```

The script below will
```
-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;
```


# Clustering

Seems like clustering is additional organization, in addition to the partition.  Grouping/sorting/indexing on an additional field (maybe more?)

```
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE extended-signal-376421.nytaxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM extended-signal-376421.nytaxi.external_yellow_tripdata;
```

Some queries to evaluate the performance/cost difference.  When you highlight and wait for the estimated data usage, the query against the clustered table won't be accurate.  
```
-- Query scans 747.34 MB
SELECT count(*) as trips
FROM nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID="1";

-- Query scans 580 MB
SELECT count(*) as trips
FROM nytaxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID="1";
```

# Partitions
Can partition based on Time or Integer, or Ingestion Time

Time partiition, daily is the default.  Daily, good way to start.  Data is usually medium sized and distribution evenly across other days

Hourly would be used for a huge amount of data coming in, and want to process it on each hour.  Hourly, you might want to consider the number of partitions created.  BigQuery partition limit is 4000.  If you expect more, maybe consider an expiration on the partitions.

Monthly or Yearly.  Smaller amount of data.  (?) Data is across different ranges

# Clustering

Order of the columns listed` is important, will determine how the data is sorted

Cluster improves filter and aggregate queries

Tables with data size < 1 GB, won't show significant improvement.  May add significant cost, because they incur meta data reads or maintenance.  May make sense for no clustering here.

Clustering, can be done on up to 4 columns

Clustering must be top level (?), non-repeated columns

Looks up `cardinality`:
the number of distinct values in a table column relative to the number of rows in the table
https://orangematter.solarwinds.com/2020/01/05/what-is-cardinality-in-a-database/


In BigQuery, you can set some things up so that if a query costs over some value, it won't run


Clustering sounds very much like indexes in SQL, although BigQuery will auto recluster

Reclustering doesn't cost anything, just runs in the backround, no cost to us


# Big Query Best practices

Cost reduction and improving query performance

Cost reduction:
Typically good to avoid select *.  Instead, specify the name of the columns.  Limits the amount of data you're accessing.

Price your queries before running them.

Use clustered or partitioned tables.

Use streaming inserts with caution.  They can drastically increae costs

Materiaolize query results in stages.  If you're using a CTE in multiple locations.  Might be take the results once and put them somewhere, maybe a temp table.  BigQuery also caches query results

Query performance:
Always filter on partitioned or clusterd or partition data

Denormalizing the data

Use nested or repeated columns.

Use external data sources appropriately used.  Don't do it too much.  Might incur more costs possibly.  

Reduce data before using a join

Do not treat WITH claus as prepared statements

Avoid oversharding tables

Avoid JavaScript user-defined functions.

Use approximate aggregation functions (HyperLogLog+++)

Order last, for query operations to maximize performance

Optimize your join patterns

As a best practice, place the table with the largest number of rows first, follwed by the table with the fewest rows, and then place the remaining tables by decreasing size.



# Internals of BigQuery

Data stored in Colossus.  This is generally a cheap storage, stores data in a columnar format.

Data is stored separate from compute, it has significantly less cost.

Most cost occured is when while reading the data or running the queries, basically the compute.

Jupiter network in BigQuery system allows 1 TB / second network speed

Dremel is the query execution engine.  Divides your query into a tree structure, and each node can run specific parts of the query

# Columar and record orientated data

Columnar data provides better aggregation results

Typically only querying certain columns, and not everything




# BigQuery Machine Learning

Target audience Data Analysts and managers.

No need for python or Java Knowledge.  Just SQL and some machine learning algorithms

No need to export data into a different system

Generally when training a machine learning model, you'd export the data from a data warehouse, build a model, train it, and then deploy it

Doing this is 





