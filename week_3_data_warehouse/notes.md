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

# Partitioning

Partitioning can greatly improve performance.  Once partitioned, if you're looking for only the data for a particular partition, the other partitions are not even factored into any querying