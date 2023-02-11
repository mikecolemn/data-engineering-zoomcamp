
# Setup

Created pipeline script that would use Prefect for retrieving FHV data files, store them locally, and then upload them into my GCS bucket.
Deployed from github to Prefect Cloud, and then run from there.

Created External table from the files in my GCS bucket.
```
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_extended-signal-376421/data/fhv/fhv_tripdata_2019-*.csv.gz']
);
```

Created a table without any partitioning
```
CREATE OR REPLACE TABLE nytaxi.fhv_tripdata_non_partitioned AS
SELECT * FROM nytaxi.external_fhv_tripdata;
```

## Question 1

Answer: `43,244,696`

Script:
```
SELECT COUNT(1)
FROM nytaxi.fhv_tripdata_non_partitioned;
```
Results: `1	43244696`

## Question 2

Answer: `0 MB for the External Table and 317.94MB for the BQ Table`

Scripts:
```
SELECT COUNT(DISTINCT affiliated_base_number)
FROM nytaxi.external_fhv_tripdata;

SELECT COUNT(DISTINCT affiliated_base_number)
FROM nytaxi.fhv_tripdata_non_partitioned;
```

Results:
```
This query will process 0 B when run

This query will process 317.94 MB when run.
```

## Question 3

Answer: `717,748`

Script
```
SELECT COUNT(1)
FROM `nytaxi.external_fhv_tripdata`
WHERE PULocationID IS NULL
AND DOLocationID IS NULL;
```

Results: `1	717748`

## Question 4

Answer: `Partition by pickup_datetime Cluster on affiliated_base_number`


## Question 5

Answer: `647.87 MB for non-partitioned table and 23.06 MB for the partitioned table`

Scripts
```
-- Create partitioned/clustered table
CREATE OR REPLACE TABLE nytaxi.fhv_tripdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM nytaxi.external_fhv_tripdata;

-- distinct counts from partitioned/clustered table
SELECT COUNT(DISTINCT affiliated_base_number)
FROM nytaxi.fhv_tripdata_non_partitioned
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- distinct counts from partitioned/clustered table
SELECT COUNT(DISTINCT affiliated_base_number)
FROM nytaxi.fhv_tripdata_partitoned_clustered
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```

Query estimates:
```
This query will process 647.87 MB when run.

This query will process 23.05 MB when run
```

## Question 6

Answer: `GCP Bucket`

## Question 7

Answer: `True`