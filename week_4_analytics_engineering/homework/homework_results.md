## Question 1

Answer: closest answer is: 61,648,442

From logs of DBT production run
```
22:34:53  4 of 5 OK created sql table model production.fact_trips ........................ [CREATE TABLE (61.6m rows, 15.1 GB processed) in 15.87s]
```

From details of production fact_trips table:
```
Storage info
Number of rows  61,635,336
```

## Question 2

Answer: 89.9/10.1

Unlisted link to my Google Looker Studio report that shows results of current production fact_trips table, based on running for question #1:
https://lookerstudio.google.com/s/iphkZnJXPJs


## Question 3

Answer: 43,244.696

SQL script to check count:
```
SELECT COUNT(*) FROM dbt_mikecolemn.stg_fhv_tripdata
```

Results
```
Row	f0_
1	43244696
```

## Question 4:

Answer: 22,998,722

Logs from development run:
```
02:18:29  1 of 1 OK created sql table model dbt_mikecolemn.fact_fhv_trips ................ [CREATE TABLE (23.0m rows, 1.9 GB processed) in 7.20s]
```

SQL script:
```
SELECT COUNT(*) FROM dbt_mikecolemn.fact_fhv_trips
```

Results:
```
Row	f0_
1	22998722
```


## Question 5:

Answer: January

Unlisted link to my Google Looker Studio report that shows FHV trip results of current production fact_fhv_trips table, :
https://lookerstudio.google.com/s/jdMy9fNfwDc
