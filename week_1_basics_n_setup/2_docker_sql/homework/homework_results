1)
command: docker build --help

Results:
      --iidfile string          Write the image ID to the file

2)
command: docker run -it --entrypoint=bash python:3.9
    pip list

Results:
3

Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
WARNING: You are using pip version 22.0.4; however, version 23.0 is available.
You should consider upgrading via the '/usr/local/bin/python -m pip install --upgrade pip' command.



3)
Script:
SELECT 
	CAST(lpep_pickup_datetime as DATE) as PickupDate,
	CAST(lpep_dropoff_datetime as DATE) as DropOffDate,
	COUNT(1)
FROM green_taxi_data
WHERE 
	CAST(lpep_pickup_datetime as DATE) = '2019-01-15'
	AND CAST(lpep_dropoff_datetime as DATE) = '2019-01-15'
GROUP BY CAST(lpep_pickup_datetime as DATE),
	CAST(lpep_dropoff_datetime as DATE) 

Answer:
"2019-01-15"	"2019-01-15"	20530

4)
Script:
SELECT 
	CAST(lpep_pickup_datetime as DATE) as PickupDate,
	trip_distance
FROM green_taxi_data g
INNER JOIN
	(
	SELECT MAX(trip_distance) as maxtrip
	FROM green_taxi_data
	) s1
ON g.trip_distance = s1.maxtrip
ORDER BY trip_distance DESC

Answer:
"2019-01-15"	117.99

5)
Script:
SELECT 
	CAST(lpep_pickup_datetime as DATE) as PickupDate,
	passenger_count,
	COUNT(1)
FROM green_taxi_data
WHERE 
	CAST(lpep_pickup_datetime as DATE) = '2019-01-01'
	AND passenger_count IN (2,3)
GROUP BY CAST(lpep_pickup_datetime as DATE),
	passenger_count

Answer:
"2019-01-01"	2	1282
"2019-01-01"	3	254

6)
Script:
SELECT 
	zpu."Zone",
	zdo."Zone",
	MAX(g.tip_amount) as maxtip
FROM green_taxi_data g
INNER JOIN zones zpu
	ON g."PULocationID" = zpu."LocationID"
INNER JOIN zones zdo
	ON g."DOLocationID" = zdo."LocationID"
WHERE 
	zpu."Zone" = 'Astoria'
GROUP BY zpu."Zone",
	zdo."Zone"
ORDER BY maxtip DESC
LIMIT 1;

Answer:
"Astoria"	"Long Island City/Queens Plaza"	88