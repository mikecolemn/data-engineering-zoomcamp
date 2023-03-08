    /*
    This is my staging fhv model build
*/

{{ config(materialized='view') }}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata_partitioned') }}
  where EXTRACT(year from pickup_datetime) IN (2019)
)
select 
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'dropOff_datetime']) }} as tripid,
    dispatching_base_num,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    Affiliated_base_number,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    --other
    cast(SR_Flag as integer) as SR_Flag

from tripdata

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

