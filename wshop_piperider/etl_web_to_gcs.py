from pathlib import Path
import pandas as pd
import numpy as np
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta

#, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=10)
@task(retries=2)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    #if randint(0,1) > 0:
    #    raise Exception

    df = pd.read_csv(dataset_url,encoding='latin1')

    # print(df.dtypes)

    return df

@task(name="CleanParquetData", log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix some dtype issues"""
    if color == 'yellow':
        df = df.astype({
                    'VendorID': 'Int64',
                    'tpep_pickup_datetime': 'datetime64',
                    'tpep_dropoff_datetime': 'datetime64',
                    'passenger_count': 'Int64',
                    'trip_distance': 'float64',
                    'RatecodeID': 'Int64',
                    'store_and_fwd_flag': 'object',
                    'PULocationID': 'Int64',
                    'DOLocationID': 'Int64',
                    'payment_type': 'Int64',
                    'fare_amount': 'float64',
                    'extra': 'float64',
                    'mta_tax': 'float64',
                    'tip_amount': 'float64',
                    'tolls_amount': 'float64',
                    'improvement_surcharge': 'float64',
                    'total_amount': 'float64',
                    'congestion_surcharge': 'float64'
                    })
    elif color == 'green':
        df = df.astype({
                    'VendorID': 'Int64',
                    'lpep_pickup_datetime': 'datetime64',
                    'lpep_dropoff_datetime': 'datetime64',
                    'store_and_fwd_flag': 'object',
                    'RatecodeID': 'Int64',
                    'PULocationID':'Int64',
                    'DOLocationID': 'Int64',
                    'passenger_count': 'Int64',
                    'trip_distance': 'float64',
                    'fare_amount': 'float64',
                    'extra': 'float64',
                    'mta_tax': 'float64',
                    'tip_amount': 'float64',
                    'tolls_amount': 'float64',
                    'ehail_fee': 'float64',
                    'improvement_surcharge': 'float64',
                    'total_amount': 'float64',
                    'payment_type': 'Int64',
                    'trip_type': 'Int64',
                    'congestion_surcharge': 'float64'
                    })
    elif color == 'fhv':
        df.rename(columns= {
                            'dropoff_datetime': 'dropOff_datetime',
                            'PULocationID': 'PUlocationID',
                            'DOLocationID': 'DOlocationID'
                            }, inplace=True)
        df = df.astype({
                    'dispatching_base_num': 'object',
                    'pickup_datetime': 'datetime64',
                    'dropOff_datetime': 'datetime64',
                    'PUlocationID': 'Int64',
                    'DOlocationID': 'Int64',
                    'SR_Flag': 'Int64',
                    'Affiliated_base_number': 'object'
                    })

    # df['RatecodeID'] = pd.to_numeric(df['RatecodeID'], downcast='float64', errors='coerce')
    # print(df.head(2))
    # print(df)
    print(f"columns: {df.dtypes}")
    # print(f"rows: {len(df)}")
    return df

@task(name="SaveLocally")
def write_local(df: pd.DataFrame, color: str, dataset_file:str) -> Path:
    """Write DataFrame out locally as a parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path  

@task(name="UploadGCS")
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-extended-signal")
    gcs_block.upload_from_path(from_path=f"{path}",to_path=path, timeout=180)
    return


@flow(name="TaxiLoad_Subflow")
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The Main ETL Function"""
    #color = "yellow"
    #year = 2021
    #month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow(name="TaxiLoad_ParentFlow")
def etl_parent_flow(
    # includes default values
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__ == '__main__':
    color = "yellow"
    # months = [1,2,3,4,5,6,7,8,9,10,11,12]
    months = [5]
    year = 2019
    etl_parent_flow(months, year, color)
    