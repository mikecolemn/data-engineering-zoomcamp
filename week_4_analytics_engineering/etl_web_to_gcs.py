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
def fetch(dataset_url: str, color: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    #if randint(0,1) > 0:
    #    raise Exception

    if color == 'yellow':
        df = pd.read_csv(
                        dataset_url,
                        dtype = {
                            'VendorID': float,
                            'tpep_pickup_datetime': str,
                            'tpep_dropoff_datetime': str,
                            'passenger_count': float,
                            'trip_distance': float,
                            'PULocationID': int,
                            'DOLocationID': int,
                            'RatecodeID': float,
                            'store_and_fwd_flag': str,
                            'payment_type': float,
                            'fare_amount': float,
                            'extra': float,
                            'mta_tax': float,
                            'improvement_surcharge': float,
                            'tip_amount': float,
                            'tolls_amount': float,
                            'total_amount': float,
                            'aongestion_surcharge': float,
                            'airport_fee': float
                            }
                        )
    elif color == 'green':
        df = pd.read_csv(
                        dataset_url,
                        dtype = {
                            'VendorID': float,
                            'lpep_pickup_datetime': str,
                            'lpep_dropoff_datetime': str,
                            'passenger_count': float,
                            'trip_distance': float,
                            'PULocationID': int,
                            'DOLocationID': int,
                            'RatecodeID': float,
                            'store_and_fwd_flag': str,
                            'payment_type': float,
                            'fare_amount': float,
                            'extra': float,
                            'mta_tax': float,
                            'improvement_surcharge': float,
                            'tip_amount': float,
                            'tolls_amount': float,
                            'total_amount': float,
                            'aongestion_surcharge': float,
                            'airport_fee': float
                            }
                        )    
    elif color == 'fhv':
        df = pd.read_csv(
                        dataset_url,
                        dtype = {
                            'dispatching_base_num': str,
                            'pickup_datetime': str,
                            'dropOff_datetime': str,
                            'PULocationID': int,
                            'DOLocationID': int,
                            'SR_Flag': int,
                            'Affiliated_base_number': str
                            }
                        )    

    # print(df)
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task(name="CleanParquetData", log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix some dtype issues"""
    if color == 'yellow':
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    elif color == 'green':
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    elif color == 'fhv':
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])

    # df['RatecodeID'] = pd.to_numeric(df['RatecodeID'], downcast='float64', errors='coerce')
    #print(df.head(2))
    #print(f"columns: {df.dtypes}")
    #print(f"rows: {len(df)}")
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

    df = fetch(dataset_url, color)
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
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    # months = [9,10,11,12]
    year = 2020
    etl_parent_flow(months, year, color)
    