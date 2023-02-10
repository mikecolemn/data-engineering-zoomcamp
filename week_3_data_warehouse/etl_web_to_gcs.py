from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta

#, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
# @task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str, color: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    if color == 'fhv':
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
    #else:

    # print(df)
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

# @task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix some dtype issues"""
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    #print(df.head(2))
    #print(f"columns: {df.dtypes}")
    #print(f"rows: {len(df)}")
    return df

# @task()
def write_local(df: pd.DataFrame, color: str, dataset_file:str) -> Path:
    """Write DataFrame out locally as a parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path  

# @task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-extended-signal")
    gcs_block.upload_from_path(from_path=f"{path}",to_path=path, timeout=180)
    return


# @flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The Main ETL Function"""
    #color = "yellow"
    #year = 2021
    #month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url, color)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

# @flow()
def etl_parent_flow(
    # includes default values
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):

    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__ == '__main__':
    color = "yellow"
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    # months = [4]
    year = 2020
    etl_parent_flow(months, year, color)
    