from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-extended-signal")
    gcs_block.get_directory(from_path=gcs_path, local_path=".")
    return Path(f"{gcs_path}")


@task()
def write_bq(df: pd.DataFrame) -> int:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-extended-signal-creds")

    rows = len(df)
    df.to_gbq(
        destination_table="dataset_zoomcamp.rides",
        project_id="extended-signal-376421",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
    return rows

@flow()
def etl_gcs_to_bq(color: str, year: int, month: int) -> int:
    #doc string
    """Main ETL flow to load data into Big Query"""
    #color="yellow"
    #year ="2021"
    #month=1

    path = extract_from_gcs(color, year, month)
    #print(path)
    df = pd.read_parquet(path)
    rows = write_bq(df)
    return rows


@flow(log_prints=True)
def q3_parent_flow(
    # includes default values
    months: list[int] = [2,3], year: int = 2019, color: str = "yellow"
):
    total_rows = 0
    for month in months:
        rows = etl_gcs_to_bq(color, year, month)
        total_rows = total_rows + rows
    print(f"Total rows loaded: {total_rows}")

if __name__ == '__main__':
    color = "green"
    months = [8,9]
    year = 2017
    q3_parent_flow(months, year, color)
