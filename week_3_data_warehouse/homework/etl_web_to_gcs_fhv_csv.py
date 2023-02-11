import os
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def retrieve_file(dataset_url: str, color:str, dataset_file: str) -> Path:
    """Retrieve data files from github repo"""
    
    # print(os.system("pwd"))
    os.system(f"wget {dataset_url} -O week_3_data_warehouse/homework/data/{color}/{dataset_file}")
    file_path = Path(f"week_3_data_warehouse/homework//data/{color}/{dataset_file}")

    return file_path

# @task()
def upload_gcs(path: Path, color: str) -> None:
    """Upload csv.gz file to GCS"""
    gcs_block = GcsBucket.load("gcs-extended-signal")
    gcs_block.upload_from_path(from_path=f"{path}",to_path=path, timeout=180)
    return


@flow()
def etl_fhv_to_gcs(color: str, year: int, month: int) -> None:
    """The Main ETL Function"""
    #color = "yellow"
    #year = 2021
    #month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"

    file_path = retrieve_file(dataset_url, color, dataset_file)
    # upload_gcs(dataset_file, color)
    print(file_path)
    upload_gcs(file_path, color)


@flow()
def etl_fhv_parent_flow(
    # includes default values
    months: list[int] = [1, 2], year: int = 2019, color: str = "fhv"
):
    for month in months:
        etl_fhv_to_gcs(color, year, month)

if __name__ == '__main__':
    color = "fhv"
    # months = list(range(1, 12+1))
    months = [2]
    year = 2020
    etl_fhv_parent_flow(months, year, color)