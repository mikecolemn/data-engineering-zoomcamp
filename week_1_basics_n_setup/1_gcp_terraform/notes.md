
# Terraform
# seems like terraform plan doesn't like below
export GOOGLE_APPLICATION_CREDENTIALS="~/git/data-engineering-zoomcamp/week_1_basics_n_setup/1_gcp_terraform/extended-signal-376421-47ee2e98a4de.json"

# seems like terraform plan likes the full path
export GOOGLE_APPLICATION_CREDENTIALS="/home/udengine/git/data-engineering-zoomcamp/week_1_basics_n_setup/1_gcp_terraform/extended-signal-376421-47ee2e98a4de.json"

# Refresh token/session, and verify authentication
gcloud auth application-default login


Locals in variable.tf are like constants
Variables are generally passed at runtime




# Google Cloud Platform


Cloud Storage = Bucket = Data Lake.  Flat files in this case.  Like a directory tree of files

Data Lake = raw data, in a more organized fashion.  Partitioned by more sensible directores and compressed (?).  csvs, jsons, parquet files


BigQuery = Data Warehouse.  More structured.  Fact and Dimension tables

