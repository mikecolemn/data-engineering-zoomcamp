from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from etl_web_to_gcs_fhv_csv import etl_fhv_parent_flow

github_block = GitHub.load("gh-zoomcamp")

github_dep = Deployment.build_from_flow(
    flow=etl_fhv_parent_flow,
    name='fhv-github-flow',
    storage=github_block,
    entrypoint="week_3_data_warehouse/homework/etl_web_to_gcs_fhv_csv.py:etl_fhv_parent_flow"
)

if __name__  == "__main__":
   github_dep.apply()
    