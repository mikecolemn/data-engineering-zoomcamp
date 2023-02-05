# Introduction to Prefect Concepts

Prefect is a workflow orchestration tool

Getting conda environment going:

* `git clone https://github.com/discdiver/prefect-zoomcamp.git` (ended up cloning this somewhere else, and then built my own files)
* `conda create -n zoom python=3.9` - creates conda environment named zoom, using the python 3.9
* `conda activate zoom` - activates and enters the environment you just set up
* `pip install -r requirements.txt` - will install everything found in the requirements.txt file

* `prefect version` - will show the version of prefect version you have
* `prefect orion start` - run this from another terminal to launch a local instance of the prefect server

Adjustments to prior scripts

* `from prefect import flow, task` - will import flow and tasks from prefect.  Flow is the most basic python object. Container of workflow logic.  Flows are like functions, take inputs, perform work, provide outputs

* `from prefect.tasks import task_input_hash` - will allow cacheing of a task, to re-use results of a prior successful run

* `from datetime import timedelta` - allows you to compare differences in days

Prefect decorators
`@flow(name="Ingest Flow")`

Flows contain tasks.  
Call the function from a task from the flow

`@task()`

Tasks are not required for flows.  They're special, receive metadata about upstream dependencies.  Can have a task wait on another task to complete

Can include additional settings for tasks, separated by commas:
* `log_prints=True`
* `retries=3`
* `cach_key_fn=task_input_hash, cache_expiration=timedelta(days=1)` - this caused some issues later when moving things to docker


Some pandas code:
Will print a count of where passenger_count = 0
Transform the dataframe to only include records where passenger_count != 0 (I should double-check this to verify total record counts change)
Then print the count of where passenger_count = 0 after the change
```python
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
```


Can pass parameters to flows

Flows can contain other flows, like a sub-flow


`prefect orion start` in another terminal will launch the prefect orion server.

They recommended runnin the `prefect config set...` command also to set some things up. 
Make sure in vs code to forward port 4200, and then you can copy out URL from the `Check out the dashboard...` line into a browser window


# UI
* `Flow runs` - shows history of runs of flows
* `Flows` - 
* `Deployments` - 
* `Blocks` - Blocks allow interaction with external systems.  Stores credentials.  Block names are immutable
* `Work Queues` - 
* `Notifications` - Set up notifications for flow runs for certain run states.  Crashed = if the intrastructure crashed
* `Task Run Concurrency` - Used to control how many tasks run at a given time


Blocks
Blocks can build upon other blocks


Prefect collections are pip installable packages

`from prefect_sqlalchemy import SqlAlchemyConnector` - prefect-sqlalchemy was one of the collections installed from the requirements.txt file

In the UI, in Blocks.  Click the + symbol to add one and slect the SQLAlchemy Connector
Block Name = give it some name like `pg-sqlalchemy`
Driver = SyncDriver, then select `postgresql+psycopg2`
Database = your database name, like `ny_taxi`
Username = your PG user, like `root`
Password = your PG user password, like `root`
Host = your postgres database server, like `localhost`.  I may have had troubles here, trying to use pgdatabase or something like that, but that's running in a container on the host, so localhost seemed to work (hope I have that right)
Port = the port for the postgres server, like `5432`
Hit save

If you go into the block, you can see the code you'd want to copy into your python file

```python
    connection_block = SqlAlchemyConnector.load("pg-sqlalchemy")
    with connection_block.get_connection(begin=False) as engine:
        some df. statements

```

Can also use secret strings in blocks


# ETL with GCP and Prefect

`from pathlib import Path` - useful for dealing with file paths
`from prefect_gcp.cloud_storage import GcsBucket` - this also came from the requirements.txt, slightly different structure

`dataset_file = f"{color}_tripdata_{year}-{month:02}"`

Way of making something fail
```python
    from random import random

    If randint(0,1) > 0:
        raise Exception

```

Black python package super helpful for auto formatting

Some more pandas stuff below,  Will update the date type for two date/time columns to be a datetime type database
Will print the first 2 rows
will print the data types of the dataframe
will print the number of records

```python
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
```


`df.to_parquet` - writes a dataframe to parquet file
`compression="gzip"` - parameter for to_parquet, using gzip compression


Google Cloud storage

Use prior terraform work to create bucket and BigQuery

`prefect block register -m prefect_gcp`  This is supposed to register blocks that we'll be using.  I don't know that I needed to do this, all the blocks were visible in the UI already

Blocks > create a block for GCS Bucket

Block Name = some name, like `gcs-extended-signal`
Bucket = the name of your bucket, like `dtc_data_lake_extended-signal-376421`
GCP Credentials, hit Add next to that

For the GCP Credentials bucket being crated now:
Block Name = `gcp-extended-signal-creds`

Service Account Info = if you previously created a .json file with credentials for a service account, you can paste the contents of that file here.  They'll appear masked in the future.  This was previously created during the terraform section

After you create the GCS Bucket block, you will be able to see below, that you can copy/paste into your python script
```python
    from prefect_gcp.cloud_storage import GcsBucket
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-extended-signal")
```

`gcs_block = GcsBucket.load("gcs-extended-signal")` - grabs credentials from the block

```python
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
```


# 2.2.4 GCS to BigQuery
Some imports and useful calls of accessing GCS

`from prefect_gcp import GcpCredentials`

`gcs_block = GcsBucket.load("gcs-extended-signal")`
`gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")`

Some more python to check out

```python
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
```

# 2.2.5

Builds a deployment .yaml file.  Below includes path to file to deploy, then the entry point of the flow (first flow that should run), and then a name for the deployment
`prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"`

In the above, you could pass additional parameters to the command.  You can change the .yaml such as adding default parameters, or you can edit through the UI after applying the deployment
`parameters: {"months": [9,10], "color": "green", "year": 2014}`

Below applies the deployment to the system
`prefect deployment apply etl_parent_flow-deployment.yaml`

Starts an agent for a work queue, to call a deployment
`prefect agent start  --work-queue "default"`

Notifications
UI > Notification > Create Notification

# 2.2.6
Scheduling

In Deployment, you can select a deployment and add a schedule.  Options are interval or cron.  RRule isn't really available from the UI.  They're pretty complex

Similar deployment build, including a cron schedule
`prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Scheduled ETL" --cron "0 0 * * *" -a`

# Dockerizing the flow code

Lots of options for deploying this code.  We'll try using a docker container

We'll build a fully function docker image

Create `Dockerfile` in a location

```
FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir  

COPY flows /opt/prefect/flows

RUN mkdir -p /opt/prefect/data/yellow
```

docker-requirements.txt
```
pandas==1.5.2
prefect-gcp[cloud_storage]==0.2.4
protobuf==4.21.11
pyarrow==10.0.1
```

Command for building the docker image
`docker image build -t mikecolemn/prefect:zoomcamp .`

Created an account at docker hub

To login to docker: `docker login`, will prompt you for user and password at that point

Push image to docker repository
`docker image push mikecolemn/prefect:zoomcamp`

Setup Docker block in Prefect UI:
Give it a name like `docker-mikecolemn`

Type = `docker-container`

Image, need to point this to your image, like `mikecolemn/prefect:zoomcamp`

Image pull policy.  Can set to Always to always pull latest image

Auto Remove.  Can set to True to always remove the image after you're done with it

Can create a deployment from a python file

In the deploy file, create an instance `docker dep = Deployment.build_from_flow(...)`

`flow` = the name of the flow to call
`name` = name of the deployment
`infrastructure` = the docker_block you created

Below should run that deploy.  This did give me a bunch of troubles that I had to troubleshoot
`python flows/03_deployments/docker_deploy.py`

Show profiles
`prefect profile ls`

Need to run this, but I think this was already set for me.  May need to change the IP if the prefect orion is on another server
`prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`

Below will run a deployment, and override a prameter
`prefect deployment run etl-parent-flow/docker-flow -p "months=[1,2]"`

Issues running the deployment were:
There was an issue with pulling the data down from the DTC github, with the fetch command.  Had to turn off caching and tinker/rerun things to get the cache to clear, possibly even redo the image build/push and the deployment build.  Error message was something like `Finished in state Failed('Flow run encountered an exception. ValueError: Path /home/udengine/.prefect/storage/b29e528913c74ab2aa057fc276687512 does not exist.\n')`


Had to check the prefect repo for an updated Dockerfile, which made the folder for the data on the image/container.  Had to move the parameterized_flow.py file to the Flows folder and re-build and push the image.  Error was something like `Finished in state Failed("Task run encountered an exception: OSError: Cannot save file into a non-existent directory: 'data/yellow'\n")`

I should investigate the one above further



# Homework

* Question 3

Ran into some timeout issues at first.  Had to bump up the timeout of the upload to GCS to 120 seconds (eventually 180 worked).  This might be worth looking at further, for future customization: https://github.com/googleapis/python-storage/issues/74

Also the fetch task started to get killed by the VM.  Only had 8 gigs of RAM allocated, and upgraded it to 16 gigs to resolve
