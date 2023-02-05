* Question 1:

Answer: 447,770

`python etl_web_to_gcs_me.py`

```
07:01:09.048 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
07:01:09.091 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
```

* Question 2:

Answer `0 5 1 * *`

```bash
prefect deployment build ./etl_web_to_gcs_me.py:etl_web_to_gcs -n "Scheduled Flow" --cron "0 5 1 * *" -a
prefect deployment apply etl_web_to_gcs-deployment.yaml
```

* Question 3:

Answer: `14,851,920`

```
prefect deployment build ./etl_gcs_to_bq_me_q3.py:q3_parent_flow -n "q3-deployment"
prefect deployment apply q3_parent_flow-deployment.yaml
prefect deployment run q3-parent-flow/q3-deployment -p "months=[2,3]" -p "color="yellow"" -p "year=2019"
```

`20:55:00.273 | INFO    | Flow run 'defiant-shrew' - Total rows loaded: 14851920`


* Question 4

Answer: `88605`

`08:50:39.243 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605`