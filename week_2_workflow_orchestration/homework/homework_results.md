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

* Question 5

Answer: `514392`

`11:19:03.329 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 514392`

Slack notification sent to my own slack workspace:
```
Prefect flow run notification
Flow run etl-parent-flow-q4/delta4-draylax entered state Completed at 2023-02-05T17:19:17.692804+00:00.
Flow ID: 7146dee6-d9d8-4b02-9e4d-f9326286ba7d
Flow run ID: fac95f94-4d60-4af3-9622-27f86b8ce75f
Flow run URL: http://127.0.0.1:4200/flow-runs/flow-run/fac95f94-4d60-4af3-9622-27f86b8ce75f
State message: All states completed.
```


* Question 6

Answer: `8`

```
Value
********
```