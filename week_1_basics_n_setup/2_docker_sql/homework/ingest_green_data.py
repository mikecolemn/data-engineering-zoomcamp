#!/usr/bin/env python
# coding: utf-8

# Ingest green trip data

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv.gz'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    os.system("wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -O taxi+_zone_lookup.csv")
    df_zones = pd.read_csv('taxi+_zone_lookup.csv')
    df_zones.to_sql(name='zones', con=engine, if_exists='replace')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()
        
        df = next(df_iter)
        
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('inserted another chunk..., took %.3f second' % (t_end - t_start))


    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest .csv data to postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port number for postgres')
    parser.add_argument('--db', help='databsae name for postgres')
    parser.add_argument('--table_name', help='table name where we will write the resutls to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
        #(params) -> NoReturn   
    main(args)