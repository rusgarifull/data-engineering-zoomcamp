#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def copy_csv_to_postgres(csv_name, db_engine, table_name, date_columns=None):
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    
    if date_columns:
        for date_col in date_columns:
            df[date_col] = pd.to_datetime(df[date_col])

    df.head(n=0).to_sql(name=table_name, con=db_engine, if_exists='replace')
    df.to_sql(name=table_name, con=db_engine, if_exists='append')

    while True: 
        t_start = time()

        df = next(df_iter)

        if date_columns:
            for date_col in date_columns:
                df[date_col] = pd.to_datetime(df[date_col])
        df.to_sql(name=table_name, con=db_engine, if_exists='append')

        t_end = time()
        print('inserted another chunk, took %.3f second' % (t_end - t_start))


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'
    date_columns = params.date_columns
    print(f"date_columns: {date_columns}")
    # download csv file
    os.system(f"wget {url} -O {csv_name}")

    # create db connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # read file and ingest into the db
    copy_csv_to_postgres(csv_name, engine, table_name, date_columns)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    parser.add_argument('--date_columns', required=False, nargs="*", type=str, default=None)

    args = parser.parse_args()

    main(args)
