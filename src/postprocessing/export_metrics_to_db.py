# -*- coding: utf-8 -*-
"""This script exports the metrics dataframes to the database.

-----
"""
import sys

sys.path.append("../..")

import logging

from datetime import datetime
import pathlib

# import click

import os
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
from pathlib import Path
import re
import math
from tqdm import tqdm


def get_mssql_engine(driver=os.getenv("DRIVER"),
                     host=os.getenv('SERVER_ADDRESS'),
                     db=os.getenv('DATABASE'),
                     user=os.getenv('user'),
                     password=os.getenv('password')):
    engine = create_engine(f'mssql+pyodbc://{user}:{password}@{host}/{db}?driver={driver}')
    return engine


def get_all_timestamped_metrics(metrics_folder="./data/processed/metrics/"):
    # get all csvs in folder
    file_paths = Path(metrics_folder).glob("*.csv")

    metric_paths_list = []
    # extract timestamp from csv prefix
    pattern = "^([0-9]+)_(.+)\\.csv$"
    for file_path in file_paths:
        match = re.search(pattern, Path(file_path).name)
        if match is not None:
            # if extraction was successful, read metric df and compose tuple
            timestamp, metric_name = match.groups()
            metric_df = pd.read_csv(file_path, encoding="iso-8859-1")
            metric_paths_list.append((timestamp, metric_name, metric_df))

    return metric_paths_list


# @click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def create_append_df_to_sql():
    #####################################
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO,
                        datefmt='%d.%m.%Y %H:%M:%S')
    #####################################

    engine = get_mssql_engine(driver="SQL+Server",
                                             host="WSISRZ0089\SISP22T",
                                             db="Atelier_DataScience",
                                             user="Atelier_DataScience_reader",
                                             password="A7D32FXXA2F&=K5QM40")

    for (timestamp, metric_name, metric_df) in tqdm(get_all_timestamped_metrics()):
        print(f"Exporting {metric_name}...")
        metric_df["Timestamp"] = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        metric_df = metric_df.drop(["Unnamed: 0"], axis=1)
        # metric_df.to_csv(f"./data/processed/metrics/{timestamp}_{metric_name}_fixed.csv", index=False)
        table_name = f"metric_{metric_name}"

        #
        insert_with_progress(metric_df, engine, table=table_name, schema="temp")
        # metric_df.to_sql(table_name, engine, schema="temp", if_exists='append', index=False)

    logging.info("Data exported successfully!")


# based on: https://leblancfg.com/benchmarks_writing_pandas_dataframe_SQL_Server.html
def chunker(seq, size):
    return [seq[pos: pos + size] for pos in range(0, len(seq), size)]


def insert_with_progress(df, engine, table="", schema=""):
    con = engine.connect()

    # Replace table
    # engine.execute(f"DROP TABLE IF EXISTS {schema}.{table};")

    # Insert with progress
    SQL_SERVER_CHUNK_LIMIT = 2099
    chunksize = math.floor(SQL_SERVER_CHUNK_LIMIT / len(df.columns))

    for chunk in tqdm(chunker(df, chunksize), desc=f"{table} SQL insertion with chunksize={chunksize}"):
        chunk.to_sql(
            name=table,
            con=con,
            if_exists="append",
            index=False,
            schema=schema,
            method="multi",
        )


if __name__ == "__main__":
    create_append_df_to_sql()

