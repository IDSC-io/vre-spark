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


def get_mssql_engine(driver=os.getenv("DRIVER"),
                     host=os.getenv('SERVER_ADDRESS'),
                     db=os.getenv('DATABASE'),
                     user=os.getenv('user'),
                    password=os.getenv('password')):
    engine = create_engine(f'mssql+pyodbc://{user}:{password}@{host}/{db}?driver={driver}')
    return engine


# @click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def create_append_df_to_sql():
    #####################################
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO,
                        datefmt='%d.%m.%Y %H:%M:%S')
    #####################################

    now_str = datetime.now().strftime("%Y%m%d%H%M%S")

    df = pd.read_csv("./data/raw/model_data/ANTIBIOTICS_INTAKE.csv", encoding="iso-8859-1")

    engine = get_mssql_engine(driver="SQL+Server",
                                             host="WSISRZ0089\SISP22T",
                                             db="Atelier_DataScience",
                                             user="Atelier_DataScience_reader",
                                             password="A7D32FXXA2F&=K5QM40")


    table_name = "example_table"
    df.to_sql(table_name, engine, schema="temp", if_exists='append', chunksize=1000)

    logging.info("Data exported successfully!")


if __name__ == "__main__":
    create_append_df_to_sql()

