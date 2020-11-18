# -*- coding: utf-8 -*-
import sys

sys.path.append(".")
sys.path.append("../..")

import click
import logging
from pathlib import Path
#from dotenv import find_dotenv, load_dotenv

from src.data.dataset_queries import pull_raw_dataset
from src.data.merge_data import merge_data
from src.data.dataset_preprocessor import cleanup_dataset


@click.command()
#@click.argument('input_filepath', type=click.Path(exists=True))
#@click.argument('output_filepath', type=click.Path())
def main():
    """
    Pulls the data from the database and stores it in data/raw.

    :param input_filepath:
    :param output_filepath:
    :return:
    """
    logger = logging.getLogger(__name__)

    logger.info('Pulling dataset from database if not available yet...')
    pull_raw_dataset()

    logger.info('Merge data together from multiple sources...')
    merge_data()

    logger.info('Cleaning up dataset...')
    cleanup_dataset()


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    #load_dotenv(find_dotenv())

    main()
