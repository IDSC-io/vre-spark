# -*- coding: utf-8 -*-
import configparser
import os
import pathlib

import click

from src.features.dataloader import DataLoader
from src.features.feature_extractor import FeatureExtractor
import logging
from configuration.basic_configuration import configuration


@click.command()
#@click.argument('input_filepath', type=click.Path(exists=True))
#@click.argument('output_filepath', type=click.Path())
def main():
    """
    Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)

    #####################################
    # Initiate data loader
    logger.info("Initiating data_loader")

    # --> Load all data:
    loader = DataLoader(hdfs_pipe=False)  # hdfs_pipe = False --> files will be loaded directly from CSV
    patient_data = loader.prepare_dataset(load_cases=True, load_partners=False, load_stays=True)
    #####################################

    #####################################
    # Create and export feature vector
    logging.info("creating feature vector")
    model_creator = FeatureExtractor()
    (features, labels, dates, v) = model_creator.prepare_features_and_labels(patient_data["patients"])

    # Export feature vector
    logging.info("exporting feature vector")
    model_creator.export_csv(features, labels, dates, v, configuration['PATHS']['csv_export_path'])


if __name__ == '__main__':
    main()


