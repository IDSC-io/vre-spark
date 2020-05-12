# Small script to test the patient import defined in the data_loader.py script

import configparser
import logging
import pathlib

from src.features.dataloader import DataLoader


if __name__ == '__main__':

    config_reader = configparser.ConfigParser()

    config_reader.read(pathlib.Path('basic_config.ini'))

    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO, datefmt='%d.%m.%Y %H:%M:%S')

    loader = DataLoader(hdfs_pipe=False)  # hdfs_pipe = False --> files will be loaded directly from CSV
    patient_data = loader.patient_data()

