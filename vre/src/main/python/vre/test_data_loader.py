# Small script to test the patient import defined in the HDFS_data_loader.py script

from HDFS_data_loader import HDFS_data_loader
from feature_extractor import feature_extractor
from networkx_graph import surface_model, create_model_snapshots
import logging
import os
import datetime
import configparser
import calendar
import pathlib

config_reader = configparser.ConfigParser()

config_reader.read(pathlib.Path('BasicConfig.ini'))

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO, datefmt='%d.%m.%Y %H:%M:%S')

loader = HDFS_data_loader(hdfs_pipe=False)  # hdfs_pipe = False --> files will be loaded directly from CSV
patient_data = loader.patient_data()


