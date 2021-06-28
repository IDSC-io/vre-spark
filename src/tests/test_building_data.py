"""
Extract VRE patient demographic information and movements/stays
"""

import logging
import os
import pathlib
import sys
import pandas as pd

from src.features.dataloader import DataLoader
from src.features.model import Patient
from datetime import datetime
from configuration.basic_configuration import configuration

def test_building_data():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    now_str = datetime.now().strftime("%Y%m%d%H%M%S")

    logger.info(f"Processing data delivery of date {now_str}")

    # --> Load all data:
    logger.info("Loading data for data delivery...")
    loader = DataLoader(hdfs_pipe=False)  # hdfs_pipe = False --> files will be loaded directly from CSV
    patient_data = loader.prepare_dataset(
        load_cases=False,
        load_partners=False,
        load_stays=False,
        load_medications=False,
        load_risks=False,
        load_chop_codes=False,
        load_surgeries=False,
        load_appointments=False,
        load_devices=False,
        load_employees=False,
        load_care_data=False,
        load_buildings=True,
        load_rooms=True,
        load_icd_codes=False)

    logger.info("...Done.")

