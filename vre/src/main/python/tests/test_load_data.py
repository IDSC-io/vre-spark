import os
import sys
sys.path.append('../vre/model')
import pytest
import configparser

from Patient import Patient
from Case import Case
from Move import Move
from Risk import Risk

from conftest import get_hdfs_pipe

# Load configuration file
config_reader = configparser.ConfigParser()
config_reader.read('../vre/BasicConfig.ini')

base_path = config_reader['PATHS']['test_data_dir']

patients_path = os.path.join(base_path, "V_DH_DIM_PATIENT_CUR.csv")
cases_path = os.path.join(base_path, "V_LA_ISH_NFAL_NORM.csv")
moves_path = os.path.join(base_path, "LA_ISH_NBEW.csv")


def test_load_patients():
    pytest.patients = dict()
    pytest.cases = dict()
    pytest.rooms = dict()
    pytest.wards = dict()
    pytest.partners = dict()

    pytest.patients = Patient.create_patient_dict(get_hdfs_pipe(patients_path))
    assert len(pytest.patients) == 3


def test_load_cases():
    pytest.cases = Case.create_case_map(get_hdfs_pipe(cases_path), pytest.patients)
    assert len(pytest.cases) == 31
    assert pytest.cases.get("0006314210", None) is not None
    assert pytest.patients.get("00003067149", None) is not None


def test_load_moves():
    Move.add_move_to_case(
        get_hdfs_pipe(moves_path),
        pytest.cases,
        pytest.rooms,
        pytest.wards,
        pytest.partners,
    )
    assert len(pytest.patients.get("00003067149").cases.get("0006314210").moves) == 12
    assert len(pytest.rooms) == 15
    assert len(pytest.wards) == 26
    assert pytest.rooms.get("BH H 116", None) is not None
    assert pytest.wards.get("INEGE 2", None) is not None
    assert len(pytest.rooms.get("BH H 116").moves) == 3
