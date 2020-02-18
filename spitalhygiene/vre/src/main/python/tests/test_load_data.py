import os

import pytest

from model.Patient import Patient
from model.Case import Case
from model.Move import Move
from model.Risk import Risk

from conftest import get_hdfs_pipe

base_path = "T:/IDSC Projects/VRE Model/Data/Test Data"
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
