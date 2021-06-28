# import os
# import sys
# sys.path.append('../features/model')
# import pytest
# import configparser
#
# from src.features.model import Patient
# from src.features.model import Case
# from src.features.model import Stay
#
# from src.tests.conftest import get_lines
# from configuration.basic_configuration import configuration
#
# base_path = configuration['PATHS']['input_dir']
#
# patients_path = os.path.join(base_path, "V_DH_DIM_PATIENT_CUR.csv")
# cases_path = os.path.join(base_path, "V_LA_ISH_NFAL_NORM.csv")
# stays_path = os.path.join(base_path, "LA_ISH_NBEW.csv")
#
#
# def test_load_patients():
#     pytest.patients = dict()
#     pytest.cases = dict()
#     pytest.rooms = dict()
#     pytest.wards = dict()
#     pytest.partners = dict()
#
#     pytest.patients = Patient.create_patient_dict(get_lines(patients_path))
#     assert len(pytest.patients) == 3
#
#
# def test_load_cases():
#     pytest.cases = Case.create_case_map(get_lines(cases_path), pytest.patients)
#     assert len(pytest.cases) == 31
#     assert pytest.cases.get("0006314210", None) is not None
#     assert pytest.patients.get("00003067149", None) is not None
#
#
# def test_load_stays():
#     Stay.add_stays_to_case(
#         get_lines(stays_path),
#         pytest.cases,
#         pytest.rooms,
#         pytest.wards,
#         pytest.partners,
#     )
#     assert len(pytest.patients.get("00003067149").cases.get("0006314210").stays) == 12
#     assert len(pytest.rooms) == 15
#     assert len(pytest.wards) == 26
#     assert pytest.rooms.get("BH H 116", None) is not None
#     assert pytest.wards.get("INEGE 2", None) is not None
#     assert len(pytest.rooms.get("BH H 116").stays) == 3
