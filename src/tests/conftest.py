import configparser
import csv
import os
import sys

import pytest

sys.path.append("../vre/model")

from src.features.model import Patient
from src.features.model import Case
from src.features.model import Stay
from src.features.model import RiskScreening
from src.features.model import Medication
from src.features.model import Chop
from src.features.model import Surgery
from src.features.model import Appointment
from src.features.model import Device
from src.features.model import Employee
from src.features.model import Room
from src.features.model import Partner
from src.features.model import Treatment
from src.features.model import ICDCode
from src.features.model import Building
from tqdm import tqdm

from configuration.basic_configuration import configuration

base_path = "data/raw/test_data/" #configuration['PATHS']['input_dir']  # Must point to the directory containing all CSV testfiles - these are patient data and may NOT be in the repo !

building_path = os.path.join(base_path, "building_identifiers.csv")
patients_path = os.path.join(base_path, "DIM_PATIENT.csv")
cases_path = os.path.join(base_path, "DIM_FALL.csv")
stays_path = os.path.join(base_path, "LA_ISH_NBEW.csv")
risks_path = os.path.join(base_path, "VRE_SCREENING_DATA.csv")
medication_path = os.path.join(base_path, "FAKT_MEDIKAMENTE.csv")
chop_path = os.path.join(base_path, "LA_CHOP_FLAT.csv")
surgery_path = os.path.join(base_path, "LA_ISH_NICP.csv")
appointments_path = os.path.join(base_path, "DIM_TERMIN.csv")
appointment_patient_path = os.path.join(base_path, "FAKT_TERMIN_PATIENT.csv")
devices_path = os.path.join(base_path, "DIM_GERAET.csv")
device_appointment_path = os.path.join(base_path, "FAKT_TERMIN_GERAET.csv")
appointment_employee_path = os.path.join(base_path, "FAKT_TERMIN_MITARBEITER.csv")
rooms_path = os.path.join(base_path, "room_identifiers.csv")
room_appointment_path = os.path.join(base_path, "FAKT_TERMIN_RAUM.csv")
partner_path = os.path.join(base_path, "LA_ISH_NGPA.csv")
partner_case_path = os.path.join(base_path, "LA_ISH_NFPZ.csv")
tacs_path = os.path.join(base_path, "TACS_DATEN.csv")
icd_path = os.path.join(base_path, "V_LA_ISH_NDIA_NORM.csv")


def get_lines(path):
    encoding = "iso-8859-1"
    lines = csv.reader(open(path, 'r', encoding=encoding), delimiter=configuration['DELIMITERS']['csv_sep'])
    next(lines, None)  # skip header
    return lines


@pytest.fixture
def patient_data():
    tqdm.pandas()

    rooms = dict()
    wards = dict()

    encoding = "iso-8859-1"
    patients = Patient.create_patient_dict(patients_path, encoding=encoding)
    cases = Case.create_case_map(cases_path, encoding, patients)

    partners = Partner.create_partner_map(partner_path, encoding)
    Partner.add_partners_to_cases(partner_case_path, encoding, cases, partners)

    Stay.add_stays_to_case(
        stays_path,
        encoding,
        cases,
        rooms,
        wards,
        partners,
    )
    RiskScreening.add_annotated_screening_data_to_patients(risks_path, encoding, patients)

    drugs = Medication.create_drug_map(get_lines(medication_path))
    Medication.add_medications_to_case(medication_path, cases)

    chops = Chop.create_chop_map(get_lines(chop_path))
    Surgery.add_surgeries_to_case(get_lines(surgery_path), cases, chops)

    appointments = Appointment.create_appointment_map(appointments_path, encoding)
    Appointment.add_appointment_to_case(get_lines(appointment_patient_path), cases, appointments)

    devices = Device.create_device_map(get_lines(devices_path))
    Device.add_device_to_appointment(device_appointment_path, appointments, devices)

    employees = Employee.create_employee_map(get_lines(appointment_employee_path))
    Employee.add_employees_to_appointment(get_lines(appointment_employee_path), appointments, employees)

    Treatment.add_care_entries_to_case(get_lines(tacs_path), cases, employees)

    buildings = Building.create_building_id_map(building_path, encoding)
    rooms, buildings, floors = Room.create_room_id_map(rooms_path, buildings, encoding)
    Room.add_rooms_to_appointment(get_lines(room_appointment_path), appointments, rooms)

    icd_codes = ICDCode.create_icd_code_map(get_lines(icd_path))
    ICDCode.add_icd_codes_to_case(get_lines(icd_path), cases)

    return dict(
        {
            "rooms": rooms,
            "wards": wards,
            "partners": partners,
            "patients": patients,
            "cases": cases,
            "drugs": drugs,
            "chops": chops,
            "appointments": appointments,
            "devices": devices,
            "employees": employees,
            "icd_codes": icd_codes
        }
    )
