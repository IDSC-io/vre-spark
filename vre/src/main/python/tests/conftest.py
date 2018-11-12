import pytest
import csv
import os
import sys
sys.path.append("T:/Git Repositories/Bitbucket/stepaza/sqooba/insel/spitalhygiene/vre/src/main/python/vre") # allows import of all classes via model.[Class]

from model.Patient import Patient
from model.Case import Case
from model.Move import Move
from model.Risk import Risk
from model.Medication import Medication
from model.Chop import Chop
from model.Surgery import Surgery
from model.Appointment import Appointment
from model.Device import Device
from model.Employee import Employee
from model.Room import Room
from model.Partner import Partner
from model.Care import Care
from model.ICD import ICD

base_path = "T:/IDSC Projects/VRE Model/Data/Test Data"  # Must point to the directory containing all CSV testfiles - these are patient data and may NOT be in the repo !
patients_path = os.path.join(base_path, "V_DH_DIM_PATIENT_CUR.csv")
cases_path = os.path.join(base_path, "V_LA_ISH_NFAL_NORM.csv")
moves_path = os.path.join(base_path, "LA_ISH_NBEW.csv")
risks_path = os.path.join(base_path, "V_LA_ISH_NRSF_NORM.csv")
deleted_risks_path = os.path.join(base_path, "deleted_screenings.csv")
medication_path = os.path.join(base_path, "V_LA_IPD_DRUG_NORM.csv")
chop_path = os.path.join(base_path, "V_DH_REF_CHOP.csv")
surgery_path = os.path.join(base_path, "LA_ISH_NICP.csv")
appointments_path = os.path.join(base_path, "V_DH_DIM_TERMIN_CUR.csv")
appointment_patient_path = os.path.join(base_path, "V_DH_FACT_TERMINPATIENT.csv")
devices_path = os.path.join(base_path, "V_DH_DIM_GERAET_CUR.csv")
device_appointment_path = os.path.join(base_path, "V_DH_FACT_TERMINGERAET.csv")
appointment_employee_path = os.path.join(base_path, "V_DH_FACT_TERMINMITARBEITER.csv")
rooms_path = os.path.join(base_path, "V_DH_DIM_RAUM_CUR.csv")
room_appointment_path = os.path.join(base_path, "V_DH_FACT_TERMINRAUM.csv")
partner_path = os.path.join(base_path, "LA_ISH_NGPA.csv")
partner_case_path = os.path.join(base_path, "LA_ISH_NFPZ.csv")
tacs_path = os.path.join(base_path, "TACS_DATEN.csv")
icd_path = os.path.join(base_path, "LA_ISH_NDIA.csv")

def get_hdfs_pipe(path):
    encoding = "iso-8859-1"
    lines = csv.reader(open(path, 'r'), delimiter=";")
    next(lines, None)  # skip header
    return lines

@pytest.fixture
def patient_data():
    rooms = dict()
    wards = dict()

    patients = Patient.create_patient_dict(get_hdfs_pipe(patients_path))
    cases = Case.create_case_map(get_hdfs_pipe(cases_path), patients)

    partners = Partner.create_partner_map(get_hdfs_pipe(partner_path))
    Partner.add_partners_to_cases(get_hdfs_pipe(partner_case_path), cases, partners)

    Move.add_move_to_case(
        get_hdfs_pipe(moves_path),
        cases,
        rooms,
        wards,
        partners,
    )
    Risk.add_risk_to_patient(get_hdfs_pipe(risks_path), patients)
    Risk.add_deleted_risk_to_patient(get_hdfs_pipe(deleted_risks_path), patients)

    drugs = Medication.create_drug_map(get_hdfs_pipe(medication_path))
    Medication.add_medication_to_case(get_hdfs_pipe(medication_path), cases)

    chops = Chop.create_chop_dict(get_hdfs_pipe(chop_path))
    Surgery.add_surgery_to_case(get_hdfs_pipe(surgery_path), cases, chops)

    appointments = Appointment.create_termin_map(get_hdfs_pipe(appointments_path))
    Appointment.add_appointment_to_case(get_hdfs_pipe(appointment_patient_path), cases, appointments)

    devices = Device.create_device_map(get_hdfs_pipe(devices_path))
    Device.add_device_to_appointment(get_hdfs_pipe(device_appointment_path), appointments, devices)

    employees = Employee.create_employee_map(get_hdfs_pipe(appointment_employee_path))
    Employee.add_employee_to_appointment(
        get_hdfs_pipe(appointment_employee_path), appointments, employees
    )

    Care.add_care_to_case(get_hdfs_pipe(tacs_path), cases, employees)

    room_id_map = Room.create_room_id_map(get_hdfs_pipe(rooms_path))
    Room.add_room_to_appointment(get_hdfs_pipe(room_appointment_path), appointments, room_id_map, rooms)

    ICD_Codes = ICD.create_icd_dict(get_hdfs_pipe(icd_path))
    ICD.add_icd_to_case(get_hdfs_pipe(icd_path), cases)

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
            "room_id_map": room_id_map,
            "icd_codes": ICD_Codes
        }
    )

