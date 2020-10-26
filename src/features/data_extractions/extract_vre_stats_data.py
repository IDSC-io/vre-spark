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


def get_general_patient_data(patients: list):
    """Return ID, birth date and gender for each patient.
    :param patients:
    :return:
    """
    df = pd.DataFrame({"Patient ID": [patient.patient_id for patient in patients],
                       "Birth date": [patient.birth_date for patient in patients],
                       "Gender": [patient.gender for patient in patients]})
    return df


def get_patient_icu_data(patients: list):
    """Return ICU stays (ICU Admission, Length of stay, Clinic at day of coding) for each patient.
    """
    rows = []
    for patient in patients:
        icu_stays = patient.get_icu_stays()
        rows.extend([[patient.patient_id, icu_stay.from_datetime, icu_stay.to_datetime, icu_stay.ward_id] for icu_stay in icu_stays])

    df = pd.DataFrame(data=rows, columns=["Patient ID", "From Datetime", "To Datetime", "ICU Ward ID"])
    return df


def get_patient_antibiotics_data(patients: list):
    """Return name of antibiotics and length of usage
    :param patients:
    :return:
    """
    base_path = configuration['PATHS']['raw_data_dir'].format("test") if configuration['PARAMETERS']['dataset'] == 'test' \
        else configuration['PATHS']['raw_data_dir'].format("model")  # absolute or relative path to directory where data is stored

    antibiotics_prescriptions_path = os.path.join(base_path, "DIM_ANTIBIOTICS_prescription_start_end.csv")
    # antibiotics_intake_path = os.path.join(base_path, "ANTIBIOTICS_INTAKE.csv")
    df = pd.read_csv(antibiotics_prescriptions_path, dtype=str, encoding="ISO-8859-1")
    df.columns = ["Patient ID", "Prescription ID", "Prescription Date", "Medication Name", "Medication ATC", "Action Type", "Action Datetime"]
    # TODO: PIDs are not numbers...
    df["Patient ID"] = pd.to_numeric(df["Patient ID"])
    patient_ids = set([int(patient.patient_id) for patient in patients])
    return df[df["Patient ID"].isin(patient_ids)]


def get_patient_stays(patients):
    """Return patient stays.
    :param patients:
    :return:
    """
    stays = []

    # # patient-device-employee interactions
    # appointments_per_patient = {}
    # for patient in patients.values():
    #     appointments_per_patient[patient.patient_id] = patient.get_appointments()

    # for (patient_id, patient_appointments) in appointments_per_patient.items():
    #     for patient_appointment in patient_appointments:
    #         for device in patient_appointment.devices:
    #             interactions.append({"node_0": "PATIENT_" + str(patient_id), "node_1": "DEVICE_" + str(device.id),
    #                                  "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})
    #
    #             for employee in patient_appointment.employees:
    #                 interactions.append({"node_0": "EMPLOYEE_" + str(employee.id), "node_1": "DEVICE_" + str(device.id),
    #                                      "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})
    #
    #             for room in patient_appointment.rooms:
    #                 interactions.append({"node_0": "ROOM_" + str(room.name), "node_1": "DEVICE_" + str(device.id),
    #                                      "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})
    #
    #         for employee in patient_appointment.employees:
    #             for room in patient_appointment.rooms:
    #                 interactions.append({"node_0": "ROOM_" + str(room.name), "node_1": "EMPLOYEE_" + str(employee.id),
    #                                      "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})

    # patient-room interactions
    stays_per_patient = {}
    patients_without_stays = 0
    for patient in patients.values():
        stays_per_patient[patient.patient_id] = patient.get_stays()

    for (patient_id, patient_stays) in stays_per_patient.items():
        patient_stays_count = 0
        for patient_stay in patient_stays:
            if not pd.isna(patient_stay.room.name):
                stays.append({"patient_id": str(patient_id), "room_id": str(patient_stay.room.name), "timestamp_begin": patient_stay.from_datetime, "timestamp_end" :patient_stay.to_datetime})
                patient_stays_count += 1

        if patient_stays_count == 0:
            patients_without_stays += 1

    logging.info(f"Exported {len(stays)} Patient stays for {len(patients.values())} patients, {patients_without_stays} without stays")

    df = pd.DataFrame.from_records(stays)
    return df


if __name__ == '__main__':
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
    patient_data = loader.patient_data(
        load_cases=True,
        load_partners=False,
        load_stays=True,
        load_medications=False,
        load_risks=True,
        load_chop_codes=False,
        load_surgeries=False,
        load_appointments=False,
        load_devices=False,
        load_employees=False,
        load_care_data=False,
        load_rooms=False,
        load_icd_codes=False)

    logger.info("...Done.\nGetting risk patients...")

    risk_patients = Patient.get_risk_patients(patient_data["patients"])

    print(f"Number of risk patients: {len(risk_patients)}")
    # print(risk_patients)

    logger.info("...Done.\nGetting contact patients...")

    contact_risk_patient_dict = Patient.get_contact_patients(patient_data["patients"])

    contact_patients = {}  # Patient.get_patients_by_ids(patient_data["patients"], contact_risk_patient_dict.keys())

    print(f"Number of contact patients found: {len(contact_patients)}")
    # print(contact_patients)

    logger.info("...Done.\nGetting remaining patients...")

    # pick up patients that are neither risk nor contact patients
    remaining_patients = {patient_id: patient_data["patients"][patient_id] for patient_id in patient_data["patients"].keys() if patient_id not in contact_patients and patient_id not in risk_patients}

    print(f"Number of remaining patients found: {len(remaining_patients)}")
    # print(remaining_patients)
    assert(len(dict(risk_patients.keys() & contact_patients.keys())) == 0 and len(dict(contact_patients.keys() & remaining_patients.keys())) == 0 and len(dict(remaining_patients.keys() & risk_patients.keys())) == 0)
    assert(len(risk_patients.keys()) + len(contact_patients.keys()) + len(remaining_patients.keys()) == len(patient_data["patients"]))

    logger.info("...Done.\nExtracting general patient data...")

    # get general data dataframes
    patient_ids_data = get_general_patient_data(list(patient_data["patients"].values()))
    risk_patient_general_data = get_general_patient_data(list(risk_patients.values()))
    # contact_patient_general_data = get_general_patient_data(list(contact_patients.values()))
    remaining_patient_general_data = get_general_patient_data(list(remaining_patients.values()))

    logger.info("...Done.\nExtracting patient ICU data...")

    # get ICU data dataframes
    risk_patient_icu_data = get_patient_icu_data(list(risk_patients.values()))
    # contact_patient_icu_data = get_patient_icu_data(list(contact_patients.values()))
    remaining_patient_icu_data = get_patient_icu_data(list(remaining_patients.values()))

    logger.info("...Done.\nExtracting patient antibiotics data...")

    # get antibiotics data dataframes
    risk_patient_antibiotics_data = get_patient_antibiotics_data(list(risk_patients.values()))
    # contact_patient_antibiotics_data = get_patient_antibiotics_data(list(contact_patients.values()))
    remaining_patient_antibiotics_data = get_patient_antibiotics_data(list(remaining_patients.values()))

    logger.info("...Done.\nExtracting patient stays...")

    patient_stays_df = get_patient_stays(patient_data["patients"])

    logger.info("...Done.\nSaving data to files...")

    # make the delivery/stats path if not available
    pathlib.Path("./data/processed/delivery/stats/").mkdir(parents=True, exist_ok=True)

    patient_ids_data.to_csv(f"./data/processed/delivery/stats/{now_str}_patient_ids.csv")
    risk_patient_general_data.to_csv(f"./data/processed/delivery/stats/{now_str}_risk_patient_general.csv")
    # contact_patient_general_data.to_csv(f"./data/processed/delivery/stats/{now_str}_contact_patient_general.csv")
    remaining_patient_general_data.to_csv(f"./data/processed/delivery/stats/{now_str}_remaining_patient_general.csv")

    risk_patient_icu_data.to_csv(f"./data/processed/delivery/stats/{now_str}_risk_patient_icu.csv")
    # contact_patient_icu_data.to_csv(f"./data/processed/delivery/stats/{now_str}_contact_patient_icu.csv")
    remaining_patient_icu_data.to_csv(f"./data/processed/delivery/stats/{now_str}_remaining_patient_icu.csv")

    risk_patient_antibiotics_data.to_csv(f"./data/processed/delivery/stats/{now_str}_risk_patient_antibiotics.csv")
    # contact_patient_antibiotics_data.to_csv(f"./data/processed/delivery/stats/{now_str}_contact_patient_antibiotics.csv")
    remaining_patient_antibiotics_data.to_csv(f"./data/processed/delivery/stats/{now_str}_remaining_patient_antibiotics.csv")

    patient_stays_df.to_csv(f"./data/processed/delivery/stats/{now_str}_patient_stays.csv")

    logger.info("...Done.")

