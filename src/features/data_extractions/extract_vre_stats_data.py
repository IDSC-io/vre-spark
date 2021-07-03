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
from tqdm import tqdm


def get_general_patient_data(patients: list):
    """Return ID, birth date and gender for each patient.
    :param patients:
    :return:
    """
    df = pd.DataFrame({"Patient ID": [patient.patient_id for patient in patients],
                       "Birth date": [patient.birth_date for patient in patients],
                       "Gender": [patient.gender for patient in patients]})
    logger.info(f"Exported {len(df)} patients with general info")
    return df


def get_patient_icu_data(patients: list):
    """Return ICU stays (ICU Admission, Length of stay, Clinic at day of coding) for each patient.
    """
    rows = []
    patient_with_icu_stay = 0
    patient_without_icu_stay = 0
    for patient in tqdm(patients):
        icu_stays = patient.get_icu_stays()

        if len(icu_stays) != 0:
            patient_with_icu_stay += 1
            rows.extend([[patient.patient_id, icu_stay.from_datetime, icu_stay.to_datetime, icu_stay.ward_id] for icu_stay in icu_stays])
        else:
            patient_without_icu_stay += 1

    df = pd.DataFrame(data=rows, columns=["Patient ID", "From Datetime", "To Datetime", "ICU Ward ID"])
    logger.info(f"Exported {len(patients)} patients, {patient_with_icu_stay} with icu stay, {patient_without_icu_stay} without icu stay")
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
    patients_with_antibiotics_df = df[df["Patient ID"].isin(patient_ids)]

    patients_with_antibiotics = df['Patient ID'].drop_duplicates().isin(patient_ids).sum()
    logger.info(f"Exported {len(patients)} patients, {patients_with_antibiotics} with antibiotics prescription, {len(patients) - patients_with_antibiotics} without antibiotics")
    return patients_with_antibiotics_df


def get_patient_icd10_data(patients: list):
    """Get ICD10 codes of each patient.

    :param patients:
    :return:
    """

    patient_icd10_codes = []
    patients_without_icd10_codes = 0
    patients_with_icd10_codes = 0
    patients_icd10_codes_qty = 0
    for patient in patients:
        patient_codes = []
        for case in patient.cases.values():
            patient_codes.extend(case.icd_codes)

        if len(patient_codes) == 0:
            patients_without_icd10_codes += 1
        else:
            patients_icd10_codes_qty += len(patient_codes)
            patients_with_icd10_codes += 1
        patient_icd10_codes.append({"patient_id": str(patient.patient_id), "patient_icd_10_codes": [code.icd_code for code in patient_codes]})

    logger.info(f"Exported {patients_icd10_codes_qty} ICD10 codes for {patients_with_icd10_codes} patients, {patients_without_icd10_codes} patients without ICD10 codes.")

    patient_icd10_codes_df = pd.DataFrame.from_records(patient_icd10_codes)
    return patient_icd10_codes_df


def get_patient_surgery_qty_data(patients: list):
    """Get number of surgeries of each patient.

    :param patients:
    :return:
    """

    patient_surgeries = []
    patients_without_surgeries = 0
    patients_with_surgeries = 0
    patients_surgery_qty = 0
    for patient in patients:
        if len(patient.get_chop_codes()) == 0:
            patients_without_surgeries += 1
        else:
            patients_with_surgeries += 1
            patients_surgery_qty += len(patient.get_chop_codes())
        patient_surgeries.append({"patient_id": str(patient.patient_id), "surgery quantity": str(len(patient.get_chop_codes()))})

    patient_surgeries_df = pd.DataFrame.from_records(patient_surgeries)

    logger.info(f"Exported {patients_surgery_qty} surgeries for {patients_with_surgeries} patients, {patients_without_surgeries} patients without surgeries.")

    return patient_surgeries_df


def get_patient_interactions(patients: list):
    """Return patient interactions (room stays, employee treatments, device interactions).
    :param patients:
    :return:
    """

    employee_interations = []
    device_interactions = []

    # patient-device-employee interactions
    appointments_per_patient = {}
    for patient in patients:
        appointments_per_patient[patient.patient_id] = patient.get_appointments()

    patients_without_employees = 0
    patients_with_employees = 0
    patients_without_devices = 0
    patients_with_devices = 0
    for (patient_id, patient_appointments) in appointments_per_patient.items():
        patient_employees = 0
        patient_devices = 0
        for patient_appointment in patient_appointments:
            for device in patient_appointment.devices:
                device_interactions.append({"patient_id": str(patient_id), "device_id": str(device.id),
                                     "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})
                patient_devices += 1

            for employee in patient_appointment.employees:
                employee_interations.append({"patient_id": str(patient_id), "employee_id": str(employee.id),
                                     "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})
                patient_employees += 1

            if patient_devices == 0:
                patients_without_devices += 1
            else:
                patients_with_devices += 1

            if patient_employees == 0:
                patients_without_employees += 1
            else:
                patients_with_employees += 1

                # for employee in patient_appointment.employees:
                #     interactions.append({"node_0": "EMPLOYEE_" + str(employee.id), "node_1": "DEVICE_" + str(device.id),
                #                          "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})

                # for room in patient_appointment.rooms:
                #     interactions.append({"node_0": "ROOM_" + str(room.name), "node_1": "DEVICE_" + str(device.id),
                #                          "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})

            # for employee in patient_appointment.employees:
            #     for room in patient_appointment.rooms:
            #         employee_interactions.append({"node_0": "ROOM_" + str(room.name), "node_1": "EMPLOYEE_" + str(employee.id),
            #                              "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})

    employee_interations_df = pd.DataFrame.from_records(employee_interations)

    device_interactions_df = pd.DataFrame.from_records(device_interactions)

    stays = []
    # patient-room interactions
    stays_per_patient = {}
    patients_without_stays = 0
    patients_with_stays = 0
    for patient in patients:
        stays_per_patient[patient.patient_id] = patient.get_stays()

    for (patient_id, patient_stays) in stays_per_patient.items():
        patient_stays_count = 0
        for patient_stay in patient_stays:  # TODO: room_id is always NA. WHY? (Exported 0 Patient stays for 0 patients, 2004681 without stays)
            if not pd.isna(patient_stay.room.get_ids()):
                stays.append({"patient_id": str(patient_id), "room_id": str(patient_stay.room.get_ids()), "timestamp_begin": patient_stay.from_datetime, "timestamp_end" :patient_stay.to_datetime})
                patient_stays_count += 1

        if patient_stays_count == 0:
            patients_without_stays += 1
        else:
            patients_with_stays += 1

    stay_df = pd.DataFrame.from_records(stays)

    logging.info(f"Exported {len(stays)} Patient stays for {patients_with_stays} patients, {patients_without_stays} without stays")
    logging.info(f"Exported {len(employee_interations)} Patient-Employee interactions for {patients_with_employees} patients, {patients_without_employees} without employees")
    logging.info(f"Exported {len(device_interactions)} Patient-Device interactions for {patients_with_devices} patients, {patients_without_devices} without devices")

    return employee_interations_df, device_interactions_df, stay_df


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
    patient_data = loader.prepare_dataset(
        load_cases=True,
        load_partners=False,
        load_stays=True,
        load_medications=False,
        load_risks=True,
        load_chop_codes=False,
        load_surgeries=True,
        load_appointments=True,
        load_devices=True,
        load_employees=True,
        load_care_data=False,
        load_rooms=False,
        load_icd_codes=True,
        load_buildings=False)

    logger.info("...Done.\nGetting risk patients...")

    risk_patients = Patient.get_risk_patients(patient_data["patients"])

    print(f"Number of risk patients: {len(risk_patients)}")
    # print(risk_patients)

    # logger.info("...Done.\nGetting contact patients...")

    # contact_risk_patient_dict = Patient.get_contact_patients(patient_data["patients"])

    contact_patients = {}  # Patient.get_patients_by_ids(patient_data["patients"], contact_risk_patient_dict.keys())

    # print(f"Number of contact patients found: {len(contact_patients)}")
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
    patients = list(patient_data["patients"].values())
    #patient_general_data = get_general_patient_data(patients)
    risk_patient_general_data = get_general_patient_data(list(risk_patients.values()))
    # contact_patient_general_data = get_general_patient_data(list(contact_patients.values()))
    remaining_patient_general_data = get_general_patient_data(list(remaining_patients.values()))

    risk_patient_general_data["Risk"] = "pp"
    remaining_patient_general_data["Risk"] = "nn"

    patient_general_data = pd.concat([risk_patient_general_data, remaining_patient_general_data]).reset_index()

    logger.info("...Done.\nExtracting patient ICU data...")
    # get ICU data dataframes
    patient_icu_data = get_patient_icu_data(patients)
    #risk_patient_icu_data = get_patient_icu_data(list(risk_patients.values()))
    # contact_patient_icu_data = get_patient_icu_data(list(contact_patients.values()))
    #remaining_patient_icu_data = get_patient_icu_data(list(remaining_patients.values()))

    logger.info("...Done.\nExtracting patient antibiotics data...")
    # get antibiotics data dataframes
    patient_antibiotics_data = get_patient_antibiotics_data(patients)
    #risk_patient_antibiotics_data = get_patient_antibiotics_data(list(risk_patients.values()))
    # contact_patient_antibiotics_data = get_patient_antibiotics_data(list(contact_patients.values()))
    #remaining_patient_antibiotics_data = get_patient_antibiotics_data(list(remaining_patients.values()))

    logger.info("...Done.\nExtracting patient diagnostics data...")
    # get ICD10 data dataframes
    patient_icd10_df = get_patient_icd10_data(patients)

    logger.info("...Done.\nExtracting patient surgery data...")
    # get surgery data dataframes
    patient_surgery_qty_df = get_patient_surgery_qty_data(patients)

    logger.info("...Done.\nExtracting patient interactions...")
    employee_interactions_df, device_interactions_df, patient_stays_df = get_patient_interactions(patients)

    logger.info("...Done.\nSaving data to files...")

    # make the delivery/stats path if not available
    pathlib.Path("./data/processed/delivery/stats/").mkdir(parents=True, exist_ok=True)

    patient_general_data.to_csv(f"./data/processed/delivery/stats/{now_str}_patient_general.csv")
    # risk_patient_general_data.to_csv(f"./data/processed/delivery/stats/{now_str}_risk_patient_general.csv")
    # contact_patient_general_data.to_csv(f"./data/processed/delivery/stats/{now_str}_contact_patient_general.csv")
    # remaining_patient_general_data.to_csv(f"./data/processed/delivery/stats/{now_str}_remaining_patient_general.csv")

    patient_icu_data.to_csv(f"./data/processed/delivery/stats/{now_str}_patient_icu.csv")
    # risk_patient_icu_data.to_csv(f"./data/processed/delivery/stats/{now_str}_risk_patient_icu.csv")
    # contact_patient_icu_data.to_csv(f"./data/processed/delivery/stats/{now_str}_contact_patient_icu.csv")
    # remaining_patient_icu_data.to_csv(f"./data/processed/delivery/stats/{now_str}_remaining_patient_icu.csv")

    patient_antibiotics_data.to_csv(f"./data/processed/delivery/stats/{now_str}_patient_antibiotics.csv")
    # risk_patient_antibiotics_data.to_csv(f"./data/processed/delivery/stats/{now_str}_risk_patient_antibiotics.csv")
    # contact_patient_antibiotics_data.to_csv(f"./data/processed/delivery/stats/{now_str}_contact_patient_antibiotics.csv")
    # remaining_patient_antibiotics_data.to_csv(f"./data/processed/delivery/stats/{now_str}_remaining_patient_antibiotics.csv")

    employee_interactions_df.to_csv(f"./data/processed/delivery/stats/{now_str}_employee_interactions.csv")
    device_interactions_df.to_csv(f"./data/processed/delivery/stats/{now_str}_device_interactions.csv")
    patient_stays_df.to_csv(f"./data/processed/delivery/stats/{now_str}_patient_stays.csv")

    patient_icd10_df.to_csv(f"./data/processed/delivery/stats/{now_str}_patient_icd10_codes.csv")
    patient_surgery_qty_df.to_csv(f"./data/processed/delivery/stats/{now_str}_patient_surgery_qty.csv")

    logger.info("...Done.")

