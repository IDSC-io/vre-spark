import logging
import sys
import pandas as pd

from src.features.dataloader import DataLoader
from src.features.model import Patient
from datetime import datetime


def get_general_patient_data(patients):
    """Return ID, birth date and gender for each patient.
    :param patients:
    :return:
    """
    df = pd.DataFrame({"Patient ID": [patient.patient_id for patient in patients],
                       "Birth date": [patient.birth_date for patient in patients],
                       "Gender": [patient.gender for patient in patients]})
    return df


def get_patient_icu_data(patients):
    """Return ICU stays (ICU Admission, Length of stay, Clinic at day of coding) for each patient.
    """
    rows = []
    for patient in patients:
        icu_stays = patient.get_icu_stays()
        rows.extend([[patient.patient_id, icu_stay.from_datetime, icu_stay.to_datetime, icu_stay.ward_id] for icu_stay in icu_stays])

    df = pd.DataFrame(data=rows, columns=["Patient ID", "From Datetime", "To Datetime", "ICU Ward ID"])
    return df


def get_patient_antibiotics_data(patients):
    """Return name of antibiotics and length of usage
    :param patients:
    :return:
    """
    # TODO: Fill with data from sql
    df = pd.DataFrame()
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


    risk_patients = Patient.get_risk_patients(patient_data["patients"])

    # print(risk_patients)

    contact_risk_patient_tuple = Patient.get_contact_and_risk_patient_ids(patient_data["patients"])

    contact_patients = Patient.get_patients_by_ids(patient_data["patients"], [contact_patient[1] for contact_patient in contact_risk_patient_tuple])

    # print(contact_patients)

    # get general data dataframes
    risk_patient_general_data = get_general_patient_data(risk_patients)
    contact_patient_general_data = get_general_patient_data(contact_patients)

    # get ICU data dataframes
    risk_patient_icu_data = get_patient_icu_data(risk_patients)
    contact_patient_icu_data = get_patient_icu_data(contact_patients)

    # get antibiotics data dataframes
    risk_patient_antibiotics_data = get_patient_antibiotics_data(risk_patients)
    contact_patient_antibiotics_data = get_patient_antibiotics_data(contact_patients)

    risk_patient_general_data.to_csv(f"./data/processed/delivery/{now_str}_risk_patient_general.csv")
    contact_patient_general_data.to_csv(f"./data/processed/delivery/{now_str}_contact_patient_general.csv")

    risk_patient_icu_data.to_csv(f"./data/processed/delivery/{now_str}_risk_patient_icu.csv")
    contact_patient_icu_data.to_csv(f"./data/processed/delivery/{now_str}_contact_patient_icu.csv")

    risk_patient_antibiotics_data.to_csv(f"./data/processed/delivery/{now_str}_risk_patient_antibiotics.csv")
    contact_patient_antibiotics_data.to_csv(f"./data/processed/delivery/{now_str}_contact_patient_antibiotics.csv")
