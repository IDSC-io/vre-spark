import logging
import os
import pathlib
import sys
import pandas as pd

from src.features.dataloader import DataLoader
from src.features.model import Patient
from datetime import datetime
from configuration.basic_configuration import configuration


def get_patient_risks(patients):
    """Return patients with risks and timestamps
    :param patients:
    :return:
    """
    patient_ids = []
    patient_risks = []
    patient_risk_timestamps = []
    for patient in patients.values():
        for risk in patient.risks.values():
            patient_ids.append("PATIENT_" + str(patient.patient_id))
            patient_risks.append(risk.result)
            patient_risk_timestamps.append(risk.recording_date)

    df = pd.DataFrame({"Node ID": patient_ids,
                       "Risk": patient_risks,
                       "Timestamp": patient_risk_timestamps})
    return df


def get_entity_interactions(patients):
    """Return entity interactions.
    :param patients:
    :return:
    """
    interactions = []

    # patient-device-employee interactions
    appointments_per_patient = {}
    for patient in patients.values():
        appointments_per_patient[patient.patient_id] = patient.get_appointments()

    for (patient_id, patient_appointments) in appointments_per_patient.items():
        for patient_appointment in patient_appointments:
            for device in patient_appointment.devices:
                interactions.append({"node_0": "PATIENT_" + str(patient_id), "node_1": "DEVICE_" + str(device.id),
                                     "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})

                for employee in patient_appointment.employees:
                    interactions.append({"node_0": "EMPLOYEE_" + str(employee.id), "node_1": "DEVICE_" + str(device.id),
                                         "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})

                for room in patient_appointment.rooms:
                    interactions.append({"node_0": "ROOM_" + str(room.name), "node_1": "DEVICE_" + str(device.id),
                                         "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})

            for employee in patient_appointment.employees:
                for room in patient_appointment.rooms:
                    interactions.append({"node_0": "ROOM_" + str(room.name), "node_1": "EMPLOYEE_" + str(employee.id),
                                         "timestamp_begin": patient_appointment.start_datetime, "timestamp_end": patient_appointment.end_datetime})

    # patient-room interactions
    stays_per_patient = {}
    patients_without_stays = 0
    for patient in patients.values():
        stays_per_patient[patient.patient_id] = patient.get_stays()

    for (patient_id, patient_stays) in stays_per_patient.items():
        patient_stays_count = 0
        for patient_stay in patient_stays:
            interactions.append({"node_0": "PATIENT_" + str(patient_id), "node_1": "ROOM_" + str(patient_stay.room.name), "timestamp_begin": patient_stay.from_datetime, "timestamp_end" :patient_stay.to_datetime})
            patient_stays_count += 1

        if patient_stays_count == 0:
            patients_without_stays += 1

    logging.info(f"Exported {len(interactions)} GNN interactions for {len(patients.values())} patients, {patients_without_stays} without stays")

    df = pd.DataFrame.from_records(interactions)
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
        load_appointments=True,
        load_devices=True,
        load_employees=True,
        load_care_data=False,
        load_rooms=True,
        load_icd_codes=False)

    node_interactions_df = get_entity_interactions(patient_data["patients"])

    risk_patients = Patient.get_risk_patients(patient_data["patients"])

    print(len(risk_patients))
    print(risk_patients)

    # get general data dataframes
    node_features_df = get_patient_risks(patient_data["patients"])

    # make the interim path if not available
    pathlib.Path("./data/processed/delivery/gnn/").mkdir(parents=True, exist_ok=True)

    node_interactions_df.to_csv(f"./data/processed/delivery/gnn/{now_str}_node_interactions.csv")

    node_features_df.to_csv(f"./data/processed/delivery/gnn/{now_str}_node_features.csv")

