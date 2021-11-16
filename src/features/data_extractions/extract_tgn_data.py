# %%
import os
print(os.getcwd())
os.chdir("../../..")
print(os.getcwd())
# %%
import logging
import pathlib
import sys

import pandas as pd

from src.features.dataloader import DataLoader
from src.features.model import Patient, patient
from datetime import datetime, date, timedelta
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


def get_node_features(patients, interactions_df):
    """Gather node features.
    
    Patient features (mostly time-invariant):
    - Gender
    - Age
    - Charlson Mean
    - Number of Surgeries
    - Number of Antibiotics

    Room features:
    - is ICU?


    Device features:
    - TODO

    Employee features:
    - TODO
    - Age?

    """
    unique_sources = interactions_df[["source", "source type"]].drop_duplicates()
    unique_destinations = interactions_df[["destination", "destination type"]].drop_duplicates()
    unique_destinations.rename({"destination": "source", "destination type": "source type"}, inplace=True)
    unique_nodes = pd.concat([unique_sources, unique_destinations]).drop_duplicates()

    node_features = []
    for _, row in unique_nodes.iterrows():
        node_id = row["source"]
        node_type = row["source type"]

        patient_age = -1
        patient_gender = 0
        # charlson_mean = 0
        number_of_surgeries = 0
        # number_of_antibiotics = 0
        is_icu = 0
        # TODO: Complete the rest of the node features
        if node_type == "PATIENT":
            today = date.today()
            birth_date = patients[node_id].birth_date
            patient_age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            patient_age = patient_age if patient_age < 120 else 0 # to exclude excessive values including those from year 1753 NaN values
            
            patient_gender = patients[node_id].gender
            # charlson_mean = patient["charlson_mean"]
            number_of_surgeries = len(patients[node_id].get_chop_codes())
            # number_of_antibiotics = patient["nantibiotics"]
        # elif node_type == "ROOM":
            # TODO: Get is_icu
            
        

        node_features.append([patient_age,
                                patient_gender,
                                # charlson_mean,
                                number_of_surgeries,
                                # number_of_antibiotics,
                                # is_icu
                                ])

    node_features_df = pd.DataFrame.from_records(node_features)
    node_features_df.columns = ["age", "gender", "surgery qty"]

def get_node_interactions(patients):
    """Gather entity interactions.

    each interaction consists of [timestamp_begin, timestamp_end, source, destination, edge_idx].

    :param patients:
    :return:
    """
    interactions = []

    source = "source"
    source_type = "source type"
    patient_type = "PATIENT"
    device_type = "DEVICE"
    room_type = "ROOM"
    employee_type = "EMPLOYEE"
    destination = "destination"
    destination_type = "destination type"
    timestamp_begin = "timestamp_begin"
    timestamp_end = "timestamp_end"

    # TODO: invert every edge

    # patient appoinments => patient-device-employee-room interaction
    patients_without_appointments = 0
    for patient in patients.values():
        patient_id = patient.patient_id
        for patient_appointment in patient.get_appointments():
            for device in patient_appointment.devices: # relate all devices to the patient
                interactions.append({source: str(patient_id), source_type: patient_type, destination: str(device.id), destination_type: device_type,
                                    timestamp_begin: patient_appointment.start_datetime, timestamp_end: patient_appointment.end_datetime})
                

            for employee in patient_appointment.employees: # relate all employees to the patient
                interactions.append({source: str(patient_id), source_type: patient_type, destination: str(employee.id), destination_type: employee_type,
                                    timestamp_begin: patient_appointment.start_datetime, timestamp_end: patient_appointment.end_datetime})

            for room in patient_appointment.rooms: # relate all rooms to the patient
                interactions.append({source: str(patient_id), source_type: patient_type, destination: str(room.room_id), destination_type: room_type,
                                    timestamp_begin: patient_appointment.start_datetime, timestamp_end: patient_appointment.end_datetime})

            # relate employees to rooms and devices
            for employee in patient_appointment.employees:
                for room in patient_appointment.rooms:
                    interactions.append({source: str(room.room_id), source_type: room_type, destination: str(employee.id), destination_type: employee_type,
                                        timestamp_begin: patient_appointment.start_datetime, timestamp_end: patient_appointment.end_datetime})

                for device in patient_appointment.devices:
                    interactions.append({source: str(device.id), source_type: device_type, destination: str(employee.id), destination_type: employee_type,
                                        timestamp_begin: patient_appointment.start_datetime, timestamp_end: patient_appointment.end_datetime})

            # relate devices to rooms
            for device in patient_appointment.devices:
                for room in patient_appointment.rooms:
                    interactions.append({source: str(device.id), source_type: device_type, destination: str(room.room_id), destination_type: room_type,
                                        timestamp_begin: patient_appointment.start_datetime, timestamp_end: patient_appointment.end_datetime})

            if len(patient.get_appointments()) == 0:
                patients_without_appointments += 1

    # patient stays => patient-room interactions
    patients_without_stays = 0
    for patient in patients.values():
        patient_id = patient.patient_id
        for patient_stay in patient.get_stays():
            interactions.append({source: str(patient_id), source_type: patient_type, destination: str(patient_stay.room.room_id), destination_type: room_type,
                                timestamp_begin: patient_stay.from_datetime, timestamp_end: patient_stay.to_datetime})

            if len(patient.get_stays()) == 0:
                patients_without_stays += 1

    # TODO: Timestamps for treatments are imprecise (date and duration in minutes), maybe they can be clarified using the room stay of the patient
    patients_without_care = 0
    for patient in patients.values():
        patient_id = patient.patient_id
        for treatment in patient.get_treatments():
            interactions.append({source: str(patient_id), source_type: patient_type, destination: str(treatment.employee_id), destination_type: employee_type,
                                timestamp_begin: treatment.date, timestamp_end: treatment.date + timedelta(minutes=treatment.duration_in_minutes)})

            # TODO: Here is also an interaction with the room the patient is staying during that time
        

    logging.info(f"Exported {len(interactions)} node interactions for {len(patients.values())} patients, {patients_without_appointments} without appointments, {patients_without_stays} without stays")

    return pd.DataFrame.from_records(interactions)


def add_node_labels(interactions):
    pass


# %%

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
    patient_data = loader.prepare_dataset(
        load_patients=True,  # patient node
        load_cases=True,  # many entities are related to cases
        load_partners=False,  # NO RELATION
        load_stays=True,  # relation between patient and room
        load_medications=False,  # TODO: potential node feature?
        load_risks=True,  # LABEL
        load_chop_codes=True,  # Node feature (for number of surgeries)
        load_surgeries=True,  # Node feature for patient
        load_appointments=True,  # relation between patient, device, room and employee
        load_devices=True,  # device node
        load_employees=True,  # employee node
        load_care_data=True,  # treatment data relates patients to employees
        load_rooms=True,  # room node
        load_icd_codes=False,  # NO RELATION: potential node feature?
        )

# %%

    import pyreadr

    file_path = "./data/raw/prediction_data/aset_300621.RData" #aset_021120.RData"
    result = pyreadr.read_r(file_path) # also works for Rds

    # done! let's see what we got
    # result is a dictionary where keys are the name of objects and the values python objects
    print(result.keys()) # let's check what objects we got
    covariates_df = result["aset"] # extract the pandas data frame for object df1

    #determine sizes of datasets
    n_df, m_df = covariates_df.shape

    print('Data Imported')

    covariates_df.to_csv("./data/raw/prediction_data/aset_300621.csv")

    covariates_df

# %%
    # TODO: produce a csv which contains all edge features in descriptive form (ml_vre_edge_verbose.csv)
    # edge features are fixed and should probably include the length of being active (as a substitute for true edge deletion)

    # produce a csv with contains all edges in descriptive form (ml_vre_graph_verbose.csv)
    node_interactions_df = get_node_interactions(patient_data["patients"])
    # TODO: add all node labels
    #node_interactions_df = add_node_labels(node_interactions_df)
    # the last interaction before the positive test includes the positive test

#%%

    # TODO: produce a csv which contains all node features in descriptive form (ml_vre_node_verbose.csv)
    # node features are fixed over time
    node_features_df = get_node_features(patient_data["patients"], node_interactions_df)

    # sources = graph_df.u.values
    # destinations = graph_df.i.values
    # edge_idxs = graph_df.idx.values
    # labels = graph_df.label.values
    # timestamps = graph_df.ts.values

    # node_interactions_df = get_entity_interactions(patient_data["patients"])

# %%

    risk_patients = Patient.get_risk_patients(patient_data["patients"])

    print(len(risk_patients))
    print(risk_patients)

    # get general data dataframes
    node_features_df = get_patient_risks(patient_data["patients"])

    # make the interim path if not available
    pathlib.Path("./data/processed/delivery/tgn/").mkdir(parents=True, exist_ok=True)

    node_interactions_df.to_csv(f"./data/processed/delivery/tgn/{now_str}_node_interactions.csv")

    node_features_df.to_csv(f"./data/processed/delivery/tgn/{now_str}_node_features.csv")

