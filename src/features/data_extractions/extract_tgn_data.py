# %%
import os

from sqlalchemy import column
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
from tqdm import tqdm


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


def get_node_features(entities, interactions_df):
    """Gather node features.
    
    Patient features (mostly time-invariant):
    - Gender
    - Age
    - TODO: Charlson Mean
    - Number of Surgeries
    - TODO: Number of Antibiotics

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
    unique_destinations.rename(columns={"destination": "source", "destination type": "source type"}, inplace=True)
    unique_nodes = pd.concat([unique_sources, unique_destinations]).drop_duplicates()

    node_features = []
    for _, row in tqdm(unique_nodes.iterrows(), total=len(unique_nodes)):
        node_id = row["source"]
        node_type = row["source type"]

        patient_age = -1
        patient_is_male = -1
        # charlson_mean = 0
        number_of_surgeries = -1
        # number_of_antibiotics = 0
        is_icu = 0
        nr_missing_patients = 0
        try:
            if node_type == "PATIENT":
                patients = entities["patients"]
                today = date.today()
                birth_date = patients[node_id].birth_date
                patient_age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
                patient_age = patient_age if patient_age < 120 else 0 # to exclude excessive values including those from year 1753 NaN values
                
                patient_is_male = 1 if patients[node_id].gender == "male" else 0  # proxy for male patients are more often contaminated
                # charlson_mean = patient["charlson_mean"]
                number_of_surgeries = len(patients[node_id].get_chop_codes())  # proxy for higher risk through surgeries
                # number_of_antibiotics = patient["nantibiotics"]
                is_icu = int(patients[node_id].has_icu_stay())  # proxy for icu patients more likely immunodeficiency etc.
            elif node_type == "ROOM":
                # Get is_icu
                rooms = entities["rooms"]
                is_icu = int(rooms[node_id].is_icu())  # proxy for icu patients, immunodeficientcy etc.
            elif node_type == "EMPLOYEE":
                employees = entities["employees"]
                # TODO: There is not much known about the employees except that they are hopefully mostly unique
                # There is possibly more info available through TACS CARE data
            elif node_type == "DEVICE":
                devices = entities["devices"]
                # TODO: There is not much known about the devices except that they are hopefully mostly unique
                # device class or type would be very helpful as a proxy for mobility and contaminable surface
        except KeyError:
            nr_missing_patients += 1
        
        node_features.append([node_type,
                                node_id,
                                patient_age,
                                patient_is_male,
                                # charlson_mean,
                                number_of_surgeries,
                                # number_of_antibiotics,
                                is_icu
                                ])

    logging.info(f"Node features for {len(unique_nodes)} nodes generated ({nr_missing_patients} missing patients)")
    
    node_features_df = pd.DataFrame.from_records(node_features)
    node_features_df.columns = ["node type", "node id", "age", "gender", "surgery qty", "is_icu"]
    node_features_df.reindex()
    return node_features_df

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

    # TODO: invert every edge (is the model assuming the directionality?)
    # TODO: Add node type as feature
    # TODO: Timestamps seem to be date only, can we improve this?

    # patient appoinments => patient-device-employee-room interaction
    patients_without_appointments = 0
    for patient in tqdm(patients.values()):
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
    # TODO: With treatments, the employee is at every patient at the same time. This might degrade the prediction.
    patients_without_care = 0
    for patient in patients.values():
        patient_id = patient.patient_id
        for treatment in patient.get_treatments():
            interactions.append({source: str(patient_id), source_type: patient_type, destination: str(treatment.employee_id), destination_type: employee_type,
                                timestamp_begin: treatment.date, timestamp_end: treatment.date + timedelta(minutes=treatment.duration_in_minutes)})

            # TODO: Here is also an interaction with the room the patient is staying during that time
        

    logging.info(f"Exported {len(interactions)} node interactions for {len(patients.values())} patients, {patients_without_appointments} without appointments, {patients_without_stays} without stays")

    interactions_df = pd.DataFrame.from_records(interactions)
    interactions_df["timestamp_begin"] = pd.to_datetime(interactions_df["timestamp_begin"])
    return interactions_df

# %%
def add_edge_features(interactions_df):
    """
    Get features of each edge.
    - Length of interaction in seconds.
    """
    interactions_df["Seconds of Interaction"] = ((interactions_df["timestamp_end"] - interactions_df["timestamp_begin"]).dt.total_seconds())
    return interactions_df 

def add_node_labels(patients, interactions_df, contamination_before_screening=timedelta(days=6)):
    interactions_df["label"] = 0
    patient_risk_rows = []
    for patient in tqdm(patients.values()):
        patient_id = patient.patient_id

        risk = None
        for risk_screening in patient.risk_screenings.values():
            if risk_screening.result != "nn" and (risk is None or risk.recording_date > risk_screening.recording_date): # get earliest positive risk
                risk = risk_screening

        patient_risk_rows.append([patient_id, risk.recording_date - contamination_before_screening if risk is not None else datetime.today()])

    patient_risk_df = pd.DataFrame.from_records(patient_risk_rows)
    patient_risk_df.columns = ["Patient ID", "Risk Date"]


    interactions_df = pd.merge(interactions_df, patient_risk_df, how="left", left_on="source", right_on="Patient ID")
    interactions_df = pd.merge(interactions_df, patient_risk_df, how="left", left_on="destination", right_on="Patient ID")

    node_condition = (
                        (interactions_df["source type"] == "PATIENT"
                        ) | (  # if either interaction partner is the patient
                          interactions_df["destination type"] == "PATIENT"
                        )) & ((
                            interactions_df['timestamp_begin'].dt.date >= interactions_df['Risk Date_x']
                        ) | (
                            interactions_df['timestamp_begin'].dt.date >= interactions_df['Risk Date_y']) # and the interaction happened after the positive screening
                    )
    interactions_df.loc[node_condition, "label"] = 1 # we label this interaction as contaminated
    interactions_df.drop(columns=["Patient ID_x", "Patient ID_y", "Risk Date_x", "Risk Date_y"], inplace=True)
    return interactions_df


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

    # produce a csv with contains all edges in descriptive form (ml_vre_graph_verbose.csv)
    node_interactions_df = get_node_interactions(patient_data["patients"])

# %%
    # add all node labels
    node_interactions_df = add_node_labels(patient_data["patients"], node_interactions_df)
    #node_interactions_df = add_node_labels(node_interactions_df)
    # the last interaction before the positive test includes the positive test

# %%
    # produce a csv which contains all edge features in descriptive form (ml_vre_edge_verbose.csv)
    # edge features are fixed and should probably include the length of being active (as a substitute for true edge deletion)
    node_interactions_df = add_edge_features(node_interactions_df)


#%%
    # produce node features (they are fixed over time)
    node_features_df = get_node_features(patient_data, node_interactions_df)

    # sources = graph_df.u.values
    # destinations = graph_df.i.values
    # edge_idxs = graph_df.idx.values
    # labels = graph_df.label.values
    # timestamps = graph_df.ts.values

# %%
    # generate a uid for all nodes 
    node_features_df['uid'] = node_features_df.index
    node_interactions_df = pd.merge(node_interactions_df, node_features_df[["uid", "node type", "node id"]], how="left", left_on=["source type", "source"], right_on=["node type", "node id"], suffixes=('', '_source'))
    node_interactions_df = pd.merge(node_interactions_df, node_features_df[["uid", "node type", "node id"]], how="left", left_on=["destination type", "destination"], right_on=["node type", "node id"], suffixes=('', '_destination'))
    node_interactions_df.drop(columns=["source type", "destination type", "source", "destination", "node type", "node id", "node type_destination", "node id_destination"], inplace=True)
    node_interactions_df.rename(columns={"uid": "source", "uid_destination": "destination"}, inplace=True)
    
    # drop broken interactions containing some NaNs
    node_interactions_df.dropna(inplace=True)
    
    # rereference the begin timestamps to the minimal timestamp in seconds and sort the interactions ascendingly in time
    min_date = node_interactions_df[["timestamp_begin"]].min()
    node_interactions_df["seconds_begin"] = node_interactions_df[["timestamp_begin"]] - min_date
    node_interactions_df["seconds_begin"] = node_interactions_df["seconds_begin"].dt.total_seconds().astype(int)
    node_interactions_df = node_interactions_df[["source", "destination", "seconds_begin", "label", "Seconds of Interaction"]]
    node_interactions_df.sort_values(by="seconds_begin", inplace=True) # the interactions should be sorted ascending in time, otherwise the tgn will complain that it is editing something in the past

    
    node_features_df.drop(columns=["node type", "node id"], inplace=True)
    node_features_df = node_features_df[["uid", "age", "gender", "surgery qty", "is_icu"]]

    # TODO: In case something goes wrong, add a small quality control print output that joins some edges with their respective node features to see if they match

# %%

    # make the interim path if not available
    pathlib.Path("./data/processed/delivery/tgn/").mkdir(parents=True, exist_ok=True)

    node_interactions_df.to_csv(f"./data/processed/delivery/tgn/{now_str}_node_interactions.csv", index=False)
# %%

    node_features_df.to_csv(f"./data/processed/delivery/tgn/{now_str}_node_features.csv", index=False)


# %%