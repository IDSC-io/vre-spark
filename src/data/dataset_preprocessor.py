import pathlib
import pandas as pd
import os

from configuration.basic_configuration import configuration


def cleanup_dataset():
    """
    Remove all the horribleness from the dataset.

    - Column names are weirdly shortened and partly english and german.
    - States of everything are horrible strings.
    :return:
    """
    raw_data_path = configuration['PATHS']['raw_data_dir'].format("test") if configuration['PARAMETERS']['dataset'] == 'test' \
        else configuration['PATHS']['raw_data_dir'].format("model")  # absolute or relative path to directory where data is stored

    interim_data_path = configuration['PATHS']['interim_data_dir'].format("test") if configuration['PARAMETERS']['dataset'] == 'test' \
        else configuration['PATHS']['interim_data_dir'].format("model")  # absolute or relative path to directory where data is stored

    # make the interim path if not available
    pathlib.Path(interim_data_path).mkdir(parents=True, exist_ok=True)

    csv_files = [each_file for each_file in os.listdir(raw_data_path) if each_file.endswith('.csv')]

    for each_file in csv_files:
        print(f"--> Fixing modelling in file {each_file} ...")

        path = pathlib.Path(raw_data_path + "/" + each_file)

        if path.name == "DIM_FALL.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Case ID", "Case Type ID", "Case Status", "Case Type", "Start Date", "End Date",
                          "Patient Type", "Patient Status"]

            df.loc[df["Case Status"] == "Fall ist abgeschlossen", "Case Status"] = "closed"
            df.loc[df["Case Status"] == "Fall ist aktuell", "Case Status"] = "open"

            df.loc[df["Case Type"] == "ambulanter Fall", "Case Type"] = "ambulatory"
            df.loc[df["Case Type"] == "stationärer Fall", "Case Type"] = "in-patient"
            df.loc[df["Case Type"] == "teilstationärer Fall", "Case Type"] = "partially in-patient"

            df.loc[df["Patient Status"] == "aktiv", "Patient Status"] = "active"
            df.loc[df["Patient Status"] == "storniert", "Patient Status"] = "cancelled"
            df = df.set_index("Case ID")
        elif path.name == "DIM_GERAET.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Device ID", "Device Name"]
            df = df.set_index("Device ID")
        elif path.name == "DIM_PATIENT.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Gender", "Birth Date", "Zip Code", "Place of Residence", "Canton", "Language"]
            df.loc[df["Gender"] == "männlich", "Gender"] = "male"
            df.loc[df["Gender"] == "weiblich", "Gender"] = "female"
            df = df.set_index("Patient ID")
        elif path.name == "DIM_RAUM.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Room ID", "Room Common Name"]
            df = df.set_index("Room ID")
        elif path.name == "DIM_TERMIN.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appointment ID", "Deleted On Source", "Description", "Type", "Type 2", "Date", "Duration in Minutes"]
            df = df.set_index("Appointment ID")
        elif path.name == "FAKT_MEDIKAMENTE.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Case ID", "Drug Name",
                          "Drug ATC ID",  # Anatomical Therapeutic Chemical Classification Syname ID
                          "Quantity",
                          "Unit",
                          "Disposition Form",
                          "Submission Date"]
            df = df.set_index(["Patient ID", "Case ID", "Submission Date"])
        elif path.name == "FAKT_TERMIN_GERAET.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appointment ID", "Device ID", "Begin", "End", "Duration in Minutes"]
            df = df.set_index(["Appointment ID", "Device ID", "Begin"])
        elif path.name == "FAKT_TERMIN_MITARBEITER.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appointment ID", "Employee ID", "Begin", "End", "Duration in Minutes"]
            df = df.set_index(["Appointment ID", "Employee ID", "Begin"])
        elif path.name == "FAKT_TERMIN_PATIENT.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appointment ID", "Patient ID", "Case ID"]
            df = df.set_index(["Appointment ID", "Patient ID", "Case ID"])
        elif path.name == "FAKT_TERMIN_RAUM.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appointment ID", "Room ID", "Room Common Name", "Begin", "End", "Duration in Minutes"]
            df = df.set_index(["Appointment ID", "Room ID", "Begin"])
        elif path.name == "LA_CHOP_FLAT.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Chop Code", "Usage Year", "Chop Description",
                          "Code Level 1", "Code Level 1 Description",
                          "Code Level 2", "Code Level 2 Description",
                          "Code Level 3", "Code Level 3 Description",
                          "Code Level 4", "Code Level 4 Description",
                          "Code Level 5", "Code Level 5 Description",
                          "Code Level 6", "Code Level 6 Description",
                          "Chop Status",
                          "Chop Catalog ID"]

            # Fix encoding error: Replace \x96 with - (\x96 is a dash in latin1: https://stackoverflow.com/a/35731443/4563947)
            df["Chop Description"] = df["Chop Description"].str.replace("\\x96", "-")
            df["Code Level 1 Description"] = df["Code Level 1 Description"].str.replace("\\x96", "-")
            df["Code Level 2 Description"] = df["Code Level 2 Description"].str.replace("\\x96", "-")
            df["Code Level 3 Description"] = df["Code Level 3 Description"].str.replace("\\x96", "-")
            df["Code Level 4 Description"] = df["Code Level 4 Description"].str.replace("\\x96", "-")
            df["Code Level 5 Description"] = df["Code Level 5 Description"].str.replace("\\x96", "-")
            df["Code Level 6 Description"] = df["Code Level 6 Description"].str.replace("\\x96", "-")

            df = df.set_index(["Chop Catalog ID"])

        elif path.name == "LA_ISH_NBEW.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            # the db designer of that source syname was just a troll
            df.columns = ["Case ID", "Serial Number", "Stay Type ID", "Stay Type", "Begin Date", "Begin Time", "Status", "End Date", "End Time", "Serial Reference", "Description",
                          "Department",  # "fachliche Organisationseinheit" (https://help.sap.com/saphelp_ewm70/helpdata/de/40/f39a3916f07e00e10000000a11402f/frameset.htm)
                          "Ward",  #  Nursing Organisational Unit/ "pflegerische Organisationseinheit = Abteilungen/Station"
                          "Organisational Unit of Entry",  # "Aufnahme-Organisationseinheit"
                          "Room ID", "Bed ID", "Is Cancelled", "Partner ID"]

            df["Begin Time"] = df["Begin Time"].str[:8]
            df["End Time"] = df["End Time"].str[:8]

            df["Begin Datetime"] = (df["Begin Date"] + " " + df["Begin Time"])
            df["End Datetime"] = (df["End Date"] + " " + df["End Time"])
            df = df.drop(labels=["Begin Date", "Begin Time", "End Date", "End Time"], axis=1)

            df["Begin Datetime"] = pd.to_datetime(df["Begin Datetime"], format="%Y-%m-%d %H:%M:%S", errors='coerce')
            df["End Datetime"] = pd.to_datetime(df["End Datetime"], format="%Y-%m-%d %H:%M:%S", errors='coerce')
            df = df.set_index(["Serial Number"])

        elif path.name == "LA_ISH_NDIA.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Case ID", "Diagnosis Key 1", "Diagnosis Category 1", "Date of Diagnosis", "DRG Category"] # FALNR,DKEY1,DKAT1,DIADT,DRG_CATEGORY
            df = df.set_index("Case ID", "Diagnosis Key 1")

        elif path.name == "LA_ISH_NDRG.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Case ID", "Cost Weight"]
            df = df.set_index("Case ID")

        elif path.name == "LA_ISH_NFPZ.csv":  # https://www.se80.co.uk/saptables/n/nfpz/nfpz.htm
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["EARZT", "FARZT", "Case ID", "Serial Number", "Partner ID", "Cancelled"] # EARZT,FARZT,FALNR,LFDNR,PERNR,STORN]
            df = df.set_index("Serial Number")

        elif path.name == "LA_ISH_NGPA.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Partner ID", "Name 1", "Name 2", "Name 3", "Country", "Zip Code", "Place of Residence", "Place of Residence", "Street", "Hospital"]
            df = df.set_index("Partner ID")

        elif path.name == "LA_ISH_NICP.csv":  # https://help.sap.com/doc/saphelp_crm60/6.0.0.14/en-US/d9/6f2fcf772644fe8e10cc3c3cca6039/frameset.htm
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Stay ID", "Catalog ID", "Chop Code", "Surgeries Quantity", "Beginning", "Location Surgery Information", "Cancelled", "Case ID", "Ward"]
            df = df.set_index(["Stay ID", "Catalog ID", "Case ID"])

        elif path.name == "TACS_DATEN.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Patient Type", "Patient Status", "Case ID", "Case Type", "Case Status", "Date of Care", "Duration of Care in Mins", "Employee Staff Number", "Employee Employment Number", "Employee Login", "Batch Run ID"]
            df = df.set_index(["Patient ID", "Employee Staff Number", "Date of Care"])

        elif path.name == "V_LA_ISH_NDIA_NORM.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Case ID", "Diagnosis Key 1", "Diagnosis Category 1", "Date of Diagnosis", "DRG Category"]
            df = df.set_index(["Case ID", "Diagnosis Key 1"])

        elif path.name == "V_LA_ISH_NRSF_NORM.csv":  # https://www.se80.co.uk/saptables/n/nrsf/nrsf.htm
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Risk Factor ID", "Description", "Creation Date", "Creation Time"]
            df = df.set_index(["Patient ID", "Risk Factor ID", "Creation Time"])

        elif path.name == "VRE_SCREENING_DATA.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", parse_dates=["Birth Date"], dtype=str)
            df.columns = ['Order ID', 'Record Date', 'Measurement Date', 'Patient Number', 'Last Name',
                          'First Name', 'Birth Date', 'Gender', 'Zip Code', 'Place of Residence',
                          'Canton', 'Patient ID', 'Requester', 'Cost Unit', 'Material Type',
                          'Transport', 'Result', 'Analysis Method', 'Screening Context']
            df = df.set_index(["Order ID"])

            df.loc[df["Gender"] == "M", "Gender"] = "male"
            df.loc[df["Gender"] == "F", "Gender"] = "female"

            # try to fix patient ids from screening data using the patient data
            patient_df = pd.read_csv(interim_data_path + "DIM_PATIENT.csv", encoding="iso-8859-1", parse_dates=["Birth Date"], dtype="str")

            def find_patient_id(row, patient_df):
                if pd.isna(row["Patient ID"]):

                    # try to find the patient id in the patient table via other properties
                    patient_query = None
                    if not pd.isna(row["Birth Date"]):
                        patient_query = patient_df["Birth Date"] == row["Birth Date"]

                    if not pd.isna(row["Gender"]):
                        patient_query = patient_query & patient_df["Gender"] == row["Gender"]

                    if not pd.isna(row["Zip Code"]):
                        patient_query = patient_query & patient_df["Zip Code"] == row["Zip Code"]

                    if patient_query is not None:  # if we found a single match, set patient id
                        patient_row = patient_df[patient_query]
                        if patient_row.shape[0] == 1:
                            row["Patient ID"] = patient_row["Patient ID"].iloc[0]
                return row

            # print(df[df["Patient ID"].isnull()].shape[0])
            df.apply(find_patient_id, args=[patient_df], axis=1)
            # print(df[df["Patient ID"].isnull()].shape[0])

            df = df.drop(labels=["Gender", "Zip Code", "Place of Residence", "Canton", "Patient Number"], axis=1)
        else:
            print(f"No fix proposed for {each_file}")
            continue

        df.to_csv(interim_data_path + path.name)


if __name__ == '__main__':
    cleanup_dataset()
