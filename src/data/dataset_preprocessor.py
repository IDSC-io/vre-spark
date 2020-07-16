import pathlib
import pandas as pd
import os

from configuration.basic_configuration import configuration


def cleanup_dataset():
    """
    Remove all the horribleness from the dataset.

    - Column names are weirdly shortened and partly english and german.
    - States of everything are horrible string.
    :return:
    """
    CSV_DIR = configuration['PATHS']['test_data_dir'] if configuration['PARAMETERS']['dataset'] == 'test' \
        else configuration['PATHS']['model_data_dir']  # absolute or relative path to directory where data is stored

    csv_files = [each_file for each_file in os.listdir(CSV_DIR) if each_file.endswith('.csv')]

    for each_file in csv_files:
        print(f"--> Fixing modelling in file {each_file} ...")

        path = pathlib.Path(CSV_DIR + "/" + each_file)

        if path.name == "DIM_FALL.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Case ID", "Case Type ID", "Case Status", "Case Type", "Start Date", "End Date",
                          "Patient Type", "Patient Status"]

            df.loc[df["Case Status"] == "Fall ist abgeschlossen", "Case Status"] = "closed"
            df.loc[df["Case Status"] == "Fall ist aktuell", "Case Status"] = "open"

            df.loc[df["Case Type"] == "ambulanter Fall", "Case Type"] = "ambulatory"
            df.loc[df["Case Type"] == "stationärer Fall", "Case Type"] = "in-patient"

            df.loc[df["Patient Status"] == "aktiv", "Patient Status"] = "active"
            df.loc[df["Patient Status"] == "storniert", "Patient Status"] = "cancelled"

        elif path.name == "DIM_GERAET.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Device ID", "Device Name"]

        elif path.name == "DIM_PATIENT.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Gender", "Birth Date", "Zip Code", "Place of Residence", "Canton", "Language"]
            df.loc[df["Gender"] == "männlich", "Gender"] = "male"
            df.loc[df["Gender"] == "weiblich", "Gender"] = "female"
        elif path.name == "DIM_RAUM.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Room ID", "Room Common Name"]
        elif path.name == "DIM_TERMIN.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appoinment ID", "Deleted On Source", "Description", "Type", "Type 2", "Date", "Duration in Minutes"]
        elif path.name == "FAKT_MEDIKAMENT.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Case ID", "Drug Name",
                          "Drug ATC ID",  # Anatomical Therapeutic Chemical Classification Syname ID
                          "Quantity",
                          "Unit",
                          "Disposition Form",
                          "Submission Date"]
        elif path.name == "FAKT_TERMIN_GERAET.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appoinment ID", "Device ID", "Begin", "End", "Duration in Minutes"]
        elif path.name == "FAKT_TERMIN_MITARBEITER.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appoinment ID", "Employee ID", "Begin", "End", "Duration in Minutes"]
        elif path.name == "FAKT_TERMIN_PATIENT.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appointment ID", "Patient ID", "Case ID"]
        elif path.name == "FAKT_TERMIN_RAUM.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Appoinment ID", "Room ID", "Room Common Name", "Begin", "End", "Duration in Minutes"]
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

        elif path.name == "LA_ISH_NBEW.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            # the db designer of that source syname was just a troll
            df.columns = ["Case ID", "Serial Number", "Movement Type ID", "Movement Type", "Begin Date", "Begin Time", "Status", "End Date", "End Time", "Serial Reference", "Description",
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

        elif path.name == "LA_ISH_NDIA.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Case ID", "Diagnosis Key 1", "Diagnosis Category 1", "Date of Diagnosis", "DRG Category"] # FALNR,DKEY1,DKAT1,DIADT,DRG_CATEGORY
        elif path.name == "LA_ISH_NDRG.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Case ID", "Cost Weight"]
        elif path.name == "LA_ISH_NFPZ.csv":  # https://www.se80.co.uk/saptables/n/nfpz/nfpz.htm
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["EARZT", "FARZT", "Case ID", "Serial Number", "Partner ID", "Cancelled"] # EARZT,FARZT,FALNR,LFDNR,PERNR,STORN]
        elif path.name == "LA_ISH_NGPA.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Partner ID", "Name 1", "Name 2", "Name 3", "Country", "Zip Code", "Place of Residence", "Place of Residence", "Street", "Hospital"]
        elif path.name == "LA_ISH_NICP.csv":  # https://help.sap.com/doc/saphelp_crm60/6.0.0.14/en-US/d9/6f2fcf772644fe8e10cc3c3cca6039/frameset.htm
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Movement ID", "Catalog ID", "Chop Code", "Surgeries Quantity", "Beginning", "Location Surgery Information", "Cancelled", "Case ID", "Ward"]
        elif path.name == "TACS_DATEN.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Patient Type", "Patient Status", "Case ID", "Case Type", "Case Status", "Date of Care", "Duration of Care in Mins", "Employee staff Number", "Employee Employment Number", "Employee Login", "Batch Run ID"]
        elif path.name == "V_LA_ISH_NDIA_NORM.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Case ID", "Diagnosis Key 1", "Diagnosis Category 1", "Date of Diagnosis", "DRG Category"]

        elif path.name == "V_LA_ISH_NRSF_NORM.csv":  # https://www.se80.co.uk/saptables/n/nrsf/nrsf.htm
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Patient ID", "Risk Factor ID", "Description", "Creation Date", "Creation Time"]

        elif path.name == "VRE_SCREENING_DATA.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Order ID", "Record Date", "Measurement Date", "First Name", "Last Name", "Birthdate", "Patient ID", "Requester", "Cost Unit", "Material Type", "Transport", "Result", "Analysis Method", "Screening Context"]

        else:
            print(f"No fix proposed for {each_file}")
            continue

        interim_path = "./data/interim/"
        df.to_csv(interim_path + path.name)


if __name__ == '__main__':
    cleanup_dataset()
