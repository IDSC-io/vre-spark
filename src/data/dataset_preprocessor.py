import pathlib
import pandas as pd
import os
import re

import requests

from configuration.basic_configuration import configuration


def cleanup_dataset(overwrite_files=False):
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

        if not overwrite_files and pathlib.Path(interim_data_path + path.name).exists():
            print(f"Skip cleanup as file exists")
            continue

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
                          "SAP Room ID", "Bed ID", "Is Cancelled", "Partner ID"]

            df["Begin Time"] = df["Begin Time"].str[:8]
            df["End Time"] = df["End Time"].str[:8]

            df["Begin Datetime"] = (df["Begin Date"] + " " + df["Begin Time"])
            df["End Datetime"] = (df["End Date"] + " " + df["End Time"])
            df = df.drop(labels=["Begin Date", "Begin Time", "End Date", "End Time"], axis=1)

            df["Begin Datetime"] = pd.to_datetime(df["Begin Datetime"], format="%Y-%m-%d %H:%M:%S", errors='coerce')
            df["End Datetime"] = pd.to_datetime(df["End Datetime"], format="%Y-%m-%d %H:%M:%S", errors='coerce')
            df = df.set_index(["Serial Number"])

            sap_nbew_rooms_df = df[["Department", "Ward", "SAP Room ID"]] #, "Bed ID"]]

            sap_nbew_rooms_df = sap_nbew_rooms_df.groupby(sap_nbew_rooms_df.columns.tolist()).size().reset_index().rename(columns={0: 'count'})

            sap_nbew_rooms_df = sap_nbew_rooms_df.fillna(value={"Room ID": "-"})

            # sap_nbew_rooms_df.rename(columns={"Room ID": "SAP Room ID"}, inplace=True)

            # sap_nbew_rooms_df = sap_nbew_rooms_df.sort_values(by="count", ascending=False)

            def extract_waveware_ids(row):
                """
                Extract waveware tokens from SAP Room IDs for Insel Hospital rooms.
                """
                row['SAP Building Abbreviation'] = pd.NA
                row['Waveware Floor ID'] = pd.NA
                row['Waveware Room ID'] = pd.NA

                if not pd.isna(row["SAP Room ID"]) and not row["SAP Room ID"].isdecimal():
                    pattern = '([A-Za-z]{2,3}[0-9]*)[\\.\\s]+([A-Za-z]*[0-9]*)[\\.N\\s-]+([0-9]+[A-Za-z]*)$'
                    match = re.search(pattern, row['SAP Room ID'])

                    if match is not None:
                        # if extraction was successful, read metric df and compose tuple
                        row['SAP Building Abbreviation'], row['Waveware Floor ID'], row['Waveware Room ID'] = match.groups()
                    else:
                        pattern = '([A-Za-z]+)[\\.\\s]*([0-9]*)[\\.N\\s-]+([0-9]+[A-Za-z]*)$'
                        match = re.search(pattern, row['SAP Room ID'])

                        if match is not None:
                            # if extraction was successful, read metric df and compose tuple
                            row['SAP Building Abbreviation'], row['Waveware Floor ID'], row['Waveware Room ID'] = match.groups()
                        else:
                            pattern = '([A-Za-z]+[0-9]*)[\\s]+([A-Za-z]*)[\\.N\\s-]*([0-9]+[A-Za-z]*)'
                            match = re.search(pattern, row['SAP Room ID'])

                            if match is not None:
                                # if extraction was successful, read metric df and compose tuple
                                row['SAP Building Abbreviation'], row['Waveware Floor ID'], row['Waveware Room ID'] = match.groups()

                return pd.Series({'SAP Building Abbreviation': row['SAP Building Abbreviation'], 'Waveware Floor ID': row['Waveware Floor ID'], 'Waveware Room ID': row['Waveware Room ID']})

            sap_nbew_rooms_df = pd.concat([sap_nbew_rooms_df, sap_nbew_rooms_df.apply(extract_waveware_ids, axis=1)], axis=1)

            df = pd.merge(df, sap_nbew_rooms_df[["SAP Room ID", "SAP Building Abbreviation", "Waveware Floor ID", "Waveware Room ID"]], on="SAP Room ID", how="left")
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
        elif path.name == "SAP_ISH_ZHC_RB_STANDORT":  # Raumbuch Standort Data (apparently Waveware)
            df = pd.read_csv(path, dtype=str)
            df.drop(["MANDT", "ERDAT", "ERNAM", "AEDAT", "AENAM", "BATCH_RUN_ID"], axis=1, inplace=True)
            df.columns = ["Waveware Campus", "Common Name"]
        elif path.name == "SAP_ISH_ZHC_RB_BUILDING":
            df = pd.read_csv(path, dtype=str)
            df.drop(["MANDT", "ERDAT", "ERNAM", "AEDAT", "AENAM", "BATCH_RUN_ID"], axis=1, inplace=True)
            df.columns = ["Waveware Campus", "Waveware Building ID", "Building Common Name"]
            df.set_index("Waveware Building ID", inplace=True)
        elif path.name == "SAP_ISH_ZHC_RB_STOCKWERK":
            df = pd.read_csv(path, dtype=str)
            df.drop(["MANDT", "ERDAT", "ERNAM", "AEDAT", "AENAM", "BATCH_RUN_ID"], axis=1, inplace=True)
            df.columns = ["Waveware Campus", "Waveware Building ID", "Waveware Floor ID", "Floor Common Name"]
            # df = pd.merge(df, sap_building_df, on="Waveware Building ID")
        elif path.name == "SAP_ISH_ZHC_RB_RAUM":
            df = pd.read_csv(path, dtype=str)
            df.drop(["MANDT", "ERDAT", "ERNAM", "AEDAT", "AENAM", "BATCH_RUN_ID"], axis=1, inplace=True)
            df.columns = ["Waveware Campus", "Waveware Building ID", "Waveware Floor ID", "Waveware Room ID", "Room Common Name", "Waveware Room Full ID"]
            df.set_index("Waveware Room Full ID", inplace=True)
            # df = pd.merge(df, sap_building_df, on="Waveware Building ID")
        elif path.name == "LA_ISH_NBAU.csv":
            df = pd.read_csv(path, dtype=str)
            # SAP klingon translation: https://www.tcodesearch.com/sap-tables/detail?id=NBAU
            df.drop(["MANDT", "TELNR", "TELFX", "TELTX", "LOEKZ", "LOUSR", "LODAT",
                                       "ERDAT", "ERUSR", "UPDAT", "UPUSR", "BEGDT", "ENDDT", "FREIG",
                                       "TALST", "ADDIN","XKOOR", "YKOOR", "BREIT", "LAENG", "ARCHV",
                                       "MIGRATED_OBJID", "BATCH_RUN_ID", "ZZBEMK", "ZZVERLEGUNG", "ZZVORHALTE",
                                       "ZZPRIVAT", "EANNR", "BETTST_TYP"], axis=1, inplace=True)

            df.columns = ["SAP Room ID", "Unit Type", "Unit Name", "SAP Room ID 1", "SAP Room ID 2",
                                            "Short Text", "Long Text", "Address Information", "Address Object",
                                            "Waveware Campus", "Waveware Building ID", "Waveware Floor ID", "Waveware Room ID"]

            df.loc[df["Unit Type"] == "Z", "Unit Type"] = "Room"
            df.loc[df["Unit Type"] == "B", "Unit Type"] = "Bettstellplatz"
            df = df[df["Unit Type"] == "Room"]

            df['SAP Building Abbreviation 1'] = pd.NA
            df['SAP Building Abbreviation 2'] = pd.NA

            def extract_campi(row):
                """
                Extract the campus of the building and room based on hints in the Unit Name.
                """
                if pd.isna(row['Waveware Campus']):
                    if row['Unit Name'].find("Aarberg") != -1:
                        row['Waveware Campus'] = 'AARB'
                    if row['Unit Name'].find("Riggisberg") != -1:
                        row['Waveware Campus'] = 'RIGG'
                    if row['Unit Name'].find("R_") != -1:
                        row['Waveware Campus'] = 'RIGG'
                    if row['Unit Name'].find("Tiefenau") != -1:
                        row['Waveware Campus'] = 'TIEF'
                    if row['Unit Name'].find("Münsigen") != -1:
                        row['Waveware Campus'] = 'MUEN'
                    if row['Unit Name'].find("Belp") != -1:
                        row['Waveware Campus'] = 'BELP'

                return row

            def extract_ids(row):
                """
                Extract waveware tokens from SAP Room IDs for Insel Hospital rooms.
                """
                if row['Waveware Campus'] not in ['Aarberg', 'Riggisberg', 'Tiefenau', 'Münsigen'] and not pd.isna(row['SAP Room ID 2']):
                    if pd.isna(row["Waveware Room ID"]):
                        pattern = '([A-Za-z]+[0-9]*)[\s]+([A-Za-z]*[0-9]*)[\.N\s-]+([0-9]+[A-Za-z]*)'
                        match = re.search(pattern, row['SAP Room ID 2'])
                        if match is not None:
                            # if extraction was successful, read metric df and compose tuple
                            row['Waveware Building ID'], row['Waveware Floor ID'], row['Waveware Room ID'] = match.groups()
                return row

            def extract_waveware_ids(row):
                """
                Run all above methods on the same row.
                """
                row = extract_campi(row)
                row = extract_ids(row)

                return pd.Series({'Waveware Campus': row['Waveware Campus'], 'SAP Building Abbreviation 2': row['SAP Building Abbreviation 2'], 'Waveware Building ID': row['Waveware Building ID'], 'Waveware Floor ID': row['Waveware Floor ID'], 'Waveware Room ID': row['Waveware Room ID']})

            df = pd.concat([df.drop(["Waveware Campus", "Waveware Building ID", "SAP Building Abbreviation 2", "Waveware Floor ID", "Waveware Room ID"], axis=1), df.apply(extract_waveware_ids, axis=1)], axis=1)

            def fix_SAP_ID_1(row):
                """
                Fix SAP ID 1 for Aarberg or unknown rooms.
                """
                if re.search('[0-9]+', row["SAP Room ID"]) is not None and (row["Waveware Campus"] != "ISB" or pd.isna(row["Waveware Campus"])):
                    row["SAP Room ID 1"] = row['SAP Room ID']

                return pd.Series({'SAP Room ID 1': row["SAP Room ID 1"]})

            # fix the Aarberg SAP Room IDs to make it possible to identify them in the SAP NBEW table data
            df = pd.concat([df.drop(["SAP Room ID 1"], axis=1), df.apply(fix_SAP_ID_1, axis=1)], axis=1)

            def extract_sap_1_ids(row):
                """
                Extract SAP abbreviations from SAP Room IDs for Insel Hospital rooms.
                """
                if row['Waveware Campus'] not in ['Aarberg', 'Riggisberg', 'Tiefenau', 'Münsigen'] and not pd.isna(row['SAP Room ID 1']):
                    pattern = '([A-Za-z]+[0-9]*)[\s]+'
                    match = re.search(pattern, row['SAP Room ID 1'])
                    if match is not None:
                        # if extraction was successful, read metric df and compose tuple
                        row['SAP Building Abbreviation 1'] = match.groups()[0]
                return pd.Series({'SAP Building Abbreviation 1': row['SAP Building Abbreviation 1']})

            # extract building abbreviation 1 from SAP Room ID 1
            df = pd.concat([df.drop(['SAP Building Abbreviation 1'], axis=1), df.apply(extract_sap_1_ids, axis=1)], axis=1)

            def extract_sap_2_ids(row):
                """
                Extract SAP abbreviations from SAP Room IDs for Insel Hospital rooms.
                """
                if row['Waveware Campus'] not in ['Aarberg', 'Riggisberg', 'Tiefenau', 'Münsigen'] and not pd.isna(row['SAP Room ID 1']):
                    pattern = '([A-Za-z]+[0-9]*)[\s]+'
                    match = re.search(pattern, row['SAP Room ID 2'])
                    if match is not None:
                        # if extraction was successful, read metric df and compose tuple
                        row['SAP Building Abbreviation 2'] = match.groups()[0]
                return pd.Series({'SAP Building Abbreviation 2': row['SAP Building Abbreviation 2']})

            # extract building abbreviation 1 from SAP Room ID 1
            df = pd.concat([df.drop(['SAP Building Abbreviation 2'], axis=1), df.apply(extract_sap_2_ids, axis=1)], axis=1)
        elif path.name == "Waveware_Auszug Gebaeudeinformation Stand 03.12.2020.csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df = df.drop(["Standort", "Parzellennummer", "Zonenplan", "Denkmalpflege", "Anlage-ID", "Bemerkung", "Eigentümer (SAP)", "Vermietung (SAP)", "Portfolio (SAP)", "Baujahr", "Gebäudetyp", "GVB-Nummer", "Amtlicher Wert", "Gebäudeversicherungswert", "Gebäudezustand", "Technologiestand HLKSE", "Techn. Ausb.standard", "Zustand Technik", "Klimatisierung", "Aufzug", "Gebäudezustand Bem.", "Status"], axis=1)
            df.columns = ["Waveware Building Full ID", "Building Code", "Waveware Building ID", "Building abbreviation", "Building Common Name", "Street", "Zip Code", "Location", "SAP-Anlage Nr."]
            df.drop(["Zip Code", "Location","SAP-Anlage Nr.", "Building Code"], axis=1, inplace=True)
            df = df[df["Building Common Name"] != "Grundstück Inselareal"]
            df = df[~pd.isna(df["Building abbreviation"])]
            # df.sort_values(by=["Building abbreviation"], inplace=True)
            # df.set_index("Waveware Building ID", inplace=True)
        elif path.name == "Waveware_Auszug Flaechenmanagement IDSC (Stand 02.07.20).csv":
            df = pd.read_csv(path, encoding="ISO-8859-1", dtype=str)
            df.columns = ["Waveware Building ID", "Building Common Name", "Waveware Floor ID", "Waveware Room ID", "Waveware Room Full ID", "Room Common Name", "Room Area", "PC Group ID", "Sub-EC(PC) Nr", "Profitcenter"]
            df = df.drop(["Room Area", "PC Group ID", "Sub-EC(PC) Nr", "Profitcenter"], axis=1)
            # df.set_index("Waveware Room Full ID", inplace=True)
        else:
            print(f"No fix proposed for {each_file}")
            continue

        df.to_csv(interim_data_path + path.name)


def improve_dataset():
    interim_data_path = configuration['PATHS']['interim_data_dir'].format("test") if configuration['PARAMETERS']['dataset'] == 'test' \
        else configuration['PATHS']['interim_data_dir'].format("model")  # absolute or relative path to directory where data is stored

    sap_building_unit_fix_df = pd.read_csv(interim_data_path + "LA_ISH_NBAU.csv", dtype=str)

    # load and prepare the stays dataframe
    sap_nbew_rooms_df = pd.read_csv(interim_data_path + "LA_ISH_NBEW.csv", dtype=str)
    sap_nbew_rooms_df = sap_nbew_rooms_df[["Department", "Ward", "SAP Building Abbreviation", "Waveware Floor ID", "SAP Room ID", "Waveware Room ID"]] #, "Bed ID"]]

    # get usage statistics of rooms
    sap_nbew_rooms_usage_count_df = sap_nbew_rooms_df.groupby(sap_nbew_rooms_df.columns.tolist()).size().reset_index().rename(columns={0: 'count'})

    nbew_sap_merge = pd.merge(sap_building_unit_fix_df, sap_nbew_rooms_usage_count_df, how="outer", left_on="SAP Room ID 1", right_on="SAP Room ID", suffixes=("_x", ""), indicator=True)
    nbew_sap_merge_both = nbew_sap_merge[nbew_sap_merge["_merge"] == "both"] #[["count", "SAP Room ID", "Unit Name","Waveware Campus", "Waveware Building ID", "Waveware Floor ID", "Waveware Room ID", "SAP Room ID 1", "SAP Room ID 2", "Department", "Ward"]]

    room_identifiers_df = nbew_sap_merge_both[["Waveware Campus", "Waveware Building ID", "Unit Name", "Unit Type", "SAP Building Abbreviation 1", "SAP Building Abbreviation 2", "Department", "Ward", "SAP Room ID 1", "SAP Room ID 2", "Waveware Floor ID", "Waveware Room ID"]]

    waveware_buildings_df = pd.read_csv(interim_data_path + "Waveware_Auszug Gebaeudeinformation Stand 03.12.2020.csv", dtype=str)
    building_abbreviation_df = pd.merge(waveware_buildings_df[["Building Common Name", "Waveware Building ID", "Building abbreviation", "Waveware Building Full ID"]], sap_building_unit_fix_df[["SAP Building Abbreviation 1", "SAP Building Abbreviation 2", "Waveware Building ID"]], how="outer", on="Waveware Building ID", indicator=True).drop_duplicates().sort_values(by="Waveware Building ID")
    building_abbreviation_df = building_abbreviation_df[building_abbreviation_df["_merge"] != "right_only"].drop(["_merge"],axis=1)
    building_abbreviation_melt_df = pd.melt(building_abbreviation_df, id_vars=['Building Common Name', "Waveware Building ID", "Waveware Building Full ID"], value_vars=['Building abbreviation', 'SAP Building Abbreviation 1','SAP Building Abbreviation 2'])
    building_abbreviation_melt_df = building_abbreviation_melt_df.drop(["variable"], axis=1).drop_duplicates().sort_values(by="Waveware Building ID")
    building_abbreviation_df = building_abbreviation_melt_df[building_abbreviation_melt_df['value'].notna()].rename(columns={"value": "Building Abbreviation"})
    building_abbreviation_df["Waveware Campus"] = "ISB"

    # find remaining rooms in Waveware by skipping the SAP building unit table
    nbew_sap_merge_fails = nbew_sap_merge[nbew_sap_merge["_merge"] == "right_only"]
    nbew_sap_merge_fails_show = nbew_sap_merge_fails[["_merge", "count", "SAP Room ID", "Department", "Ward", "SAP Building Abbreviation", "Waveware Floor ID", "Waveware Room ID"]]
    # nbew_sap_merge_fails_show.sort_values(by="count", ascending=False)

    merge_fails = pd.merge(nbew_sap_merge_fails_show.drop(["_merge"], axis=1), building_abbreviation_df, how="outer", left_on="SAP Building Abbreviation", right_on="Building Abbreviation", indicator=True)
    merge_fails = merge_fails[merge_fails["_merge"] == "both"] # .sort_values(by="count", ascending=False)
    # TODO: 13 rooms not matchable in order of importance (LH, P1, P2, LHG)
    # merge_fails[merge_fails["_merge"] == 'left_only']

    mergable = merge_fails[["Waveware Campus", "Waveware Building ID", "SAP Building Abbreviation", "Department", "Ward", "SAP Room ID", "Waveware Floor ID", "Waveware Room ID"]]
    mergable.rename(columns={"SAP Room ID": "SAP Room ID 1", "SAP Building Abbreviation": "SAP Building Abbreviation 1"}, inplace=True)
    mergable["SAP Building Abbreviation 2"] = mergable["SAP Building Abbreviation 1"]
    mergable["SAP Room ID 2"] = mergable["SAP Room ID 1"]
    room_identifiers_df = pd.concat([room_identifiers_df, mergable])
    room_identifiers_df.to_csv(interim_data_path + "room_identifiers.csv")

    # floor_identifiers_df = room_identifiers_df[["Waveware Campus", "Waveware Building ID", "SAP Building Abbreviation 1", "SAP Building Abbreviation 2", "Waveware Floor ID"]]
    # floor_identifiers_df = floor_identifiers_df.drop_duplicates()
    # floor_identifiers_df.to_csv(interim_data_path + "floor_identifiers.csv")

    building_identifiers_df = room_identifiers_df[["Waveware Campus", "Waveware Building ID", "SAP Building Abbreviation 1", "SAP Building Abbreviation 2"]]
    building_identifiers_df = building_identifiers_df.drop_duplicates()

    def get_long_lat(street_string):
        response = requests.get(f"https://nominatim.openstreetmap.org/search?q={street_string.replace(' ', '+')}+Bern&format=json")
        types = []
        for loc in response.json():
            types.append(loc["type"] + ": " + loc["display_name"][:15])
            if loc["type"] in ["hospital", "childcare", "clinic"]:
                id_string = loc["type"] + ": " + loc["display_name"][:15]
                long_lat = (loc["lon"], loc["lat"])
                return pd.Series({'Type': id_string, 'Long/Lat': long_lat})

        id_string = response.json()[0]["type"] + ": " + response.json()[0]["display_name"][:15]
        long_lat = (response.json()[0]["lon"], response.json()[0]["lat"])
        return pd.Series({'Type': id_string, 'Long/Lat': long_lat})

    waveware_buildings_coords_df = pd.concat([waveware_buildings_df, waveware_buildings_df["Street"].apply(lambda s: get_long_lat(s))], axis=1)
    waveware_buildings_coords_df["Longitude"] = waveware_buildings_coords_df["Long/Lat"].apply(lambda ll: float(ll[0]))
    waveware_buildings_coords_df["Latitude"] = waveware_buildings_coords_df["Long/Lat"].apply(lambda ll: float(ll[1]))
    waveware_buildings_coords_df.drop(["Long/Lat"], axis=1, inplace=True)
    # TODO:several buildings are gone here (PH7, HausX)
    building_identifiers_df = pd.merge(building_identifiers_df, waveware_buildings_coords_df, on="Waveware Building ID")
    building_identifiers_df.drop(["Building abbreviation", "Type", "Unnamed: 0"], axis=1, inplace=True)
    building_identifiers_df.to_csv(interim_data_path + "building_identifiers.csv")


if __name__ == '__main__':
    cleanup_dataset()
