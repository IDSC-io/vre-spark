import pathlib
import pandas as pd
import os

from configuration.basic_configuration import configuration


def merge_data():
    raw_data_path = configuration['PATHS']['raw_data_dir'].format("test") if configuration['PARAMETERS']['dataset'] == 'test' \
        else configuration['PATHS']['raw_data_dir'].format("model")  # absolute or relative path to directory where data is stored

    # VRE screening data
    df1 = pd.read_csv(raw_data_path + "2018_10_18_sdvre_tot_28_10_18.csv", delimiter=";", encoding="cp850", dtype=str)
    df2 = pd.read_csv(raw_data_path + "2018_10_18_sdvre_tot_28_10_18_2.csv", encoding="ISO-8859-1", dtype=str)
    df3 = pd.read_excel(raw_data_path + "2020_08_24_VREweekly_200824-0926.xlsx", dtype=str)
    df4 = pd.read_excel(raw_data_path + "Liste_VRE_200729.xlsx", dtype=str)

    df1.columns = ["Order ID", "Record Date", "Measurement Date", "Patient Number", "Last Name", "First Name", "Birth Date", "Age", "Gender", "Zip Code", "Place of Residence", "Canton", "Country", "Patient ID", "Case ID", "Requester", "Cost Unit", "Material Type", "Transport", "Culture Germ1", "Culture Germ2"]
    df1 = df1.drop(labels=["Age", "Country", "Case ID"], axis=1)
    df2.columns = ["Order ID", "Record Date", "Measurement Date", "First Name", "Last Name", "Birth Date", "Patient ID", "Requester", "Cost Unit", "Material Type", "Transport", "Result", "Analysis Method", "Screening Context"]
    df3.columns = ["Order ID", "Record Date", "Measurement Date", "Patient Number", "Last Name", "First Name", "Birth Date", "Requester", "Cost Unit", "Material Type", "Transport", "Culture Germ1", "Culture Germ2", "Index", "Result PCR",  "vanA C(t)", "vanA Qual", "vanB C(t)", "vanB Qual", "E.faecium C(t)", "E.faecium Qual"]
    df4.columns = ["Record Date", "Measurement Date", "Screening Context", "Patient ID", "Last Name", "First Name", "Birth Date", "Gender", "Hospital", "Ward", "Clinic", "Type of Resistency", "MLS", "Infection", "Explanation", "Clearance", "Encoding"]

    df4["Result"] = df4["Type of Resistency"]
    df4.loc[df4["Gender"] == "M", "Gender"] = "male"
    df4.loc[df4["Gender"] == "F", "Gender"] = "female"

    df_combined = df1.append([df2, df3, df4])

    df_combined = df_combined.set_index(["Order ID"])

    df_sorted = df_combined.sort_index()

    def set_missing_values(row):
        if pd.isna(row["Result"]):
            culture_germ1 = row["Culture Germ1"]
            culture_germ2 = row["Culture Germ2"]
            result_pcr = row["Result PCR"]
            if result_pcr != "nn" and not pd.isna(result_pcr):
                row["Result"] = result_pcr
                row["Analysis Method"] = "PCR"
            elif culture_germ1 != "nn" and not pd.isna(culture_germ1):
                row["Result"] = culture_germ1
                row["Analysis Method"] = "Kolonie"
            elif culture_germ2 != "nn" and not pd.isna(culture_germ2):
                row["Result"] = culture_germ2
                row["Analysis Method"] = "Kolonie"
            elif not pd.isna(result_pcr):
                row["Result"] = result_pcr
                row["Analysis Method"] = "PCR"
            elif not pd.isna(culture_germ1):
                row["Result"] = culture_germ1
                row["Analysis Method"] = "Kolonie"
            elif not pd.isna(culture_germ2):
                row["Result"] = culture_germ2
                row["Analysis Method"] = "Kolonie"
            else:
                row["Result"] = "nn"
                row["Analysis Method"] = ""

        return row

    df_sorted.apply(set_missing_values, axis=1)

    df_dropped = df_sorted.drop(labels=["Result PCR", "Index", "vanA C(t)", "vanA Qual", "vanB C(t)", "vanB Qual", "E.faecium C(t)", "E.faecium Qual", "Culture Germ1",
                                        "Culture Germ2", "Hospital", "Ward", "Clinic", "Type of Resistency", "MLS", "Infection", "Explanation", "Clearance", "Encoding"], axis=1)

    print(df_dropped.columns)
    df_dropped.to_csv(raw_data_path + "VRE_SCREENING_DATA.csv")


if __name__ == '__main__':
    merge_data()
