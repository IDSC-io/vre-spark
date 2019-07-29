from datetime import datetime
import logging


class Medication:
    def __init__(
        self,
        patient_id,
        case_id,
        drug_text,
        drug_atc,
        drug_quantity,
        drug_unit,
        drug_dispform,
        drug_submission,
    ):
        self.patient_id = patient_id
        self.case_id = case_id
        self.drug_text = drug_text
        self.drug_atc = drug_atc
        self.drug_quantity = drug_quantity
        self.drug_unit = drug_unit
        self.drug_dispform = drug_dispform
        self.drug_submission = datetime.strptime(
            drug_submission, "%Y-%m-%d %H:%M:%S"
        )

    def is_antibiotic(self):
        """
        Does the ATC code start with J01?
        :return: boolean
        """
        return self.drug_atc.startswith("J01")

    def create_drug_map(lines):
        """
        Creates a dictionary of ATC codes to human readable drug names based on the entries in the CSV file.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_LA_IPD_DRUG_NORM
        ["PATIENTID",   "CASEID",       "DRUG_TEXT",                                "DRUG_ATC", "DRUG_QUANTITY",    "DRUG_UNIT",    "DRUG_DISPFORM",    "DRUG_SUBMISSION"]
        ["00001711342", "0006437617",   "Torem Tabl 10 mg (Torasemid)",             "C03CA04",  "2.0000000000000",  "Stk",          "p.o.",             "2018-03-24 09:52:28.0000000"]
        ["00001711342", "0006437617",   "Ecofenac Sandoz Lipogel 1 % (Diclofenac)", "M02AA15",  "1.0000000000000",  "Dos",          "lokal / topisch",  "2018-03-24 09:52:28.0000000"]

        Returns: A dictionary mapping drug codes to their respective text description --> {'B02BA01' : 'NaCl Braun Inf LÃ¶s 0.9 % 500 ml (Natriumchlorid)', ... }
        """
        logging.debug("create_drug_map")
        drugs = dict()
        for line in lines:
            if drugs.get(line[3], None) is None:
                drugs[line[3]] = line[2]
        logging.info(f"{len(drugs)} drugs created")
        return drugs

    def add_medication_to_case(lines, cases):
        """
        Read all the medication entries from the V_LA_IPD_DRUG_NORM table, and adds the drug (Medication() object) to the corresponding Case() (in cases) via Case.add_medication().
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object).
        For a schema of the V_LA_IPD_DRUG_NORM table, see the function create_drug_map() above.

        :param cases: Dictionary mapping case ids to Case()       --> {'0005976205' : Case(), ... }
        """
        logging.debug("add_medication_to_case")
        nr_cases_not_found = 0
        nr_malformed = 0
        nr_ok = 0
        for line in lines:
            if len(line) != 8:
                nr_malformed += 1
                continue
            medication = Medication(*line)
            if cases.get(medication.case_id, None) is None:
                nr_cases_not_found += 1
                continue
            cases.get(medication.case_id).add_medication(medication)
            nr_ok += 1
        logging.info(f"{nr_ok} ok, {nr_cases_not_found} cases not found, {nr_malformed} malformed")
