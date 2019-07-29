from datetime import datetime
import logging


class Risk:
    def __init__(self, auftrag_nbr, erfassung, entnahme, pat_nr, sex, PID, fall_nr, auftraggeber, kostenstelle,
                 material_type, transport, resultat, vreih, analyse_methode, screening_context, ward, options):
        """Initiates a Risk (i.e. Screening) object.
                - Auftrag_Nummer
        - Erfassung
        - Entnahme
        - Patient_Number
        - Geschlecht
        - PID
        - Fall_Nummer
        - Auftraggeber
        - Kostenstelle
        - Material_Typ
        - Transport
        - Resultat
        - vreih
        - Analyse_Methode
        - Screening_Context
        - Ward
        - Options
        :param patient_id:
        :param screen_code:
        :param entry_date:
        :param context:
        """
        self.auftrag_nbr = auftrag_nbr
        self.erfassung = datetime.strptime(erfassung, '%d.%m.%Y').date() if erfassung != '' else None
        self.entnahme = datetime.strptime(entnahme, '%d.%m.%Y').date() if entnahme != '' else None
        self.patient_nbr = pat_nr
        self.patient_sex = sex
        # Important note: adjust PID to have 11 digits, as it corresponds to [Patient_Number] + [Prüfziffer]
        #   --> used for matching to patient IDs in the VRE model !
        self.pid = ''.join(['0' for i in range(0, 11-len(PID))] + [PID])
        self.fall_nbr = fall_nr
        self.auftraggeber = auftraggeber
        self.kostenstelle = kostenstelle
        self.material_type = material_type
        self.transport = transport
        self.result = resultat
        self.vreih = vreih
        self.analysis_method = analyse_methode
        self.screening_context = screening_context
        self.ward = ward
        self.options = options

    def old_init(self, patient_id, screen_code, entry_date, context):
        print('hi') # old __init function

    def NOLONGERUSED_add_risk_to_patient(lines, patients):
        """Reads the risk csv, create Risk objects from the rows and adds these to the respective patients.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_LA_ISH_NRSF_NORM
        ["PATIENTID",   "RSFNR",    "KZTXT",                            "ERDAT",        "ERTIM"]
        ["00004887743", "000042",   "Screening auf VRE. Spezielle L",   "2018-08-23",   "10:25:30"]
        ["00004963016", "000042",   "Screening auf VRE. Spezielle L",   "2018-05-09",   "15:48:48"]

        :param patients: Dictionary mapping patient ids to Patient() --> {'00008301433' : Patient(), ... }
        """
        logging.debug("add_risk_to_patient")
        nr_not_found = 0
        nr_ok = 0
        for line in lines:
            risk = Risk(*line)
            if patients.get(risk.patient_id, None) is not None:
                patients[risk.patient_id].add_risk(risk)
            else:
                nr_not_found += 1
                continue
            nr_ok += 1
        logging.info(f"{nr_ok} risks added, {nr_not_found} patients not found for risk")

    def NOLONGERUSED_add_deleted_risk_to_patient(lines, patients):
        '''
        Read the deleted risk csv and creates Risk objects with code '000142' for deleted VRE screening from the rows.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:

        >> TABLE NAME: deleted_screenings
        ["VNAME",       "NNAME",    "PATIENTID",    "GBDAT",        "ScreeningDate"]
        ["Margarete",   "Bucher",   "00014616742",  "1963-11-11",   "2018-03-12"]
        ["Edouard",     "Kurth",    "00014533820",  "1954-02-15",   "2018-02-16"]

        Discard entries which are missing the date of screening.

        :param patients: Dictionary mapping patient ids to Patient() --> {'00008301433' : Patient(), ... }
        '''
        logging.debug("add_deleted_risk_to_patient")
        nr_not_found = 0
        nr_malformed = 0
        nr_ok = 0
        for line in lines:
            if len(line[4]) > 0 and line[4] != "NULL":
                deleted_risk = Risk(*[line[2], '000142', 'Deleted VRE Screening', line[4], '00:00:00'])
            else:
                nr_malformed += 1
                continue
            if patients.get(deleted_risk.patient_id, None) is not None:
                patients[deleted_risk.patient_id].add_risk(deleted_risk)
            else:
                nr_not_found += 1
                continue
            nr_ok += 1
        logging.info(f"{nr_ok} risks added, {nr_not_found} patients not found for deleted risk, {nr_malformed} malformed.")

    @staticmethod
    def add_screening_data_to_patients(lines, patient_dict):
        """Adds screening data to all patients in the model.

        Reads the custom-made VRE_Screenings_Final.csv file, which is produced in the Python Project "VRE Aggregation".
        This file is created from manually extracted IFIK data, and cannot be currently extracted automatically on
        Hadoop. This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator
        object). The underlying file is the ONLY necessary file for VRE results, and contains an aggregation of
        all IFIK screening results. Note that this file also contains an `Options` column which is used as a basis for
        validating ward data, since a particular screening type may be active in multiple wards simultaneously. As a
        consequence, the `Move()` data associated with all patients in the model must be used to correctly assign the
        ward in which a particular screening has taken place. The raw input file contains the following columns:

        - Auftrag_Nummer
        - Erfassung
        - Entnahme
        - Patient_Number
        - Geschlecht
        - PID
        - Fall_Nummer
        - Auftraggeber
        - Kostenstelle
        - Material_Typ
        - Transport
        - Resultat
        - vreih
        - Analyse_Methode
        - Screening_Context
        - Ward
        - Options

        More details on the exact structure of the read-in file can be found in the Python Project "VRE Aggregation".

        Note that all screening results are converted into a Risk() object, validated, and added to Patient() objects
        in patient_dict using the ``Patient().add_risk()`` function.

        Args:
            lines (iterator):       iterator object of the to-be-read file `not` containing the header line
            patient_dict (dict):    Dictionary mapping patient ids to Patient() --> {'00008301433' : Patient(), ... }
        """
        move_wards = []
        screening_wards = []
        logging.debug("adding_all_screenings_to_patients")
        nr_pat_not_found = 0
        nr_ok = 0
        for line in lines:
            this_risk = Risk(*line)
            if patient_dict.get(this_risk.pid) is None:  # Check whether or not PID exists
                nr_pat_not_found += 1
                continue
            potential_moves = patient_dict.get(this_risk.pid).get_location_info(focus_date=this_risk.erfassung)
            if len(potential_moves) > 0:  # indicates at least one potential match
                move_wards.append('+'.join([each_move.org_fa for each_move in potential_moves]))
                screening_wards.append(this_risk.options)
                nr_ok += 1
        # print results to file
        with open('match_results.txt', 'w') as writefile:
            for i in range(len(move_wards)):
                writefile.write(f"{move_wards[i]}; {screening_wards[i]}\n")





            #     patient_dict[this_risk.patient_id].add_risk(this_risk)
            #     nr_ok += 1
            # else:
            #     nr_pat_not_found += 1
            #     continue
        logging.info(f"{nr_ok} screenings added, {nr_pat_not_found} patients from VRE screening data not found.")






