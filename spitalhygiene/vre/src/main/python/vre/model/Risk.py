from datetime import datetime
import logging

class Risk:
    def __init__(self, patient_id, rfs_nr, kz_txt, er_dat, er_tim):
        self.patient_id = patient_id
        self.rfs_nr = int(rfs_nr)
        self.kz_txt = kz_txt
        self.er_dt = datetime.strptime( er_dat + " " + er_tim, "%Y-%m-%d %H:%M:%S.0000000" )

    def add_risk_to_patient(lines, patients):
        """Reads the risk csv, create Risk objects from the rows and adds these to the respective patients.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_LA_ISH_NRSF_NORM
        ["PATIENTID",   "RSFNR",    "KZTXT",                            "ERDAT",        "ERTIM"]
        ["00004887743", "000042",   "Screening auf VRE. Spezielle L",   "2018-08-23",   "10:25:30.0000000"]
        ["00004963016", "000042",   "Screening auf VRE. Spezielle L",   "2018-05-09",   "15:48:48.0000000"]

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

    def add_deleted_risk_to_patient(lines, patients):
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
                deleted_risk = Risk(*[line[2], '000142', 'Deleted VRE Screening', line[4], '00:00:00.0000000'])
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
