import logging
from datetime import datetime

from tqdm import tqdm
import pandas as pd
import re

from src.features.model.data_model_constants import CaseEnum


class Case:
    def __init__(
            self,
            case_id,
            patient_id,
            case_type_id,
            case_status,
            case_type,
            begin_date,
            end_date,
            patient_type,
            patient_status,
    ):
        self.case_id = case_id
        self.patient_id = "".join(c for c in patient_id if c.isdigit()).zfill(11)  # drop nondigits and extend the patient id to length 11 to get a standardized representation
        self.case_type_id = case_type_id
        self.case_status = case_status
        self.case_type = case_type
        self.begin_date = begin_date
        self.end_date = end_date
        self.patient_type = patient_type
        self.patient_status = patient_status
        self.appointments = []
        self.cares = []
        self.surgeries = []
        self.stays = dict()
        self.stays_start = None
        self.stays_end = None
        self.referrers = set()
        self.patient = None
        self.medications = []
        self.icd_codes = []

    def is_inpatient_case(self):
        """
        Is this an inpatient case?
        :return:
        """
        # TODO: Hardcoded label! Extract to config
        return self.case_type == CaseEnum.inpatient_case

    def open_before_or_at_date(self, dt):
        """
        Did the stays of this case start before or at dt?
        :param dt: datetime.date
        :return:
        """
        if self.stays_start is None:
            return False
        else:
            return self.stays_start.date() <= dt

    def closed_after_or_at_date(self, dt):
        """
        Did the stays of this case end after or at dt?
        :param dt: datetime.date
        :return:
        """
        if self.stays_end is None:
            return False
        else:
            return self.stays_end.date() >= dt

    def add_referrer(self, partner):
        """
        Add a business partner to the set of referrers for this case.
        Referrers can be found in SAP IS-H table NFPZ, or in NBEW.
        :param partner:
        :return:
        """
        self.referrers.add(partner)

    def add_surgery(self, surgery):
        """
        Add a surgery to this case.
        :param surgery:
        :return:
        """
        self.surgeries.append(surgery)

    def add_medication(self, medication):
        """
        Add a medication to this case.
        :param medication: Medication
        :return:
        """
        self.medications.append(medication)

    def add_appointment(self, appointment):
        self.appointments.append(appointment)

    def add_care(self, care):
        self.cares.append(care)

    def add_stay(self, stay):
        self.stays[stay.serial_number] = stay
        if stay.to_datetime is not None and ((self.stays_end is None) or (stay.to_datetime > self.stays_end)):
            self.stays_end = stay.to_datetime
        if stay.from_datetime is not None and ((self.stays_start is None) or (stay.from_datetime < self.stays_start)):
            self.stays_start = stay.from_datetime

    def correct_stay_enddt(self):
        """
        This is required because we can't trust the end date of the stayment data.
        Call this only after all the stayment data is loaded!
        Helper function to fix missing stayment end dates and times of this Fall:
        Set the end date/time as the start date/time of the next stay. Only use end date/time if there
        is no next stay.
        """
        sorted_keys = sorted(self.stays)
        for i, lfd_nr in enumerate(sorted_keys):
            if i < (len(sorted_keys) - 1):
                self.stays[lfd_nr].to_datetime = self.stays[sorted_keys[i + 1]].from_datetime

    def add_patient(self, p):
        self.patient = p

    def add_icd_code(self, icd_code):
        """
        Adds an ICD() code object to the self.icd_codes list.

        :param icd_code: an ICD() object
        """
        self.icd_codes.append(icd_code)

    def get_length_of_stay(self):
        """
        The length of stay is the duration between the start time of the first stayment of the case and
        the end time of the last stayment.
        :return:
        """
        if self.case_type != "1":
            return None
        if self.stays_end is None:
            return datetime.now() - self.stays_start
        else:
            return self.stays_end - self.stays_start

    def get_length_of_stay_until(self, dt):
        """
        Timedelta between stays_start and stays_end or dt, whichever comes first.
        :param dt: datetime.datetime
        :return: datetime.timedelta
        """
        if self.stays_end is None or (self.stays_end > dt):
            return dt - self.stays_start
        else:
            return self.stays_end - self.stays_start

    def get_stays_before_dt(self, dt):
        """
        The list of stays that start before a given datetime.
        :param dt:  datetime.datetime
        :return:    List of stays
        """
        stays = []
        for stay in self.stays.values():
            if stay.from_datetime < dt:
                stays.append(stay)
        return stays

    @staticmethod
    def create_case_map(csv_path, encoding, patients, load_fraction=1.0, load_seed=7):
        """
        Read the case csv and create Case objects from the rows. Populate a dict with cases (case_id -> case) that are not 'storniert'. Note that the function goes both ways, i.e. it adds
        Cases to Patients and vice versa. This function will be called by the HDFS_data_loader.patient_data() function. The lines argument corresponds to a csv.reader() instance
        which supports the iterator protocol (see documentation for csv.reader in module "csv"). Each iteration over lines will contain a list of the following values
        (EXCLUDING the header line):
        >> TABLE NAME: V_LA_ISH_NFAL_NORM
        [ "PATIENTID",      "CASEID",       "CASETYP",          "CASESTATUS",   "FALAR",    "BEGDT",    "ENDDT",        "PATIENTTYP",       "PATIENTSTATUS"] --> header line
        [ "00008769940",    "0003536421",   "Standard Fall",    "storniert",    "2",        "",         "",             "Standard Patient", "aktiv"]
        [ "00008770123",    "0003473241",   "Standard Fall",    "aktiv",        "2",        "",         "2010-12-31",   "Standard Patient", "aktiv"]

        :param patients: Dictionary mapping patient ids to Patient() objects --> {"00001383264" : Patient(), "00001383310" : Patient(), ...}

        :return: Dictionary mapping case ids to Case() objects --> {"0003536421" : Case(), "0003473241" : Case(), ...}
        """
        logging.debug("create_case_map")

        case_df = pd.read_csv(csv_path, encoding=encoding, parse_dates=["Start Date", "End Date"], dtype=str)

        if load_fraction != 1.0:
            case_df = case_df.sample(frac=load_fraction, random_state=load_seed)
        # in principle they are all int, history makes them a varchar/string
        # case_df["Patient ID"] = case_df["Patient ID"].apply(lambda id: re.sub("\D", "", id)) # remove all non-digits from id
        # case_df["Case ID"] = case_df["Case ID"].astype(int)
        # case_df["Patient ID"] = case_df["Patient ID"].astype(int)
        #case_objects = case_df.progress_apply(lambda row: Case(*row.to_list()), axis=1)
        case_objects = list(map(lambda row: Case(*row), tqdm(case_df.values.tolist())))
        del case_df

        import_count = 0
        nr_not_found = 0
        nr_ok = 0
        nr_not_inpatient_case = 0
        nr_case_not_active = 0
        cases = dict()
        for case in tqdm(case_objects):
            # TODO: Rewrite to pandas
            # TODO: Hardcoded label, extract to configuration
            # TODO: Look into the consequences of adding closed cases
            if case.case_status == "open" or case.case_status == "closed":  # exclude entries where "CASESTATUS" is "storniert"
                cases[case.case_id] = case
                if case.patient_id in patients.keys():
                    patients[case.patient_id].add_case(case)
                    case.add_patient(patients[case.patient_id])
                    import_count += 1
                else:
                    nr_not_found += 1
                    continue
            else:
                nr_case_not_active += 1
                continue
            nr_ok += 1

        logging.info(f"{nr_ok} cases ok, {nr_not_found} patients not found, {nr_case_not_active} cases not active")
        return cases
