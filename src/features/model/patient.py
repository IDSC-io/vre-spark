import datetime
import logging

from dateutil.relativedelta import relativedelta
from tqdm import tqdm
# from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from src.features.model.data_model_constants import ICUs
from src.features.model import Stay, Appointment

from typing import List


class Patient:

    def __init__(self, patient_id, gender, birth_date, zip_code, place_of_residence, canton, language):
        self.patient_id = "".join(c for c in patient_id if c.isdigit()).zfill(11)  # drop nondigits and extend the patient id to length 11 to get a standardized representation
        self.gender = gender
        self.birth_date = birth_date
        self.zip_code = zip_code
        self.place_of_residence = place_of_residence
        self.canton = canton
        self.language = language
        self.cases = dict();  """ dictionary mapping case ids to case objects"""
        self.risks = dict();  """dictionary mapping dt.dt() objects to Risk() objects, indicating at which datetime a particular VRE code has been entered in one of the Insel systems """

    def get_relevant_case_and_date(self):
        case = self.get_relevant_case()
        if case is None:
            return (None, None)
        relevant_date = self.get_relevant_date()
        dt = datetime.datetime.combine(
            relevant_date, datetime.datetime.min.time()
        )  # need datetime, not date
        return (case, dt)

    def add_case(self, case):
        self.cases[case.case_id] = case

    def add_risk(self, risk):
        """
        Adds a new risk to the Patients risk dict.
        :param risk:
        :return:
        """
        self.risks[risk.recording_date] = risk

    def has_risk(self, risk_list=None):
        """
        Returns true if there if at least one of the risk_code, risk_text tuples are found in the Patient's risk dict.
        risk_text can be none if the text does not matter. False if none of the risks are found.
        :param code_text_list:
        :return:
        """
        # TODO[BE]: Extend to any kind of risks again
        for risk in self.risks.values():
            if risk.result != 'nn':
                return True

        return False

        # if code_text_list is None:
        #     code_text_list = [(32, None), (42, None), (142, None)]
        #
        # for code_text in code_text_list:
        #     if self.risks.get(code_text[0], None) is not None:
        #         if (
        #                 code_text[1] is None
        #                 or self.risks[code_text[0]].description == code_text[1]
        #         ):
        #             return True
        # return False

    def get_risk_date(self, code_text_list=None):
        """
        Identify risk date for a patient.

        :param code_text_list: currently a list of the form --> [(32, None), (42, None), (142, None)] (default value)

        :return:    Date of the first risk from the code_text_list that is found in the Patient's risk dict, in the form of a datetime.datetime() object
                    If none of the risks is found, None is returned instead.
        """
        if code_text_list is None:
            code_text_list = [(32, None), (42, None), (142, None)]

        for code_text in code_text_list:
            if self.risks.get(code_text[0], None) is not None:
                if code_text[1] is None or self.risks[code_text[0]].description == code_text[1]:
                    return self.risks[code_text[0]].er_dt
        return None

    def get_age(self):
        """
        Calculates age at relevant date, based on birth date.
        None if no birth date is in the data or no relevant date
        :return:
        """
        dt = self.get_relevant_date()
        if dt is None:
            return None
        if self.birth_date is None:
            return None

        return (
                dt.year
                - self.birth_date.year
                - ((dt.month, dt.day) < (self.birth_date.month, self.birth_date.day))
        )

    def get_relevant_date(self, dt=datetime.datetime.now().date()):
        """
        Definition of relevant date:
        For patients with risk factor: The date attached to the risk factor.
        For patients without risk factor: Date as provided in dt, default date = now.
        :return:
        """
        risk_dt = self.get_risk_date()
        if risk_dt is not None:
            dt = risk_dt.date()
        return dt

    def get_relevant_case(self, dt=datetime.datetime.now().date(), since=datetime.datetime(2017, 12, 31, 0, 0).date()):
        """
        Definition of relevant case:
        The most recent stationary case, which was still open during relevant date or closed after "since" date.
        :param dt:      Relevant date for patients without risk factor.
        :param since:   Relevant case must still be open at "since"
        :return:        A Case() object in case there is a relevant case, or None otherwise
        """
        # TODO: Relevant case is different from research study to operationalisation!
        relevant_dt = self.get_relevant_date(dt)

        # candidate relevant case must be
        #   1. stationary
        #   2. open before "relevant_dt"
        #   3. closed after "since"
        # from all candidates we take the one with highest closing date

        relevant_case = None
        for case in self.cases.values():
            if (
                    case.is_inpatient_case()
                    and case.open_before_or_at_date(relevant_dt)
                    and case.closed_after_or_at_date(since)
            ):
                if relevant_case is None or case.closed_after_or_at_date(relevant_case.stays_end.date()):
                    # Here we make sure to consider only the LATEST case, by comparing whether case() was closed after the closing date of the relevant case
                    #  --> update relevant case !
                    relevant_case = case
        return relevant_case

    def get_referrers(self):
        """
        Find referrers for relevant case.
        :return:
        """
        case = self.get_relevant_case()
        return case.referrers

    def get_length_of_relevant_case(self):
        """
        For the relevant case, length of stay is defined as the period between the stays_start and stays_end,
        or between stays_start and relevant date if the case is still open at relevant date.
        For non-stationary cases, length of stay is not defined (None).
        :return: datetime.timedelta, None if no relevant case
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        return case.get_length_of_stay_until(dt)

    def get_relevant_rooms(self):
        """
        Set of names of rooms visited during relevant case before relevant date.
        Rooms visits can come from SAP IS-H or from RAP.
        :return: set of room names, None if no relevant case
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        rooms = set()
        # stays from SAP IS-H
        stays = case.get_stays_before_dt(dt)
        for stay in stays:
            if stay.room is not None:
                rooms.add(stay.room.name)
        # appointments from RAP
        for appointment in case.appointments:
            if appointment.date < dt:
                for room in appointment.rooms:
                    rooms.add(room.name)
        return rooms

    def has_icu_stay(self, icu_wards=None):
        """
        The patient has a stay in ICU during relevant case before relevant date if there is a stay to one of the organizational units provided in orgs.
        :param icu_wards: list of ICU organizational unit names
        :return: boolean, False if no relevant case
        """
        if icu_wards is None:
            icu_wards = ICUs

        return self.has_stay_on_ward(icu_wards)

    def get_stays(self) -> List[Stay]:
        """
        Get all stays of a patient.
        :return:
        """
        stays = []
        for case in self.cases.values():
            stays.extend(case.stays.values())

        return stays

    def has_stay_on_ward(self, wards):

        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return False
        stays = case.get_stays_before_dt(dt)
        for stay in stays:
            if stay.ward_id in wards:
                return True
        return False

    def get_icu_stays(self, icu_wards=None):

        if icu_wards is None:
            icu_wards = ICUs

        return self.get_ward_stays(icu_wards)

    def get_ward_stays(self, wards):
        # (case, dt) = self.get_relevant_case_and_date()
        # if case is None:
        #     return []
        # TODO: Rethink ward stays
        # stays = case.get_stays_before_dt(dt)
        ward_stays = []
        for case in self.cases.values():
            for stay in case.stays.values():
                if stay.ward.name in wards:
                    ward_stays.append(stay)

        return ward_stays

    def get_nr_cases(self, delta=relativedelta(years=1)):
        """
        How many SAP cases (inpatient and outpatient) did the patient have in one year (or delta provided) before the relevant date.
        (stays_start is before relevant_date and stays_end after relevant_date - delta
        :return: int
        """
        nr_cases = 0
        relevant_date = self.get_relevant_date()
        dt = datetime.datetime.combine(
            relevant_date, datetime.datetime.min.time()
        )  # need datetime, not date
        for case in self.cases.values():
            if case.stays_start is not None and case.stays_end is not None:
                if case.stays_start <= dt and case.stays_end >= (dt - delta):
                    nr_cases += 1
        return nr_cases

    def get_antibiotic_exposure(self):
        """
        Which antibiotics did the patient get, during which days in the relevant case, before the relevant date.
        :return: dict from ATC code to set of date, None if no relevant case
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        relevant_medications = dict()
        for medication in case.medications:
            if medication.is_antibiotic() and (
                    case.stays_start <= medication.drug_submission <= dt
            ):
                if relevant_medications.get(medication.drug_atc, None) is None:
                    date_set = set()
                    date_set.add(medication.drug_submission.date())
                    relevant_medications[medication.drug_atc] = date_set
                else:
                    relevant_medications[medication.drug_atc].add(
                        medication.drug_submission.date()
                    )

        return relevant_medications

    def get_chop_codes(self, ):
        """
        Which chop codes (surgeries) was the patient exposed to.
        :return: Set of Chops
        """
        # TODO: Reimplement relevant case and date
        # (case, dt) = self.get_relevant_case_and_date()
        # if case is None:
        #     return None
        chops = set()
        for case in self.cases.values():
            for surgery in case.surgeries:
                # if surgery.date <= dt:
                chops.add(surgery.chop)
        return chops

    def has_surgery(self):
        """
        Does the patient have any chop codes?
        :return: boolean
        """
        relevant_chops = self.get_chop_codes()
        return False if relevant_chops is None else len(relevant_chops) > 0

    def get_dispform(self):
        """
        What kind of forms of drug administration happened during the relevant case before the relevant date?
        :return: set of names of administration forms, None if no relevant case
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        dispforms = set()
        for medication in case.medications:
            if case.stays_start <= medication.drug_submission <= dt:
                dispforms.add(medication.drug_dispform)
        return dispforms

    def get_involved_employees(self):
        """
        employee IDS and duration of care or appointment of employees that were involved in appointments or care
        during the relevant case, before the relevant date.
        :return: set of employee_id
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        employees = dict()
        for appointment in case.appointments:
            if appointment.date < dt:
                for employee in appointment.employees:
                    employees[employee.id] = (
                        appointment.duration_in_mins
                        if employees.get(employee.id, None) is None
                        else employees[employee.id] + appointment.duration_in_mins
                    )
        for care in case.cares:
            if care.date < dt:
                employees[care.employee.id] = (
                    care.duration_in_minutes
                    if employees.get(care.employee.id, None) is None
                    else employees[care.employee.id] + care.duration_in_minutes
                )
        return employees

    def get_involved_devices(self):
        """
        Names of devices that were used during the relevant case, before the relevant date.
        :return: set of device names, None if no relevant case
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        device_names = set()
        for appointment in case.appointments:
            if appointment.date < dt:
                for device in appointment.devices:
                    device_names.add(device.name)
        return device_names

    def get_referring_partners(self):
        """
        Returns the referring partner(s) for the relevant case.
        :return: set of Partner
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        return case.referrers

    def get_screening_label(self):
        """
        All patients are categorized with the following labels:
        -1: no relevant case
        0: unknown
        1: screening with unconclusive results
        2: screening negative
        3: screening positive
        Higher number labels take precedence over lower ones.
        :return: int
        """
        label = -1
        if self.get_relevant_case() is not None:
            label = 0
            if self.has_risk([(42, None)]):
                label = 1
            if self.has_risk([(142, None)]):
                label = 2
            if self.has_risk([(32, None)]):
                label = 3
        return label

    def get_feature_vector(self):
        """
        Creates the sparse feature vector for this patient.

        :return:    Dictionary mapping feature names to the values for this patient --> {"length_of_stay" : 47, "nr_cases" : 3, ... }
                    If the patient has no relevant case, None is returned instead.
        """
        features = dict()
        if self.get_relevant_case() is None:
            return None

        length_of_stay = self.get_length_of_relevant_case()
        features["length_of_stay"] = length_of_stay.days

        features["nr_cases"] = self.get_nr_cases()

        features["age"] = self.get_age()
        features["gender"] = self.gender
        features["language"] = self.language
        features["zip_code"] = self.zip_code
        features["canton"] = self.canton

        features["surgery"] = self.has_surgery()

        features["icu"] = self.has_icu_stay(  # True if the patient has spent time in one of the ICU units, False otherwise
            ICUs
        )

        antibiotic_exposure = self.get_antibiotic_exposure()
        for antibiotic_atc, days_administered in antibiotic_exposure.items():
            features["antibiotic=" + antibiotic_atc] = len(days_administered)

        dispforms = self.get_dispform()
        if dispforms is not None:
            for dispform in dispforms:
                features["dispform=" + dispform] = True

        chops = self.get_chop_codes()
        if chops is not None:
            for chop in chops:
                features["chop=" + chop.get_lowest_level_code()] = True

        stat_rooms = self.get_relevant_rooms()
        for room in stat_rooms:
            features["room=" + room] = True

        stat_employees = self.get_involved_employees()
        for employee_id, employee_duration in stat_employees.items():
            features["employee=" + employee_id] = employee_duration

        stat_devices = self.get_involved_devices()
        for device in stat_devices:
            features["device=" + device] = True

        # Extract all ICD codes from the relevant case ONLY
        relevant_case = self.get_relevant_case()  # returns a Case() object corresponding to the relevant case for this patient
        for icd_code in relevant_case.icd_codes:  # Case.idc_codes is a list of ICD() objects for this particular case
            features['icd=' + icd_code.icd_code] = True

        return features

    def get_location_info(self, focus_date, comparison_type='exact'):
        """Returns ward, room and bed information for a patient at a specific date.

        This function will go through all Stay() objects of each Cases() object of this patient, and return a tuple of
        Stay() objects for which

        Stay().bwi_dt <= focus_date <= Stay().bwe_dt

        The exact location of a patient at focus_date can then be extracted from the ``Stay().org_fa``,
        ``Stay().org_pf``, ``Stay().org_au``, ``Stay().zimmr`` and ``Stay().bett`` attributes.

        Args:
            focus_date (datetime.date()):   Date for which all stays are to be extracted from a patient
            comparison_type (str):          Type of comparison between Stay() objects and focus_date. If set to
                                            ``exact`` (the default), only Stay() objects with non-None Stay().bwi_dt
                                            `and` Stay().bwe_dt attributes will be considered.

        Returns:
            tuple:   tuple of Stay() objects for which Stay().bwi_dt < focus_date < Stay().bwe_dt
        """
        location_stays = []

        for each_case in self.cases.values():
            for each_stay in each_case.stays.values():
                if comparison_type == 'exact' and (each_stay.from_datetime is None or each_stay.to_datetime is None):
                    continue
                # bwi_dt_date = datetime.date(each_stay.bwi_dt.day, each_stay.bwi_dt.month, each_stay.bwi_dt.day)
                # bwe_dt_date = datetime.date(each_stay.bwe_dt.day, each_stay.bwe_dt.month, each_stay.bwe_dt.day)
                if each_stay.from_datetime.date() <= focus_date <= each_stay.to_datetime.date():
                    location_stays.append(each_stay)

        return tuple(location_stays)

    def get_stays_at_date(self, target_date):
        """Extracts all hospital stays from cases for this patient at ``target_date``.

        This function returns a list of all stays for this patient that took place at ``target_date``. This should
        ideally be only a single entry, since a patient cannot be stationed in two or more places simultaneously.
        However, a situation may arise when a patient is transferred between wards, in which case there will be two
        stays matching the criteria.

        Args:
            target_date (datetime.date()):  Date for which stays will be extracted form this patient's cases.

        Returns:
            list:   List of Stay() objects which have taken place for this patient at ``target_date``.
        """
        candidate_stays = []
        for each_case in self.cases.values():
            # print(each_case.stays)
            for each_stay in each_case.stays.values():
                stay_start = each_stay.from_datetime.date() if each_stay.from_datetime is not None else None
                stay_end = each_stay.to_datetime.date() if each_stay.to_datetime is not None else None

                if stay_start is None or stay_end is None:
                    continue
                else:
                    if stay_start <= target_date <= stay_end:
                        candidate_stays.append(each_stay)

        return candidate_stays

    def get_appointments(self) -> List[Appointment]:
        """
        Get all appointments of this patient
        :return:
        """
        appointments = []
        for each_case in self.cases.values():
            appointments.extend(each_case.appointments)

        return appointments

    @staticmethod
    def create_patient_dict(csv_path, encoding, load_fraction=1.0, load_seed=7, is_verbose=True):
        """
        Read the patient csv and create Patient objects from the rows.
        Populate a dict (patient_id -> patient). This function will be called by the HDFS_data_loader.patient_data() function. The lines argument corresponds to a csv.reader() instance
        which supports the iterator protocol (see documentation for csv.reader in module "csv"). Each iteration over lines will contain a list of the following values
        (EXCLUDING the header line):
        >> TABLE NAME: V_DH_DIM_PATIENT_CUR
        [ "PATIENTID" ,      "GESCHLECHT" ,  "GEBURTSDATUM" ,   "PLZ" ,     "WOHNORT" ,         "KANTON" ,  "SPRACHE"] --> header line (for documentation purposes only)
        [ "00001383264" ,    "weiblich" ,    "1965-03-15" ,     "3072" ,    "Ostermundigen" ,   "BE" ,      "Deutsch"]
        [ "00001383310" ,    "weiblich" ,    "1949-02-11" ,     "3006" ,    "Bern" ,            "BE" ,      "Russisch"]

        Returns: Dictionary mapping PATIENTID to Patient() objects, i.e. {"00001383264" : Patient(), "00001383310" : Patient(), ...}
        """
        logging.debug("create_patient_dict")
        import_count = 0
        patients = dict()
        patient_df = pd.read_csv(csv_path, encoding=encoding, parse_dates=["Birth Date"], dtype=str)

        if load_fraction != 1.0:
            patient_df = patient_df.sample(frac=load_fraction, random_state=load_seed)
        # patient_df["Patient ID"] = patient_df["Patient ID"].astype(int) # in principle it should be an int, history makes it a varchar/string

        # Parallel execution seems slower
        # def create_patient(index_row_tuple):
        #     return Patient(*(index_row_tuple[1].to_list()))
        #
        # use as many threads as possible, default: min(32, os.cpu_count()+4)
        # with ThreadPoolExecutor() as executor:
        #     patient_objects = executor.map(create_patient, tqdm(patient_df.iterrows(), total=len(patient_df)))
        #patient_objects = patient_df.progress_apply(lambda row: Patient(*row.to_list()), axis=1)
        patient_objects = list(map(lambda row: Patient(*row), tqdm(patient_df.values.tolist(), disable=not is_verbose)))
        del patient_df
        for patient in tqdm(patient_objects, disable=not is_verbose):
            patients[patient.patient_id] = patient
            import_count += 1

        logging.info(f"{len(patients)} patients created")
        return patients

    def __repr__(self):
        return str(dict((key, value) for key, value in self.__dict__.items()
                    if not callable(value) and not key.startswith('__')))

    def __str__(self):
        return self.__repr__()

    # patient related queries

    @staticmethod
    def get_contact_patients_for_case(c, contact_patients, with_details=True):
        # TODO: Make contact patient dependent on VRE positive date
        for stay in tqdm(c.stays.values()):
            # contacts in the same room
            if stay.to_datetime is not None and stay.room is not None:
                stays_in_range = stay.room.get_stays_during(stay.from_datetime, stay.to_datetime)
                for stay_in_range in stays_in_range:
                    if not with_details and stay_in_range.case.patient_id in contact_patients:
                        continue

                    if stay_in_range.case is not None and stay.case is not None:
                        if stay_in_range.case.case_type_id == "1" and stay_in_range.type_id in ["1", "2", "3"] and stay_in_range.to_datetime is not None:


                            if with_details:
                                if stay_in_range.case.patient_id not in contact_patients:
                                    contact_patients[stay_in_range.case.patient_id] = []

                                start_overlap = max(stay.from_datetime, stay_in_range.from_datetime)
                                end_overlap = min(stay.to_datetime, stay_in_range.to_datetime)

                                contact_patients[stay_in_range.case.patient_id].append(
                                    (
                                        stay.case.patient_id,  # risk patient
                                        stay_in_range.case.patient_id,  # contact patient
                                        start_overlap,
                                        end_overlap,
                                        stay.room.room_id if stay.room is not None else stay.room_id,
                                        "contact_room",
                                    )
                                )
                            else:
                                if stay_in_range.case.patient_id not in contact_patients:
                                    contact_patients[stay_in_range.case.patient_id] = (
                                            stay.case.patient_id,  # risk patient
                                            stay_in_range.case.patient_id,  # contact patient
                                            "contact_room",
                                        )
            # contacts in the same ward (ORGPF)
            if stay.to_datetime is not None and stay.ward is not None:
                stays_in_range = stay.ward.get_stays_during(stay.from_datetime, stay.to_datetime)
                for stay_in_range in stays_in_range:
                    if not with_details and stay_in_range.case.patient_id in contact_patients:
                        continue

                    if stay_in_range.case is not None and stay.case is not None:
                        if stay_in_range.case.case_type_id == "1" \
                                and stay_in_range.type_id in ["1", "2", "3"] \
                                and stay_in_range.to_datetime is not None \
                                and (stay_in_range.room_id is None or stay.room_id is None or stay_in_range.room_id != stay.room_id):
                            if with_details:

                                if stay_in_range.case.patient_id not in contact_patients:
                                    contact_patients[stay_in_range.case.patient_id] = []

                                start_overlap = max(stay.from_datetime, stay_in_range.from_datetime)
                                end_overlap = min(stay.to_datetime, stay_in_range.to_datetime)

                                contact_patients[stay_in_range.case.patient_id].append(
                                    (
                                        stay.case.patient_id,  # risk patient
                                        stay_in_range.case.patient_id,  # contact patient
                                        start_overlap,
                                        end_overlap,
                                        stay.ward.name if stay.ward is not None else stay.ward_id,
                                        "contact_ward",
                                    )
                                )
                            else:
                                if stay_in_range.case.patient_id not in contact_patients:
                                    contact_patients[stay_in_range.case.patient_id] = (
                                            stay.case.patient_id,  # risk patient
                                            stay_in_range.case.patient_id,  # contact patient
                                            "contact_ward",
                                        )

    @staticmethod
    def get_risk_patients(patients):
        risk_patients = {}
        for p in patients.values():
            if p.has_risk():
                risk_patients[p.patient_id] = p
        return risk_patients

    @staticmethod
    def get_contact_patients(patients, with_details=True):
        contact_patients = {}
        for p in tqdm(patients.values()):
            if p.has_risk():
                for c in p.cases.values():
                    if c.case_type_id == "1":  # only stationary cases # TODO: cast to int for faster comparison
                        if c.stays_end is not None:  # and c.stays_end > datetime.datetime.now() - relativedelta(years=1): # TODO: Think again about this
                            Patient.get_contact_patients_for_case(c, contact_patients, with_details=with_details)
        return contact_patients

    @staticmethod
    def get_patients_by_ids(all_patients: dict, patient_ids):
        return {patient_id: all_patients[patient_id] for patient_id in tqdm(patient_ids) if patient_id in all_patients}

    def __repr__(self):
        return str(dict((key, value) for key, value in self.__dict__.items()
                    if not callable(value) and not key.startswith('__')))

    def __str__(self):
        return self.__repr__()