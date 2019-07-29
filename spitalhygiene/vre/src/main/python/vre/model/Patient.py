import datetime
import logging
from dateutil.relativedelta import relativedelta


class Patient:
    def __init__(self, patient_id, geschlecht, geburtsdatum, plz, wohnort, kanton, sprache):
        self.patient_id = patient_id
        self.geschlecht = geschlecht
        self.geburtsdatum = None
        if geburtsdatum:
            self.geburtsdatum = datetime.datetime.strptime(geburtsdatum, "%Y-%m-%d")
        self.plz = plz
        self.wohnort = wohnort
        self.kanton = kanton
        self.sprache = sprache
        self.cases = dict()
        self.risks = dict() # dictionary mapping dt.dt() objects to Risk() objects, indicating at which datetime a particular VRE code has been entered in one of the Insel systems

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
        self.risks[risk.entry_date] = risk

    def has_risk(self, code_text_list=[(32, None), (42, None), (142, None)]):
        """
        Returns true if there if at least one of the risk_code, risk_text tuples are found in the Patient's risk dict.
        risk_text can be none if the text does not matter. False if none of the risks is found.
        :param code_text_list:
        :return:
        """
        for code_text in code_text_list:
            if self.risks.get(code_text[0], None) is not None:
                if (
                    code_text[1] is None
                    or self.risks[code_text[0]].kz_txt == code_text[1]
                ):
                    return True
        return False

    def get_risk_date(self, code_text_list=[(32, None), (42, None), (142, None)]):
        """
        Identify risk date for a patient.

        :param code_text_list: currently a list of the form --> [(32, None), (42, None), (142, None)] (default value)

        :return:    Date of the first risk from the code_text_list that is found in the Patient's risk dict, in the form of a datetime.datetime() object
                    If none of the risks is found, None is returned instead.
        """
        for code_text in code_text_list:
            if self.risks.get(code_text[0], None) is not None:
                if code_text[1] is None or self.risks[code_text[0]].kz_txt == code_text[1]:
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
        if self.geburtsdatum is None:
            return None

        return (
            dt.year
            - self.geburtsdatum.year
            - ((dt.month, dt.day) < (self.geburtsdatum.month, self.geburtsdatum.day))
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

    def get_relevant_case(self, dt=datetime.datetime.now().date(), since=datetime.datetime(2017, 12, 31, 0, 0).date() ):
        """
        Definition of relevant case:
        The most recent stationary case, which was still open during relevant date or closed after "since" date.
        :param dt:      Relevant date for patients without risk factor.
        :param since:   Relevant case must still be open at "since"
        :return:        A Case() object in case there is a relevant case, or None otherwise
        """
        relevant_dt = self.get_relevant_date(dt)

        # candidate relevant case must be
        #   1. stationary
        #   2. open before "relevant_dt"
        #   3. closed after "since"
        # from all candidates we take the one with highest closing date

        relevant_case = None
        for case in self.cases.values():
            if (
                case.is_stationary()
                and case.open_before_or_at_date(relevant_dt)
                and case.closed_after_or_at_date(since)
            ):
                if relevant_case is None or case.closed_after_or_at_date( relevant_case.moves_end.date() ):
                    # Here we make sure to consider only the LATEST case, by comparing whether case() was closed after the closing date of the relevant case --> update relevant case !
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
        For the relevant case, length of stay is defined as the period between the moves_start and moves_end,
        or between moves_start and relevant date if the case is still open at relevant date.
        For non-stationary cases, length of stay is not defined (None).
        :param dt: datetime.datetime
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
        # moves from SAP IS-H
        moves = case.get_moves_before_dt(dt)
        for move in moves:
            if move.room is not None:
                rooms.add(move.room.name)
        # appointments from RAP
        for appointment in case.appointments:
            if appointment.termin_datum < dt:
                for room in appointment.rooms:
                    rooms.add(room.name)
        return rooms

    def has_icu_stay(self, orgs):
        """
        The patient has a stay in ICU during relevant case before relevant date if there is a move to one of the organizational units provided in orgs.
        :param orgs: list of ICU organizational unit names
        :return: boolean, False if no relevant case
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return False
        moves = case.get_moves_before_dt(dt)
        for move in moves:
            if move.org_pf in orgs:
                return True
        return False

    def get_nr_cases(self, delta=relativedelta(years=1)):
        """
        How many SAP cases (stationary and ambulatory) did the patient have in one year (or delta provided) before the relevant date.
        (moves_start is before relevant_date and moves_end after relevant_date - delta
        :return: int
        """
        nr_cases = 0
        relevant_date = self.get_relevant_date()
        dt = datetime.datetime.combine(
            relevant_date, datetime.datetime.min.time()
        )  # need datetime, not date
        for case in self.cases.values():
            if case.moves_start is not None and case.moves_end is not None:
                if case.moves_start <= dt and case.moves_end >= (dt - delta):
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
                case.moves_start <= medication.drug_submission <= dt
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

    def get_chop_codes(self):
        """
        Which chop codes was the patient exposed to during the relevant case, before the relevant date.
        :return: Set of Chops, None if no relevant case
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        relevant_chops = set()
        for surgery in case.surgeries:
            if surgery.bgd_op <= dt:
                relevant_chops.add(surgery.chop)
        return relevant_chops

    def has_surgery(self):
        """
        Does the patient have a chop code during relevant case before relevant date?
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
            if case.moves_start <= medication.drug_submission <= dt:
                dispforms.add(medication.drug_dispform)
        return dispforms

    def get_employees(self):
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
            if appointment.termin_datum < dt:
                for employee in appointment.employees:
                    employees[employee.mitarbeiter_id] = (
                        appointment.dauer_in_min
                        if employees.get(employee.mitarbeiter_id, None) is None
                        else employees[employee.mitarbeiter_id] + appointment.dauer_in_min
                    )
        for care in case.cares:
            if care.dt < dt:
                employees[care.employee.mitarbeiter_id] = (
                    care.duration_in_minutes
                    if employees.get(care.employee.mitarbeiter_id, None) is None
                    else employees[care.employee.mitarbeiter_id] + care.duration_in_minutes
                )
        return employees

    def get_devices(self):
        """
        Names of devices that were used during the relevant case, before the relevant date.
        :return: set of device names, None if no relevant case
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        device_names = set()
        for appointment in case.appointments:
            if appointment.termin_datum < dt:
                for device in appointment.devices:
                    device_names.add(device.geraet_name)
        return device_names

    def get_partner(self):
        """
        Returns the referring partner(s) for the relevant case.
        :return: set of Partner
        """
        (case, dt) = self.get_relevant_case_and_date()
        if case is None:
            return None
        return case.referrers

    def get_label(self):
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

    def get_features(self):
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
        features["gender"] = self.geschlecht
        features["language"] = self.sprache
        features["plz"] = self.plz
        features["kanton"] = self.kanton

        features["surgery"] = self.has_surgery()

        features["icu"] = self.has_icu_stay( # True if the patient has spent time in one of the ICU units, False otherwise
            [
                "IN E GR",
                "INEBL 1",
                "INEBL 2",
                "INEGE 1",
                "INEGE 2",
                "E108-09",
                "E116",
                "E113-15",
                "E120-21",
            ]
        )

        antibiotic_exposure = self.get_antibiotic_exposure()
        for antibiotic_atc, days_administred in antibiotic_exposure.items():
            features["antibiotic=" + antibiotic_atc] = len(days_administred)

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

        stat_employees = self.get_employees()
        for employee_id, employee_duration in stat_employees.items():
            features["employee=" + employee_id] = employee_duration

        stat_devices = self.get_devices()
        for device in stat_devices:
            features["device=" + device] = True

        # Extract all ICD codes from the relevant case ONLY
        relevant_case = self.get_relevant_case() # returns a Case() object corresponding to the relevant case for this patient
        for icd_code in relevant_case.icd_codes: # Case.idc_codes is a list of ICD() objects for this particular case
            features['icd=' + icd_code.icd_code] = True

        return features

    def get_location_info(self, focus_date, comparison_type='exact'):
        """Returns ward, room and bed information for a patient at a specific date.

        This function will go through all Move() objects of each Cases() object of this patient, and return a tuple of
        Move() objects for which

        Move().bwi_dt <= focus_date <= Move().bwe_dt

        The exact location of a patient at focus_date can then be extracted from the ``Move().org_fa``,
        ``Move().org_pf``, ``Move().org_au``, ``Move().zimmr`` and ``Move().bett`` attributes.

        Args:
            focus_date (datetime.date()):   Date for which all moves are to be extracted from a patient
            comparison_type (str):          Type of comparison between Move() objects and focus_date. If set to
                                            ``exact`` (the default), only Move() objects with non-None Move().bwi_dt
                                            `and` Move().bwe_dt attributes will be considered.

        Returns:
            tuple:   tuple of Move() objects for which Move().bwi_dt < focus_date < Move().bwe_dt
        """
        location_moves = []

        for each_case in self.cases.values():
            for each_move in each_case.moves.values():
                if comparison_type == 'exact' and (each_move.bwi_dt is None or each_move.bwe_dt is None):
                    continue
                # bwi_dt_date = datetime.date(each_move.bwi_dt.day, each_move.bwi_dt.month, each_move.bwi_dt.day)
                # bwe_dt_date = datetime.date(each_move.bwe_dt.day, each_move.bwe_dt.month, each_move.bwe_dt.day)
                if each_move.bwi_dt.date() <= focus_date <= each_move.bwe_dt.date():
                    location_moves.append(each_move)

        return tuple(location_moves)

    @staticmethod
    def create_patient_dict(lines):
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
        patients = dict()
        for line in lines:
            patient = Patient(*line)
            patients[patient.patient_id] = patient

        logging.info(f"{len(patients)} patients created")
        return patients
