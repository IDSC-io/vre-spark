import logging
from datetime import datetime


class Case:
    def __init__(
            self,
            patient_id,
            case_id,
            case_typ,
            case_status,
            fal_ar,
            beg_dt,
            end_dt,
            patient_typ,
            patient_status,
    ):
        self.patient_id = patient_id
        self.case_id = case_id
        self.case_typ = case_typ
        self.case_status = case_status
        self.fal_ar = fal_ar
        self.beg_dt = None
        if beg_dt and beg_dt != "NULL":
            self.beg_dt = datetime.strptime(beg_dt, "%Y-%m-%d")
        self.end_dt = None
        if end_dt and end_dt != "NULL":
            self.end_dt = datetime.strptime(end_dt, "%Y-%m-%d")
        self.patient_typ = patient_typ
        self.patient_status = patient_status
        self.appointments = []
        self.cares = []
        self.surgeries = []
        self.moves = dict()
        self.moves_start = None
        self.moves_end = None
        self.referrers = set()
        self.patient = None
        self.medications = []
        self.icd_codes = []

    def is_stationary(self):
        """
        Stationary cases have "1" FALAR in SAP table NFAL
        :return:
        """
        return self.fal_ar == "1"

    def open_before_or_at_date(self, dt):
        """
        Did the moves of this case start before or at dt?
        :param dt: datetime.date
        :return:
        """
        if self.moves_start is None:
            return False
        else:
            return self.moves_start.date() <= dt

    def closed_after_or_at_date(self, dt):
        """
        Did the moves of this case end after or at dt?
        :param dt: datetime.date
        :return:
        """
        if self.moves_end is None:
            return False
        else:
            return self.moves_end.date() >= dt

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

    def add_move(self, move):
        self.moves[move.lfd_nr] = move
        if move.bwe_dt is not None and ((self.moves_end is None) or (move.bwe_dt > self.moves_end)):
            self.moves_end = move.bwe_dt
        if move.bwi_dt is not None and ((self.moves_start is None) or (move.bwi_dt < self.moves_start)):
            self.moves_start = move.bwi_dt

    def correct_move_enddt(self):
        """
        This is required because we can't trust the end date of the movement data.
        Call this only after all the movement data is loaded!
        Helper function to fix missing movement end dates and times of this Fall:
        Set the end date/time as the start date/time of the next move. Only use end date/time if there
        is no next move.
        """
        sorted_keys = sorted(self.moves)
        for i, lfd_nr in enumerate(sorted_keys):
            if i < (len(sorted_keys) - 1):
                self.moves[lfd_nr].bwe_dt = self.moves[sorted_keys[i + 1]].bwi_dt

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
        The length of stay is the duration between the start time of the first movement of the case and
        the end time of the last movement.
        :return:
        """
        if self.fal_ar != "1":
            return None
        if self.moves_end is None:
            return datetime.now() - self.moves_start
        else:
            return self.moves_end - self.moves_start

    def get_length_of_stay_until(self, dt):
        """
        Timedelta between moves_start and moves_end or dt, whichever comes first.
        :param dt: datetime.datetime
        :return: datetime.timedelta
        """
        if self.moves_end is None or (self.moves_end > dt):
            return dt - self.moves_start
        else:
            return self.moves_end - self.moves_start

    def get_moves_before_dt(self, dt):
        """
        The list of moves that start before a given datetime.
        :param dt:  datetime.datetime
        :return:    List of moves
        """
        moves = []
        for move in self.moves.values():
            if move.bwi_dt < dt:
                moves.append(move)
        return moves

    @staticmethod
    def create_case_map(lines, patienten, load_limit=None):
        """
        Read the case csv and create Case objects from the rows. Populate a dict with cases (case_id -> case) that are not 'storniert'. Note that the function goes both ways, i.e. it adds
        Cases to Patients and vice versa. This function will be called by the HDFS_data_loader.patient_data() function. The lines argument corresponds to a csv.reader() instance
        which supports the iterator protocol (see documentation for csv.reader in module "csv"). Each iteration over lines will contain a list of the following values
        (EXCLUDING the header line):
        >> TABLE NAME: V_LA_ISH_NFAL_NORM
        [ "PATIENTID",      "CASEID",       "CASETYP",          "CASESTATUS",   "FALAR",    "BEGDT",    "ENDDT",        "PATIENTTYP",       "PATIENTSTATUS"] --> header line
        [ "00008769940",    "0003536421",   "Standard Fall",    "storniert",    "2",        "",         "",             "Standard Patient", "aktiv"]
        [ "00008770123",    "0003473241",   "Standard Fall",    "aktiv",        "2",        "",         "2010-12-31",   "Standard Patient", "aktiv"]

        :param patienten: Dictionary mapping patient ids to Patient() objects --> {"00001383264" : Patient(), "00001383310" : Patient(), ...}

        :return: Dictionary mapping case ids to Case() objects --> {"0003536421" : Case(), "0003473241" : Case(), ...}
        """
        logging.debug("create_case_map")
        import_count = 0
        nr_not_found = 0
        nr_ok = 0
        nr_not_stationary = 0
        cases = dict()
        for line in lines:
            fall = Case(*line)
            # if fall.fal_ar != '1': # exclude non-stationary cases (for stationary cases: Case().falar == '1' !)
            #     nr_not_stationary += 1 # NOW INCLUDED DIRECTLY IN THE SQL QUERY
            #     continue
            if fall.case_status == "aktiv":  # exclude entries where "CASESTATUS" is "storniert"
                cases[fall.case_id] = fall
                if patienten.get(fall.patient_id, None) is not None:
                    patienten[fall.patient_id].add_case(fall)
                    fall.add_patient(patienten[fall.patient_id])
                    import_count += 1
                    if load_limit is not None and import_count > load_limit:
                        break
                else:
                    nr_not_found += 1
                    continue
            else:
                continue
            nr_ok += 1

        logging.info(f"{nr_ok} ok, {nr_not_found} patients not found, {nr_not_stationary} cases not stationary")
        return cases
