import logging

class Partner:
    '''
    Defines a Business partner as stored in SAP IS-H table NGPA.
    This is used to track referring physicians for the patients' cases.
    The cases that were referred by a physician are tracked in referred_cases.
    '''
    def __init__(self, gp_art, name1, name2, name3, land, pstlz, ort, ort2, stras, krkhs):
        '''
        Constructor. Subset of fields from IS-H table NGPA.
        :param gp_art:
        :param name1:
        :param name2:
        :param name3:
        :param land:
        :param pstlz:
        :param ort:
        :param ort2:
        :param stras:
        :param krkhs:
        '''
        self.gp_art = gp_art
        self.name1 = name1
        self.name2 = name2
        self.name3 = name3
        self.land = land
        self.pstlz = pstlz
        self.ort = ort
        self.ort2 = ort2
        self.stras = stras
        self.krkhs = krkhs

        self.referred_cases = dict()

    def add_case(self, case):
        '''
        Track the cases that a physician has referred.
        :param case:
        :return:
        '''
        self.referred_cases[case.case_id] = case

    @staticmethod
    def create_partner_map(lines):
        '''
        Creates and returns a map of business partners from a csv reader. This function will be called by the HDFS_data_loader.patient_data() function. The lines argument corresponds to a
        csv.reader() instance which supports the iterator protocol (see documentation for csv.reader in module "csv"). Each iteration over lines will contain a list of the
        following values (EXCLUDING the header line):
        >> TABLE NAME: LA_ISH_NGPA
        [ "GPART",      "NAME1",        "NAME2",            "NAME3",    "LAND", "PSTLZ",    "ORT",  "ORT2", "STRAS",    "KRKHS"] --> header line
        [ "1001503842", "Fontanellaz",  "Christian David",  "",         "CH",   "3010",     "Bern", "",     "",         ""]
        [ "1001503845", "Dulcey",       "Andrea Sara",      "",         "CH",   "3010",     "Bern", "",     "",         ""]

        Returns: Dictionary mapping partners to Partner() objects --> {'1001503842' : Partner(), '1001503845' : Partner(), ... }
        '''
        nr_malformed = 0
        partners = dict()
        for line in lines:
            if len(line) != 10:
                nr_malformed += 1
                continue
            partner = Partner(*line)
            partners[partner.gp_art] = partner

        logging.info(f"{len(partners)} created, {nr_malformed} partners malformed")
        return partners

    @staticmethod
    def add_partners_to_cases(lines, cases, partners):
        '''
        Reads lines from csv reader originating from SAP IS-H table NFPZ, and updates the referring physician (Partner() object) from partners to the corresponding case,
        and also adds the corresponding Case() to Partner() from cases. This function is called by the HDFS_data_loader.patient_data() method.
        The table used for updating cases has the following structure:
        >> TABLE NAME: LA_ISH_NFPZ
        [ "EARZT",  "FARZT",    "FALNR",        "LFDNR",    "PERNR",        "STORN"]
        [ "H",      "2",        "0006451992"    "1",        "0010217016"    ""]
        [ "H",      "2",        "0006451992",   "3",        "0010217016",   ""]

        Referring physicians (EARZT = 'U') are added only to cases which are NOT cancelled, i.e. STORN != 'X'.
        '''
        logging.debug("add_partners_to_cases")
        nr_cases_not_found = 0
        nr_partners_not_found = 0
        nr_ok = 0
        for line in lines:
            if line[0] == 'U' and line[5] != 'X':  # line[5] corresponds to the "STORN" column ('X' --> storniert)
                if cases.get(line[2], None) is None:
                    nr_cases_not_found += 1
                    continue
                if partners.get(line[4], None) is None:
                    nr_partners_not_found += 1
                    continue
                cases[line[2]].add_referrer(partners[line[4]])
                partners[line[4]].add_case(cases[line[2]])
                nr_ok += 1
        logging.info(f"{nr_ok} ok, {nr_cases_not_found} cases not found, {nr_partners_not_found} partners not found")
