import logging

from tqdm import tqdm
import pandas as pd


class Partner:
    """
    Defines a Business partner as stored in SAP IS-H table NGPA.
    This is used to track referring physicians for the patients' cases.
    The cases that were referred by a physician are tracked in referred_cases.
    """

    def __init__(self, partner_id, name1, name2, name3, country, zip_code, location, location2, street, krkhs):
        """
        Constructor. Subset of fields from IS-H table NGPA.
        :param partner_id:
        :param name1:
        :param name2:
        :param name3:
        :param country:
        :param zip_code:
        :param location:
        :param location2:
        :param street:
        :param krkhs:
        """
        self.partner_id = partner_id
        self.name1 = name1
        self.name2 = name2
        self.name3 = name3
        self.country = country
        self.zip_code = zip_code
        self.location = location
        self.location2 = location2

        # not used fields
        self.street = street
        self.krkhs = krkhs

        self.referred_cases = dict()

    def add_case(self, case):
        """
        Track the cases that a physician has referred.
        :param case:
        :return:
        """
        self.referred_cases[case.case_id] = case

    @staticmethod
    def create_partner_map(csv_path, encoding):
        """
        Creates and returns a map of business partners from a csv reader. This function will be called by the HDFS_data_loader.patient_data() function. The lines argument corresponds to a
        csv.reader() instance which supports the iterator protocol (see documentation for csv.reader in module "csv"). Each iteration over lines will contain a list of the
        following values (EXCLUDING the header line):
        >> TABLE NAME: LA_ISH_NGPA
        [ "GPART",      "NAME1",        "NAME2",            "NAME3",    "LAND", "PSTLZ",    "ORT",  "ORT2", "STRAS",    "KRKHS"] --> header line
        [ "1001503842", "Fontanellaz",  "Christian David",  "",         "CH",   "3010",     "Bern", "",     "",         ""]
        [ "1001503845", "Dulcey",       "Andrea Sara",      "",         "CH",   "3010",     "Bern", "",     "",         ""]

        Returns: Dictionary mapping partners to Partner() objects --> {'1001503842' : Partner(), '1001503845' : Partner(), ... }
        """

        partner_df = pd.read_csv(csv_path, encoding=encoding)
        partner_df["Partner ID"] = partner_df["Partner ID"].astype(int)
        #partner_objects = partner_df.progress_apply(lambda row: Partner(*row.to_list()), axis=1)
        partner_objects = list(map(lambda row: Partner(*row), tqdm(partner_df.values.tolist())))
        del partner_df
        partners = dict()
        for partner in tqdm(partner_objects):
            partners[partner.partner_id] = partner

        logging.info(f"{len(partners)} partners created")
        return partners

    @staticmethod
    def add_partners_to_cases(csv_path, encoding, cases, partners):
        """
        Reads lines from csv reader originating from SAP IS-H table NFPZ, and updates the referring physician (Partner() object) from partners to the corresponding case,
        and also adds the corresponding Case() to Partner() from cases. This function is called by the HDFS_data_loader.patient_data() method.
        The table used for updating cases has the following structure:
        >> TABLE NAME: LA_ISH_NFPZ
        [ "EARZT",  "FARZT",    "FALNR",        "LFDNR",    "PERNR",        "STORN"]
        [ "H",      "2",        "0006451992"    "1",        "0010217016"    ""]
        [ "H",      "2",        "0006451992",   "3",        "0010217016",   ""]

        Referring physicians (EARZT = 'U') are added only to cases which are NOT cancelled, i.e. STORN != 'X'.
        """
        case_partners_df = pd.read_csv(csv_path, encoding=encoding, dtype=str)
        # in principle they are all int, history makes them a varchar/string
        # case_partners_df["Case ID"] = case_partners_df["Case ID"].astype(int)
        # case_partners_df["Partner ID"] = case_partners_df["Partner ID"].astype(int)

        logging.debug("add_partners_to_cases")
        nr_cases_not_found = 0
        nr_partners_not_found = 0
        nr_not_physician = 0
        nr_cancelled = 0
        nr_ok = 0
        # TODO: Rewrite loop to pandas
        for i, row in tqdm(case_partners_df.iterrows(), total=len(case_partners_df)):
            if row["EARZT"] == 'U':
                if row["Cancelled"] != 'X':  # corresponds to the "Cancelled" column ('X' --> cancelled)
                    if cases.get(row["Case ID"], None) is None:
                        nr_cases_not_found += 1
                        continue
                    partner_id = int(row["Partner ID"])
                    if partners.get(partner_id, None) is None:
                        nr_partners_not_found += 1
                        continue
                    cases[row["Case ID"]].add_referrer(partners[partner_id])
                    partners[partner_id].add_case(cases[row["Case ID"]])
                    nr_ok += 1
                else:
                    nr_cancelled += 1
            else:
                nr_not_physician += 1

        logging.info(f"{nr_ok} partners ok, {nr_cases_not_found} cases not found, {nr_partners_not_found} partners not found, {nr_not_physician} not physicians, {nr_cancelled} cancelled cases")
