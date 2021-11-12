import logging
from datetime import datetime

from tqdm import tqdm
import itertools


class Surgery:
    """
    A surgery maps a case to a chop code.
    Table schema:
    [LFDBEW],[ICPMK],[ICPML],[ANZOP],[BGDOP],[LSLOK],[STORN],[FALNR],[ORGPF]
    """

    def __init__(self, stay_id, catalog_id, case_id, chop_code, surgeries_qty, date, location_surgery_info, cancelled, ward):
        self.date = datetime.strptime(date, "%Y-%m-%d")  # date of surgery
        self.catalog_id = catalog_id  # catalog ID
        self.chop_code = "Z" + chop_code  # chop code (comes without leading Z!)
        self.case_id = case_id  # case ID in SAP!
        self.cancelled = cancelled  # cancelled ("" or "X")?
        self.ward = ward  # ward

        self.chop = None

        # unused fields
        self.stay_id = stay_id
        self.surgeries_qty = surgeries_qty
        self.location_surgery_info = location_surgery_info

    @staticmethod
    def add_surgeries_to_case(lines, cases, chops, is_verbose=True):
        """
        Creates Surgery() objects from a csv reader, and performs the following:
        --> Finds the CHOP code in the chops dict (given the chop code and catalog id, which is read from the csv)
        --> Finds the case with the correct case id and adds the surgery to the case.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: LA_ISH_NICP
        ["LFDBEW", "ICPMK", "ICPML",    "ANZOP", "BGDOP",       "LSLOK", "STORN", "FALNR",      "ORGPF"]
        ["0",      "10",    "96.08",    "0",     "2011-12-23",  "",      "",      "0003807449", "Q MITTE"]
        ["0",      "10",    "87.41.00", "0",     "2011-12-27",  "",      "",      "0003807449", "Q MITTE"]

        :param cases: Dictionary mapping case ids to Case() objects                         --> {"0003536421" : Case(), "0003473241" : Case(), ...}
        :param chops: Dictionary mapping the chopcode_katalogid entries to Chop() objects   --> { 'Z39.61.10_11': Chop(), ... }
        """
        logging.debug("add_surgery_to_case")
        nr_chop_not_found = 0
        nr_case_not_found = 0
        nr_surgery_cancelled = 0
        nr_ok = 0
        lines_iters = itertools.tee(lines, 2)
        for line in tqdm(lines_iters[1], total=sum(1 for _ in lines_iters[0]), disable=is_verbose):
            surgery = Surgery(*line)
            if surgery.cancelled == 'X':  # ignore 'cancelled' surgeries
                nr_surgery_cancelled += 1
                continue
            # TODO: Find out why chop codes are not found
            chop = chops.get(surgery.chop_code + "_" + surgery.catalog_id, None)
            case = cases.get(surgery.case_id, None)
            if chop is not None:
                surgery.chop = chop
            else:
                nr_chop_not_found += 1
                continue
            if case is not None:
                case.add_surgery(surgery)
                chop.add_case(case)
            else:
                nr_case_not_found += 1
                continue
            nr_ok += 1
        logging.info(f"{nr_ok} surgeries ok, {nr_case_not_found} cases not found, {nr_chop_not_found} chop codes not found, {nr_surgery_cancelled} surgeries cancelled")

    def __repr__(self):
        return str(dict((key, value) for key, value in self.__dict__.items()
                if not callable(value) and not key.startswith('__')))

    def __str__(self):
        return self.__repr__()