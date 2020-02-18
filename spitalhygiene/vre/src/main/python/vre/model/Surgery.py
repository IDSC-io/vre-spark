from datetime import datetime
import logging

class Surgery:
    '''
    A surgery maps a case to a chop code.
    Table schema:
    [LFDBEW],[ICPMK],[ICPML],[ANZOP],[BGDOP],[LSLOK],[STORN],[FALNR],[ORGPF]
    '''
    def __init__(self, LFDBEW,ICPMK,ICPML,ANZOP,BGDOP,LSLOK,STORN,FALNR,ORGPF):
        self.bgd_op = datetime.strptime(BGDOP, "%Y-%m-%d") # date of surgery
        self.lfd_bew = LFDBEW
        self.icpmk = ICPMK # catalog ID
        self.icpml = "Z" + ICPML # chop code (comes without leading Z!)
        self.anzop = ANZOP # ??
        self.lslok = LSLOK # ??
        self.fall_nr = FALNR # case ID in SAP!
        self.storn = STORN # cancelled ("" or "X")?
        self.org_pf = ORGPF # ward

        self.chop = None

    def add_surgery_to_case(lines, cases, chops):
        '''
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
        '''
        logging.debug("add_surgery_to_case")
        nr_chop_not_found = 0
        nr_case_not_found = 0
        nr_ok = 0
        for line in lines:
            surgery = Surgery(*line)
            chop = chops.get(surgery.icpml + "_" + surgery.icpmk, None)
            case = cases.get(surgery.fall_nr, None)
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
        logging.info(f"{nr_ok} ok, {nr_case_not_found} cases not found, {nr_chop_not_found} chop codes not found")