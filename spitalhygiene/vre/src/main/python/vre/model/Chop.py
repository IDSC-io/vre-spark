import logging

class Chop:
    def __init__(
        self,
        chop_code,
        chop_verwendungsjahr,
        chop,
        chop_code_level1,
        chop_level1,
        chop_code_level2,
        chop_level2,
        chop_code_level3,
        chop_level3,
        chop_code_level4,
        chop_level4,
        chop_code_level5,
        chop_level5,
        chop_code_level6,
        chop_level6,
        chop_status,
        chop_sap_katalog_id,
    ):
        self.chop_code = chop_code
        self.chop_verwendungsjahr = chop_verwendungsjahr
        self.chop = chop
        self.chop_code_level1 = chop_code_level1
        self.chop_level1 = chop_level1
        self.chop_code_level2 = chop_code_level2
        self.chop_level2 = chop_level2
        self.chop_code_level3 = chop_code_level3
        self.chop_level3 = chop_level3
        self.chop_code_level4 = chop_code_level4
        self.chop_level4 = chop_level4
        self.chop_code_level5 = chop_code_level5
        self.chop_level5 = chop_level5
        self.chop_code_level6 = chop_code_level6
        self.chop_level6 = chop_level6
        self.chop_status = chop_status
        self.chop_sap_katalog_id = chop_sap_katalog_id

        self.cases = []  # Keep track of all cases that have this chop code

    def add_case(self, case):
        """
        Adds a new case to this chop code.
        """
        self.cases.append(case)

    def get_detailed_chop(self):
        '''
        Get the description text from the highest available level for this code.
        :return:
        '''
        for field in [
            self.chop_level6,
            self.chop_level5,
            self.chop_level4,
            self.chop_level3,
            self.chop_level2,
            self.chop_level1,
        ]:
            if field is not None and len(field) > 0 and field != "NULL":
                return field

    def get_lowest_level_code(self):
        '''
        The level 2 code represents the first number of the code.
        E.g.: Z89.07.24 -> Z89
        :return:
        '''
        return self.chop_code_level2

    def create_chop_dict(lines):
        """
        Creates and returns a dict of all chop codes. The key of a chop code is <code>_<catalog> (different catalogs exist for different years).
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_REF_CHOP
        ["CHOPCODE",  "CHOPVERWENDUNGSJAHR", "CHOP",                              "CHOPCODELEVEL1", "CHOPLEVEL1",                                               "CHOPCODELEVEL2",   ...
        ["Z62.99.30", "2016",                "Entnahme von Hoden- oder Neben...", "C11",            "Operationen an den männlichen Geschlechtsorganen (60–64)", "Z62",              ...
        ["Z62.99.99", "2011",                "Sonst. Operationen an den Ho",      "C9",             "Operationen am Verdauungstrakt (42–54)",                   "Z62",              ...
        <-- continued -->
        "CHOPLEVEL2",               "CHOPCODELEVEL3", "CHOPLEVEL3",                         "CHOPCODELEVEL4", "CHOPLEVEL4",                         "CHOPCODELEVEL5", "CHOPLEVEL5", ...
        "Operationen an den Hoden", "Z62.9",          "Sonstige Operationen an den Hoden",  "Z62.99",         "Sonstige Operationen an den Hoden",  "Z62.99.0",       "Detail der Subkategorie 62.99", ...
        "Operationen an den Hoden", "Z62.9",          "Sonstige Operationen an den Hoden",  "Z62.99",         "Sonstige Operationen an den Hoden",  "Z62.99.99",      "Sonstige Operationen an den Hoden, sonstige", ...
        <-- continued -->
        "CHOPCODELEVEL6",   "CHOPLEVEL6",                                                                                   "CHOPSTATUS",   "CHOPSAPKATALOGID"]
        "Z62.99.30",        "Entnahme von Hoden- oder Nebenhodengewebe zur Aufbereitung für die künstliche Insemination",   "0",            "16"]
        "",                 "",                                                                                             "1",            "10"]

        :return Dictionary mapping the chopcode_katalogid entries to Chop() obects --> { 'Z39.61.10_11': Chop(), ... }
        """
        logging.debug("create_chop_dict")
        chops = dict()
        for line in lines:
            chop = Chop(*line)
            chops[chop.chop_code + "_" + chop.chop_sap_katalog_id] = chop # based on the schema in the docstring, this would yield "Z62.99.30_16" or "Z62.99.99_10"
        logging.info(f"{len(chops)} chops created")
        return chops

    def chop_code_stats(self, chops):
        """
        Print frequency of different chop codes.
        """
        for chop in chops.values():
            print(
                str(len(chop.cases))
                + ": "
                + chop.get_detailed_chop()
                + " "
                + chop.chop_code
            )
