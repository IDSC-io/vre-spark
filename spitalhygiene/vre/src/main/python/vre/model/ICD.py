import datetime
import logging

class ICD:
    def __init__(self, falnr, dkey1, dkat, diadt, drg_cat):
        self.fall_nummer = falnr
        self.icd_code = dkey1
        self.catalogue_year = int(dkat) # catalogue year of the ICD code (2-digit integer) - may or may not be relevant
        self.diagnosis_dt = datetime.datetime.strptime(diadt, '%Y-%m-%d') # date when ICD code was set
        self.drg_category = drg_cat # single character specifying the DRG category

        self.cases = [] # List containing all cases with this particular ICD code (currently not used)

    def add_case(self, case):
        """
        Adds a case to this particular ICD code (currently not used).
        """
        self.cases.append(case)

    def create_icd_dict(lines):
        """
        Creates and returns a dictionary of all icd codes.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: LA_ISH_NDIA
        ['FALNR',       'DKEY1',    'DKAT1',    'DIADT',        'DRG_CATEGORY']
        ['0001832682',  'A41.9',    '17',       '2018-02-27',   'P']
        ['0001832682',  'R65.1',    '17',       '2018-02-27',   'S']

        :return: Dictionary mapping the icd_code entries to ICD() objects --> { 'Z12.8': ICD(), ... }
        """
        logging.debug("Creating ICD dictionary")
        icd_dict = {}

        for each_line in lines:
            this_icd = ICD(*each_line)
            icd_dict[this_icd.icd_code] = this_icd
        # Write success to log and return dictionary
        logging.info(f'Successfully created {len(icd_dict.values())} ICD entries')
        return icd_dict

    def add_icd_to_case(lines, cases):
        """
        Adds ICD codes to cases based on the ICD.fall_nummer attribute. For details on how each line in the lines iterator object is formatted, see the function create_icd_dict() above.

        :param lines: Iterator object returning data directly from csv
        :param cases: Dictionary mapping case ids to Case() objects --> {"0003536421" : Case(), "0003473241" : Case(), ...}
        """
        logging.debug("Adding ICD codes to cases")
        cases_not_found = 0
        cases_found = 0
        unique_case_ids = [] # list of unique case ids processed

        for each_line in lines:
            this_icd = ICD(*each_line)
            if cases.get(this_icd.fall_nummer) is not None: # default value for .get() is None
                cases.get(this_icd.fall_nummer).add_icd_code(this_icd)
                cases_found += 1
                unique_case_ids.append(cases.get(this_icd.fall_nummer))
            else:
                cases_not_found += 1
        logging.info(f'Added {cases_found} ICD codes to {len(set(unique_case_ids))} relevant cases, {cases_not_found} cases not found')










