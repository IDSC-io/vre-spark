# -*- coding: utf-8 -*-
"""This script contains the ``ICD`` class used in the VRE model.

-----
"""

import datetime
import logging

from tqdm import tqdm


class ICDCode:
    """Models an ``ICD`` object.
    """

    def __init__(self, case_nr, icd_code, catalogue_year, diagnosis_date, drg_category):
        self.case_nr = case_nr
        self.icd_code = icd_code
        self.catalogue_year = int(catalogue_year)  # catalogue year of the ICD code (2-digit integer) - may or may not be relevant
        self.diagnosis_date = datetime.datetime.strptime(diagnosis_date, '%Y-%m-%d')  # date when ICD code was set
        self.drg_category = drg_category  # single character specifying the DRG category

        self.cases = []  # List containing all cases with this particular ICD code (currently not used)

    def add_case(self, case):
        """Adds a case to this ICD's ``self.cases`` attribute.

        Args:
            case (Case() Object):   Case() object to add.
        """
        self.cases.append(case)

    @staticmethod
    def create_icd_code_map(lines):
        """Creates and returns a dictionary of all icd codes.

        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object).
        The underlying table in the Atelier_DataScience is LA_ISH_NDIA and structured as follows:

        ============ ======= ======= ============ ==============
        'FALNR'      'DKEY1' 'DKAT1' 'DIADT'      'DRG_CATEGORY'
        ============ ======= ======= ============ ==============
        '0001832682' 'A41.9' '17'    '2018-02-27' 'P'
        '0001832682' 'R65.1' '17'    '2018-02-27' 'S'
        ============ ======= ======= ============ ==============

        Args:
            lines (iterator() object):  csv iterator from which data will be read

        Returns:
            dict:
                Dictionary mapping the icd_code entries to ICD() objects

                :math:`\\longrightarrow` ``{ 'Z12.8': ICD(), ... }``
        """
        logging.debug("Creating ICD dictionary")
        icd_dict = {}

        lines_iters = itertools.tee(lines, 2)
        for line in tqdm(lines_iters[1], total=sum(1 for _ in lines_iters[0])):
            this_icd = ICDCode(*each_line)
            icd_dict[this_icd.icd_code] = this_icd
        # Write success to log and return dictionary
        logging.info(f'Successfully created {len(icd_dict.values())} ICD entries')
        return icd_dict

    @staticmethod
    def add_icd_codes_to_case(lines, cases):
        """Adds ICD codes to cases based on the ICD.fall_nummer attribute.

        For details on how each line in the lines iterator object is formatted, please refer to the function
        create_icd_dict() above.

        Args:
            lines (iterator() object):  csv iterator from which data will be read
            cases (dict):               Dictionary mapping case ids to Case() objects
                                        :math:`\\longrightarrow` ``{"0003536421" : Case(), "0003473241" : Case(), ...}``
        """
        logging.debug("Adding ICD codes to cases")
        cases_not_found = 0
        cases_found = 0
        unique_case_ids = []  # list of unique case ids processed

        for each_line in tqdm(lines):
            this_icd = ICDCode(*each_line)
            if cases.get(this_icd.case_nr) is not None:  # default value for .get() is None
                cases.get(this_icd.case_nr).add_icd_code(this_icd)
                cases_found += 1
                unique_case_ids.append(cases.get(this_icd.case_nr))
            else:
                cases_not_found += 1
        logging.info(f'Added {cases_found} ICD codes to {len(set(unique_case_ids))} relevant cases,'
                     f'{cases_not_found} cases not found')
