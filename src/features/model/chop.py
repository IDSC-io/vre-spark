# -*- coding: utf-8 -*-
"""This script contains the ``CHOP`` class used in the VRE model.

-----
"""

import logging

from tqdm import tqdm
import itertools


class Chop:
    """Models a ``CHOP`` code.
    """

    def __init__(
            self,
            chop_sap_catalog_id,
            chop_code,
            chop_year_of_usage,
            chop_description,
            chop_code_level1,
            chop_level1_description,
            chop_code_level2,
            chop_level2_description,
            chop_code_level3,
            chop_level3_description,
            chop_code_level4,
            chop_level4_description,
            chop_code_level5,
            chop_level5_description,
            chop_code_level6,
            chop_level6_description,
            chop_status
    ):
        self.chop_sap_catalog_id = chop_sap_catalog_id
        self.chop_code = chop_code
        self.chop_year_of_usage = chop_year_of_usage
        self.chop_description = chop_description
        self.chop_code_level1 = chop_code_level1
        self.chop_level1_description = chop_level1_description
        self.chop_code_level2 = chop_code_level2
        self.chop_level2_description = chop_level2_description
        self.chop_code_level3 = chop_code_level3
        self.chop_level3_description = chop_level3_description
        self.chop_code_level4 = chop_code_level4
        self.chop_level4_description = chop_level4_description
        self.chop_code_level5 = chop_code_level5
        self.chop_level5_description = chop_level5_description
        self.chop_code_level6 = chop_code_level6
        self.chop_level6_description = chop_level6_description
        self.chop_status = chop_status

        self.cases = []  # Keep track of all cases that have this chop code

    def add_case(self, case):
        """Adds a case to the self.cases list.

        Args:
            case (Case() Object):   Case() object to append.
        """
        self.cases.append(case)

    def get_detailed_chop(self):
        """Returns description text from the highest available level for this CHOP.

        Returns:
            str:    Highest available level for this CHOP code.
        """
        for field in [
            self.chop_level6_description,
            self.chop_level5_description,
            self.chop_level4_description,
            self.chop_level3_description,
            self.chop_level2_description,
            self.chop_level1_description,
        ]:
            if field is not None and len(field) > 0 and field != "NULL":
                return field

    def get_lowest_level_code(self):
        """Returns the lowest level of the CHOP code.

        The lowest level is is level 2 and represented as the *first number* of the Code:

        ``Z89.07.24`` :math:`\\longrightarrow` ``Z89``

        Returns:
            str:    Lowest available level for this CHOP code.
        """
        return self.chop_code_level2

    @staticmethod
    def create_chop_map(lines, is_verbose=True):
        """Creates and returns a dict of all chop codes.

        The key of a chop code is ``<code>_<catalog>`` - different catalogs exist for different years. This function
        will be called by the ``HDFS_data_loader.patient_data()`` function (lines is an iterator object). The underlying
        table in Atelier_DataScience is called ``V_DH_REF_CHOP`` and structured as follows:

        ========= =================== ================================= ============== =========================================================== ============== ======================== ============== ================================= ============== ================================= ============== ==================================== ============== ================================== ========== ================
        CHOPCODE  CHOPVERWENDUNGSJAHR CHOP                              CHOPCODELEVEL1 CHOPLEVEL1                                                  CHOPCODELEVEL2 CHOPLEVEL2               CHOPCODELEVEL3 CHOPLEVEL3                        CHOPCODELEVEL4 CHOPLEVEL4                        CHOPCODELEVEL5 CHOPLEVEL5                           CHOPCODELEVEL6 CHOPLEVEL6                         CHOPSTATUS CHOPSAPKATALOGID
        ========= =================== ================================= ============== =========================================================== ============== ======================== ============== ================================= ============== ================================= ============== ==================================== ============== ================================== ========== ================
        Z62.99.30 2016                Entnahme von Hoden- oder Neben... C11            Operationen an den männlichen Geschlechtsorganen (60–64)    Z62            Operationen an den Hoden Z62.9          Sonstige Operationen an den Hoden Z62.99         Sonstige Operationen an den Hoden Z62.99.0       Detail der Subkategorie 62.99        Z62.99.30      Entnahme von Hoden- oder Nebenh... 0          16
        Z62.99.99 2011                Sonst. Operationen an den Ho...   C9             Operationen am Verdauungstrakt (42–54)                      Z62            Operationen an den Hoden Z62.9          Sonstige Operationen an den Hoden Z62.99         Sonstige Operationen an den Hoden Z62.99.99      Sonstige Operationen an den Hoden...                                                   1          10
        ========= =================== ================================= ============== =========================================================== ============== ======================== ============== ================================= ============== ================================= ============== ==================================== ============== ================================== ========== ================

        Args:
            lines (iterator() object):   csv iterator from which data will be read

        Returns:
            dict:
                Dictionary mapping the chopcode_katalogid entries to Chop() objects

                :math:`\\longrightarrow` ``{ 'Z39.61.10_11': Chop(), ... }``
        """
        logging.debug("create_chop_dict")
        chops = dict()
        lines_iters = itertools.tee(lines, 2)
        for line in tqdm(lines_iters[1], total=sum(1 for _ in lines_iters[0]), disable=not is_verbose):
            chop = Chop(*line)
            chops[chop.chop_code + "_" + chop.chop_sap_catalog_id] = chop
            # based on the schema in the docstring, this would yield "Z62.99.30_16" or "Z62.99.99_10"
        logging.info(f"{len(chops)} chops created")
        return chops

    @staticmethod
    def chop_code_stats(chops):
        """Print frequency of different chop codes to console.

        Args:
            chops (dict):   Dictionary mapping the chopcode_katalogid entries to Chop() objects

                            :math:`\\longrightarrow` ``{ 'Z39.61.10_11': Chop(), ... }``
        """
        for chop in chops.values():
            print(
                str(len(chop.cases))
                + ": "
                + chop.get_detailed_chop()
                + " "
                + chop.chop_code
            )
