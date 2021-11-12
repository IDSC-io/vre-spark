# -*- coding: utf-8 -*-
"""This script contains the ``Medication`` class used in the VRE model.

-----
"""

import logging
from datetime import datetime
import concurrent.futures
import pandas as pd

from tqdm import tqdm


class Medication:
    """Models a ``Medication`` object.
    """

    def __init__(
            self,
            patient_id,
            case_id,
            drug_submission,
            drug_text,
            drug_atc,
            drug_quantity,
            drug_unit,
            drug_dispform,
    ):
        self.patient_id = patient_id.zfill(11)  # extend the patient id to length 11 to get a standardized representation
        self.case_id = case_id
        self.drug_text = drug_text
        self.drug_atc = drug_atc
        self.drug_quantity = drug_quantity
        self.drug_unit = drug_unit
        self.drug_dispform = drug_dispform
        self.drug_submission = drug_submission

    def is_antibiotic(self):
        """Returns the antibiotic status of a Medication.

        Antibiotics are identified via the prefix ``J01`` in the ``self.drug_atc`` attribute.

        Returns:
            bool:   Whether or not the medication ``self.drug_atc`` attribute starts with ``J01``.
        """
        return self.drug_atc.startswith("J01")

    @staticmethod
    def create_drug_map(csv_path, cases, encoding, is_verbose=True):
        """Creates a dictionary of ATC codes to human readable drug names.

        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object).
        The underlying table in the Atelier_DataScience is called V_LA_IPD_DRUG_NORM and structured as follows:

        =========== ========== ======================================== ======== =============== ========= =============== ===========================
        PATIENTID   CASEID     DRUG_TEXT                                DRUG_ATC DRUG_QUANTITY   DRUG_UNIT DRUG_DISPFORM   DRUG_SUBMISSION
        =========== ========== ======================================== ======== =============== ========= =============== ===========================
        00001711342 0006437617 Torem Tabl 10 mg (Torasemid)             C03CA04  2.0000000000000 Stk       p.o.            2018-03-24 09:52:28.0000000
        00001711342 0006437617 Ecofenac Sandoz Lipogel 1 % (Diclofenac) M02AA15  1.0000000000000 Dos       lokal / topisch 2018-03-24 09:52:28.0000000
        =========== ========== ======================================== ======== =============== ========= =============== ===========================

        Args:
            lines (iterator() object):  csv iterator from which data will be read

        Returns:
            dict:
                Dictionary mapping drug codes to their respective text description

                :math:`\\longrightarrow` ``{'B02BA01' : 'NaCl Braun Inf LÃ¶s 0.9 % 500 ml (Natriumchlorid)', ... }``
        """
        logging.debug("create_drug_map")
        nr_cases_not_found = 0
        medications = dict()
        medication_df = pd.read_csv(csv_path, encoding=encoding, parse_dates=["Submission Date"], dtype=str)
        # medication_objects = medication_df.progress_apply(lambda row: Medication(*row.to_list()), axis=1)
        medication_objects = list(map(lambda row: Medication(*row), tqdm(medication_df.values.tolist(), disable=not is_verbose)))
        del medication_df
        # TODO: This generates just a list of medications for each case, but this might not be what we want.
        for medication in tqdm(medication_objects):
            if medications.get(medication.case_id, None) is None:
                medications[medication.case_id] = []
            medications[medication.case_id].append(medication)
            
            if medication.case_id not in cases:
                nr_cases_not_found += 1
                continue

            cases.get(medication.case_id).add_medication(medication)

        logging.info(f"{len(medications)} medications created, {nr_cases_not_found} cases not found")
        return medications

    def __repr__(self):
        return str(dict((key, value) for key, value in self.__dict__.items()
                    if not callable(value) and not key.startswith('__')))

    def __str__(self):
        return self.__repr__()