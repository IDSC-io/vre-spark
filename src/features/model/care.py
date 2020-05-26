# -*- coding: utf-8 -*-
"""This script contains the ``Care`` class used in the VRE model.

-----
"""

import logging
from datetime import datetime

from tqdm import tqdm

from src.features.model import Employee


class Care:
    """Models an entry in TACS.
    """

    def __init__(
            self,
            patient_id,
            patient_type,
            patient_status,
            case_id,
            case_type,
            case_status,
            date,
            duration_in_minutes,
            employee_nr,
            employee_employment_nr,
            employee_login,
            batch_run_id,
    ):
        self.patient_id = patient_id
        self.case_id = case_id
        try:
            self.date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            self.date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        self.duration_in_minutes = int(duration_in_minutes)
        self.employee_nr = employee_nr

        self.employee = None

    def add_employee(self, employee):
        """Assigns an employee to the ``self.employee`` attribute.

        Note:
            Only one employee can be assigned to Care() objects!

        Args:
            employee (Employee() Object):   Employee() object to assign.
        """
        self.employee = employee

    @staticmethod
    def add_care_entries_to_case(lines, cases, employees):
        """Adds the entries from TACS as instances of Care() objects to the respective Case().

        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object).
        The underlying table in the Atelier_DataScience is called ``TACS_DATEN`` and structured as follows:

        ================= ================ ============== =========== ============= =========== ===================== ====================== ========================== ============================= ================= ============
        patient_patientid patient_typ      patient_status fall_nummer fall_typ      fall_status datum_betreuung       dauer_betreuung_in_min mitarbeiter_personalnummer mitarbeiter_anstellungsnummer mitarbeiter_login BATCH_RUN_ID
        ================= ================ ============== =========== ============= =========== ===================== ====================== ========================== ============================= ================= ============
        00013768220       Standard Patient aktiv          0006422111  Standard Fall aktiv       2018-03-11 00:00:00.0 3                      0301119                    00026556                      I0301119          870
        00000552828       Standard Patient aktiv          0006454306  Standard Fall aktiv       2018-04-10 00:00:00.0 20                     0025908                    00014648                      I0025908          870
        ================= ================ ============== =========== ============= =========== ===================== ====================== ========================== ============================= ================= ============

        Args:
            lines (iterator()   object):   csv iterator from which data will be read
            cases (dict):       Dictionary mapping case ids to Case() objects

                                --> ``{"0003536421" : Case(), "0003473241" : Case(), ...}``
            employees (dict):   Dictionary mapping employee_ids to Employee() objects

                                --> ``{'0032719' : Employee(), ... }``
        """
        logging.debug("add_care_to_case")
        nr_case_not_found = 0
        nr_employee_created = 0
        nr_employee_found = 0
        for line in tqdm(lines):
            care = Care(*line)

            # discard if we don't have the case
            case = cases.get(care.case_id, None)
            if case is None:
                nr_case_not_found += 1
                continue
            case.add_care(care)

            # create employee if not already existing
            employee = employees.get(care.employee_nr, None)
            if employee is None:
                employee = Employee(care.employee_nr)
                nr_employee_created += 1
            else:
                nr_employee_found += 1
            employees[employee.id] = employee

            care.add_employee(employee)
        logging.info(f"{nr_case_not_found} cases not found, "
                     f"{nr_employee_created} employees created, {nr_employee_found} employees found")
