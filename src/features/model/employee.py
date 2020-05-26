# -*- coding: utf-8 -*-
"""This script contains the ``Employee`` class used in the VRE model.

-----
"""

import logging

from tqdm import tqdm


class Employee:
    """Models an employee (doctor, nurse, etc) from RAP.
    """

    def __init__(self, id):
        self.id = id

    @staticmethod
    def create_employee_map(lines):
        """Reads the appointment to employee file and creates an Employee().


        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object).
        The underlying table in the Atelier_DataScience is called V_DH_FACT_TERMINMITARBEITER and structured as follows:

        ======== ============= ======================= ======================== ==========
        TERMINID MITARBEITERID TERMINSTART_TS          TERMINENDE_TS            DAUERINMIN
        ======== ============= ======================= ======================== ==========
        521664   0063239       2003-11-11 07:30:00.000 2003-11-11 08:00:00.0000 30.000000
        521754   X33671        2003-11-10 09:15:00.000 2003-11-10 09:45:00.0000 30.000000
        ======== ============= ======================= ======================== ==========

        Args:
            lines (iterator() object):  csv iterator from which data will be read

        Returns:
            dict:
                Dictionary mapping employee_ids to Employee() objects

                :math:`\\longrightarrow` ``{'0032719' : Employee(), ... }``
        """
        logging.debug("create_employee_map")
        employee_dict = dict()
        for line in tqdm(lines):
            employee = line[1]
            employee_dict[employee] = Employee(employee)
        logging.info(f"{len(employee_dict)} employees created")
        return employee_dict

    @staticmethod
    def add_employees_to_appointment(lines, appointments, employees):
        """Adds Employee() in employees to an Appointment().

        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object).
        The underlying table is V_DH_FACT_TERMINMITARBEITER, and is identical to the one defined in the
        create_employee_map() method above.

        Args:
            lines (iterator() object):  csv iterator from which data will be read
            appointments (dict):        Dictionary mapping appointment ids to Appointment() objects

                                        :math:`\\longrightarrow` ``{ '36830543' : Appointment(), ... }``
            employees (dict):           Dictionary mapping employee_ids to Employee() objects

                                        :math:`\\longrightarrow` ``{'0032719' : Employee(), ... }``
        """
        logging.debug("add_employee_to_appointment")
        nr_employee_not_found = 0
        nr_appointment_not_found = 0
        nr_ok = 0
        for line in tqdm(lines):
            employee_id = line[1]
            appointment_id = line[0]
            if appointments.get(appointment_id, None) is None:
                nr_appointment_not_found += 1
                continue
            if employees.get(employee_id, None) is None:
                nr_employee_not_found += 1
                continue
            appointments[appointment_id].add_employee(employees[employee_id])
            nr_ok += 1
        logging.info(f"{nr_ok} employees linked to appointment, {nr_appointment_not_found} appointments not found, "
                     f"{nr_employee_not_found} employees not found")
