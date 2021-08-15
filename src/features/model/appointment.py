# -*- coding: utf-8 -*-
"""This script contains the ``Appointment`` class used in the VRE model.

-----
"""

import itertools
import logging
from datetime import timedelta

import pandas as pd
from tqdm import tqdm


class Appointment:
    """Models an appointment from RAP.
    """

    def __init__(self, id, is_deleted, description, type_nr, type, date, duration_in_mins):
        self.id = id
        self.is_deleted = is_deleted
        self.description = description
        self.type_nr = type_nr
        self.type = type
        self.date = date
        self.start_datetime = None
        self.end_datetime = None

        self.case = None

        try:
            self.duration_in_mins = int(float(duration_in_mins))
        except ValueError as e:
            self.duration_in_mins = 0

        self.start_datetime = self.date
        self.end_datetime = self.date + timedelta(self.duration_in_mins)

        self.devices = []
        self.employees = []
        self.rooms = []

    def add_device(self, device):
        """Adds a device to the self.devices() list of this appointment.

        Args:
            device (Device() Object):   Device() object to append to this appointment.
        """
        self.devices.append(device)

    def add_room(self, room):
        """Adds a room to the self.rooms() list of this appointment.

        Args:
            room (Room() Object):   Room() object to append to this appointment.
        """
        self.rooms.append(room)

    def add_employee(self, employee):
        """Adds an employee to the self.employees() list of this appointment.

        Args:
            employee (Employee() Object):   Employee() object to append to this appointment.
        """
        self.employees.append(employee)

    @staticmethod
    def create_appointment_map(csv_path, encoding, from_range=None, to_range=None, is_verbose=True):
        """Loads the appointments from a csv reader instance.

        This function will be called by the ``HDFS_data_loader.patient_data()`` function (lines is an iterator object).
        The underlying table in the Atelier_DataScience is named ``V_DH_DIM_TERMIN_CUR`` and structured as follows:

        ======== ========== ================= ================ =============== ======================== ==========
        TERMINID IS_DELETED TERMINBEZEICHNUNG TERMINART        TERMINTYP       TERMINDATUM              DAUERINMIN
        ======== ========== ================= ================ =============== ======================== ==========
        957219   0          K90 HINF          K90 HINF         Patiententermin 2005-02-04 00:00:00.0000 90.00000
        957224   0          Konsultation 15'  Konsultation 15' Patiententermin 2005-02-03 00:00:00.0000 15.00000
        ======== ========== ================= ================ =============== ======================== ==========

        Args:
            lines (iterator() object):   csv iterator from which data will be read

        Returns:
            dict:
                Dictionary mapping appointment ids to Appointment() objects

                --> ``{ '36830543' : Appointment(), ... }``
        """
        logging.debug("create_appointment_map")
        nr_malformed = 0
        nr_ok = 0
        appointments = dict()
        appointment_df = pd.read_csv(csv_path, encoding=encoding, parse_dates=["Date"], dtype=str)

        if from_range is not None:
            appointment_df = appointment_df.loc[appointment_df['Date'] > from_range]

        if to_range is not None:
            appointment_df = appointment_df.loc[appointment_df['Date'] <= to_range]

        # appointment_objects = appointment_df.progress_apply(lambda row: Appointment(*row.to_list()), axis=1)
        appointment_objects = list(map(lambda row: Appointment(*row), tqdm(appointment_df.values.tolist(), disable=not is_verbose)))
        del appointment_df
        for appointment in tqdm(appointment_objects, disable=not is_verbose):
            appointments[appointment.id] = appointment
            nr_ok += 1

        logging.info(f"{nr_ok} appointments ok, {nr_malformed} appointments malformed")
        return appointments

    @staticmethod
    def add_appointment_to_case(lines, cases, appointments, is_verbose=True):
        """Adds Appointment() objects to the SAP cases based on lines read from a csv file.

        This function will be called by the ``HDFS_data_loader.patient_data()`` function (lines is an iterator object).
        The underlying table in the Atelier_DataScience is called ``V_DH_FACT_TERMINPATIENT`` and structured as follows:

        ======== =========== ==========
        TERMINID PATIENTID   FALLID
        ======== =========== ==========
        35672314 00008210020 0005660334
        17255155 00002042800 0004017880
        ======== =========== ==========

        Args:
            lines (iterator() object):   csv iterator from which data will be read
            cases (dict):               Dictionary mapping case ids to Case() objects

                                        --> ``{ "0003536421" : Case(), "0003473241" : Case(), ...}``

            appointments (dict):        Dictionary mapping appointment ids to Appointment() objects

                                        --> ``{ '36830543' : Appointment(), ... }``
        """
        logging.debug("add_appointment_to_case")
        nr_appointment_not_found = 0
        nr_case_not_found = 0
        nr_ok = 0
        lines_iters = itertools.tee(lines, 2)
        for line in tqdm(lines_iters[1], total=sum(1 for _ in lines_iters[0]), disable=not is_verbose):
            appointment_id = line[0]
            case_id = line[2]
            if appointments.get(appointment_id, None) is None:
                nr_appointment_not_found += 1
                continue
            if cases.get(case_id, None) is None:
                nr_case_not_found += 1
                continue
            cases[case_id].add_appointment(appointments[appointment_id])
            appointments[appointment_id].case = cases[case_id]
            nr_ok += 1

        deleted_appointments = [appointment_id for appointment_id, appointment in appointments.items() if appointment.case is None]

        for a in deleted_appointments:
            appointments.pop(a)

        logging.info(f"{nr_ok} appointments linked to cases, {nr_case_not_found} cases not found, "
                     f"{nr_appointment_not_found} appointments not found, {len(deleted_appointments)} appointments deleted without case")
