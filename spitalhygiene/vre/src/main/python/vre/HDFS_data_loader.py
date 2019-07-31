# -*- coding: utf-8 -*-
"""This script contains all functions for loading data from CSV or HDFS, and controls the creation of all
objects required for the VRE model.

-----
"""

import subprocess
import csv
import os
import logging
import sys
import configparser

# make sure to append the correct path regardless where script is called from
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'model'))

from Patient import Patient
from Risk import Risk
from Case import Case
from Room import Room
from Move import Move
from Medication import Medication
from Appointment import Appointment
from Device import Device
from Employee import Employee
from Chop import Chop
from Surgery import Surgery
from Partner import Partner
from Care import Care
from ICD import ICD

###############################################################################################################


class HDFS_data_loader:
    """Loads all the csv files from HDFS and creates the data model.
    """

    def __init__(self, hdfs_pipe=True):
        # Load configuration file
        config_reader = configparser.ConfigParser()
        config_reader.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'BasicConfig.ini'))

        self.load_test_data = config_reader['PARAMETERS']['data_basis'] == 'test'
        
        self.base_path = config_reader['PATHS']['model_data_dir'] if self.load_test_data is False \
            else config_reader['PATHS']['test_data_dir']

        logging.debug(f"base_path: {self.base_path}")

        self.devices_path = os.path.join(self.base_path, "V_DH_DIM_GERAET_CUR.csv")
        self.patients_path = os.path.join(self.base_path, "V_DH_DIM_PATIENT_CUR.csv")
        self.cases_path = os.path.join(self.base_path, "V_LA_ISH_NFAL_NORM.csv")
        self.moves_path = os.path.join(self.base_path, "LA_ISH_NBEW.csv")
        self.risks_path = os.path.join(self.base_path, "V_LA_ISH_NRSF_NORM.csv")
        # self.deleted_risks_path = os.path.join(self.base_path, "deleted_screenings.csv")
        self.appointments_path = os.path.join(self.base_path, "V_DH_DIM_TERMIN_CUR.csv")
        self.device_appointment_path = os.path.join(self.base_path, "V_DH_FACT_TERMINGERAET.csv")
        self.appointment_patient_path = os.path.join(self.base_path, "V_DH_FACT_TERMINPATIENT.csv")
        self.rooms_path = os.path.join(self.base_path, "V_DH_DIM_RAUM_CUR.csv")
        self.room_appointment_path = os.path.join(self.base_path, "V_DH_FACT_TERMINRAUM.csv")
        self.appointment_employee_path = os.path.join(self.base_path, "V_DH_FACT_TERMINMITARBEITER.csv")
        self.medication_path = os.path.join(self.base_path, "V_LA_IPD_DRUG_NORM.csv")
        self.bwart_path = os.path.join(self.base_path, "BWTYP-BWART.csv")
        self.partner_path = os.path.join(self.base_path, "LA_ISH_NGPA.csv")
        self.partner_case_path = os.path.join(self.base_path, "LA_ISH_NFPZ.csv")
        self.chop_path = os.path.join(self.base_path, "V_DH_REF_CHOP.csv")
        self.surgery_path = os.path.join(self.base_path, "LA_ISH_NICP.csv")
        self.tacs_path = os.path.join(self.base_path, "TACS_DATEN.csv")
        self.icd_path = os.path.join(self.base_path, "LA_ISH_NDIA_NORM.csv")
        self.VRE_screenings_path = os.path.join(self.base_path, "V_VRE_SCREENING_DATA.csv")

        self.hdfs_pipe = hdfs_pipe  # binary attribute specifying whether to read data Hadoop (True) or CSV (False)

        self.file_delim = config_reader['DELIMITERS']['csv_sep']  # delimiter character for reading CSV files

    def get_hdfs_pipe(self, path):
        """Loads a datafile from HDFS.

        Loads the datafile specified in path from the Hadoop file system, and returns the file **without header** as a
        csv.reader() instance. This function is used in the method patient_data() if hdfs_pipe is ``True``
        (the default).

        Args:
            path (str): full path to file in HDFS to be loaded.

        Returns:
            ``csv.reader()`` instance **not** containing the header of the file.
        """
        logging.debug(f"get_hdfs_pipe: {path}")
        encoding = "iso-8859-1"
        cat = subprocess.Popen(["hadoop", "fs", "-cat", path], stdout=subprocess.PIPE)
        output = cat.communicate()[0].decode(encoding)
        lines = csv.reader(output.splitlines(), delimiter=self.file_delim)
        next(lines, None)  # skip header
        return lines

    def get_csv_file(self, csv_path):
        """Loads a datafile from CSV.

        Loads the datafile specified in csv_path, and returns the file **without header** as a csv.reader() instance.
        ``csv_path`` must be an **absolute** filepath. This function is used in the method patient_data() if hdfs_pipe
        is ``False``.

        Args:
             csv_path (str): full path to csv file.

        Important:
            Since the csv.reader() instance is returned by this functions via ``open(csv_path, ...)``, these files may
            not be properly closed !

        Returns:
            ``csv.reader()`` instance **not** containing the header of the file.
        """
        logging.debug(f"csv_path: {csv_path}")
        encoding = "iso-8859-1"
        output = csv.reader(open(csv_path, 'r', encoding=encoding), delimiter=self.file_delim)
        # Note --> Test Data are ';'-delimited, but original data are ','-delimited !
        next(output, None)  # ignore the header line
        return output

    def patient_data(self, risk_only=False):
        """Prepares patient data based on all results obtained from the SQL queries.

        If self.hdfs_pipe is ``True``, this will use the :meth:`get_hdfs_pipe()` method. Otherwise, the
        :meth:`get_csv_file()` method is used.

        Args:
            risk_only (bool):       Whether or not to use only risk data (defaults to ``False``).

        Returns:
            dict:   Dictionary containing all VRE-relevant objects of the form

                    { "rooms" :math:`\\longrightarrow` *Rooms*,

                    "wards" :math:`\\longrightarrow` *Wards*, etc. }

            Please refer to the ``vre/src/main/python/vre/model`` folder documentation for more details on the
            various objects.
        """
        rooms = dict()  # dictionary mapping room names to Room() objects --> {'BH N 123' : Room(), ... }
        wards = dict()

        logging.info(f"Processing patient data (load_test_data is {self.load_test_data}, hdfs_pipe is {self.hdfs_pipe},"
                     f" base_path set to {self.base_path}).")

        # Load Patient data from table: V_DH_DIM_PATIENT_CUR
        logging.info("loading patient data")
        patients = Patient.create_patient_dict(self.get_hdfs_pipe(self.patients_path) if self.hdfs_pipe is True
                                               else self.get_csv_file(self.patients_path))

        # Load Case data from table: V_LA_ISH_NFAL_NORM
        logging.info("loading case data")
        cases = Case.create_case_map(self.get_hdfs_pipe(self.cases_path) if self.hdfs_pipe is True
                                     else self.get_csv_file(self.cases_path), patients)

        # Load Partner data from table: LA_ISH_NGPA
        partners = Partner.create_partner_map(self.get_hdfs_pipe(self.partner_path) if self.hdfs_pipe is True
                                              else self.get_csv_file(self.partner_path))
        Partner.add_partners_to_cases(  # This will update partners from table: LA_ISH_NFPZ
            self.get_hdfs_pipe(self.partner_case_path) if self.hdfs_pipe is True
            else self.get_csv_file(self.partner_case_path), cases, partners)

        # Load Move data from table: LA_ISH_NBEW
        logging.info("loading move data")
        Move.add_move_to_case(self.get_hdfs_pipe(self.moves_path) if self.hdfs_pipe is True
                              else self.get_csv_file(self.moves_path), cases, rooms, wards, partners)
        # --> Note: Move() objects are not part of the returned dictionary, they are only used in
        #                           Case() objects --> Case().moves = [1 : Move(), 2 : Move(), ...]

        # ----------------------------------------------------------------
        # Load Risk data --> ADJUST THIS SECTION !
        logging.info("loading risk data")
        # ## --> OLD VERSION: from table: V_LA_ISH_NRSF_NORM
        # Risk.add_risk_to_patient( self.get_hdfs_pipe(self.risks_path) if self.hdfs_pipe is True
        # else self.get_csv_file(self.risks_path), patients )
        # Risk.add_deleted_risk_to_patient( # Update data from table: deleted_screenings
        #     self.get_hdfs_pipe(self.deleted_risks_path) if self.hdfs_pipe is True
        # else self.get_csv_file(self.deleted_risks_path), patients
        # )
        # ## --> NEW VERSION: from file VRE_Screenings_Final.csv
        Risk.add_screening_data_to_patients(lines=self.get_hdfs_pipe(self.VRE_screenings_path)
                                            if self.hdfs_pipe is True
                                            else self.get_csv_file(self.VRE_screenings_path), patient_dict=patients)

        if risk_only:
            logging.info("keeping only risk patients")
            patients_risk = dict()
            for patient in patients.values():
                if patient.get_label() > 0:
                    patients_risk[patient.patient_id] = patient
            patients = patients_risk
        logging.info(f"{len(patients)} patients")
        # ----------------------------------------------------------------

        # Load Drug data from table: V_LA_IPD_DRUG_NORM
        logging.info("loading drug data")
        drugs = Medication.create_drug_map(self.get_hdfs_pipe(self.medication_path) if self.hdfs_pipe is True
                                           else self.get_csv_file(self.medication_path))
        Medication.add_medication_to_case(  # Update is based on the same table
            self.get_hdfs_pipe(self.medication_path) if self.hdfs_pipe is True
            else self.get_csv_file(self.medication_path), cases)

        # Load CHOP data from table: V_DH_REF_CHOP
        logging.info("loading chop data")
        chops = Chop.create_chop_dict(self.get_hdfs_pipe(self.chop_path) if self.hdfs_pipe is True
                                      else self.get_csv_file(self.chop_path))

        # Add Surgery data to cases from table: LA_ISH_NICP
        Surgery.add_surgery_to_case(self.get_hdfs_pipe(self.surgery_path) if self.hdfs_pipe is True
                                    else self.get_csv_file(self.surgery_path), cases, chops)
        # Surgery() objects are not part of the returned dictionary

        # Load Appointment data from table: V_DH_DIM_TERMIN_CUR
        logging.info("loading appointment data")
        appointments = Appointment.create_termin_map(self.get_hdfs_pipe(self.appointments_path)
                                                     if self.hdfs_pipe is True
                                                     else self.get_csv_file(self.appointments_path))

        # Add Appointments to cases from table: V_DH_FACT_TERMINPATIENT
        logging.info('Adding appointments to cases')
        Appointment.add_appointment_to_case(self.get_hdfs_pipe(self.appointment_patient_path) if self.hdfs_pipe is True
                                            else self.get_csv_file(self.appointment_patient_path),
                                            cases, appointments)

        # Load Device data from table: V_DH_DIM_GERAET_CUR
        logging.info("loading device data")
        devices = Device.create_device_map(self.get_hdfs_pipe(self.devices_path) if self.hdfs_pipe is True
                                           else self.get_csv_file(self.devices_path))

        # Add Device data to Appointments from table: V_DH_FACT_TERMINGERAET
        Device.add_device_to_appointment(self.get_hdfs_pipe(self.device_appointment_path) if self.hdfs_pipe is True
                                         else self.get_csv_file(self.device_appointment_path),
                                         appointments, devices)

        # Load Employee data (RAP) from table: V_DH_FACT_TERMINMITARBEITER
        logging.info("loading employee data from RAP")
        employees = Employee.create_employee_map(self.get_hdfs_pipe(self.appointment_employee_path)
                                                 if self.hdfs_pipe is True
                                                 else self.get_csv_file(self.appointment_employee_path))

        # Add Employees to Appointments using the same table
        Employee.add_employee_to_appointment(self.get_hdfs_pipe(self.appointment_employee_path)
                                             if self.hdfs_pipe is True
                                             else self.get_csv_file(self.appointment_employee_path),
                                             appointments, employees)

        # Add Care data to Cases from table: TACS_DATEN
        logging.info("Adding Care data to Cases from TACS")
        Care.add_care_to_case(self.get_hdfs_pipe(self.tacs_path) if self.hdfs_pipe is True
                              else self.get_csv_file(self.tacs_path), cases, employees)
        # --> Note: Care() objects are not part of the returned dictionary, they are only used in
        #               Case() objects --> Case().cares = [Care(), Care(), ...] (list of all cares for each case)

        # Add Room data to Appointments from table: V_DH_FACT_TERMINRAUM
        logging.info('Adding rooms to appointments')
        Room.add_room_to_appointment(self.get_hdfs_pipe(self.room_appointment_path) if self.hdfs_pipe is True
                                     else self.get_csv_file(self.room_appointment_path), appointments, rooms)
        logging.info(f"Dataset contains in total {len(rooms)} Rooms")

        # Add ICD codes to cases from table: LA_ISH_NDIA_NORM
        icd_codes = ICD.create_icd_dict(self.get_hdfs_pipe(self.icd_path) if self.hdfs_pipe is True
                                        else self.get_csv_file(self.icd_path))
        ICD.add_icd_to_case(self.get_hdfs_pipe(self.icd_path) if self.hdfs_pipe is True
                            else self.get_csv_file(self.icd_path), cases)

        return dict(
            {
                "rooms": rooms,
                "wards": wards,
                "partners": partners,
                "patients": patients,
                "cases": cases,
                "drugs": drugs,
                "chops": chops,
                "appointments": appointments,
                "devices": devices,
                "employees": employees,
                # "room_id_map": room_id_map,  # --> no longer used
                'icd_codes': icd_codes
            }
        )

