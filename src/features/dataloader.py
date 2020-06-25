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
from configuration.basic_configuration import configuration

# make sure to append the correct path regardless where script is called from
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'model'))

from src.features.model import Patient
from src.features.model import Risk
from src.features.model import Case
from src.features.model import Room
from src.features.model import Move
from src.features.model import Medication
from src.features.model import Appointment
from src.features.model import Device
from src.features.model import Employee
from src.features.model import Chop
from src.features.model import Surgery
from src.features.model import Partner
from src.features.model import Care
from src.features.model import ICDCode

###############################################################################################################


class DataLoader:
    """Loads all the csv files and creates the data model.
    """

    def __init__(self, hdfs_pipe=True):

        self.load_test_data = configuration['PARAMETERS']['dataset'] == 'test'

        if not self.load_test_data:
            self.base_path = configuration['PATHS']['model_data_dir']
        else:
            self.base_path = configuration['PATHS']['test_data_dir']

        logging.debug(f"base_path: {self.base_path}")

        self.devices_path = os.path.join(self.base_path, "DIM_GERAET.csv")
        self.patients_path = os.path.join(self.base_path, "DIM_PATIENT.csv")
        self.cases_path = os.path.join(self.base_path, "DIM_FALL.csv")
        self.moves_path = os.path.join(self.base_path, "LA_ISH_NBEW.csv")
        self.risks_path = os.path.join(self.base_path, "DIM_RISIKO.csv")
        self.appointments_path = os.path.join(self.base_path, "DIM_TERMIN.csv")
        self.device_appointment_path = os.path.join(self.base_path, "FAKT_TERMIN_GERAET.csv")
        self.appointment_patient_path = os.path.join(self.base_path, "FAKT_TERMIN_PATIENT.csv")
        self.rooms_path = os.path.join(self.base_path, "DIM_RAUM.csv")
        self.room_appointment_path = os.path.join(self.base_path, "FAKT_TERMIN_RAUM.csv")
        self.appointment_employee_path = os.path.join(self.base_path, "FAKT_TERMIN_MITARBEITER.csv")
        self.medication_path = os.path.join(self.base_path, "FAKT_MEDIKAMENTE.csv")
        self.partner_path = os.path.join(self.base_path, "LA_ISH_NGPA.csv")
        self.partner_case_path = os.path.join(self.base_path, "LA_ISH_NFPZ.csv")
        self.chop_path = os.path.join(self.base_path, "LA_CHOP_FLAT.csv")
        self.surgery_path = os.path.join(self.base_path, "LA_ISH_NICP.csv")
        self.tacs_path = os.path.join(self.base_path, "TACS_DATEN.csv")
        self.icd_path = os.path.join(self.base_path, "V_LA_ISH_NDIA_NORM.csv")
        self.VRE_screenings_path = os.path.join(self.base_path, "VRE_SCREENING_DATA.csv")

        #self.VRE_ward_screenings_path = os.path.join(self.base_path, "WARD_SCREENINGS.csv")
        #self.oe_pflege_map_path = os.path.join(self.base_path, "OE_PFLEGE_MAP.csv")

        self.hdfs_pipe = hdfs_pipe  # binary attribute specifying whether to read data Hadoop (True) or CSV (False)

        self.file_delim = configuration['DELIMITERS']['csv_sep']  # delimiter character for reading CSV files

        self.load_limit = None if configuration['PARAMETERS']['load_limit'] is None \
                                else configuration['PARAMETERS']['load_limit']

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

    def patient_data(self, load_partners=True, load_medications=True, risk_only=False):
        """Prepares patient data based on all results obtained from the SQL queries.

        If self.hdfs_pipe is ``True``, this will use the :meth:`get_hdfs_pipe()` method. Otherwise, the
        :meth:`get_csv_file()` method is used.

        Args:
            load_partners (bool):   Whether or not to load partners (defaults to ``True``).
            load_medications (bool) Whether to not to load medications (defaults to ``True``).
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
                                               else self.get_csv_file(self.patients_path),
                                               load_limit=self.load_limit)

        # Load Case data from table: V_LA_ISH_NFAL_NORM
        logging.info("loading case data")
        cases = Case.create_case_map(self.get_hdfs_pipe(self.cases_path) if self.hdfs_pipe is True
                                     else self.get_csv_file(self.cases_path), patients,
                                     load_limit=self.load_limit)

        # Load Partner data from table: LA_ISH_NGPA
        partners = {}
        if load_partners:
            logging.info("loading partner data")
            partners = Partner.create_partner_map(self.get_hdfs_pipe(self.partner_path) if self.hdfs_pipe is True
                                                  else self.get_csv_file(self.partner_path))
            logging.info("adding partners to cases")
            Partner.add_partners_to_cases(  # This will update partners from table: LA_ISH_NFPZ
                self.get_hdfs_pipe(self.partner_case_path) if self.hdfs_pipe is True
                else self.get_csv_file(self.partner_case_path), cases, partners)
        else:
            logging.info("loading partner data omitted.")

        # Load Move data from table: LA_ISH_NBEW
        logging.info("loading move data")
        Move.add_moves_to_case(self.get_hdfs_pipe(self.moves_path) if self.hdfs_pipe is True
                               else self.get_csv_file(self.moves_path), cases, rooms, wards, partners,
                               load_limit=self.load_limit)
        # --> Note: Move() objects are not part of the returned dictionary, they are only used in
        #                           Case() objects --> Case().moves = [1 : Move(), 2 : Move(), ...]

        # TODO: ward screenings and care map data is gone. Readd it.
        # # Generate ward screening overview map
        # screen_map = Risk.generate_screening_overview_map(self.get_hdfs_pipe(self.VRE_ward_screenings_path)
        #                                                   if self.hdfs_pipe is True
        #                                                   else self.get_csv_file(self.VRE_ward_screenings_path))
        # # --> this yields a dictionary mapping dt.date() objects to tuples of (ward_name, screening_type)
        # # i.e. of the form {'2018-10-22' : ('O SUED', 'W'), '2018-09-15' : ('IB BLAU', 'E'), ...}
        #
        # # Generate OE_pflege_map
        # oe_pflege_map = Risk.generate_oe_pflege_map(self.get_hdfs_pipe(self.oe_pflege_map_path)
        #                                             if self.hdfs_pipe is True
        #                                             else self.get_csv_file(self.oe_pflege_map_path))

        # --> yields a dictionary mapping "inofficial" ward names to official ones found in the OE_pflege_abk column
        #       of the dbo.INSEL_MAP table in the Atelier_DataScience. This name allows linkage to Waveware !
        # i.e. of the form {'BEWA' : 'C WEST', 'E 121' : 'E 120-21', ...}

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
        # TODO: Analyze how risks are added to patients to ensure VRE-positive patients are properly annotated
        Risk.add_annotated_screening_data_to_patients(self.get_hdfs_pipe(self.VRE_screenings_path)
                                                      if self.hdfs_pipe is True
                                                      else self.get_csv_file(self.VRE_screenings_path),
                                                      patient_dict=patients)

        if risk_only:
            logging.info("keeping only risk patients")
            patients_risk = dict()
            for patient in patients.values():
                if patient.get_screening_label() > 0:
                    patients_risk[patient.patient_id] = patient
            patients = patients_risk
        logging.info(f"{len(patients)} patients")
        # ----------------------------------------------------------------
        # Load Drug data from table: V_LA_IPD_DRUG_NORM
        drugs = {}

        if load_medications:
            logging.info("loading drug data")
            drugs = Medication.create_drug_map(self.get_hdfs_pipe(self.medication_path) if self.hdfs_pipe is True
                                               else self.get_csv_file(self.medication_path))
            Medication.add_medications_to_case(  # Update is based on the same table
                self.get_hdfs_pipe(self.medication_path) if self.hdfs_pipe is True
                else self.get_csv_file(self.medication_path), cases)
        else:
            logging.info("loading drug data omitted.")

        # Load CHOP data from table: V_DH_REF_CHOP
        logging.info("loading chop data")
        chops = Chop.create_chop_map(self.get_hdfs_pipe(self.chop_path) if self.hdfs_pipe is True
                                      else self.get_csv_file(self.chop_path))

        # Add Surgery data to cases from table: LA_ISH_NICP
        Surgery.add_surgeries_to_case(self.get_hdfs_pipe(self.surgery_path) if self.hdfs_pipe is True
                                    else self.get_csv_file(self.surgery_path), cases, chops)
        # Surgery() objects are not part of the returned dictionary

        # Load Appointment data from table: V_DH_DIM_TERMIN_CUR
        logging.info("loading appointment data")
        appointments = Appointment.create_appointment_map(self.get_hdfs_pipe(self.appointments_path)
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
        Employee.add_employees_to_appointment(self.get_hdfs_pipe(self.appointment_employee_path)
                                             if self.hdfs_pipe is True
                                             else self.get_csv_file(self.appointment_employee_path),
                                              appointments, employees)

        # Add Care data to Cases from table: TACS_DATEN
        logging.info("Adding Care data to Cases from TACS")
        Care.add_care_entries_to_case(self.get_hdfs_pipe(self.tacs_path) if self.hdfs_pipe is True
                              else self.get_csv_file(self.tacs_path), cases, employees)
        # --> Note: Care() objects are not part of the returned dictionary, they are only used in
        #               Case() objects --> Case().cares = [Care(), Care(), ...] (list of all cares for each case)

        # Add Room data to Appointments from table: V_DH_FACT_TERMINRAUM
        logging.info('Adding rooms to appointments')
        Room.add_rooms_to_appointment(self.get_hdfs_pipe(self.room_appointment_path) if self.hdfs_pipe is True
                                     else self.get_csv_file(self.room_appointment_path), appointments, rooms)
        logging.info(f"Dataset contains in total {len(rooms)} Rooms")

        # Add ICD codes to cases from table: LA_ISH_NDIA_NORM
        icd_codes = ICDCode.create_icd_code_map(self.get_hdfs_pipe(self.icd_path) if self.hdfs_pipe is True
                                        else self.get_csv_file(self.icd_path))
        ICDCode.add_icd_codes_to_case(self.get_hdfs_pipe(self.icd_path) if self.hdfs_pipe is True
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

