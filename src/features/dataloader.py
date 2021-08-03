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
from src.features.model.building import Building

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'model'))

from tqdm import tqdm

from src.features.model import Patient
from src.features.model import Risk
from src.features.model import Case
from src.features.model import Room
from src.features.model import Stay
from src.features.model import Medication
from src.features.model import Appointment
from src.features.model import Device
from src.features.model import Employee
from src.features.model import Chop
from src.features.model import Surgery
from src.features.model import Partner
from src.features.model import Treatment
from src.features.model import ICDCode

###############################################################################################################


class DataLoader:
    """Loads all the csv files and creates the data model.
    """

    def __init__(self, hdfs_pipe=False):

        tqdm.pandas()

        self.load_test_data = configuration['PARAMETERS']['dataset'] == 'test'

        self.base_path = configuration['PATHS']['interim_data_dir'].format("test") if configuration['PARAMETERS']['dataset'] == 'test' \
            else configuration['PATHS']['interim_data_dir'].format("model")  # absolute or relative path to directory where data is stored

        logging.debug(f"base_path: {self.base_path}")

        self.patients_path = os.path.join(self.base_path, "DIM_PATIENT.csv")
        self.devices_path = os.path.join(self.base_path, "DIM_GERAET.csv")
        self.buildings_path = os.path.join(self.base_path, "building_identifiers.csv")
        self.rooms_path = os.path.join(self.base_path, "room_identifiers.csv")
        self.cases_path = os.path.join(self.base_path, "DIM_FALL.csv")
        self.stays_path = os.path.join(self.base_path, "LA_ISH_NBEW.csv")

        self.appointments_path = os.path.join(self.base_path, "DIM_TERMIN.csv")
        self.appointment_device_path = os.path.join(self.base_path, "FAKT_TERMIN_GERAET.csv")
        self.appointment_patient_path = os.path.join(self.base_path, "FAKT_TERMIN_PATIENT.csv")
        self.appointment_room_path = os.path.join(self.base_path, "FAKT_TERMIN_RAUM.csv")
        self.appointment_employee_path = os.path.join(self.base_path, "FAKT_TERMIN_MITARBEITER.csv")

        # TODO: Sort data variables by node attributes and edge information
        self.medication_path = os.path.join(self.base_path, "FAKT_MEDIKAMENTE.csv")
        self.partner_path = os.path.join(self.base_path, "LA_ISH_NGPA.csv")
        self.case_partner_path = os.path.join(self.base_path, "LA_ISH_NFPZ.csv")
        self.chop_path = os.path.join(self.base_path, "LA_CHOP_FLAT.csv")
        self.surgery_path = os.path.join(self.base_path, "LA_ISH_NICP.csv")
        self.tacs_care_path = os.path.join(self.base_path, "TACS_DATEN.csv")
        self.icd_codes_path = os.path.join(self.base_path, "V_LA_ISH_NDIA_NORM.csv")
        self.vre_screenings_path = os.path.join(self.base_path, "VRE_SCREENING_DATA.csv")

        #self.VRE_ward_screenings_path = os.path.join(self.base_path, "WARD_SCREENINGS.csv")
        #self.oe_pflege_map_path = os.path.join(self.base_path, "OE_PFLEGE_MAP.csv")

        self.hdfs_pipe = hdfs_pipe  # binary attribute specifying whether to read data Hadoop (True) or CSV (False)

        self.file_delim = configuration['DELIMITERS']['csv_sep']  # delimiter character for reading CSV files

        self.encoding = "iso-8859-1"

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

    def prepare_dataset(self,
                        load_patients=True,
                        load_cases=True,
                        load_partners=True,
                        load_stays=True,
                        load_medications=False,  # TODO: Medications are not used in code and take long to load
                        load_risks=True,
                        risk_only=False,
                        load_chop_codes=True,
                        load_surgeries=True,
                        load_appointments=True,
                        load_devices=True,
                        load_employees=True,
                        load_care_data=True,
                        load_buildings=True,
                        load_rooms=True,
                        load_icd_codes=True,
                        from_range=None,
                        to_range=None,
                        load_fraction=1.0,
                        load_fraction_seed=7):
        """Prepares dataset based on extracted data.

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
        wards = dict()  # TODO: Preload wards if necessary in the future, they are in the rooms_identifiers.csv

        logging.info(f"Processing data (load_test_data is {self.load_test_data}, hdfs_pipe is {self.hdfs_pipe},"
                     f" base_path set to {self.base_path}).")

        # load Patient data from table: DIM_PATIENT
        if load_patients:
            logging.info("[AGENT] loading patient data...")
            patients = Patient.create_patient_dict(self.patients_path, self.encoding,
                                                   load_fraction=load_fraction, load_seed=load_fraction_seed)

        else:
            patients = dict()
            logging.info("[AGENT] loading patients omitted.")

        if load_buildings:
            logging.info("[AGENT ATTRIBUTE] loading building data..")
            buildings = Building.create_building_id_map(self.buildings_path, self.encoding)
        else:
            buildings = dict()
            logging.info("[AGENT ATTRIBUTE] preloading buildings omitted.")

        if load_rooms:
            logging.info("[AGENT] loading room data...")
            rooms, buildings, floors = Room.create_room_id_map(self.rooms_path, buildings, self.encoding, load_limit=self.load_limit)
        else:
            rooms = dict()
            floors = dict()
            logging.info("[AGENT] preloading rooms omitted.")

        # load Case data from table: DIM_FALL
        cases = {}
        partners = {}
        if load_cases or load_partners or load_stays:
            logging.info("[INTERACTION] loading case data...")
            cases = Case.create_case_map(self.cases_path, self.encoding, patients,
                                         load_fraction=load_fraction, load_seed=load_fraction_seed)

            # load Partner data from table: LA_ISH_NGPA
            if load_partners:
                logging.info("[AGENT ATTRIBUTE] loading partner data...")
                partners = Partner.create_partner_map(self.partner_path, encoding=self.encoding)
                logging.info("adding partners to cases")
                Partner.add_partners_to_cases(  # This will update partners from table: LA_ISH_NFPZ
                    self.case_partner_path, self.encoding, cases, partners)
            else:
                logging.info("[AGENT ATTRIBUTE] loading partner data omitted.")

            # load Stay data from table: LA_ISH_NBEW
            if load_stays:
                logging.info("[INTERACTION] loading stay data...")
                Stay.add_stays_to_case(self.stays_path, self.encoding, cases, rooms, wards, partners,
                                       from_range=from_range, to_range=to_range, load_fraction=load_fraction, load_seed=load_fraction_seed)
                # --> Note: Stay() objects are not part of the returned dictionary, they are only used in
                #                           Case() objects --> Case().stays = [1 : Stay(), 2 : Stay(), ...]
            else:
                logging.info("[INTERACTION] loading stays omitted.")
        else:
            logging.info("[INTERACTION] loading cases omitted.")

        # TODO: ward screenings and care map data are broken. Readd it.
        # Generate ward screening overview map
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

        # load Risk data
        if load_risks:
            logging.info("[RISK] loading risk screening data...")
            # ## --> OLD VERSION: from table: V_LA_ISH_NRSF_NORM
            # Risk.add_risk_to_patient( self.get_hdfs_pipe(self.risks_path) if self.hdfs_pipe is True
            # else self.get_csv_file(self.risks_path), patients )
            # Risk.add_deleted_risk_to_patient( # Update data from table: deleted_screenings
            #     self.get_hdfs_pipe(self.deleted_risks_path) if self.hdfs_pipe is True
            # else self.get_csv_file(self.deleted_risks_path), patients)

            # add risks to patients to ensure VRE-positive patients are properly annotated
            Risk.add_annotated_screening_data_to_patients(self.vre_screenings_path,
                                                          self.encoding,
                                                          patient_dict=patients, from_range=from_range, to_range=to_range)
        else:
            logging.info("[RISK] loading risk screening data omitted.")

        if risk_only:
            logging.info("keeping only risk patients")
            patients_risk = dict()
            for patient in patients.values():
                if patient.get_screening_label() > 0:
                    patients_risk[patient.patient_id] = patient
            patients = patients_risk
            logging.info(f"Loaded {len(patients)} patients")

        # load Drug/Medication data from table: FAKT_MEDIKAMENTE
        medications = {}
        if load_medications:
            logging.info("[AGENT ATTRIBUTE] loading medication data...")
            medications = Medication.create_drug_map(self.medication_path, cases, self.encoding)
        else:
            logging.info("[AGENT ATTRIBUTE] loading medication data omitted.")

        # load CHOP surgery codes data from table: LA_CHOP_FLAT
        chops = {}
        if load_chop_codes or load_surgeries:
            logging.info("[AGENT ATTRIBUTE] loading surgeries chop data...")
            chops = Chop.create_chop_map(self.get_hdfs_pipe(self.chop_path) if self.hdfs_pipe is True
                                          else self.get_csv_file(self.chop_path))

            # Add Surgery data to cases from table: LA_ISH_NICP
            logging.info("[AGENT ATTRIBUTE] loading surgeries data...")
            Surgery.add_surgeries_to_case(self.get_hdfs_pipe(self.surgery_path) if self.hdfs_pipe is True
                                          else self.get_csv_file(self.surgery_path), cases, chops)
            # Surgery() objects are not part of the returned dictionary
        else:
            logging.info("[AGENT ATTRIBUTE] loading surgeries and chop data omitted.")

        # load Appointment data from table: DIM_TERMIN
        appointments = {}
        devices = {}
        employees = {}
        if load_appointments or load_devices or load_employees:
            logging.info("[INTERACTON] loading appointment data")
            appointments = Appointment.create_appointment_map(self.appointments_path, self.encoding, from_range, to_range)

            # Add Appointments to cases from table: FAKT_TERMIN_PATIENT
            logging.info('Adding appointments to cases')
            Appointment.add_appointment_to_case(self.get_hdfs_pipe(self.appointment_patient_path) if self.hdfs_pipe is True
                                                else self.get_csv_file(self.appointment_patient_path),
                                                cases, appointments)

            if load_devices:
                # Load Device data from table: DIM_GERAET
                logging.info("[AGENT] loading device data")
                devices = Device.create_device_map(self.get_hdfs_pipe(self.devices_path) if self.hdfs_pipe is True
                                                   else self.get_csv_file(self.devices_path))

                # Add Device data to Appointments from table: FAKT_TERMIN_GERAET
                logging.info("[INTERACTION] adding devices to appointments")
                Device.add_device_to_appointment(self.get_hdfs_pipe(self.appointment_device_path) if self.hdfs_pipe is True
                                                 else self.get_csv_file(self.appointment_device_path),
                                                 appointments, devices)
            else:
                logging.info("[AGENT] loading devices omitted.")

            # load Employee data (RAP) from table: FAKT_TERMIN_MITARBEITER
            if load_care_data or load_employees:
                logging.info("[AGENT] loading employee data from RAP")
                employees = Employee.create_employee_map(self.appointment_employee_path, encoding=self.encoding)

                # Add Employees to Appointments using the same table
                logging.info("[AGENT] add employees to appointments")
                Employee.add_employees_to_appointment(self.get_hdfs_pipe(self.appointment_employee_path)
                                                      if self.hdfs_pipe is True
                                                      else self.get_csv_file(self.appointment_employee_path),
                                                      appointments, employees)
                if load_care_data:
                    # Add Treatment/Care data to Cases from table: TACS_DATEN
                    logging.info("[INTERACTION] Adding Treatment/Care data to Cases from TACS")
                    Treatment.add_care_entries_to_case(self.tacs_care_path, self.encoding, cases, employees, from_range, to_range)
                    # --> Note: Care() objects are not part of the returned dictionary, they are only used in
                    #               Case() objects --> Case().cares = [Care(), Care(), ...] (list of all cares for each case)
                else:
                    logging.info("[INTERACTION] loading treatment/care data omitted.")

            else:
                logging.info("[AGENT] loading employees omitted.")

            # add Room data to Appointments from table: V_DH_FACT_TERMINRAUM
            if load_rooms:
                logging.info('[INTERACTION] Adding rooms to appointments')
                Room.add_rooms_to_appointment(self.get_hdfs_pipe(self.appointment_room_path) if self.hdfs_pipe is True
                                              else self.get_csv_file(self.appointment_room_path), appointments, rooms)
                logging.info(f"Dataset contains in total {len(rooms)} Rooms")
            else:
                logging.info("[INTERACTION] adding rooms to appointments omitted.")
        else:
            logging.info("[INTERACTION] loading appointments omitted.")

        icd_codes = {}
        if load_icd_codes:
            logging.info("[AGENT ATTRIBUTE] Adding ICD codes to cases")
            # Add ICD codes to cases from table: LA_ISH_NDIA_NORM
            icd_codes = ICDCode.create_icd_code_map(self.get_hdfs_pipe(self.icd_codes_path) if self.hdfs_pipe is True
                                            else self.get_csv_file(self.icd_codes_path))
            ICDCode.add_icd_codes_to_case(self.get_hdfs_pipe(self.icd_codes_path) if self.hdfs_pipe is True
                                else self.get_csv_file(self.icd_codes_path), cases)
        else:
            logging.info("[AGENT ATTRIBUTE] loading ICD codes omitted.")

        # load Buildings
        # load Floors
        # improve Room

        dataset = dict(
            {
                "rooms": rooms,
                "floors": floors,
                "buildings": buildings,
                "wards": wards,
                "partners": partners,
                "patients": patients,
                "cases": cases,
                "drugs": medications,
                "chops": chops,
                "appointments": appointments,
                "devices": devices,
                "employees": employees,
                'icd_codes': icd_codes
            }
        )

        logging.info(f"##################################################################################")
        logging.info(f"Dataset load finished.")
        logging.info(f"Data overview:")
        logging.info(f"--> Patients: {len(patients)} [AGENT]")
        logging.info(f"--> Cases: {len(cases)} [INTERACTION]")
        logging.info(f"--> Drugs/Medications: {len(medications)} [AGENT ATTRIBUTE]")
        logging.info(f"--> Chop/Surgery Codes: {len(chops)} [AGENT ATTRIBUTE]")
        logging.info(f"--> ICD Codes: {len(icd_codes)} [AGENT ATTRIBUTE]")

        logging.info(f"--> Rooms: {len(rooms)} [AGENT]")
        logging.info(f"--> Floors: {len(floors)} [AGENT ATTRIBUTE]")
        logging.info(f"--> Buildings: {len(buildings)} [AGENT ATTRIBUTE]")
        logging.info(f"--> Wards: {len(wards)} [AGENT ATTRIBUTE]")

        logging.info(f"--> Partners: {len(partners)} [AGENT]")
        logging.info(f"--> Devices: {len(devices)} [AGENT]")
        logging.info(f"--> Employees: {len(employees)} [AGENT]")

        logging.info(f"--> Appointments: {len(appointments)} [INTERACTION]")

        logging.info(f"##################################################################################")

        return dataset
