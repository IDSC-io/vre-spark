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
from src.features.model import RiskScreening
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


        self.patients_path = os.path.join(self.base_path, "DIM_PATIENT.csv")                                   # AGENT
        self.medication_path = os.path.join(self.base_path, "FAKT_MEDIKAMENTE.csv")                            # AGENT ATTRIBUTE
        self.icd_codes_path = os.path.join(self.base_path, "V_LA_ISH_NDIA_NORM.csv")                           # AGENT ATTRIBUTE
        self.vre_screenings_path = os.path.join(self.base_path, "VRE_SCREENING_DATA.csv")                      # AGENT ATTRIBUTE

        self.cases_path = os.path.join(self.base_path, "DIM_FALL.csv")                                         # INTERACTION
        self.stays_path = os.path.join(self.base_path, "LA_ISH_NBEW.csv")                                      # INTERACTION
        self.appointments_path = os.path.join(self.base_path, "DIM_TERMIN.csv")                                # INTERACTION
        self.appointment_patient_path = os.path.join(self.base_path, "FAKT_TERMIN_PATIENT.csv")                # INTERACTION
        self.appointment_device_path = os.path.join(self.base_path, "FAKT_TERMIN_GERAET.csv")                  # INTERACTION
        self.appointment_room_path = os.path.join(self.base_path, "FAKT_TERMIN_RAUM.csv")                      # INTERACTION
        self.appointment_employee_path = os.path.join(self.base_path, "FAKT_TERMIN_MITARBEITER.csv")           # INTERACTION + AGENT
        self.surgery_path = os.path.join(self.base_path, "LA_ISH_NICP.csv")                                    # INTERACTION
        self.chop_path = os.path.join(self.base_path, "LA_CHOP_FLAT.csv")                                      # INTERACTION ATTRIBUTE
        self.tacs_care_path = os.path.join(self.base_path, "TACS_DATEN.csv")                                   # INTERACTION

        self.devices_path = os.path.join(self.base_path, "DIM_GERAET.csv")                                     # AGENT
        self.rooms_path = os.path.join(self.base_path, "room_identifiers.csv")                                 # AGENT
        self.buildings_path = os.path.join(self.base_path, "building_identifiers.csv")                         # AGENT ATTRIBUTE

        self.partner_path = os.path.join(self.base_path, "LA_ISH_NGPA.csv")                                    # INTERACTION ATTRIBUTE (?) COULD BE AGENT
        self.case_partner_path = os.path.join(self.base_path, "LA_ISH_NFPZ.csv")                               # INTERACTION ATTRIBUTE (?)

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
                        load_risks=True,
                        load_medications=False,  # TODO: Medications are not used in code and take long to load
                        load_icd_codes=True,     # TODO: ICD Codes are not used in the code

                        load_cases=True,
                        load_partners=True,      # TODO: Partners are not used in the code
                        load_stays=True,
                        load_appointments=True,
                        load_care_data=True,
                        load_surgeries=True,
                        load_chop_codes=True,

                        load_employees=True,
                        load_devices=True,
                        load_rooms=True,
                        load_buildings=True,

                        risk_only=False,

                        # limit loading via time ranges
                        from_range=None,
                        to_range=None,

                        # limit loading regarding patient locations
                        load_patients_in_locations=None,

                        load_fraction=1.0,
                        load_fraction_seed=7,
                        is_verbose=True):
        """Prepares dataset based on extracted data.

        Args:
            load_partners (bool):   Whether or not to load partners (defaults to ``True``).
            load_medications (bool) Whether to not to load medications (defaults to ``True``).
            risk_only (bool):       Whether or not to use only risk data (defaults to ``False``).

                    :param load_patients: load patients
                    :param load_risks: load patient risk node attribute
                    :param load_medications: load patient medications
                    :param load_icd_codes: load ICD-10 code diagnoses
                    :param load_cases: load cases of patients
                    :param load_partners: load medical partners of medical process
                    :param load_stays: load statys, which relate patients to rooms
                    :param load_appointments: load appointments, which relate patients, rooms, devices and employees
                    :param load_care_data:  load treatments, which relate patients and employees
                    :param load_surgeries: load surgeries
                    :param load_chop_codes: load chop-codes
                    :param load_employees: load employees
                    :param load_devices: load devices
                    :param load_rooms: load rooms
                    :param load_buildings: load buildings, which relate rooms to geographical locations

                    :param risk_only:

                    :param from_range: entities selected with interactions only starting from
                    :param to_range: entities select with interaction only going on up to

                    :param load_patients_in_locations: load only patients residing in indicated locations
                    :param load_fraction: load only a fraction of data (debugging purposes)
                    :param load_fraction_seed: load fraction with fixed seed (for reproducibility)
                    :param is_verbose: be verbose during load
        Returns:
            dict:   Dictionary containing all model objects of the form

                    { "rooms" :math:`\\longrightarrow` *Rooms*,

                    "wards" :math:`\\longrightarrow` *Wards*, etc. }

        """
        if load_patients_in_locations is None:
            load_patients_in_locations = []
         
        wards = dict()  # TODO: Preload wards if necessary in the future, they are in the rooms_identifiers.csv

        if is_verbose:
            logging.info(f"Processing data (load_test_data is {self.load_test_data}, hdfs_pipe is {self.hdfs_pipe},"
                         f" base_path set to {self.base_path}).")

        # load Patient data from table: DIM_PATIENT
        if load_patients or load_risks or risk_only or load_medications:
            if is_verbose:
                logging.info("[AGENT] loading patient data...")
            patients = Patient.create_patient_dict(self.patients_path, self.encoding,
                                                   load_fraction=load_fraction, load_seed=load_fraction_seed, is_verbose=is_verbose)

            # load Risk data
            if load_risks:
                if is_verbose:
                    logging.info("[AGENT ATTRIBUTE] loading risk screening data...")
                # add risks to patients to ensure VRE-positive patients are properly annotated
                RiskScreening.add_annotated_screening_data_to_patients(self.vre_screenings_path,
                                                              self.encoding,
                                                              patient_dict=patients, from_range=from_range, to_range=to_range, is_verbose=is_verbose)
            else:
                if is_verbose:
                    logging.info("[AGENT ATTRIBUTE] loading risk screening data omitted.")

            if risk_only:
                if is_verbose:
                    logging.info("keeping only risk patients")
                patients_risk = dict()
                for patient in patients.values():
                    if patient.get_screening_label() > 0:
                        patients_risk[patient.patient_id] = patient
                patients = patients_risk
                if is_verbose:
                    logging.info(f"Keeping {len(patients)} risk patients")
        else:
            patients = dict()
            if is_verbose:
                logging.info("[AGENT] loading patients omitted.")

        if load_buildings:
            if is_verbose:
                logging.info("[AGENT ATTRIBUTE] loading building data..")
            buildings = Building.create_building_id_map(self.buildings_path, self.encoding, is_verbose=is_verbose)
        else:
            buildings = dict()
            if is_verbose:
                logging.info("[AGENT ATTRIBUTE] preloading buildings omitted.")

        if load_rooms:
            if is_verbose:
                logging.info("[AGENT] loading room data...")
            rooms, buildings, floors = Room.create_room_id_map(self.rooms_path, buildings, self.encoding, load_limit=self.load_limit, is_verbose=is_verbose)
        else:
            rooms = dict()
            floors = dict()
            if is_verbose:
                logging.info("[AGENT] preloading rooms omitted.")

        # load Case data from table: DIM_FALL
        cases = {}
        partners = {}
        medications = {}
        if load_cases or load_partners or load_stays:
            if is_verbose:
                logging.info("[INTERACTION] loading case data...")
            cases = Case.create_case_map(self.cases_path, self.encoding, patients,
                                         load_fraction=load_fraction, load_seed=load_fraction_seed, is_verbose=is_verbose)

            # load Drug/Medication data from table: FAKT_MEDIKAMENTE
            if load_medications:
                if is_verbose:
                    logging.info("[AGENT ATTRIBUTE] loading medication data...")
                medications = Medication.create_drug_map(self.medication_path, cases, self.encoding, is_verbose=is_verbose)
            else:
                if is_verbose:
                    logging.info("[AGENT ATTRIBUTE] loading medication data omitted.")

            # load Partner data from table: LA_ISH_NGPA
            if load_partners:
                if is_verbose:
                    logging.info("[INTERACTION ATTRIBUTE] loading partner data...")
                partners = Partner.create_partner_map(self.partner_path, encoding=self.encoding, is_verbose=is_verbose)
                logging.info("adding partners to cases")
                Partner.add_partners_to_cases(  # This will update partners from table: LA_ISH_NFPZ
                    self.case_partner_path, self.encoding, cases, partners, is_verbose=is_verbose)
            else:
                if is_verbose:
                    logging.info("[INTERACTION ATTRIBUTE] loading partner data omitted.")

            # load Stay data from table: LA_ISH_NBEW
            if load_stays:
                if is_verbose:
                    logging.info("[INTERACTION] loading stay data...")
                Stay.add_stays_to_case(self.stays_path, self.encoding, cases, rooms, wards, partners,
                                       from_range=from_range, to_range=to_range, locations=load_patients_in_locations,
                                       load_fraction=load_fraction, load_seed=load_fraction_seed, is_verbose=is_verbose)
                # --> Note: Stay() objects are not part of the returned dictionary, they are only used in
                #                           Case() objects --> Case().stays = [1 : Stay(), 2 : Stay(), ...]

                if len(load_patients_in_locations) != 0:
                    nr_non_location_patients = 0
                    location_patients = dict()
                    for patient in patients.values():
                        if len(patient.get_stays()) != 0:
                            location_patients[patient.patient_id] = patient
                        else:
                            nr_non_location_patients += 1
                            # drop cases of excluded patient
                            for case_id in patient.cases:
                                cases.pop(case_id)
                    patients = location_patients
                    logging.info(f"Excluded {nr_non_location_patients} patients without stay in locations {load_patients_in_locations}")
            else:
                if is_verbose:
                    logging.info("[INTERACTION] loading stays omitted.")
        else:
            if is_verbose:
                logging.info("[INTERACTION] loading cases, partners and stays omitted.")

        # load Appointment data from table: DIM_TERMIN
        appointments = {}
        devices = {}
        employees = {}
        if load_appointments or load_devices or load_employees:
            if is_verbose:
                logging.info("[INTERACTON] loading appointment data")
            appointments = Appointment.create_appointment_map(self.appointments_path, self.encoding, from_range, to_range, is_verbose=is_verbose)

            # Add Appointments to cases from table: FAKT_TERMIN_PATIENT
            if is_verbose:
                logging.info('Adding appointments to cases')
            Appointment.add_appointment_to_case(self.get_hdfs_pipe(self.appointment_patient_path) if self.hdfs_pipe is True
                                                else self.get_csv_file(self.appointment_patient_path),
                                                cases, appointments, is_verbose=is_verbose)

            if load_devices:
                # Load Device data from table: DIM_GERAET
                if is_verbose:
                    logging.info("[AGENT] loading devices")
                devices = Device.create_device_map(self.get_hdfs_pipe(self.devices_path) if self.hdfs_pipe is True
                                                   else self.get_csv_file(self.devices_path), is_verbose=is_verbose)

                # Add Device data to Appointments from table: FAKT_TERMIN_GERAET
                if is_verbose:
                    logging.info("[INTERACTION] adding devices to appointments")
                Device.add_device_to_appointment(self.get_hdfs_pipe(self.appointment_device_path) if self.hdfs_pipe is True
                                                 else self.get_csv_file(self.appointment_device_path),
                                                 appointments, devices, is_verbose=is_verbose)
            else:
                if is_verbose:
                    logging.info("[AGENT] loading devices omitted.")

            # add Room data to Appointments from table: V_DH_FACT_TERMINRAUM
            if load_rooms:
                if is_verbose:
                    logging.info('[INTERACTION] Adding rooms to appointments')
                Room.add_rooms_to_appointment(self.get_hdfs_pipe(self.appointment_room_path) if self.hdfs_pipe is True
                                              else self.get_csv_file(self.appointment_room_path), appointments, rooms, locations=load_patients_in_locations, is_verbose=is_verbose)
                if is_verbose:
                    logging.info(f"Dataset contains in total {len(rooms)} Rooms")
            else:
                if is_verbose:
                    logging.info("[INTERACTION] adding rooms to appointments omitted.")

            # load Employee data (RAP) from table: FAKT_TERMIN_MITARBEITER
            if load_care_data or load_employees:
                if is_verbose:
                    logging.info("[AGENT] loading employees")
                employees = Employee.create_employee_map(self.appointment_employee_path, encoding=self.encoding, is_verbose=is_verbose)

                # Add Employees to Appointments using the same table
                if is_verbose:
                    logging.info("[AGENT] add employees to appointments")
                Employee.add_employees_to_appointment(self.get_hdfs_pipe(self.appointment_employee_path)
                                                      if self.hdfs_pipe is True
                                                      else self.get_csv_file(self.appointment_employee_path),
                                                      appointments, employees, is_verbose=is_verbose)
                if load_care_data:
                    # Add Treatment/Care data to Cases from table: TACS_DATEN
                    if is_verbose:
                        logging.info("[INTERACTION] Adding Treatment/Care data to Cases from TACS")
                    Treatment.add_care_entries_to_case(self.tacs_care_path, self.encoding, cases, employees, from_range, to_range, is_verbose=is_verbose)
                    # --> Note: Care() objects are not part of the returned dictionary, they are only used in
                    #               Case() objects --> Case().cares = [Care(), Care(), ...] (list of all cares for each case)
                else:
                    if is_verbose:
                        logging.info("[INTERACTION] loading treatment/care data omitted.")

            else:
                if is_verbose:
                    logging.info("[AGENT] loading employees omitted.")
        else:
            if is_verbose:
                logging.info("[INTERACTION] loading appointments omitted.")

        # TODO: care map data are broken. Readd it.
        # # Generate OE_pflege_map
        # oe_pflege_map = Risk.generate_oe_pflege_map(self.get_hdfs_pipe(self.oe_pflege_map_path)
        #                                             if self.hdfs_pipe is True
        #                                             else self.get_csv_file(self.oe_pflege_map_path))

        # --> yields a dictionary mapping "inofficial" ward names to official ones found in the OE_pflege_abk column
        #       of the dbo.INSEL_MAP table in the Atelier_DataScience. This name allows linkage to Waveware !
        # i.e. of the form {'BEWA' : 'C WEST', 'E 121' : 'E 120-21', ...}

        # load CHOP surgery codes data from table: LA_CHOP_FLAT
        chops = {}
        if load_chop_codes or load_surgeries:
            if is_verbose:
                logging.info("[AGENT ATTRIBUTE] loading surgeries chop data...")
            chops = Chop.create_chop_map(self.get_hdfs_pipe(self.chop_path) if self.hdfs_pipe is True
                                          else self.get_csv_file(self.chop_path), is_verbose=is_verbose)

            # Add Surgery data to cases from table: LA_ISH_NICP
            if is_verbose:
                logging.info("[AGENT ATTRIBUTE] loading surgeries data...")
            Surgery.add_surgeries_to_case(self.get_hdfs_pipe(self.surgery_path) if self.hdfs_pipe is True
                                          else self.get_csv_file(self.surgery_path), cases, chops, is_verbose=is_verbose)
            # Surgery() objects are not part of the returned dictionary
        else:
            if is_verbose:
                logging.info("[AGENT ATTRIBUTE] loading surgeries and chop data omitted.")

        # Add ICD codes to cases from table: LA_ISH_NDIA_NORM
        icd_codes = {}
        if load_icd_codes:
            if is_verbose:
                logging.info("[AGENT ATTRIBUTE] Adding ICD codes to cases")
            icd_codes = ICDCode.create_icd_code_map(self.get_hdfs_pipe(self.icd_codes_path) if self.hdfs_pipe is True
                                            else self.get_csv_file(self.icd_codes_path), is_verbose=is_verbose)
            ICDCode.add_icd_codes_to_case(self.get_hdfs_pipe(self.icd_codes_path) if self.hdfs_pipe is True
                                else self.get_csv_file(self.icd_codes_path), cases)
        else:
            if is_verbose:
                logging.info("[AGENT ATTRIBUTE] loading ICD codes omitted.")

        dataset = dict(
            {
                "patients": patients,
                "cases": cases,
                "rooms": rooms,
                "floors": floors,
                "buildings": buildings,
                "wards": wards,
                "partners": partners,
                "medications": medications,
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
