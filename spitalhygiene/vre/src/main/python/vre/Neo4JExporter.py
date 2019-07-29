import configparser
import csv
import datetime
import os
import logging

class Neo4JExporter:
    """
    Class responsible for data export in Neo4J-compatible format.
    """
    def __init__(self):
        """
        Loads the configuration file and makes its contents available via the self.config attribute (e.g. self.config['PATHS']['some_dir']
        """
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'BasicConfig.ini')) # makes configuration file entries available as self.config['PATHS']['XXX']

        self.init_date = datetime.datetime.now().strftime('%Y%m%d%H%M%S') # timestamp at initiation - used as a suffix for all exported files
        logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO, datefmt='%d.%m.%Y %H:%M:%S')

    def write_patient(self, patients):
        """
        Export patient data for all patients with a relevant case.

        :param patients: Dictionary mapping PATIENTID to Patient() objects, i.e. {"00001383264" : Patient(), "00001383310" : Patient(), ...}
        """
        written_count = 0
        total_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'patients_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow(
                [
                    "PATNR:ID",
                    "type:LABEL",
                    "risk:LABEL",
                    "risk_datetime:datetime{timezone:Europe/Bern}",
                ] ) # Write file header
            for key, patient in patients.items():
                total_count += 1
                l = "False"
                d = None
                if patient.has_risk():
                    l = "True"
                    d = patient.get_risk_date() # returns either a dt.dt() object or None
                if patient.get_relevant_case() is not None: # filter for patients with a relevant case
                    csvwriter.writerow(
                        [
                            patient.patient_id,
                            "Patient",
                            l,
                            d.strftime("%Y-%m-%dT%H:%M") if d is not None else None,
                        ] )
                    written_count += 1
        logging.info(f'Created {written_count} patients with a relevant case out of {total_count} patients.')


    def write_patient_patient(self, contact_pats, patients):
        """
        Export all contacts between patients.

        :param contact_pats: List containing tuples of length 6 of either the format:   (source_pat_id, dest_pat_id, start_overlap_dt, end_overlap_dt, room_name, "kontakt_raum")
                                                                       or the format:   (source_pat_id, dest_pat_id, start_overlap_dt, end_overlap_dt, ward_name, "kontakt_org")
        :param patients: Dictionary mapping PATIENTID to Patient() objects, i.e. {"00001383264" : Patient(), "00001383310" : Patient(), ...}
        """
        contact_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'patient_patient_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    "from:datetime{timezone:Europe/Bern}",
                    "to:datetime{timezone:Europe/Bern}",
                    "room",
                    ":TYPE",
                ] )
            for contact in contact_pats: # list of tuples --> can be directly printed !
                if patients[contact[0]].get_relevant_case() is not None and patients[contact[1]].get_relevant_case() is not None: # make sure to include only patients with a relevant date !
                    csvwriter.writerow(contact)
                    contact_count += 1
        logging.info(f'Created {contact_count} patient contacts.')


    def write_room(self, rooms):
        """
        Export all room data.

        :param rooms: Dictionary mapping room names to a Room() object --> {'BH N 125' : Room(), ... }
        """
        room_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'zimmer_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow(["Name:ID", "type:LABEL"]) # header
            for k, r in rooms.items():
                csvwriter.writerow([r.name, "Room"])
                room_count += 1
        logging.info(f'Created {room_count} rooms.')


    def write_patient_room(self, patients):
        """
        Export room data only for patients with a relevant case in the sense: patient_id --[in]--> room_name

        :param patients:    Dictionary mapping PATIENTID to Patient() objects, i.e. {"00001383264" : Patient(), "00001383310" : Patient(), ...}
        """
        relcase_count = 0
        patient_count = 0
        room_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'patient_zimmer_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    "from:datetime{timezone:Europe/Bern}",
                    "to:datetime{timezone:Europe/Bern}",
                    ":TYPE",
                ] ) # header
            for patient in patients.values():
                patient_count += 1
                pat_rel_case = patient.get_relevant_case()
                if pat_rel_case is not None:
                    relcase_count += 1
                    for move in pat_rel_case.moves.values(): # the Case().moves attribute is a dictionary mapping the order of moves to Moves() objects
                        if move.room is not None:
                            csvwriter.writerow(
                                [
                                    patient.patient_id,
                                    move.room.name,
                                    move.bwi_dt.strftime("%Y-%m-%dT%H:%M"),
                                    move.bwe_dt.strftime("%Y-%m-%dT%H:%M"),
                                    "in",
                                ])
                            room_count += 1
        logging.info(f'Wrote {room_count} rooms (based on moves) for {relcase_count} patients with relevant cases out of {patient_count} patients.')


    def write_bed(self, rooms):
        """
        Export all beds involved in all rooms.

        :param rooms: Dictionary mapping room names to a Room() object --> {'BH N 125' : Room(), ... }
        """
        room_count = 0
        bed_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'bett_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow(["Name:ID", "type:LABEL"])
            for k, r in rooms.items():
                room_count += 1
                for b in r.beds:
                    bed_count += 1
                    csvwriter.writerow([b, "Bed"])
        logging.info(f'Created {bed_count} beds from {room_count} rooms.')


    def write_room_bed(self, rooms):
        """
        Assign beds to rooms.
        :param rooms: Dictionary mapping room names to a Room() object --> {'BH N 125' : Room(), ... }
        """
        room_count = 0
        bed_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'zimmer_bett_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow([":START_ID", ":END_ID", ":TYPE"])
            for k, r in rooms.items():
                room_count += 1
                for b in r.beds:
                    csvwriter.writerow([r.name, b, "in"])
                    bed_count += 1
        logging.info(f'Assigned {bed_count} beds to {room_count} rooms.')


    def write_device(self, devices):
        """
        Exports all devices.

        :param rooms: Dictionary mapping room names to a Room() object --> {'BH N 125' : Room(), ... }
        """
        device_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'geraet_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow([":ID", "type:LABEL", "name"])
            for k, g in devices.items():
                csvwriter.writerow([g.geraet_id, "Device", g.geraet_name])
                device_count += 1
        logging.info(f'Created {device_count} devices.')


    def write_patient_device(self, patients):
        """
        Exports all contacts between patients and a specific "geraet" if the contact happened during the patient's relevant case.

        :param patients:    Dictionary mapping PATIENTID to Patient() objects, i.e. {"00001383264" : Patient(), "00001383310" : Patient(), ...}
        """
        pat_count = 0
        relcase_count = 0
        device_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'patient_geraet_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow( [":START_ID", ":END_ID", ":TYPE", "from:datetime{timezone:Europe/Bern}"] )
            for k, patient in patients.items():
                pat_count += 1
                pat_rel_case = patient.get_relevant_case()
                if pat_rel_case is not None: # consider only patients with a relevant case
                    relcase_count += 1
                    for t in pat_rel_case.appointments: # consider only appointments in the relevant case
                        for d in t.devices:
                            csvwriter.writerow(
                                [
                                    patient.patient_id,
                                    d.geraet_id,
                                    "used",
                                    t.termin_datum.strftime("%Y-%m-%dT%H:%M"),
                                ] )
                            device_count += 1
        logging.info(f'Added {device_count} devices for {relcase_count} cases from {pat_count} patients.')

    def write_drug(self, drugs):
        """
        Export all drugs.

        :param drugs: A dictionary mapping drug codes to their respective text description --> {'B02BA01' : 'NaCl Braun Inf LÃ¶s 0.9 % 500 ml (Natriumchlorid)', ... }
        """
        drug_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'drugs_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow([":ID", "type:LABEL", "name"])
            for k, d in drugs.items():
                csvwriter.writerow([k, "Drug", d])
                drug_count += 1
        logging.info(f'Created {drug_count} drugs.')


    def write_patient_medication(self, patients):
        """
        Export exposure to drugs for all patients with a relevant case.

        :param patients:    Dictionary mapping PATIENTID to Patient() objects, i.e. {"00001383264" : Patient(), "00001383310" : Patient(), ...}
        """
        pat_count = 0
        exposure_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'patient_medication_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow(
                [
                    ":START_ID",
                    ":END_ID",
                    ":TYPE",
                    "from:datetime{timezone:Europe/Bern}",
                    "to:datetime{timezone:Europe/Bern}",
                ] ) # header
            for k, p in patients.items():
                pat_count += 1
                exposure = p.get_antibiotic_exposure() # this will return None if the patient has no relevant case
                if exposure is None:
                    continue
                for k, d in exposure.items(): # d is a set of datetime.date() objects
                    date_list = list(d)
                    min_date = date_list[date_list.index(min(date_list))]
                    max_date = date_list[date_list.index(max(date_list))]
                    csvwriter.writerow(
                        [
                            p.patient_id,
                            k,
                            "administered",
                            min_date.strftime("%Y-%m-%dT%H:%M"),
                            max_date.strftime("%Y-%m-%dT%H:%M"),
                        ] )
                    exposure_count += 1
        logging.info(f'Created {exposure_count} exposures from {pat_count} patients.')


    def write_employees(self, employees):
        """
        Export all employees.

        :param employees: Dictionary mapping employee_ids to Employee() objects --> {'0032719' : Employee(), ... }
        """
        employee_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'employees_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow([":ID", "type:LABEL", "name"])
            for k, e in employees.items():
                csvwriter.writerow([k, "Employee", k])
                employee_count += 1
        logging.info(f'Created {employee_count} employees.')

    def write_patient_employee(self, patients):
        """
        Export all contacts between an employee and a patient during the patient's relevant case.

        :param patients: Dictionary mapping PATIENTID to Patient() objects, i.e. {"00001383264" : Patient(), "00001383310" : Patient(), ...}
        """
        pat_count = 0
        relcase_count = 0
        contact_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'patient_employee_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow( [":START_ID", ":END_ID", ":TYPE", "from:datetime{timezone:Europe/Bern}"] )
            for k, p in patients.items():
                pat_rel_case = p.get_relevant_case() # returns None if patient has no relevant case
                pat_count += 1
                if pat_rel_case is not None:
                    relcase_count += 1
                    for t in pat_rel_case.appointments:
                        for e in t.employees:
                            if e.mitarbeiter_id != "-1": # indicates an unknown mitarbeiter - these cases are ignored
                                csvwriter.writerow(
                                    [
                                        p.patient_id,
                                        e.mitarbeiter_id,
                                        "appointment_with",
                                        t.termin_datum.strftime("%Y-%m-%dT%H:%M"),
                                    ] )
                                contact_count += 1
        logging.info(f'Created {contact_count} contacts in {relcase_count} relevant cases from {pat_count} patients.')


    def write_referrer(self, partners):
        """
        Export all referrers.

        :param partners: Dictionary mapping partners to Partner() objects --> {'1001503842' : Partner(), '1001503845' : Partner(), ... }
        """
        refer_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'referrers_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow(
                [
                    ":ID",
                    "type:LABEL",
                    "name1",
                    "name2",
                    "name3",
                    "land",
                    "plz",
                    "ort",
                    "ort2",
                ] )
            for partner in partners.values():
                csvwriter.writerow(
                    [
                        partner.gp_art,
                        "Referrer",
                        partner.name1,
                        partner.name2,
                        partner.name3,
                        partner.land,
                        partner.pstlz,
                        partner.ort,
                        partner.ort2,
                    ] )
                refer_count += 1
        logging.info(f'Created {refer_count} referrers.')


    def write_referrer_patient(self, patients):
        """
        Export referrers which came into contact with patients during their relevant case.

        :param patients: Dictionary mapping PATIENTID to Patient() objects, i.e. {"00001383264" : Patient(), "00001383310" : Patient(), ...}
        """
        patient_count = 0
        referrer_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'referrer_patient_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow( [":END_ID", ":START_ID", ":TYPE", "from:datetime{timezone:Europe/Bern}"] )
            for patient in patients.values():
                patient_count += 1
                patient_relcase = patient.get_relevant_case()
                if patient_relcase is not None:
                    if patient_relcase.moves_start is not None:
                        for referrer in patient_relcase.referrers:
                            csvwriter.writerow(
                                [
                                    patient.patient_id,
                                    referrer.gp_art,
                                    "referring",
                                    patient_relcase.moves_start.strftime("%Y-%m-%dT%H:%M"),
                                ] )
                            referrer_count += 1
        logging.info(f'Created {referrer_count} referrals from {patient_count} patients.')


    def write_chop_code(self, chops):
        """
        Export all chop codes.

        :param chops: Dictionary mapping the chopcode_katalogid entries to Chop() obects --> { 'Z39.61.10_11': Chop(), ... }
        """
        chop_count = 0
        filtered_chops = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'chops_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow(
                [
                    ":ID",
                    "type:LABEL",
                    "level1",
                    "level2",
                    "level3",
                    "level4",
                    "level5",
                    "level6",
                    "latest_catalog",
                ] )
            # Extract only the latest CHOP codes, given by the highest value in each chop.chop_sap_katalog_id() value
            chop_dict = {} # maps chop codes (without the SAP katalog number) to tuples of (SAP_Katalog_ID, chop object)
            for each_chop in chops.values():
                chop_count += 1
                if each_chop.chop_code not in chop_dict.keys():
                    chop_dict[each_chop.chop_code] = (int(each_chop.chop_sap_katalog_id), each_chop)
                else: # indicates that the CHOP code has already been appended
                    if int(each_chop.chop_sap_katalog_id) > chop_dict[each_chop.chop_code][0]:
                        chop_dict[each_chop.chop_code] = (int(each_chop.chop_sap_katalog_id), each_chop) # overwrite the previous entry with the "more current" katalog ID
            # for key, value in cho
            # final_chop_list = [] # holds lists of values to be appended
            for chop_tuple in chop_dict.values():
                csvwriter.writerow(
                    [
                        chop_tuple[1].chop_code,
                        "CHOP",
                        chop_tuple[1].chop_level1.replace('\n', ' '), # Note that some of the descriptions may contain newline characters
                        chop_tuple[1].chop_level2.replace('\n', ' '),
                        chop_tuple[1].chop_level3.replace('\n', ' '),
                        chop_tuple[1].chop_level4.replace('\n', ' '),
                        chop_tuple[1].chop_level5.replace('\n', ' '),
                        chop_tuple[1].chop_level6.replace('\n', ' '),
                        chop_tuple[1].chop_sap_katalog_id.replace('\n', ' ')
                    ] )
                filtered_chops += 1
        logging.info(f'Created {filtered_chops} latest CHOP codes out of {chop_count} total CHOP codes.')

    def write_chop_patient(self, patients):
        """
        Export CHOP codes for patients during their relevant case.

        :param patients: Dictionary mapping PATIENTID to Patient() objects, i.e. {"00001383264" : Patient(), "00001383310" : Patient(), ...}
        """
        pat_count = 0
        relcase_count = 0
        chop_count = 0
        with open(os.path.join(self.config['PATHS']['neo4j_dir'], 'chop_patient_' + self.init_date + '.csv'), "w") as csvfile:
            csvwriter = csv.writer( csvfile, delimiter=self.config['DELIMITERS']['csv_sep'], quotechar='"', quoting=csv.QUOTE_MINIMAL )
            csvwriter.writerow( [":START_ID", ":END_ID", ":TYPE", "from:datetime{timezone:Europe/Bern}"] )
            for patient in patients.values():
                pat_count += 1
                pat_relcase = patient.get_relevant_case()
                if pat_relcase is not None:
                    relcase_count += 1
                    for surgery in pat_relcase.surgeries:
                        csvwriter.writerow(
                            [
                                patient.patient_id,
                                surgery.chop.chop_code,
                                "surgery",
                                surgery.bgd_op.strftime("%Y-%m-%dT%H:%M"),
                            ] )
                        chop_count += 1
        logging.info(f'Created {chop_count} surgeries (CHOP codes) for {relcase_count} relevant cases from {pat_count} patients.')

