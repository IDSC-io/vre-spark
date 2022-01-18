import logging
from datetime import datetime

from tqdm import tqdm
import pandas as pd

from src.features.model import Room
from src.features.model import Ward

import numpy as np


class Stay:
    def __init__(self, serial_number, case_id, type_id, type, status, serial_reference, description, department,
                 ward_id, unit_of_entry, sap_room_id, bed, cancelled, partner_id, begin_datetime, end_datetime,
                 sap_building_abbreviation, ww_floor_id, ww_room_id):
        self.case_id = case_id
        self.serial_number = serial_number
        self.type_id = type_id
        self.type = type
        self.from_datetime = begin_datetime
        self.status = status
        self.to_datetime = end_datetime
        self.description = description
        self.ward_id = ward_id
        self.room_id = sap_room_id
        self.sap_building_abbreviation = sap_building_abbreviation
        self.ww_floor_id = ww_floor_id
        self.ww_room_id = ww_room_id
        self.bed = bed
        self.cancelled = cancelled
        self.partner_id = partner_id
        self.room = None
        self.ward = None
        self.case = None

        # unused fields
        self.serial_reference = serial_reference
        self.department = department
        self.unit_of_entry = unit_of_entry

    def __str__(self):
        return str({"Case ID": self.case_id,
                    "Serial Number": self.serial_number,
                    "Type": self.type,
                    "From Date": self.from_datetime,
                    "To Date": self.to_datetime,
                    "Description": self.description,
                    "Room ID": self.room_id})

    def add_room(self, r):
        self.room = r

    def add_ward(self, ward):
        self.ward = ward

    def add_case(self, c):
        self.case = c

    def get_duration(self):
        end_dt = self.to_datetime if self.to_datetime is not None else datetime.now()
        return end_dt - self.from_datetime

    @staticmethod
    def create_bwart_map(lines):
        bwart = dict()
        for line in lines:
            bwart[line[0]] = line[1]
        return bwart

    @staticmethod
    def add_stays_to_case(csv_path, encoding, cases, rooms, wards, partners, from_range, to_range, locations=None, load_fraction=1.0, load_seed=7, is_verbose=True):
        """
        Reads the stays csv and performs the following:
        --> creates a Stay() object from the read-in line data
        --> Adds the created Stay() to the corresponding Case()
        --> Extracts the Room() from each Stay() and adds them "to each other"
        --> Extracts the Ward() from each Stay() and adds them "to each other"
        --> Adds referring hospital to the Case() and vice versa
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The table from which entries are read is structured as follows:
        >> TABLE NAME: LA_ISH_NBEW
        ["FALNR",      "LFDNR", "BEWTY",  "BWART",      "BWIDT",        "BWIZT",    "STATU", "BWEDT",       "BWEZT",        "LFDREF", "KZTXT",                "ORGFA", "ORGPF", "ORGAU", "ZIMMR", "BETT", "STORN", "EXTKH"]
        ["0004496041", "3",     "4",      "SB",         "2014-01-17",   "16:15:00", "30",    "2014-01-17",  "16:15:00.000", "0",      "",                     "DIAA",  "DIAA",  "",      "",      "",      "",     ""]
        ["0004496042", "1",     "4",      "BE",         "2014-03-10",   "08:15:00", "30",    "2014-03-10",  "08:15:00.000", "0",      "ej/ n CT um 10.30 h", "ENDA",  "ENDA",  "IICA",  "",      "",      "",     ""]

        :param cases:   Dictionary mapping case ids to Case()       --> {'0005976205' : Case(), ... }
        :param rooms:    Dictionary mapping room names to Room()     --> {'BH N 125' : Room(), ... }
        :param room_ids: Dictionary mapping room IDs to Room()       --> {'127803' : Room(), ... }
        :param wards:    Dictionary mapping ward names to Ward()     --> {'N NORD' : Ward(), ... }
        :param partners: Dictionary mapping partner ids to Partner() --> {'0010000990' : Partner(), ... }
        # TODO: Solve ward chaos
        """
        stay_df = pd.read_csv(csv_path, encoding=encoding, parse_dates=["Begin Datetime", "End Datetime"], dtype=str)
        if load_fraction != 1.0:
            stay_df = stay_df.sample(frac=load_fraction, random_state=load_seed)
        # in principle they are all int, history makes them a varchar/string
        # stay_df["Case ID"] = stay_df["Case ID"].astype(int)

        # TODO: SAP NBEW without Room ID is dropped. Is that correct?
        stay_df = stay_df[~pd.isna(stay_df["SAP Room ID"])]

        # TODO: Why are there so many duplicates in the table? Is this still LA?
        stay_df = stay_df.drop_duplicates(subset=["Case ID", "SAP Room ID", "Bed ID", "Begin Datetime", "End Datetime"])

        if from_range is not None:
            stay_df = stay_df.loc[stay_df['Begin Datetime'] > from_range]

        if to_range is not None:
            stay_df = stay_df.loc[stay_df['End Datetime'] <= to_range]

        if locations is not None:
            # get all stays of cases of which at least one stay is in one of the requested locations
            location_stays_df = stay_df[stay_df["SAP Room ID"].str.contains('|'.join(locations))]
            stay_df = stay_df[stay_df["Case ID"].isin(location_stays_df["Case ID"])]

        # stay_objects = stay_df.progress_apply(lambda row: Stay(*row.to_list()), axis=1)
        stay_objects = list(map(lambda row: Stay(*row), tqdm(stay_df.values.tolist(), disable=not is_verbose)))
        del stay_df
        logging.debug("add_stay_to_case")
        nr_not_found = 0
        nr_not_formatted = 0
        nr_ok = 0
        nr_wards_updated = 0
        nr_rooms_created = 0
        # TODO: Rewrite parts of loop to pandas checks before making all objects
        for stay in tqdm(stay_objects, disable=not is_verbose):
                if cases.get(stay.case_id, None) is not None:
                    cases[stay.case_id].add_stay(stay)
                    stay.add_case(cases[stay.case_id])
                else:
                    nr_not_found += 1
                    continue
                ward = None
                # Add ward to stay and vice versa
                if stay.ward_id != "" and not pd.isna(stay.ward_id):
                    if wards.get(stay.ward_id, None) is None:
                        ward = Ward(stay.ward_id)
                        wards[stay.ward_id] = ward
                    wards[stay.ward_id].add_stay(stay)
                    stay.add_ward(wards[stay.ward_id])
                # Add stay to room and vice versa (including an update of the Room().ward attribute)
                if stay.room_id != "" and not pd.isna(stay.room_id):
                    if rooms.get(stay.room_id, None) is None:
                        # Note that the rooms are primarily identified through their name
                        # The names in this file come from SAP (without an associated ID), so they will NOT match the names already present in the rooms dictionary !
                        this_room = Room(sap_room_id1=stay.room_id)
                        rooms[stay.room_id] = this_room
                        nr_rooms_created += 1
                        # print(stay.room_id)

                        # In order to extract the Room ID, we need to 'backtrace' the key in room_ids for which room_ids[key] == stay.zimmr (this will not be available for most Rooms)
                        # If a backtrace is not possible, the room object will be initiated without an ID
                        # correct_room_id = [(value_tuple[0], value_tuple[1].name) for value_tuple in room_ids.items() if value_tuple[1] == stay.zimmr]
                        # if len(correct_room_id) == 1:
                        #     logging.info(f"Found room ID for room name {stay.zimmr} !")
                        #     r = Room(stay.zimmr, correct_room_id[0][0] ) # correct_room_id at this point will be a list containing one tuple --> [ ('123456', 'BH O 128') ]
                        # else:
                        #     r = Room(stay.zimmr) # Create the Room() object without providing an ID
                        # rooms[stay.zimmr] = r
                    # Then add the ward to this room, and update stays with rooms and vice versa
                    rooms[stay.room_id].add_ward(ward)
                    rooms[stay.room_id].add_stay(stay)
                    stay.add_room(rooms[stay.room_id])
                    nr_wards_updated += 1
                # Parse patients from external referrers
                if stay.partner_id != "":
                    if stay.case is not None and partners.get(stay.partner_id, None) is not None:
                        partners[stay.partner_id].add_case(stay.case)
                        stay.case.add_referrer(partners[stay.partner_id])
                nr_ok += 1

        logging.info(f"{nr_ok} stays ok, {nr_not_found} cases not found, {nr_not_formatted} malformed, {nr_wards_updated} wards updated, {nr_rooms_created} new rooms created")
    # TODO: Leads to stackoverflow
    # def __repr__(self):
    #     return str(dict((key, value) for key, value in self.__dict__.items()
    #             if not callable(value) and not key.startswith('__')))
    #
    # def __str__(self):
    #     return self.__repr__()