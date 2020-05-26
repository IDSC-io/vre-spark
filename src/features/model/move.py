import logging
from datetime import datetime

from tqdm import tqdm

from src.features.model import Room
from src.features.model import Ward


class Move:
    def __init__(
            self,
            case_id,
            lfd_nr,
            type_id,
            type,
            from_date,
            from_time,
            status,
            to_date,
            to_time,
            lfd_ref,
            description,
            org_fa,
            org_pf,
            org_au,
            room_id,
            bed,
            cancelled,
            partner_id
    ):
        self.case_id = case_id
        self.lfd_nr = int(lfd_nr)
        self.type_id = type_id
        self.type = type
        self.from_datetime = datetime.strptime(
            from_date + " " + from_time[:-1], "%Y-%m-%d %H:%M:%S.%f"  # parsing milliseconds: https://stackoverflow.com/questions/698223/how-can-i-parse-a-time-string-containing-milliseconds-in-it-with-python
        )
        self.status = status
        self.to_datetime = None
        try:
            if to_time == '24:00:00.000000000':
                to_time = "23:59:59.000000000"

            self.to_datetime = datetime.strptime(
                to_date + " " + to_time, "%Y-%m-%d %H:%M:%S.000000000"
            )
        except ValueError:
            pass
        self.description = description
        self.ward_id = org_pf
        self.room_id = room_id
        self.bed = bed
        self.cancelled = cancelled
        self.partner_id = partner_id
        self.room = None
        self.ward = None
        self.case = None

        # unused fields
        self.lfd_ref = lfd_ref
        self.org_fa = org_fa
        self.org_au = org_au

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
    def add_moves_to_case(lines, cases, rooms, wards, partners, load_limit=None):
        """
        Reads the moves csv and performs the following:
        --> creates a Move() object from the read-in line data
        --> Adds the created Move() to the corresponding Case()
        --> Extracts the Room() from each Move() and adds them "to each other"
        --> Extracts the Ward() from each Move() and adds them "to each other"
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
        """
        logging.debug("add_move_to_case")
        nr_not_found = 0
        nr_not_formatted = 0
        nr_ok = 0
        nr_wards_updated = 0
        for line in tqdm(lines):
            if len(line) != 18:
                nr_not_formatted += 1
            else:
                move = Move(*line)
                # don't consider cancelled movements
                # if move.storn == "X":  # NOW INCLUDED DIRECTLY IN THE SQL QUERY
                #     continue

                if cases.get(move.case_id, None) is not None:
                    cases[move.case_id].add_move(move)
                    move.add_case(cases[move.case_id])
                else:
                    nr_not_found += 1
                    continue
                ward = None
                # Add ward to move and vice versa
                if move.ward_id != "" and move.ward_id != "NULL":
                    if wards.get(move.ward_id, None) is None:
                        ward = Ward(move.ward_id)
                        wards[move.ward_id] = ward
                    wards[move.ward_id].add_move(move)
                    move.add_ward(wards[move.ward_id])
                # Add move to room and vice versa (including an update of the Room().ward attribute)
                if move.room_id != "" and move.room_id != "NULL":
                    if rooms.get(move.room_id, None) is None:
                        # Note that the rooms are primarily identified through their name
                        # The names in this file come from SAP (without an associated ID), so they will NOT match the names already present in the rooms dictionary !
                        this_room = Room(move.room_id)
                        rooms[move.room_id] = this_room

                        # In order to extract the Room ID, we need to 'backtrace' the key in room_ids for which room_ids[key] == move.zimmr (this will not be available for most Rooms)
                        # If a backtrace is not possible, the room object will be initiated without an ID
                        # correct_room_id = [(value_tuple[0], value_tuple[1].name) for value_tuple in room_ids.items() if value_tuple[1] == move.zimmr]
                        # if len(correct_room_id) == 1:
                        #     logging.info(f"Found room ID for room name {move.zimmr} !")
                        #     r = Room(move.zimmr, correct_room_id[0][0] ) # correct_room_id at this point will be a list containing one tuple --> [ ('123456', 'BH O 128') ]
                        # else:
                        #     r = Room(move.zimmr) # Create the Room() object without providing an ID
                        # rooms[move.zimmr] = r
                    # Then add the ward to this room, and update moves with rooms and vice versa
                    rooms[move.room_id].add_ward(ward)
                    rooms[move.room_id].add_move(move)
                    move.add_room(rooms[move.room_id])
                    nr_wards_updated += 1
                # Parse patients from external referrers
                if move.partner_id != "":
                    if move.case is not None and partners.get(move.partner_id, None) is not None:
                        partners[move.partner_id].add_case(move.case)
                        move.case.add_referrer(partners[move.partner_id])
                nr_ok += 1
                if load_limit is not None and nr_ok > load_limit:
                    break

        logging.info(f"{nr_ok} moves ok, {nr_not_found} cases not found, {nr_not_formatted} malformed, {nr_wards_updated} wards updated")
