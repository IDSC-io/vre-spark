from datetime import datetime
from model.Bed import Bed
import logging


class Room:
    '''
    Models a room in the hospital and contains lists of moves and appointments that happened in this room.
    '''
    def __init__(self, name):
        self.name = name
        self.moves = []
        self.appointments = []
        self.beds = dict()

    def add_move(self, move):
        '''
        Add a Move from SAP IS-H to this room.
        In case the Move has information about the bed, the Bed is added to the room (if not yet exists).
        :param move: Move
        :return:
        '''
        self.moves.append(move)
        if move.bett is not None and move.bett is not "":
            if self.beds.get(move.bett, None) is None:
                b = Bed(move.bett)
                self.beds[move.bett] = b
            self.beds[move.bett].add_move(move)

    def add_appointment(self, appointment):
        '''
        Add an appointment from RAP to this room.
        :param appointment: Appointment
        :return:
        '''
        self.appointments.append(appointment)

    def get_moves_during(self, start_dt, end_dt):
        '''
        List of moves that overlap with the start_dt, end_dt time interval.
        :param start_dt: datetime.datetime
        :param end_dt: datetime.datetime
        :return: List of Move
        '''
        overlapping_moves = []
        for move in self.moves:
            e_dt = move.bwe_dt if move.bwe_dt is not None else datetime.now()
            if e_dt >= start_dt and move.bwi_dt <= end_dt:
                overlapping_moves.append(move)
        return overlapping_moves

    def create_room_id_map(lines):
        '''
        Initializes the dictionary mapping room ids to names (NOT Room() objects!) based on the provided csv file.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_DIM_RAUM_CUR
        ["RAUMID",  "RAUMNAME"]
        ["128307",  "Kollmann Zahraa [00025783]"]
        ["80872","  Audiometrie"]

        :return: Dictionary mapping room ids to room names  --> {'127803' : 'BH N 125', ... } (does NOT map to objects !)
        '''
        logging.debug("create_room_id_map")
        room_id_map = dict()
        for line in lines:
            room_id_map[line[0]] = line[1]
        logging.info(f"{len(room_id_map)} rooms created")
        return room_id_map

    def add_room_to_appointment(lines, appointments, room_id_map, rooms):
        """
        Reads room data from the csv file and adds the created Room() to an Appointment() in appointments and vice versa.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_FACT_TERMINRAUM
        ["TERMINID",    "RAUMID",   "TERMINSTART_TS",           "TERMINENDE_TS",            "DAUERINMIN"]
        ["2295658",     "11190",    "2007-12-10 08:45:00.0000", "2007-12-10 10:15:00.0000", "90.000000"]
        ["2410965",     "61994",    "2008-02-21 14:00:00.0000", "2008-02-21 15:00:00.0000", "60.000000"]

        :param appointments:    Dictionary mapping appointment ids to Appointment() objects --> { '36830543' : Appointment(), ... }
        :param room_id_map:     Dictionary mapping room ids to room names                   --> {'127803' : 'BH N 125', ... } (does NOT map to objects !)
        :param rooms:           Dictionary mapping room names to a Room() object            --> {'BH N 125' : Room(), ... }
        """
        logging.debug("add_room_to_appointment")
        nr_appointments_not_found = 0
        nr_rooms_not_found = 0
        nr_ok = 0
        for line in lines:
            appointment_id = line[0]
            room_id = line[1]
            if appointments.get(appointment_id, None) is None:
                nr_appointments_not_found += 1
                continue
            if room_id_map.get(room_id, None) is None:
                nr_rooms_not_found += 1
                continue
            name = room_id_map[room_id]
            if rooms.get(name, None) is None:
                room = Room(name)
                rooms[name] = room
            rooms[name].add_appointment(appointments[appointment_id])
            appointments[appointment_id].add_room(rooms[name])
            nr_ok += 1
        logging.info(f"{nr_ok} rooms added to appointments, {nr_appointments_not_found} appointments not found, {nr_rooms_not_found} rooms not found")
