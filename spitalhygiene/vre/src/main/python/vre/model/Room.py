from datetime import datetime
from Bed import Bed
import logging


class Room:
    '''
    Models a room in the hospital and contains lists of moves and appointments that happened in this room.
    '''
    def __init__(self, name):
        """
        Note that most rooms will not have an ID ! (for some reason yet to be discovered)

        :param name:    Room name
        """
        self.name = name
        self.ids = []
        self.ward = None
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

    def add_id(self, id, system):
        """
        Adds an (id, system) tuple to the Room().ids attribute list if id has not already been added - this prevents duplicate entries.
        This list should be useful in determining whether or not there are overlapping room names among different hospital systems.

        :param id:      id string
        :param system:  system string
        """
        if id not in [id_tuple[0] for id_tuple in self.ids]:
            self.ids.append( (str(id), str(system)) )

    def add_appointment(self, appointment):
        '''
        Add an appointment from RAP to this room.
        :param appointment: Appointment
        :return:
        '''
        self.appointments.append(appointment)

    def add_ward(self, ward):
        """
        Updates the self.ward attribute.

        :param ward: ward object to be set as the new attribute.
        """
        self.ward = ward

    def get_ids(self):
        """
        Returns all ids in the self.ids attribute.
        :return: a @-delimited list of [room_id]_[system] for all (id, system) tuples in the self.ids attribute list, or None if the list is empty.
        """
        if len(self.ids) == 0:
            return None
        else:
            return '@'.join([each_tuple[0] + '_' + each_tuple[1] for each_tuple in self.ids])

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

    def create_room_id_map(lines, rooms):
        '''
        Initializes the dictionary mapping room ids to Room() objects based on the provided csv file.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_DIM_RAUM_CUR
        ["RAUMID",  "RAUMNAME"]
        ["128307",  "Kollmann Zahraa [00025783]"]
        ["80872","  Audiometrie"]

        The function will also update the provided rooms dictionary.

        :param rooms:   Dictionary mapping room names to Room() objects --> {'BH N 129' : Room(), ... }
        :return:        Dictionary mapping room ids to Room() objects   --> {'127803' : Room(), ... }
        '''
        logging.debug("create_room_id_map")
        room_id_map = dict()
        for line in lines:
            room_obj = Room(line[1])
            room_obj.add_id(id = line[0], system = 'Polypoint')
            # Update the room_id_map dictionary
            room_id_map[line[0]] = Room(line[1])
            # Update the rooms dictionary
            if line[1] not in rooms.keys():
                rooms[line[1]] = room_obj
            else:
                rooms[line[1]].add_id(id = line[0], system = 'Polypoint')

        logging.info(f"{len(room_id_map)} rooms created")
        return room_id_map

    def add_room_to_appointment(lines, appointments, rooms):
        """
        Reads room data from the csv file and adds the created Room() to an Appointment() in appointments and vice versa.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_FACT_TERMINRAUM --> LEFT JOIN V_DH_DIM_RAUM --> ON [V_DH_FACT_TERMINRAUM].RAUMID = V_DH_DIM_RAUM_CUR.RAUMID
        ["TERMINID",    "RAUMID",   "RAUMNAME", "TERMINSTART_TS",           "TERMINENDE_TS",            "DAUERINMIN"]
        ["2295658",     "11190",    "P2 A534",  "2007-12-10 08:45:00.0000", "2007-12-10 10:15:00.0000", "90.000000"]
        ["2410965",     "61994",    "C 316",    "2008-02-21 14:00:00.0000", "2008-02-21 15:00:00.0000", "60.000000"]

        :param appointments:    Dictionary mapping appointment ids to Appointment() objects --> { '36830543' : Appointment(), ... }
        :param room_id_map:     Dictionary mapping room ids to Room() object                --> {'127803' : Room(), ... }
        :param rooms:           Dictionary mapping room names to a Room() object            --> {'BH N 125' : Room(), ... }
        """
        logging.debug("add_room_to_appointment")
        nr_appointments_not_found = 0
        nr_rooms_not_found = 0
        nr_ok = 0
        for line in lines:
            appointment_id = line[0]
            room_id = line[1]
            room_name = line[2]
            if appointments.get(appointment_id, None) is None:
                nr_appointments_not_found += 1
                continue
            # if room_id_map.get(room_id, None) is None:
            #     nr_rooms_not_found += 1
            #     continue
            # name = room_id_map[room_id]
            if rooms.get(room_name, None) is None:
                new_room = Room(room_name)
                new_room.add_id(id = room_id, system = 'Polypoint' )
                rooms[room_name] = new_room
            rooms[room_name].add_appointment(appointments[appointment_id])
            appointments[appointment_id].add_room(rooms[room_name])
            nr_ok += 1
        logging.info(f"{nr_ok} rooms added to appointments, {nr_appointments_not_found} appointments not found, {nr_rooms_not_found} rooms not found")
