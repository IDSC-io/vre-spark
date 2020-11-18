import logging
from datetime import datetime
import itertools

from tqdm import tqdm

from src.features.model import Bed


class Room:
    """
    Models a room in the hospital and contains lists of stays and appointments that happened in this room.
    """

    def __init__(self, name):
        """
        Note that most rooms will not have an ID ! (for some reason yet to be discovered)

        :param name:    Room name
        """
        self.name = name
        self.ids = []
        self.ward = None
        self.stays = []
        self.appointments = []
        self.beds = dict()

    def add_stay(self, stay):
        """
        Add a Stay from SAP IS-H to this room.
        In case the Stay has information about the bed, the Bed is added to the room (if not yet exists).
        :param stay: Stay
        :return:
        """
        self.stays.append(stay)
        if stay.bed is not None and stay.bed is not "":
            if self.beds.get(stay.bed, None) is None:
                b = Bed(stay.bed)
                self.beds[stay.bed] = b
            self.beds[stay.bed].add_stay(stay)

    def add_id(self, id, system):
        """
        Adds an (id, system) tuple to the Room().ids attribute list if id has not already been added - this prevents duplicate entries.
        This list should be useful in determining whether or not there are overlapping room names among different hospital systems.

        :param id:      id string
        :param system:  system string
        """
        if id not in [id_tuple[0] for id_tuple in self.ids]:
            self.ids.append((str(id), str(system)))

    def add_appointment(self, appointment):
        """
        Add an appointment from RAP to this room.
        :param appointment: Appointment
        :return:
        """
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

    def get_stays_during(self, start_dt, end_dt):
        """
        List of stays that overlap with the start_dt, end_dt time interval.
        :param start_dt: datetime.datetime
        :param end_dt: datetime.datetime
        :return: List of Stay
        """
        overlapping_stays = []
        for stay in self.stays:
            e_dt = stay.to_datetime if stay.to_datetime is not None else datetime.now()
            if e_dt >= start_dt and stay.from_datetime <= end_dt:
                overlapping_stays.append(stay)
        return overlapping_stays

    @staticmethod
    def create_room_id_map(lines, rooms):
        """
        Initializes the dictionary mapping room ids to Room() objects based on the provided csv file.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_DIM_RAUM_CUR
        ["RAUMID",  "RAUMNAME"]
        ["128307",  "Kollmann Zahraa [00025783]"]
        ["80872","  Audiometrie"]

        The function will also update the provided rooms dictionary.

        :param rooms:   Dictionary mapping room names to Room() objects --> {'BH N 129' : Room(), ... }
        :return:        Dictionary mapping room ids to Room() objects   --> {'127803' : Room(), ... }
        """
        logging.debug("create_room_id_map")
        room_id_map = dict()
        for line in lines:
            room_obj = Room(line[1])
            room_obj.add_id(id=line[0], system='Polypoint')
            # Update the room_id_map dictionary
            room_id_map[line[0]] = Room(line[1])
            # Update the rooms dictionary
            if line[1] not in rooms.keys():
                rooms[line[1]] = room_obj
            else:
                rooms[line[1]].add_id(id=line[0], system='Polypoint')

        logging.info(f"{len(room_id_map)} rooms created")
        return room_id_map

    @staticmethod
    def add_rooms_to_appointment(lines, appointments, rooms):
        """
        Reads room data from the csv file and adds the created Room() to an Appointment() in appointments and vice versa.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_FACT_TERMINRAUM --> LEFT JOIN V_DH_DIM_RAUM --> ON [V_DH_FACT_TERMINRAUM].RAUMID = V_DH_DIM_RAUM_CUR.RAUMID
        ["TERMINID",    "RAUMID",   "RAUMNAME", "TERMINSTART_TS",           "TERMINENDE_TS",            "DAUERINMIN"]
        ["2295658",     "11190",    "P2 A534",  "2007-12-10 08:45:00.0000", "2007-12-10 10:15:00.0000", "90.000000"]
        ["2410965",     "61994",    "C 316",    "2008-02-21 14:00:00.0000", "2008-02-21 15:00:00.0000", "60.000000"]

        :param appointments:    Dictionary mapping appointment ids to Appointment() objects --> { '36830543' : Appointment(), ... }
        :param rooms:           Dictionary mapping room names to a Room() object            --> {'BH N 125' : Room(), ... }
        """
        logging.debug("add_room_to_appointment")
        nr_appointments_not_found = 0
        nr_rooms_not_found = 0
        nr_ok = 0
        lines_iters = itertools.tee(lines, 2)
        for line in tqdm(lines_iters[1], total=sum(1 for _ in lines_iters[0])):
            appointment_id = line[0]
            room_id = line[1]
            appointment_start = line[2]
            room_name = line[3]
            appointment_end = line[4]
            if appointments.get(appointment_id, None) is None:
                nr_appointments_not_found += 1
                continue
            # if room_id_map.get(room_id, None) is None:
            #     nr_rooms_not_found += 1
            #     continue
            # name = room_id_map[room_id]
            if rooms.get(room_name, None) is None:
                new_room = Room(room_name)
                new_room.add_id(id=room_id, system='Polypoint')
                rooms[room_name] = new_room
            rooms[room_name].add_appointment(appointments[appointment_id])
            appointments[appointment_id].add_room(rooms[room_name])

            # TODO: Fix date parsing and store start and end for multiple rooms
            if appointment_start != '':
                appointments[appointment_id].start_datetime = datetime.strptime(appointment_start[:-1], "%Y-%m-%d %H:%M:%S.%f")

            if appointment_end != '':
                appointments[appointment_id].end_datetime = datetime.strptime(appointment_end[:-1], "%Y-%m-%d %H:%M:%S.%f")

            nr_ok += 1
        logging.info(f"{nr_ok} rooms added to appointments, {nr_appointments_not_found} appointments not found, {nr_rooms_not_found} rooms not found")
