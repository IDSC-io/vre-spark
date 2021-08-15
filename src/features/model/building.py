import pandas as pd
from tqdm import tqdm
import logging


class Building:
    """
    Models a building in the hospital.
    """

    def __init__(self, campus_id=None, ww_building_id=None,
                 sap_building_abbreviation1=None, sap_building_abbreviation2=None,
                 ww_building_fullid=None, building_common_name=None, street=None, longitude=None, latitude=None):
        """
        """
        self.campus_id = campus_id
        self.ww_building_id = ww_building_id
        self.sap_building_abbreviation1 = sap_building_abbreviation1
        self.sap_building_abbreviation2 = sap_building_abbreviation2
        self.sap_building_abbreviations = [x for x in [sap_building_abbreviation1, sap_building_abbreviation2] if x is not None]
        self.ww_building_full_id = ww_building_fullid
        self.building_common_name = building_common_name
        self.street = street
        self.longitude = float(longitude)
        self.latitude = float(latitude)

        self.ids = []
        [self.add_id(room_id, "SAP") for room_id in self.sap_building_abbreviations]
        self.add_id(self.ww_building_id, "Waveware")
        self.floors = dict()
        self.rooms = dict()

    def add_id(self, id, system):
        """
        Adds an (id, system) tuple to the Building().ids attribute list if id has not already been added - this prevents duplicate entries.
        This list should be useful in determining whether or not there are overlapping building names among different hospital systems.

        :param id:      id string
        :param system:  system string
        """
        if id not in [id_tuple[0] for id_tuple in self.ids] and not pd.isna(id):
            self.ids.append((str(id), str(system)))

    @staticmethod
    def create_building_id_map(csv_path, encoding, is_verbose=True):
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
        logging.debug("create_room_dict")
        buildings_df = pd.read_csv(csv_path, encoding=encoding, dtype=str, index_col=0)
        buildings_objects = list(map(lambda row: Building(*row), tqdm(buildings_df.values.tolist(), disable=not is_verbose)))
        buildings = {}
        for building in buildings_objects:
            buildings[building.ww_building_id] = building
            for sap_abbreviation in building.sap_building_abbreviations:
                buildings[sap_abbreviation] = building

        return buildings

    def get_record(self):
        return {"Campus ID": self.campus_id, "WW Building ID": self.ww_building_id,
                "SAP Building Abbreviation 1":self.sap_building_abbreviation1, "SAP Building Abbreviation 2":self.sap_building_abbreviation2,
                "Building Full ID": self.ww_building_full_id, "Building Common Name": self.building_common_name, "Street": self.street, "Longitude": self.longitude, "Latitude": self.latitude}
