import pandas as pd


class Building:
    """
    Models a building in the hospital.
    """

    def __init__(self, campus_id=None, ww_building_id=None,
                 sap_building_abbreviation1=None, sap_building_abbreviation2=None):
        """
        """
        self.campus_id = campus_id
        self.ww_building_id = ww_building_id
        self.sap_building_abbreviations = [x for x in [sap_building_abbreviation1, sap_building_abbreviation2] if x is not None]
        # self.name = name
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