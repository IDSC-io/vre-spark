
class Floor:
    """
    Models a floor in the hospital.
    """

    def __init__(self, campus_id=None, ww_building_id=None,
                 sap_building_abbreviation1=None, sap_building_abbreviation2=None,
                 ww_floor_id=None):
        """
        """
        self.campus_id = campus_id
        self.ww_building_id = ww_building_id
        self.sap_building_abbreviations = [x for x in [sap_building_abbreviation1, sap_building_abbreviation2] if x is not None]
        self.ww_floor_id = ww_floor_id
        self.floor_id = self.ww_building_id + " " + self.ww_floor_id if self.ww_building_id is not None and self.ww_floor_id is not None else None
        self.rooms = dict()
