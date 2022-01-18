from datetime import datetime


class Ward:
    def __init__(self, name):
        self.name = name
        self.stays = []
        self.appointments = []

    def add_stay(self, stay):
        self.stays.append(stay)

    def get_stays_during(self, start_dt, end_dt):
        overlapping_stays = []
        for m in self.stays:
            e_dt = m.to_datetime if m.to_datetime is not None else datetime.now()
            if e_dt >= start_dt and m.from_datetime <= end_dt:
                overlapping_stays.append(m)
        return overlapping_stays
    # TODO: Leads to stackoverflow
    # def __repr__(self):
    #     return str(dict((key, value) for key, value in self.__dict__.items()
    #             if not callable(value) and not key.startswith('__')))
    #
    # def __str__(self):
    #     return self.__repr__()