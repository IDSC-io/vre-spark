from datetime import datetime


class Ward:
    def __init__(self, name):
        self.name = name
        self.moves = []
        self.appointments = []

    def add_move(self, move):
        self.moves.append(move)

    @staticmethod
    def get_moves_during(r, start_dt, end_dt):
        overlapping_moves = []
        for m in r.moves:
            e_dt = m.to_datetime if m.to_datetime is not None else datetime.now()
            if e_dt >= start_dt and m.from_datetime <= end_dt:
                overlapping_moves.append(m)
        return overlapping_moves
