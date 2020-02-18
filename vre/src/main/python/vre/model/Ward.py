from datetime import datetime

class Ward:
    def __init__(self, name):
        self.name = name
        self.moves = []
        self.appointments = []

    def add_move(self, move):
        self.moves.append(move)

    def get_moves_during(r, start_dt, end_dt):
        overlapping_moves = []
        for m in r.moves:
            e_dt = m.bwe_dt if m.bwe_dt is not None else datetime.now()
            if e_dt >= start_dt and m.bwi_dt <= end_dt:
                overlapping_moves.append(m)
        return overlapping_moves

