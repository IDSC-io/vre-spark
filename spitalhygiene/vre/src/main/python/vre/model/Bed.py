from datetime import datetime

class Bed:
    def __init__(self, name):
        self.name = name
        self.moves = []
    def add_move(self, move):
        self.moves.append(move)