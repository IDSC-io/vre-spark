# -*- coding: utf-8 -*-
"""This script contains the ``Bed`` class used in the VRE model.

-----
"""

from datetime import datetime

class Bed:
    """Models a Bed.
    """
    def __init__(self, name):
        self.name = name
        self.moves = []

    def add_move(self, move):
        """Adds a move to the self.moves() list of this bed.

        Args:
            move (Move() Object):   Move() object to append.
        """
        self.moves.append(move)
