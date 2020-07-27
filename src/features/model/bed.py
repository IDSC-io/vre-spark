# -*- coding: utf-8 -*-
"""This script contains the ``Bed`` class used in the VRE model.

-----
"""


class Bed:
    """Models a Bed.
    """

    def __init__(self, name):
        self.name = name
        self.stays = []

    def add_stay(self, stay):
        """Adds a stay to the self.stays() list of this bed.

        Args:
            stay (Stay() Object):   Stay() object to append.
        """
        self.stays.append(stay)
