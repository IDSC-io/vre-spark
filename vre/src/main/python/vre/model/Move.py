from model.Room import Room
from model.Ward import Ward
from datetime import datetime
import logging


class Move:
    def __init__(
        self,
        fal_nr,
        lfd_nr,
        bew_ty,
        bw_art,
        bwi_dt,
        bwi_zt,
        statu,
        bwe_dt,
        bwe_zt,
        lfd_ref,
        kz_txt,
        org_fa,
        org_pf,
        org_au,
        zimmr,
        bett,
        storn,
        ext_kh
    ):
        self.fal_nr = fal_nr
        self.lfd_nr = int(lfd_nr)
        self.bew_ty = bew_ty
        self.bw_art = bw_art
        self.bwi_dt = datetime.strptime(
            bwi_dt + " " + bwi_zt, "%Y-%m-%d %H:%M:%S.0000000"
        )
        self.statu = statu
        self.bwe_dt = None
        try:
            if bwe_zt == '24:00:00.000000000': bwe_zt = "23:59:59.000000000"
            self.bwe_dt = datetime.strptime(
                bwe_dt + " " + bwe_zt, "%Y-%m-%d %H:%M:%S.000000000"
            )
        except ValueError:
            pass
        self.ldf_ref = lfd_ref
        self.kz_txt = kz_txt
        self.org_fa = org_fa
        self.org_pf = org_pf
        self.org_au = org_au
        self.zimmr = zimmr
        self.bett = bett
        self.storn = storn
        self.ext_kh = ext_kh
        self.room = None
        self.ward = None
        self.case = None

    def add_room(self, r):
        self.room = r

    def add_ward(self, ward):
        self.ward = ward

    def add_case(self, c):
        self.case = c

    def get_duration(self):
        end_dt = self.bwe_dt if self.bwe_dt is not None else datetime.now()
        return end_dt - self.bwi_dt

    def create_bwart_map(lines):
        bwart = dict()
        for line in lines:
            bwart[line[0]] = line[1]
        return bwart

    def add_move_to_case(lines, faelle, rooms, wards, partners):
        """
        Reads the moves csv and performs the following:
        --> creates a Move() object from the read-in line data
        --> Adds the created Move() to the corresponding Case()
        --> Extracts the Room() from each Move() and adds them "to each other"
        --> Extracts the Ward() from each Move() and adds them "to each other"
        --> Adds referring hospital to the Case() and vice versa
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The table from which entries are read is structured as follows:
        >> TABLE NAME: LA_ISH_NBEW
        ["FALNR",      "LFDNR", "BEWTY",  "BWART",      "BWIDT",        "BWIZT",        "STATU", "BWEDT",       "BWEZT",        "LFDREF", "KZTXT",                "ORGFA", "ORGPF", "ORGAU", "ZIMMR", "BETT", "STORN", "EXTKH"]
        ["0004496041", "3",     "4",      "SB",         "2014-01-17",   "16:15:00.000", "30",    "2014-01-17",  "16:15:00.000", "0",      "",                     "DIAA",  "DIAA",  "",      "",      "",      "",     ""]
        ["0004496042", "1",     "4",      "BE",         "2014-03-10",   "08:15:00.000", "30",    "2014-03-10",  "08:15:00.000", "0",      "ej/ nü CT um 10.30 h", "ENDA",  "ENDA",  "IICA",  "",      "",      "",     ""]

        :param faelle:   Dictionary mapping case ids to Case()       --> {'0005976205' : Case(), ... }
        :param rooms:    Dictionary mapping room names to Room()     --> {'BH N 125' : Room(), ... }
        :param wards:    Dictionary mapping ward names to Ward()     --> {'N NORD' : Ward(), ... }
        :param partners: Dictionary mapping partner ids to Partner() --> {'0010000990' : Partner(), ... }
        """
        logging.debug("add_move_to_case")
        nr_not_found = 0
        nr_not_formatted = 0
        nr_ok = 0
        for counter, line in enumerate(lines):
            if len(line) != 18:
                nr_not_formatted += 1
            else:
                move = Move(*line)
                # don't consider cancelled movements
                if move.storn == "X":
                    continue

                if faelle.get(move.fal_nr, None) is not None:
                    faelle[move.fal_nr].add_move(move)
                    move.add_case(faelle[move.fal_nr])
                else:
                    nr_not_found += 1
                    continue
                if move.zimmr != "" and move.zimmr != "NULL":
                    if rooms.get(move.zimmr, None) is None:
                        r = Room(move.zimmr)
                        rooms[move.zimmr] = r
                    rooms[move.zimmr].add_move(move)
                    move.add_room(rooms[move.zimmr])
                if move.org_pf != "" and move.org_pf != "NULL":
                    if wards.get(move.org_pf, None) is None:
                        ward = Ward(move.org_pf)
                        wards[move.org_pf] = ward
                    wards[move.org_pf].add_move(move)
                    move.add_ward(wards[move.org_pf])
                if move.ext_kh != "":
                    if move.case is not None and partners.get(move.ext_kh, None) is not None:
                        partners[move.ext_kh].add_case(move.case)
                        move.case.add_referrer(partners[move.ext_kh])
                nr_ok += 1

        logging.info(f"{nr_ok} ok, {nr_not_found} cases not found, {nr_not_formatted} malformed")
