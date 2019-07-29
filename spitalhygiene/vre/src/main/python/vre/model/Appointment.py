from datetime import datetime
import logging


class Appointment:
    """
    Models an appointment from RAP.
    Schema: [TERMINID],[IS_DELETED],[TERMINBEZEICHNUNG],[TERMINART],[TERMINTYP],[TERMINDATUM],[DAUERINMIN]
    """

    def __init__(
        self,
        termin_id,
        is_deleted,
        termin_bezeichnung,
        termin_art,
        termin_typ,
        termin_datum,
        dauer_in_min,
    ):
        self.termin_id = termin_id
        self.is_deleted = is_deleted
        self.termin_bezeichnung = termin_bezeichnung
        self.termin_art = termin_art
        self.termin_typ = termin_typ
        self.termin_datum = datetime.strptime(termin_datum, "%Y-%m-%d %H:%M:%S")
        try:
            self.dauer_in_min = int(float(dauer_in_min))
        except ValueError as e:
            self.dauer_in_min = 0

        self.devices = []
        self.employees = []
        self.rooms = []

    def add_device(self, device):
        self.devices.append(device)

    def add_room(self, room):
        self.rooms.append(room)

    def add_employee(self, employee):
        self.employees.append(employee)

    def create_termin_map(lines):
        """
        Loads the appointments from csv reader.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_DIM_TERMIN_CUR
        ["TERMINID", "IS_DELETED",  "TERMINBEZEICHNUNG",    "TERMINART",        "TERMINTYP",        "TERMINDATUM",              "DAUERINMIN"]
        ["957219",   "0",           "K90 HINF",             "K90 HINF",         "Patiententermin",  "2005-02-04 00:00:00.0000", "90.00000"]
        ["957224",   "0",           "Konsultation 15'",     "Konsultation 15'", "Patiententermin",  "2005-02-03 00:00:00.0000", "15.00000"]

        Returns: A dictionary mapping appointment ids to Appointment() objects --> { '36830543' : Appointment(), ... }
        """
        logging.debug("create_termin_map")
        nr_malformed = 0
        nr_ok = 0
        appointments = dict()
        for line in lines:
            if len(line) != 7:
                nr_malformed += 1
                continue
            else:
                appointment = Appointment(*line)
                appointments[appointment.termin_id] = appointment
            nr_ok += 1
        logging.info(f"{nr_ok} ok, {nr_malformed} appointments malformed")
        return appointments

    def add_appointment_to_case(lines, cases, appointments):
        """
        Adds Appointment() objects to the SAP cases based on lines read from a csv file.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_FACT_TERMINPATIENT
        ["TERMINID", "PATIENTID",   "FALLID"]
        ["35672314", "00008210020", "0005660334"]
        ["17255155", "00002042800", "0004017880"]

        :param cases:           Dictionary mapping case ids to Case() objects                   --> {"0003536421" : Case(), "0003473241" : Case(), ...}
        :param appointments:    Dictionary mapping appointment ids to Appointment() objects     --> { '36830543' : Appointment(), ... }
        """
        logging.debug("add_appointment_to_case")
        nr_appointment_not_found = 0
        nr_case_not_found = 0
        nr_ok = 0
        for line in lines:
            appointment_id = line[0]
            case_id = line[2]
            if appointments.get(appointment_id, None) is None:
                nr_appointment_not_found += 1
                continue
            if cases.get(case_id, None) is None:
                nr_case_not_found += 1
                continue
            cases[case_id].add_appointment(appointments[appointment_id])
            nr_ok += 1
        logging.info(
            f"{nr_ok} ok, {nr_case_not_found} cases not found, {nr_appointment_not_found} appointments not found"
        )
