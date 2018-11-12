import logging
from datetime import datetime
from model.Employee import Employee


class Care:
    """
    This represents an entry in TACS.
    Schema: patient_patientid],[patient_typ],[patient_status],[fall_nummer],[fall_typ],[fall_status],[datum_betreuung],[dauer_betreuung_in_min],[mitarbeiter_personalnummer],[mitarbeiter_anstellungsnummer],[mitarbeiter_login],[BATCH_RUN_ID]
    """

    def __init__(
        self,
        patient_patientid,
        patient_typ,
        patient_status,
        fall_nummer,
        fall_typ,
        fall_status,
        datum_betreuung,
        dauer_betreuung_in_min,
        mitarbeiter_personalnummer,
        mitarbeiter_anstellungsnummer,
        mitarbeiter_login,
        batch_run_id,
    ):
        self.patient_id = patient_patientid
        self.case_id = fall_nummer
        try:
            self.dt = datetime.strptime(datum_betreuung, "%Y-%m-%d %H:%M:%S.0")
        except ValueError as e:
            self.dt = datetime.strptime(datum_betreuung, "%Y-%m-%d %H:%M:%S.000")
        self.duration_in_minutes = int(dauer_betreuung_in_min)
        self.employee_nr = mitarbeiter_personalnummer

        self.employee = None

    def add_employee(self, employee):
        self.employee = employee

    def add_care_to_case(lines, cases, employees):
        """
        Adds the entries from TACS as instances of Care() to the respective Case().
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: TACS_DATEN
        ["patient_patientid",   "patient_typ",      "patient_status",   "fall_nummer",  "fall_typ",      "fall_status", "datum_betreuung",       "dauer_betreuung_in_min",  ...
        ["00013768220",         "Standard Patient", "aktiv",            "0006422111",   "Standard Fall", "aktiv",       "2018-03-11 00:00:00.0", "3",                       ...
        ["00000552828",         "Standard Patient", "aktiv",            "0006454306",   "Standard Fall", "aktiv",       "2018-04-10 00:00:00.0", "20",                      ...
        <-- continued -->
        "mitarbeiter_personalnummer", "mitarbeiter_anstellungsnummer",  "mitarbeiter_login",    "BATCH_RUN_ID"]
        "0301119",                    "00026556",                       "I0301119",             "870"]
        "0025908",                    "00014648",                       "I0025908",             "870"]

        :param cases:       Dictionary mapping case ids to Case() objects           --> {"0003536421" : Case(), "0003473241" : Case(), ...}
        :param employees:   Dictionary mapping employee_ids to Employee() objects   --> {'0032719' : Employee(), ... }
        """
        logging.debug("add_care_to_case")
        nr_case_not_found = 0
        nr_employee_created = 0
        nr_employee_found = 0
        for line in lines:
            care = Care(*line)

            # discard if we don't have the case
            case = cases.get(care.case_id, None)
            if case is None:
                nr_case_not_found += 1
                continue
            case.add_care(care)

            # create employee if not already existing
            employee = employees.get(care.employee_nr, None)
            if employee is None:
                employee = Employee(care.employee_nr)
                nr_employee_created += 1
            else:
                nr_employee_found += 1
            employees[employee.mitarbeiter_id] = employee

            care.add_employee(employee)
        logging.info(
            f"{nr_case_not_found} cases not found, {nr_employee_created} employees created, {nr_employee_found} employees found"
        )
