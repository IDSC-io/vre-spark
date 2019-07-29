import logging


class Employee:
    """
    Models an employee (doctor, nurse, etc) from RAP.
    """

    def __init__(self, mitarbeiter_id):
        self.mitarbeiter_id = mitarbeiter_id

    def create_employee_map(lines):
        """
        Read the appointment to employee file and create Employee() objects.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_FACT_TERMINMITARBEITER
        ["TERMINID",    "MITARBEITERID", "TERMINSTART_TS",          "TERMINENDE_TS",            "DAUERINMIN"]
        ["521664",      "0063239",       "2003-11-11 07:30:00.000", "2003-11-11 08:00:00.0000", "30.000000"]
        ["521754",      "X33671",        "2003-11-10 09:15:00.000", "2003-11-10 09:45:00.0000", "30.000000"]

        Returns: Dictionary mapping employee_ids to Employee() objects --> {'0032719' : Employee(), ... }
        """
        logging.debug("create_employee_map")
        employee_dict = dict()
        for line in lines:
            employee = line[1]
            employee_dict[employee] = Employee(employee)
        logging.info(f"{len(employee_dict)} employees created")
        return employee_dict

    def add_employee_to_appointment(lines, appointments, employees):
        """
        Adds Employee() in employees to Appointment() in appointments based on the lines read from the csv file.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object).
        The underlying table is V_DH_FACT_TERMINMITARBEITER, and is identical to the one defined in the create_employee_map() method above.

        :param appointments:    Dictionary mapping appointment ids to Appointment() objects     --> { '36830543' : Appointment(), ... }
        :param employees:       Dictionary mapping employee_ids to Employee() objects           --> {'0032719' : Employee(), ... }
        """
        logging.debug("add_employee_to_appointment")
        nr_employee_not_found = 0
        nr_appointment_not_found = 0
        nr_ok = 0
        for line in lines:
            employee_id = line[1]
            appointment_id = line[0]
            if appointments.get(appointment_id, None) is None:
                nr_appointment_not_found += 1
                continue
            if employees.get(employee_id, None) is None:
                nr_employee_not_found += 1
                continue
            appointments[appointment_id].add_employee(employees[employee_id])
            nr_ok += 1
        logging.info(
            f"{nr_ok} ok, {nr_appointment_not_found} appointments not found, {nr_employee_not_found} employees not found"
        )
