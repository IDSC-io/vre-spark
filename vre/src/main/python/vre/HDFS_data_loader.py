import subprocess
import csv
import os
import logging


from model.Patient import Patient
from model.Risk import Risk
from model.Case import Case
from model.Room import Room
from model.Move import Move
from model.Medication import Medication
from model.Appointment import Appointment
from model.Device import Device
from model.Employee import Employee
from model.Chop import Chop
from model.Surgery import Surgery
from model.Partner import Partner
from model.Care import Care
from model.ICD import ICD

###############################################################################################################

class HDFS_data_loader:
    """
    Loads all the csv files from HDFS and creates the data model.
    """

    def __init__(self, base_path="/data/vre/", hdfs_pipe = True):

        logging.debug(f"base_path: {base_path}")

        self.devices_path = os.path.join(base_path, "V_DH_DIM_GERAET_CUR.csv")
        self.patients_path = os.path.join(base_path, "V_DH_DIM_PATIENT_CUR.csv")
        self.cases_path = os.path.join(base_path, "V_LA_ISH_NFAL_NORM.csv")
        self.moves_path = os.path.join(base_path, "LA_ISH_NBEW.csv")
        self.risks_path = os.path.join(base_path, "V_LA_ISH_NRSF_NORM.csv")
        self.deleted_risks_path = os.path.join(base_path, "deleted_screenings.csv")
        self.appointments_path = os.path.join(base_path, "V_DH_DIM_TERMIN_CUR.csv")
        self.device_appointment_path = os.path.join(
            base_path, "V_DH_FACT_TERMINGERAET.csv"
        )
        self.appointment_patient_path = os.path.join(
            base_path, "V_DH_FACT_TERMINPATIENT.csv"
        )
        self.rooms_path = os.path.join(base_path, "V_DH_DIM_RAUM_CUR.csv")
        self.room_appointment_path = os.path.join(base_path, "V_DH_FACT_TERMINRAUM.csv")
        self.appointment_employee_path = os.path.join(
            base_path, "V_DH_FACT_TERMINMITARBEITER.csv"
        )
        self.medication_path = os.path.join(base_path, "V_LA_IPD_DRUG_NORM.csv")
        self.bwart_path = os.path.join(base_path, "BWTYP-BWART.csv")
        self.partner_path = os.path.join(base_path, "LA_ISH_NGPA.csv")
        self.partner_case_path = os.path.join(base_path, "LA_ISH_NFPZ.csv")
        self.chop_path = os.path.join(base_path, "V_DH_REF_CHOP.csv")
        self.surgery_path = os.path.join(base_path, "LA_ISH_NICP.csv")
        self.tacs_path = os.path.join(base_path, "TACS_DATEN.csv")
        self.icd_path = os.path.join(base_path, "LA_ISH_NDIA.csv")

        self.hdfs_pipe = hdfs_pipe # binary attribute specifying whether data are to be read from Hadoop (True) or directly from CSV (False)

    def get_hdfs_pipe(self, path):
        '''Loads the datafile specified in path from the Hadoop file system, and returns the file WITHOUT HEADER as a csv.reader() instance.
        This function is used in the method patient_data() below if hdfs_pipe == True (the default)'''
        logging.debug(f"get_hdfs_pipe: {path}")
        encoding = "iso-8859-1"
        cat = subprocess.Popen(["hadoop", "fs", "-cat", path], stdout=subprocess.PIPE)
        output = cat.communicate()[0].decode(encoding)
        lines = csv.reader(output.splitlines(), delimiter=",")
        next(lines, None)  # skip header
        return lines

    def get_csv_file(self, csv_path):
        '''Loads the datafile specified in csv_path, and returns the file WITHOUT HEADER as a csv.reader() instance.
        csv_path must be an ABSOLUTE filepath. This function is used in the method patient_data() below if hdfs_pipe != True.
        IMPORTANT: since the csv.reader() instance is returned by this functions via open(csv_path, ...), these files may not be properly closed !'''
        logging.debug(f"csv_path: {csv_path}")
        encoding = "iso-8859-1"
        output = csv.reader(open(csv_path, 'r', encoding=encoding), delimiter=';') # Note: Test Data are ';'-delimited, but original data are ','-delimited !
        next(output, None) # ignore the header line
        return output

    def patient_data(self, risk_only=False):
        """
        Prepares patient data based on all results obtained from the SQL queries.
        If self.hdfs_pipe == True, will use the self.get_hdfs_pipe() method, otherwise the self.get_csv_file() method is used.
        """
        rooms = dict()
        wards = dict()
        
        logging.info(f"Processing patient data (hdfs_pipe is {self.hdfs_pipe})")

        # Load Patient data from table: V_DH_DIM_PATIENT_CUR
        logging.info("loading patient data")
        patients = Patient.create_patient_dict(self.get_hdfs_pipe(self.patients_path) if self.hdfs_pipe == True else self.get_csv_file(self.patients_path))

        # Load Case data from table: V_LA_ISH_NFAL_NORM
        logging.info("loading case data")
        cases = Case.create_case_map(self.get_hdfs_pipe(self.cases_path) if self.hdfs_pipe == True else self.get_csv_file(self.cases_path), patients)

        # Load Partner data from table: LA_ISH_NGPA
        partners = Partner.create_partner_map(self.get_hdfs_pipe(self.partner_path) if self.hdfs_pipe == True else self.get_csv_file(self.partner_path))
        Partner.add_partners_to_cases( # This will update partners from table: LA_ISH_NFPZ
            self.get_hdfs_pipe(self.partner_case_path) if self.hdfs_pipe == True else self.get_csv_file(self.partner_case_path), cases, partners
        )

        # Load Move data from table: LA_ISH_NBEW
        logging.info("loading move data")
        Move.add_move_to_case(self.get_hdfs_pipe(self.moves_path) if self.hdfs_pipe == True else self.get_csv_file(self.moves_path),
                              cases, rooms, wards, partners )

        # Load Risk data from table: V_LA_ISH_NRSF_NORM
        logging.info("loading risk data")
        Risk.add_risk_to_patient(self.get_hdfs_pipe(self.risks_path) if self.hdfs_pipe == True else self.get_csv_file(self.risks_path), patients)
        Risk.add_deleted_risk_to_patient( # Update data from table: deleted_screenings
            self.get_hdfs_pipe(self.deleted_risks_path) if self.hdfs_pipe == True else self.get_csv_file(self.deleted_risks_path), patients
        )

        if risk_only:
            logging.info("keeping only risk patients")
            patients_risk = dict()
            for patient in patients.values():
                if patient.get_label() > 0:
                    patients_risk[patient.patient_id] = patient
            patients = patients_risk
        logging.info(f"{len(patients)} patients")

        # Load Drug data from table: V_LA_IPD_DRUG_NORM
        logging.info("loading drug data")
        drugs = Medication.create_drug_map(self.get_hdfs_pipe(self.medication_path) if self.hdfs_pipe == True else self.get_csv_file(self.medication_path))
        Medication.add_medication_to_case( # Update is based on the same table
            self.get_hdfs_pipe(self.medication_path) if self.hdfs_pipe == True else self.get_csv_file(self.medication_path), cases
        )

        # Load CHOP data from table: V_DH_REF_CHOP
        logging.info("loading chop data")
        chops = Chop.create_chop_dict(self.get_hdfs_pipe(self.chop_path) if self.hdfs_pipe == True else self.get_csv_file(self.chop_path))

        # Add Surgery data to cases from table: LA_ISH_NICP
        Surgery.add_surgery_to_case(self.get_hdfs_pipe(self.surgery_path) if self.hdfs_pipe == True else self.get_csv_file(self.surgery_path), cases, chops)

        # Load Appointment data from table: V_DH_DIM_TERMIN_CUR
        logging.info("loading appointment data")
        appointments = Appointment.create_termin_map( self.get_hdfs_pipe(self.appointments_path) if self.hdfs_pipe == True else self.get_csv_file(self.appointments_path) )

        # Add Appointments to cases from table: V_DH_FACT_TERMINPATIENT
        Appointment.add_appointment_to_case( self.get_hdfs_pipe(self.appointment_patient_path) if self.hdfs_pipe == True else self.get_csv_file(self.appointment_patient_path),
                                             cases, appointments )

        # Load Device data from table: V_DH_DIM_GERAET_CUR
        logging.info("loading device data")
        devices = Device.create_device_map(self.get_hdfs_pipe(self.devices_path) if self.hdfs_pipe == True else self.get_csv_file(self.devices_path))

        # Add Device data to Appointments from table: V_DH_FACT_TERMINGERAET
        Device.add_device_to_appointment( self.get_hdfs_pipe(self.device_appointment_path) if self.hdfs_pipe == True else self.get_csv_file(self.device_appointment_path),
                                          appointments, devices )

        # Load Employee data (RAP) from table: V_DH_FACT_TERMINMITARBEITER
        logging.info("loading employee data from RAP")
        employees = Employee.create_employee_map(
            self.get_hdfs_pipe(self.appointment_employee_path) if self.hdfs_pipe == True else self.get_csv_file(self.appointment_employee_path)
        )
        # Add Employees to Appointments using the same table
        Employee.add_employee_to_appointment( self.get_hdfs_pipe(self.appointment_employee_path) if self.hdfs_pipe == True else self.get_csv_file(self.appointment_employee_path),
                                              appointments, employees )

        # Add Care data to Cases from table: TACS_DATEN
        logging.info("Adding Care data to Cases from TACS")
        Care.add_care_to_case(self.get_hdfs_pipe(self.tacs_path) if self.hdfs_pipe == True else self.get_csv_file(self.tacs_path), cases, employees)
        # Care() objects are not part of the returned dictionary, they are only used in Case() objects --> Case().cares = [Care(), Care(), ...] (list of all cares for each case)

        # Load Room data from table: V_DH_DIM_RAUM_CUR
        logging.info("loading room data")
        room_id_map = Room.create_room_id_map(self.get_hdfs_pipe(self.rooms_path) if self.hdfs_pipe == True else self.get_csv_file(self.rooms_path))

        # Add Room data to Appointments from table: V_DH_FACT_TERMINRAUM
        Room.add_room_to_appointment(self.get_hdfs_pipe(self.room_appointment_path) if self.hdfs_pipe == True else self.get_csv_file(self.room_appointment_path),
                                     appointments, room_id_map, rooms )

        # Add ICD codes to cases from table: LA_ISH_DIA
        ICD_Codes = ICD.create_icd_dict(self.get_hdfs_pipe(self.icd_path) if self.hdfs_pipe == True else self.get_csv_file(self.icd_path))
        ICD.add_icd_to_case(self.get_hdfs_pipe(self.icd_path) if self.hdfs_pipe == True else self.get_csv_file(self.icd_path), cases)


        return dict(
            {
                "rooms": rooms,
                "wards": wards,
                "partners": partners,
                "patients": patients,
                "cases": cases,
                "drugs": drugs,
                "chops": chops,
                "appointments": appointments,
                "devices": devices,
                "employees": employees,
                "room_id_map": room_id_map,
                'icd_codes' : ICD_Codes
            }
        )


            


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    loader = HDFS_data_loader()
    patient_data = loader.patient_data(risk_only=True)
