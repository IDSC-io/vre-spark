import os

################################################################################################################
### Contains important functions for writing data used in the VRE model to file, so that the underlying data
### can be inspected and potentially purified manually
################################################################################################################
### define functions for writing to various files

this_filepath = os.path.dirname(os.path.realpath(__file__))
csv_sep = ';'

def write_geraet(geraet_tuple, filepath = os.path.join(this_filepath, 'Geraet.csv'), csv_sep = csv_sep):
    """
    Will write the two entries in geraet_tuple (tuple of length 2) to filepath using csv_sep.
    :param geraet:      Tuple of length 2
    :param filepath:    Path of the CSV file to write to in "a" mode
    :param csv_sep:     CSV separator to use in the file
    """
    try:
        with open(filepath, 'a') as writefile:
            writefile.write(f"{geraet_tuple[0]}{csv_sep}{geraet_tuple[1]}\n")
    except Exception as e:
        print(f"Error: {e}")

def write_patient_case(case, has_risk, filepath = os.path.join(this_filepath, 'Pat_Case.csv'), csv_sep = csv_sep):
    """
    Will write the relevant case and associated patient ID (each case also has an associated patient ID) to filepath using csv_sep.
    :param case:        A Case() object
    :param has_risk:    Boolean indicating whether or not the patient had a risk during his or her relevant stay (i.e. was ever in contact with any VRE screening)
    :param filepath:    Path of the CSV file to write to in "a" mode
    :param csv_sep:     CSV separator to use in the file
    """
    try:
        with open(filepath, 'a') as writefile:
            writefile.write(f"{case.patient_id}{csv_sep}{case.case_id}{csv_sep}{has_risk}\n")
    except Exception as e:
        print(f"Error: {e}")

def write_employee(employee_id, filepath = os.path.join(this_filepath, 'Employee.csv')):
    """
    Will write employee_id to filepath using csv_sep.
    :param employee:    string
    :param filepath:    Path of the CSV file to write to in "a" mode
    """
    try:
        with open(filepath, 'a') as writefile:
            writefile.write(f"{employee_id}\n")
    except Exception as e:
        print(f"Error: {e}")

def write_room(room, filepath = os.path.join(this_filepath, 'Room.csv') ):
    """
    Will write room to filepath using csv_sep.
    :param room:        string
    :param filepath:    Path of the CSV file to write to in "a" mode
    """
    try:
        with open(filepath, 'a') as writefile:
            writefile.write(f"{room}\n")
    except Exception as e:
        print(f"Error: {e}")



