################################################################################################################
#  Contains important functions for exporting count distributions (i.e. appearances) of various
# entities in the VRE model
################################################################################################################


def get_room_distribution(appointment_dict, file_path, write_mode='w', csv_sep=';'):
    """
    This function will count the occurrence of all Room() objects based on the data contained in all Appointments() of the VRE model. This information
    is extracted from the Appointment().rooms attribute. The written file contains:
    --> Room names
    --> Occurrence counts

    :param appointment_dict:    A dictionary mapping appointment.termin_id to corresponding Appointment() objects
    :param file_path:           Path for writing to file
    :param write_mode:          Write mode for file_path (default is 'w')
    :param csv_sep:             Separator for CSV files (default is ';')
    """
    room_counts = {}  # dictionary containing room counts
    for apmnt in appointment_dict.values():
        for room_obj in apmnt.rooms:
            if room_obj.name not in room_counts.keys():
                room_counts[room_obj.name] = 1
            else:  # increment occurrence of room
                room_counts[room_obj.name] += 1
    # Then write results to file
    room_names = sorted(list(room_counts.keys()))
    with open(file_path, write_mode) as writefile:
        writefile.write(f"Room_Name{csv_sep}Count\n")
        for each_name in room_names:
            writefile.write(f"{each_name}{csv_sep}{room_counts[each_name]}" + '\n')
    print('\nSuccessfully wrote room counts to file !\n')


def get_ward_distribution(case_dict, file_path, write_mode='w', csv_sep=';'):
    """
    This function will count the occurrence of all Ward() objects in the data.
    This information is extracted from the Case().stays attribute, since each Stay() object has an attribute Stay().ward . The written file contains
    --> Ward names
    --> Occurrence counts

    :param case_dict:   A dictionary mapping Case().case_id to corresponding Case() objects
    :param file_path:   Path for written file
    :param write_mode:  Write mode for file_path (default is 'w')
    :param csv_sep:     Separator for CSV files (default is ';')
    """
    ward_count = {}
    for each_case in case_dict.values():
        for each_stay in each_case.stays.values():
            if each_stay.ward is None:
                continue
            if each_stay.ward.name not in ward_count.keys():
                ward_count[each_stay.ward.name] = 1
            else:
                ward_count[each_stay.ward.name] += 1
    # Write results to file
    ward_names = sorted(list(ward_count.keys()))
    with open(file_path, write_mode) as writefile:
        writefile.write(f"Ward_Name{csv_sep}Count\n")
        for each_name in ward_names:
            writefile.write(f"{each_name}{csv_sep}{ward_count[each_name]}" + '\n')
    print('\nSuccessfully wrote ward counts to file !\n')


def collect_patient_info(pat_dict, file_path, write_mode='w', csv_sep=';'):
    """
    This function will collect important information on all patients in the VRE dataset. This information will be taken from various sources, most
    importantly the Patient().cases attribute list. The information collected (and printed to file) includes:
    --> Patient ID
    --> Number of cases
    --> Number of appointments
    --> Total duration of all appointments
    --> number of stays
    --> Number of surgeries
    --> Number of medications

    :param pat_dict:    Dictionary mapping Patient().patient_id to corresponding Patient() objects
    :param file_path:   Path for written file
    :param write_mode:  Write mode for file_path (default is 'w')
    :param csv_sep:     Separator for CSV files (default is ';')
    """
    pat_counts = {}  # dictionary mapping patient ids to the various measures described in the docstring
    for pat_tuple in pat_dict.items():
        if pat_tuple[0] not in pat_counts.keys():
            pat_counts[pat_tuple[0]] = {}
        # Extract number of cases
        pat_counts[pat_tuple[0]]['Case Count'] = len(list(pat_tuple[1].cases))
        # Extract other required metrics
        apmnt_count = 0
        apmnt_duration = 0
        stay_count = 0
        surgery_count = 0
        medication_count = 0
        for each_case in pat_tuple[1].cases:
            apmnt_count += len(each_case.appointments)
            for each_apmnt in each_case.appointments:
                apmnt_duration += each_apmnt.duration_in_mins
            stay_count += len(list(each_case.stays.keys()))
            surgery_count += len(each_case.surgeries)
            medication_count += len(each_case.medications)
        # Add all extracted measures to pat_counts
        pat_counts[pat_tuple[0]]['Appointment Count'] = apmnt_count
        pat_counts[pat_tuple[0]]['Appointment Duration'] = apmnt_duration
        pat_counts[pat_tuple[0]]['Stay Count'] = stay_count
        pat_counts[pat_tuple[0]]['Surgery Count'] = surgery_count
        pat_counts[pat_tuple[0]]['Medication Count'] = medication_count
    # Then write results to file
    with open(file_path, write_mode) as write_file:
        write_file.write(
            f"Patient_ID{csv_sep}Case_Count{csv_sep}Appointment_Count{csv_sep}Appointment_Duration{csv_sep}Stay_Count{csv_sep}Surgery_Count{csv_sep}Medication_Count\n")
        for each_tuple in pat_counts.items():
            data_list = [each_tuple[0],
                         each_tuple[1]['Case Count'],
                         each_tuple[1]['Appointment Count'],
                         each_tuple[1]['Appointment Duration'],
                         each_tuple[1]['Stay Count'],
                         each_tuple[1]['Surgery Count'],
                         each_tuple[1]['Medication Count']]
        write_file.write(csv_sep.join(data_list) + '\n')
    print('\nSuccessfully collected patient information !\n')
