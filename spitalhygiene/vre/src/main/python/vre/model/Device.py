import logging

class Device:
    '''
    Models a device from RAP.
    Schema: GERAETID;GERAETNAME
    '''
    def __init__(self, geraet_id, geraet_name):
        self.geraet_id = geraet_id
        self.geraet_name = geraet_name

    def create_device_map(lines):
        '''
        Loads all devices into a dictionary based on lines in the csv file.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_DIM_GERAET_CUR
        ["GERAETID",    "GERAETNAME"]
        ["82250",       "ANS-Fix"]
        ["162101",      "Waage"]

        Returns: A dictionary mapping device_ids to Device() object --> {'64174' : Device(), ... }
        '''
        logging.debug("create_device_map")
        devices = dict()
        for line in lines:
            device = Device(*line)
            devices[device.geraet_id] = device
        logging.info(f"{len(devices)} devices created")
        return devices

    def add_device_to_appointment(lines, appointments, devices):
        '''
        Adds the device in devices to the respective appointment in appointments based on lines in the csv file.
        This function will be called by the HDFS_data_loader.patient_data() function (lines is an iterator object). The underlying table is structured as follows:
        >> TABLE NAME: V_DH_FACT_TERMINGERAET
        ["TERMINID", "GERAETID",    "TERMINSTART_TS",           "TERMINENDE_TS",            "DAUERINMIN"]
        ["26266554", "123223",      "2015-04-03 13:45:00.0000", "2015-04-03 15:45:00.0000", "120.000000"]
        ["23678836", "38006",       "2014-07-31 10:00:00.0000", "2014-07-31 10:30:00.0000", "30.000000"]

        :param appointments:    Dictionary mapping appointment ids to Appointment() objects     --> { '36830543' : Appointment(), ... }
        :param devices:         Dictionary mapping device_ids to Device() object                --> {'64174' : Device(), ... }
        '''
        logging.debug("add_device_to_appointment")
        nr_appointment_not_found = 0
        nr_device_not_found = 0
        nr_ok = 0
        for line in lines:
            appointment_id = line[0]
            if appointments.get(appointment_id, None) is None:
                nr_appointment_not_found += 1
                continue
            device_id = line[1]
            if devices.get(device_id, None) is None:
                nr_device_not_found += 1
                continue
            appointments[appointment_id].add_device(devices[device_id])
            nr_ok += 1
        logging.info(
            f"{nr_ok} ok, {nr_appointment_not_found} appointments not found, {nr_device_not_found} devices not found"
        )
