from sklearn.feature_extraction import DictVectorizer
from sklearn import tree
import numpy as np
import pandas as pd
from vre.HDFS_data_loader import HDFS_data_loader
import logging


class features_extractor:
    """
    Creates pandas dataframes with features, labels and relevant dates, and exports them to csv, gephi and neo4j
    """
    def prepare_features_and_labels(self, patients):
        """
        Creates the feature np array and label np array (along with relevant dates np array).

        :param patients: Dictionary mapping patient ids to Patient() objects --> {"00001383264" : Patient(), "00001383310" : Patient(), ...}

        :return: (features, labels, dates, v)
        """
        risk_factors = []
        labels = []
        dates = []
        for patient in patients.values():
            patient_features = patient.get_features()   # Dictionary of the form --> {"length_of_stay" : 47, "nr_cases" : 3, ... }
            if patient_features is not None:
                risk_factors.append(patient_features)
                labels.append(patient.get_label())      # patient.get_label() will return an integer between -1 and 3
                dates.append(patient.get_risk_date())   # patient.get_risk_date() will return a datetime.datetime() object corresponding to the risk date associated with the label
        v = DictVectorizer(sparse=False)
        features = v.fit_transform(risk_factors)        # features.shape yields a tuple of dimensions in the format (nrow, ncol), or a tuple (LENGTH, ) in case of only 1 dimension (i.e. a list)
        # print(features)
        # print(features.shape)
        # print(type(features))
        ###############################################################################################################
        # features is a numpy.ndarray() object with one row per patient containing the "fitted" risk factors for each patient in the one-of-K fashion described in the .fit_transform() documentation
        # E.g. for a categorical value with levels "a", "b", and "c", it will contain three columns containing 0 or 1 and corresponding to value=a, value=b and value=c
        ###############################################################################################################
        # filter = ~np.isnan(features).any(axis=1)
        # features_filtered = features[~np.isnan(features).any(axis=1)]
        labels_np = np.asarray(labels)
        # labels_np is a 1-D numpy.ndarray() object based on labels, which is a list of labels between -1 and 3 for each patient in the dataset (e.g. ndarray([1 2 2 -1 3 3 2 1 1 2]) )
        # print(labels_np)
        # print(type(labels_np))
        ###############################################################################################################
        # labels_np
        # labels_filtered = labels_np[filter]
        dates_np = np.asarray(dates)
        # same structure as labels_np, but contains the risk date for each patient in the dataset as a datetime.datetime() object
        ###############################################################################################################
        # dates_filtered = dates_np[filter]

        return (features, labels_np, dates_np, v)

    def export_csv(self, features, labels, dates, v, file_path):
        """
        Combines features, labels and dates in a pandas.DataFrame(), and exports all data to the csv file given in file_path.

        :param features:    numpy.ndarray() with one row per patient containing the "fitted" risk factors for each patient in the one-of-K fashion
        :param labels:      1-D numpy.ndarray() containing the labels for each patient (integers between -1 and 3)
        :param dates:       1-D numpy.ndarray() containing risk dates labels for each patient
        :param v:           sklearn.feature_extraction.DictVectorizer() object with which the features parameter was created
        """
        sorted_cols = [k for k in sorted(v.vocabulary_, key=v.vocabulary_.get)] # v.vocabulary_ is a dictionary mapping feature names to feature indices
        df = pd.DataFrame(data=features, columns=sorted_cols)
        df["label"] = labels
        df["diagnosis_date"] = dates
        df.to_csv(file_path, sep=",", encoding="utf-8", index=False) # index = False will prevent writing row names in a separate, unlabeled column

    def export_gephi(
        self,
        features,
        labels,
        dates,
        v,
        edge_filename="edge_list.csv",
        node_filename="node_list.csv",
    ):
        """
        Export the node list and edge list for visualisation in Gephi
        :param features:
        :param labels:
        :param dates:
        :param v:
        :return:
        """

        room_vocabulary = dict(
            [(k, v) for k, v in v.vocabulary_.items() if k.startswith("room")]
        )
        room_cols = list([v for k, v in room_vocabulary.items()])

        employee_vocabulary = dict(
            [(k, v) for k, v in v.vocabulary_.items() if k.startswith("employee")]
        )
        employee_cols = list([v for k, v in employee_vocabulary.items()])

        device_vocabulary = dict(
            [(k, v) for k, v in v.vocabulary_.items() if k.startswith("device")]
        )
        device_cols = list([v for k, v in device_vocabulary.items()])

        edge_list = open(edge_filename, "w")
        edge_list.write("Source,Target,Weight,Type,Art\n")
        nr_patients = len(features)
        for ind in range(nr_patients):
            # only include patients with screening (labels 1,2,3)
            print(labels.item(ind))
            if labels.item(ind) >= 1:
                print(ind)
                for j in range((ind + 1), nr_patients):
                    print(j)
                    # only include patients with screening (labels 1,2,3)
                    if labels.item(j) >= 1:
                        # edge goes from older to newer relevant date
                        source_target_str = str(ind) + "," + str(j)
                        if dates[ind] is None or dates[j] is None or dates[ind] > dates[j]:
                            source_target_str = str(j) + "," + str(ind)
                        # shared rooms
                        weight_rooms = sum(
                            np.logical_and(features[ind, room_cols], features[j, room_cols])
                        )
                        if weight_rooms > 0:
                            edge_list.write(
                                source_target_str
                                + ","
                                + str(weight_rooms)
                                + ',"directed","rooms"\n'
                            )
                        # shared employees
                        weight_employees = sum(
                            np.logical_and(
                                features[ind, employee_cols], features[j, employee_cols]
                            )
                        )
                        if weight_employees > 0:
                            edge_list.write(
                                source_target_str
                                + ","
                                + str(weight_employees)
                                + ',"directed","employees"\n'
                            )
                        # shared devices
                        weight_devices = sum(
                            np.logical_and(features[ind, device_cols], features[j, device_cols])
                        )
                        if weight_devices > 0:
                            edge_list.write(
                                source_target_str
                                + ","
                                + str(weight_devices)
                                + ',"directed","devices"\n'
                            )

        edge_list.close()

        node_list = open(node_filename, "w")
        node_list.write("Id,Label,Start,Category\n")
        for ind, dt in enumerate(dates):
            infection = ""
            if dt is not None:
                infection = dt.strftime("%Y-%m-%d")
            node_list.write(str(ind) + ',"' + infection + '",0,' + str(labels[ind]) + "\n")

        node_list.close()

if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO, datefmt='%d.%m.%Y %H:%M:%S')

    logging.info("Initiating HDFS_data_loader")
    # loader = HDFS_data_loader(base_path='T:/IDSC Projects/VRE Model/Data/Model Data', hdfs_pipe = False) # hdfs_pipe = False --> files will be loaded directly from CSV
    loader = HDFS_data_loader(base_path="T:/IDSC Projects/VRE Model/Data/Test Data", hdfs_pipe=False)  # For testing purposes --> switch delimiter to ";" !
    patient_data = loader.patient_data()

    logging.info("creating model")
    model_creator = features_extractor()
    (features, labels, dates, v) = model_creator.prepare_features_and_labels(patient_data["patients"])

    logging.info("exporting data")
    model_creator.export_csv(features, labels, dates, v, "patients.csv")

    ##########################################################################
    ### For overview purposes (works only on test data)

    # Room object
    # print('\nRoom object')
    # for attribute in ['name', 'moves', 'appointments', 'beds']:
    #     print(getattr(patient_data['rooms']['BH N 125'], attribute), type(getattr(patient_data['rooms']['BH N 125'], attribute)))
    #
    # # Bed object
    # print('\nBed object')
    # for attribute in ['name', 'moves']:
    #     print(getattr(patient_data['rooms']['BH N 125'].beds['BHN125F'], attribute), type(getattr(patient_data['rooms']['BH N 125'].beds['BHN125F'], attribute)))
    #
    # # Moves object
    # print('\nMoves object')
    # for attribute in ['fal_nr', 'lfd_nr','bew_ty','bw_art','bwi_dt','statu','bwe_dt','ldf_ref','kz_txt','org_fa','org_pf','org_au','zimmr','bett','storn','ext_kh', 'room', 'ward', 'case']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0], attribute)))
    #
    # # Ward object
    # print('\nWard object')
    # for attribute in ['name', 'moves', 'appointments']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].ward, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].ward, attribute)))
    #
    # # Case object
    # print('\nCase object')
    # for attribute in ['patient_id','case_id','case_typ','case_status','fal_ar','beg_dt','end_dt','patient_typ','patient_status','appointments','cares','surgeries','moves','moves_start','moves_end','referrers','patient','medications']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case, attribute)))
    #
    # # Patient object
    # print('\nPatient object')
    # for attribute in ['patient_id','geschlecht','geburtsdatum','plz','wohnort','kanton','sprache','cases','risks']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.patient, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.patient, attribute)))
    #
    # # Care object
    # print('\nCare object')
    # for attribute in ['patient_id','case_id','dt','duration_in_minutes','employee_nr','employee']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.cares[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.cares[0], attribute)))
    #
    # # Employee object
    # print('\nEmployee object')
    # for attribute in ['mitarbeiter_id']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.cares[0].employee, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.cares[0].employee, attribute)))
    #
    # # Appointment object
    # print('\nAppointment object')
    # for attribute in ['termin_id','is_deleted','termin_bezeichnung','termin_art','termin_typ','termin_datum','dauer_in_min','devices','employees','rooms']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.appointments[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.appointments[0], attribute)))
    #
    # # Surgery object
    # print('\nSurgery object')
    # for attribute in ['bgd_op','lfd_bew','icpmk','icpml','anzop','lslok','fall_nr','storn','org_pf','chop']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.surgeries[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.surgeries[0], attribute)))
    #
    # # Chop object
    # print('\nChop object')
    # for attribute in ['chop_code','chop_verwendungsjahr','chop','chop_code_level1','chop_level1','chop_code_level2','chop_level2','chop_code_level3','chop_level3','chop_code_level4','chop_level4','chop_code_level5','chop_level5','chop_code_level6','chop_level6','chop_status','chop_sap_katalog_id','cases']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.surgeries[0].chop, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.surgeries[0].chop, attribute)))
    #
    # # Medication object
    # print('\nMedication object')
    # for attribute in ['patient_id','case_id','drug_text','drug_atc','drug_quantity','drug_unit','drug_dispform','drug_submission']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.medications[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.medications[0], attribute)))
    #
    # # Partner object - these are found in the 'referrers' set attribute, which is why the attribute is converted to a list()
    # print('\nPartner object')
    # for attribute in ['gp_art','name1','name2','name3','land','pstlz','ort','ort2','stras','krkhs','referred_cases']:
    #     print(getattr(list(patient_data['rooms']['BH N 125'].moves[0].case.referrers)[0], attribute), type(getattr(list(patient_data['rooms']['BH N 125'].moves[0].case.referrers)[0], attribute)))
    #
    # # Device object
    # print('\nDevice object')
    # for attribute in ['geraet_id','geraet_name']:
    #     print(getattr(patient_data['devices']['64174'], attribute), type(getattr(patient_data['devices']['64174'], attribute)))
    #
    # # Risk object
    # print('\nRisk object --> see class definition')
    ##########################################################################

    logging.info("data exported successfully")



