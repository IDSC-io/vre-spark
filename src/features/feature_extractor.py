# -*- coding: utf-8 -*-
"""This script contains the feature_extractor class, which controls the following aspects of VRE:

- Preparation of the feature vector
- Extraction of patient-patient contacts
- Export to various sources (Gephi, CSV, Neo4J, etc.)

-----
"""

from sklearn.feature_extraction import DictVectorizer
import numpy as np
import pandas as pd
import os
import datetime
from dateutil import relativedelta


class FeatureExtractor:
    """Creates pandas dataframes with features, labels and relevant dates, and provides export functions to various
    target systems.
    """
    @staticmethod
    def prepare_features_and_labels(patients):
        """*Internal function used in various data exports.*

        Creates the feature ``np.array()`` and label ``np.array()``, along with relevant dates.

        Args:
            patients (dict):    Dictionary mapping patient ids to Patient() objects of the form
                                ``{"00001383264":  Patient(), "00001383310":  Patient(), ...}``

        Returns:
            tuple: tuple of length 4 of the form :math:`\\longrightarrow` *(features, labels, dates, v)*

            Please refer to function code for more details.
        """
        risk_factors = []
        labels = []
        dates = []
        for patient in patients.values():
            patient_features = patient.get_features()  # Dictionary --> {"length_of_stay" : 47, "nr_cases" : 3, ... }
            if patient_features is not None:
                risk_factors.append(patient_features)
                labels.append(patient.get_label())     # patient.get_label() will return an integer between -1 and 3
                dates.append(patient.get_risk_date())
                # patient.get_risk_date() will return a datetime.datetime() object corresponding to the label risk date
        v = DictVectorizer(sparse=False)
        features = v.fit_transform(risk_factors)

        # TODO: Maybe this is wrong
        labels_np = np.array(labels)
        dates_np = np.array(dates)

        return features, labels_np, dates_np, v

    @staticmethod
    def export_csv(features, labels, dates, v, file_path):
        """Function for exporting features, labels and dates to CSV.

        Combines features, labels and dates in a ``pandas.DataFrame()``, and exports all data to the csv file given
        in file_path.

        Args:
            features (numpy.ndarray()): ``numpy.ndarray()`` with one row per patient containing "fitted" risk factors
                                        for each patient in the one-of-K fashion
            labels (numpy.ndarray()):   1-D ``numpy.ndarray()`` containing the labels for each patient
                                        (integers between -1 and 3)
            dates (numpy.ndarray()):    1-D ``numpy.ndarray()`` containing risk dates for each patient
            v:                          ``sklearn.feature_extraction.DictVectorizer()`` object with which the *features*
                                        parameter was created
            file_path (str):            Absolute path to exported csv file
        """
        sorted_cols = [k for k in sorted(v.vocabulary_, key=v.vocabulary_.get)]
        # --> v.vocabulary_ is a dictionary mapping feature names to feature indices
        df = pd.DataFrame(data=features, columns=sorted_cols)
        df["label"] = labels
        df["diagnosis_date"] = dates
        df.to_csv(file_path, sep=",", encoding="utf-8", index=False)
        # --> index = False will prevent writing row names in a separate, unlabeled column

    @staticmethod
    def export_gephi(features, labels, dates, v, dest_path='.', csv_sep=','):
        """Exports  the node and edge list in Gephi-compatible format for visualisation.

        Edges and nodes will be exported to files ``dest_path/edge_list.csv`` and ``dest_path/node_list.csv``,
        respectively.

        Args:
            features (numpy.ndarray()): ``numpy.ndarray()`` with one row per patient containing the "fitted" risk
                                        factors for each patient in the one-of-K fashion.
            labels (numpy.ndarray()):   1-D ``numpy.ndarray()`` containing the labels for each patient
                                        (integers between -1 and 3)
            dates (numpy.ndarray()):    1-D ``numpy.ndarray()`` containing risk dates for each patient
            v:                          ``sklearn.feature_extraction.DictVectorizer()`` object with which the features
                                        parameter was created
            dest_path (str):            path into which the edge_list.csv and node_list.csv files will be exported
                                        (defaults to the current working directory)
            csv_sep (str):              separator for exported csv files (defaults to ``,``)
        """
        # --> Process Rooms
        room_vocabulary = {key: value for key, value in v.vocabulary_.items() if key.startswith("room")}
        # Dictionary of the form --> 'room=INE GE06': 176, 'room=KARR EKG': 183, ...}

        # Maps the feature (i.e. column) names to their respective column index (useful for array slicing later)
        room_cols = list(room_vocabulary.values())  # list of indices in v containing room information (for slicing)

        # --> Process Employees
        employee_vocabulary = {key: value for key, value in v.vocabulary_.items() if key.startswith("employee")}
        # Dictionary of the form --> 'employee=DHGE035': 126, 'employee=0075470': 178, ...}

        # Maps the feature (i.e. column) names to their respective column index (useful for array slicing later)
        employee_cols = list(employee_vocabulary.values())

        # --> Process Devices
        device_vocabulary = {key: value for key, value in v.vocabulary_.items() if key.startswith("device")}
        # Dictionary of the form --> 'device=ECC': 14, 'device=XY12344': 251, ...}

        # Maps the feature (i.e. column) names to their respective column index (useful for array slicing later)
        device_cols = list(device_vocabulary.values())

        #####################################
        # --> Write EDGE list
        #####################################
        edge_list = open(os.path.join(dest_path, 'edge_list.csv'), "w")
        edge_list.write(csv_sep.join(['Source', 'Target', 'Weight', 'Type', 'Art\n']))
        nr_patients = len(features)
        for ind in range(nr_patients):
            if labels.item(ind) >= 1:  # only include patients with screening (labels 1,2,3)
                for j in range((ind + 1), nr_patients):
                    if labels.item(j) >= 1:  # only include patients with screening (labels 1,2,3)
                        #####################################
                        # --> Process Sources & Targets
                        #####################################
                        # edge goes from older to newer relevant date
                        source_target_str = str(ind) + csv_sep + str(j)
                        # Sources and targets are given as integers from 0 to nr_patients - 1 (NOT as patient ids)
                        if dates[ind] is None or dates[j] is None or dates[ind] > dates[j]:
                            # indicates that relevant date for patient[j] is older --> switch direction of edge
                            source_target_str = str(j) + csv_sep + str(ind)
                        #####################################
                        # --> Process shared rooms (used as an edge weight)
                        #####################################
                        weight_rooms = sum(np.logical_and(features[ind, room_cols], features[j, room_cols]))
                        # logical_and() performs list-wise comparison on multiple lists of the same length and returns
                        #   True if all entries at any position are > 0 and False otherwise
                        if weight_rooms > 0:
                            # Only write to edge_list if weight is > 0 - otherwise, this indicates that patients had
                            #   never shared the same room (same applies for all edge weights)
                            edge_list.write(source_target_str + csv_sep + str(weight_rooms) + csv_sep + "directed" +
                                            csv_sep + "rooms\n")
                        #####################################
                        # --> Process shared employees (used as an edge weight)
                        #####################################
                        weight_employees = sum(np.logical_and(features[ind, employee_cols], features[j, employee_cols]))
                        if weight_employees > 0:
                            edge_list.write(source_target_str + csv_sep + str(weight_employees) + csv_sep + 'directed'
                                            + csv_sep + 'employees\n')
                        #####################################
                        # --> Process shared devices (used as an edge weight)
                        #####################################
                        weight_devices = sum( np.logical_and(features[ind, device_cols], features[j, device_cols]) )
                        if weight_devices > 0:
                            edge_list.write(source_target_str + csv_sep + str(weight_devices) + csv_sep + 'directed' +
                                            csv_sep + 'devices\n')
        # Close file
        edge_list.close()

        #####################################
        # --> Write NODES list
        #####################################
        node_list = open(os.path.join(dest_path, 'node_list.csv'), "w")
        node_list.write(csv_sep.join(['Id', 'Label', 'Start', 'Category\n']))
        for ind, dt in enumerate(dates):  # dates is a vector of either relevant dt.dt() objects or None
            infection = ""
            if dt is not None:
                infection = dt.strftime("%Y-%m-%d")
            node_list.write(str(ind) + csv_sep + infection + csv_sep + '0' + csv_sep + str(labels[ind]) + "\n")

        # Close file
        node_list.close()

    @staticmethod
    def get_contact_patients_for_case(cases, contact_pats):
        """Extracts contact patients for specific cases.

        Appends tuples of length 6 (see param contact_pats) directly to contact_pats, which is a list recording all
        patient contacts.

        Args:
            cases (dict):           Dictionary mapping case ids to Case() objects --> {"0003536421" : Case(),
                                    "0003473241" : Case(), ...}
            contact_pats (list):    List containing tuples of length 6 of either the format: `(source_pat_id,
                                    dest_pat_id, start_overlap_dt, end_overlap_dt, room_name, "kontakt_raum")` in the
                                    case of a contact room, or the format `(source_pat_id, dest_pat_id,
                                    start_overlap_dt, end_overlap_dt, ward_name, "kontakt_org")` in the case of a
                                    contact organization.
        """
        for move in cases.moves.values():
            ####################################################
            # --> Extract contacts in the same room
            ####################################################
            if move.bwe_dt is not None and move.room is not None:
                overlapping_moves = move.room.get_moves_during(move.bwi_dt, move.bwe_dt)
                # get_moves_during() returns a list of all moves overlapping with the .bwi_dt - .bwe_dt interval
                for overlap_move in overlapping_moves:
                    if overlap_move.case is not None and move.case is not None:
                        if overlap_move.case.fal_ar == "1" and overlap_move.bew_ty in ["1", "2", "3"] and \
                                overlap_move.bwe_dt is not None:
                            start_overlap = max(move.bwi_dt, overlap_move.bwi_dt)
                            end_overlap = min(move.bwe_dt, overlap_move.bwe_dt)
                            contact_pats.append(
                                (
                                    move.case.patient_id,
                                    overlap_move.case.patient_id,
                                    start_overlap.strftime("%Y-%m-%dT%H:%M"),
                                    end_overlap.strftime("%Y-%m-%dT%H:%M"),
                                    move.room.name,
                                    "kontakt_raum",
                                ))  # append data in the form of a tuple
            ####################################################
            # --> Extract contacts in the same ward (ORGPF)
            ####################################################
            if move.bwe_dt is not None and move.ward is not None:
                overlapping_moves = move.ward.get_moves_during(move.bwi_dt, move.bwe_dt)
                for overlap_move in overlapping_moves:
                    if overlap_move.case is not None and move.case is not None:
                        if overlap_move.case.fal_ar == "1" and overlap_move.bew_ty in ["1", "2", "3"] and \
                                overlap_move.bwe_dt is not None and \
                                (overlap_move.zimmr is None or move.zimmr is None or overlap_move.zimmr != move.zimmr):
                            start_overlap = max(move.bwi_dt, overlap_move.bwi_dt)
                            end_overlap = min(move.bwe_dt, overlap_move.bwe_dt)
                            contact_pats.append(
                                (
                                    move.case.patient_id,
                                    overlap_move.case.patient_id,
                                    start_overlap.strftime("%Y-%m-%dT%H:%M"),
                                    end_overlap.strftime("%Y-%m-%dT%H:%M"),
                                    move.ward.name,
                                    "kontakt_org",
                                ))

    def get_contact_patients(self, patients):
        """Extracts all patient contacts.

        Extract all contacts between patients in the same room and same ward which occurred during the last year.

        Args:
            patients (dict):    Dictionary mapping patient ids to Patient() objects --> {"00001383264" : Patient(),
                                "00001383310" : Patient(), ...}

        Returns:
            list: List containing tuples of length 6 of either the format

            `(source_pat_id, dest_pat_id, start_overlap_dt, end_overlap_dt, room_name, "kontakt_raum")`

            or the format

            `(source_pat_id, dest_pat_id, start_overlap_dt, end_overlap_dt, ward_name, "kontakt_org")`
        """
        contact_pats = []
        for pat in patients.values():
            if pat.has_risk():
                for case in pat.cases.values():
                    # if case.fal_ar == "1": # only stationary cases
                    if case.moves_end is not None and case.moves_end > \
                            datetime.datetime.now() - relativedelta.relativedelta(years=1):
                        self.get_contact_patients_for_case(case, contact_pats)
        return contact_pats

