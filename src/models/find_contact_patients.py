import datetime
import logging
import sys
from dateutil.relativedelta import relativedelta

from sklearn.model_selection import cross_val_score
from sklearn.tree import DecisionTreeClassifier, export_graphviz

from src.features.dataloader import DataLoader
from src.features.feature_extractor import FeatureExtractor


def get_contact_patients_for_case(c, ps):
    for move in c.moves.values():
        # contacts in the same room
        if move.to_datetime is not None and move.room is not None:
            o = move.room.get_moves_during(move.from_datetime, move.to_datetime)
            for n in o:
                if n.case is not None and move.case is not None:
                    if n.case.case_type == "1" and n.type_id in ["1", "2", "3"] and n.to_datetime is not None:
                        # if n.case.patient_id > m.case.patient_id and n.bwe_dt is not None:
                        # if n.bwe_dt is not None:
                        start_overlap = max(move.from_datetime, n.from_datetime)
                        end_overlap = min(move.to_datetime, n.to_datetime)
                        ps.append(
                            (
                                move.case.patient_id,
                                n.case.patient_id,
                                start_overlap.strftime("%Y-%m-%dT%H:%M"),
                                end_overlap.strftime("%Y-%m-%dT%H:%M"),
                                move.room.name,
                                "kontakt_raum",
                            )
                        )
        # contacts in the same ward (ORGPF)
        if move.to_datetime is not None and move.ward is not None:
            o = move.ward.get_moves_during(move.from_datetime, move.to_datetime)
            for n in o:
                if n.case is not None and move.case is not None:
                    if n.case.case_type == "1" \
                            and n.type_id in ["1", "2", "3"] \
                            and n.to_datetime is not None \
                            and (n.room_id is None or move.room_id is None or n.room_id != move.room_id):
                        start_overlap = max(move.from_datetime, n.from_datetime)
                        end_overlap = min(move.to_datetime, n.to_datetime)
                        ps.append(
                            (
                                move.case.patient_id,
                                n.case.patient_id,
                                start_overlap.strftime("%Y-%m-%dT%H:%M"),
                                end_overlap.strftime("%Y-%m-%dT%H:%M"),
                                move.ward.name,
                                "kontakt_org",
                            )
                        )


def get_contact_patients(patients):
    ps = []
    for p in patients.values():
        if p.has_risk():
            for c in p.cases.values():
                if c.case_type == "1":  # only stationary cases
                    if c.moves_end is not None and c.moves_end > datetime.datetime.now() - relativedelta(years=1):
                        get_contact_patients_for_case(c, ps)
    return ps


if __name__ == '__main__':

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # --> Load all data:
    loader = DataLoader(hdfs_pipe=False)  # hdfs_pipe = False --> files will be loaded directly from CSV
    patient_data = loader.patient_data(
                                       load_cases=True,
                                       load_partners=False,
                                       load_moves=True,
                                       load_chop_codes=False,
                                       load_surgeries=False,
                                       load_appointments=False,
                                       load_devices=False,
                                       load_employees=False,
                                       load_care_data=False,
                                       load_rooms=False,
                                       load_icd_codes=False)

    #####################################
    # Create and export feature vector
    # logger.info("creating feature vector")
    # model_creator = FeatureExtractor()
    # (features, labels, dates, v) = model_creator.prepare_features_and_labels(patient_data["patients"])

    # which patients were screened positive after relevant case ends?
    for patient in patient_data["patients"].values():
        if patient.has_risk([(32, None)]):
            (case, dt) = patient.get_relevant_case_and_date()
            if case is None:
                print(patient.patient_id + ",-1," + str(patient.get_risk_date().date()))
            else:
                delta = dt - case.moves_end if dt > case.moves_end else datetime.timedelta(0)
                print(patient.patient_id + "," + str(delta.days) + "," + str(patient.get_risk_date().date()))

    get_contact_patients(patient_data["patients"])

    # features_pos = features[labels == 3, :]
    # features_neg = features[labels == 2, :]
    # for i in range(features.shape[1]):
    #     p = features_pos[:, i].sum() / features_pos.shape[0]
    #     n = features_neg[:, i].sum() / features_neg.shape[0]
    #     if p > 0.05 and p / n > 2:
    #         print(f"{v.feature_names_[i]} {p/n} {features_pos[:,i].sum()}")
    #
    # features_pos = features[labels == 3, :]
    # features_neg = features[labels == 1, :]
    # for i in range(features.shape[1]):
    #     p = features_pos[:, i].sum() / features_pos.shape[0]
    #     n = features_neg[:, i].sum() / features_neg.shape[0]
    #     if p > 0.05 and p / n > 2:
    #         print(f"{v.feature_names_[i]} {p/n} {features_pos[:,i].sum()}")

    # features_ml = features[(labels == 1) | (labels == 3), :]
    # labels_ml = labels[(labels == 1) | (labels == 3)]

    # clf = DecisionTreeClassifier(random_state=0, max_depth=4)
    # clf.fit(features_ml, labels_ml)
    # cross_val_score(clf, features_ml, labels_ml, cv=10).mean()
    # export_graphviz(clf, "tree.dot", feature_names=v.feature_names_)

    nr = 0
    for patient in patient_data['patients'].values():
        if patient.has_risk([(32, None)]):
            (case, risk_date) = patient.get_relevant_case_and_date()
            case_id = case.case_id if case is not None else ""
            risk_date_str = str(risk_date) if risk_date is not None else ""
            nr += 1
            print(str(nr) + "," + patient.patient_id + "," + case_id + "," + risk_date_str)
