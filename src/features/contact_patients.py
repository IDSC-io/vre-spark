import datetime
import logging
import sys
import csv
from dateutil.relativedelta import relativedelta

from sklearn.model_selection import cross_val_score
from sklearn.tree import DecisionTreeClassifier, export_graphviz

from src.features.dataloader import DataLoader
from src.features.feature_extractor import FeatureExtractor
from src.features.model import Patient

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
                                       load_stays=True,
                                       load_medications=False,
                                       load_risks=True,
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
    # for patient in patient_data["patients"].values():
    #     if patient.has_risk():
    #         (case, dt) = patient.get_relevant_case_and_date()
    #         if case is None:
    #             print(patient.patient_id + ",-1," + str(patient.get_risk_date().date()))
    #         else:
    #             delta = dt - case.stays_end if dt > case.stays_end else datetime.timedelta(0)
    #             print(patient.patient_id + "," + str(delta.days) + "," + str(patient.get_risk_date().date()))

    risk_patients = Patient.get_risk_patients(patient_data["patients"])

    print(risk_patients)

    contact_patients = Patient.get_contact_and_risk_patient_ids(patient_data["patients"])

    print(contact_patients)

    risk_pids = []

    for risk_patient in risk_patients:
        risk_pids.append(risk_patient.patient_id)

    print(risk_pids)

    with open('risk_pids.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        for pid in risk_pids:
            writer.writerow([pid])

    contact_pids = []
    for contact_patient in contact_patients:
        contact_pids.append(contact_patient[1])

    print(contact_pids)

    with open('contact_pids.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        for pid in contact_pids:
            writer.writerow([pid])

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

    # nr = 0
    # for patient in patient_data['patients'].values():
    #     if patient.has_risk():
    #         (case, risk_date) = patient.get_relevant_case_and_date()
    #         case_id = case.case_id if case is not None else ""
    #         risk_date_str = str(risk_date) if risk_date is not None else ""
    #         nr += 1
    #         print(str(nr) + "," + patient.patient_id + "," + case_id + "," + risk_date_str)
