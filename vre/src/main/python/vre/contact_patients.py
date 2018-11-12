

def get_contact_patients_for_case(c, ps):
    for move in c.moves.values():
        # contacts in the same room
        if move.bwe_dt is not None and move.room is not None:
            o = move.room.get_moves_during(move.bwi_dt, move.bwe_dt)
            for n in o:
                if n.case is not None and move.case is not None:
                    if n.case.fal_ar == "1" and n.bew_ty in ["1", "2", "3"] and n.bwe_dt is not None:
                    #if n.case.patient_id > m.case.patient_id and n.bwe_dt is not None:
                    #if n.bwe_dt is not None:
                        start_overlap = max(move.bwi_dt, n.bwi_dt)
                        end_overlap = min(move.bwe_dt, n.bwe_dt)
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
        if move.bwe_dt is not None and move.ward is not None:
            o = move.ward.get_moves_during(move.bwi_dt, move.bwe_dt)
            for n in o:
                if n.case is not None and move.case is not None:
                    if n.case.fal_ar == "1" \
                            and n.bew_ty in ["1", "2", "3"] \
                            and n.bwe_dt is not None \
                            and (n.zimmr is None or move.zimmr is None or n.zimmr != move.zimmr):
                        start_overlap = max(move.bwi_dt, n.bwi_dt)
                        end_overlap = min(move.bwe_dt, n.bwe_dt)
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
                if c.fal_ar == "1": # only stationary cases
                    if c.moves_end is not None and c.moves_end > datetime.now() - relativedelta(years=1):
                        get_contact_patients_for_case(c, ps)
    return ps


import datetime

# which patients were screened positive after relevant case ends?
for patient in patient_data["patients"].values():
    if patient.has_risk([(32, None)]):
        (case, dt) = patient.get_relevant_case_and_date()
        if case is None:
            print(patient.patient_id + ",-1," + str(patient.get_risk_date().date()))
        else:
            delta = dt - case.moves_end if dt > case.moves_end else datetime.timedelta(0)
            print(patient.patient_id + "," + str(delta.days) + "," + str(patient.get_risk_date().date()))

features_pos = features[labels == 3, :]
features_neg = features[labels == 2, :]
for i in range(features.shape[1]):
    p = features_pos[:,i].sum() / features_pos.shape[0]
    n = features_neg[:,i].sum() / features_neg.shape[0]
    if p > 0.05 and p/n > 2:
        print(f"{v.feature_names_[i]} {p/n} {features_pos[:,i].sum()}")


features_pos = features[labels==3, :]
features_neg = features[labels==1, :]
for i in range(features.shape[1]):
    p = features_pos[:,i].sum() / features_pos.shape[0]
    n = features_neg[:,i].sum() / features_neg.shape[0]
    if p > 0.05 and p/n > 2:
        print(f"{v.feature_names_[i]} {p/n} {features_pos[:,i].sum()}")

features_ml = features[(labels==1) | (labels==3), :]
labels_ml = labels[(labels == 1) | (labels == 3)]

clf = DecisionTreeClassifier(random_state=0, max_depth=4)
clf.fit(features_ml, labels_ml)
cross_val_score(clf, features_ml, labels_ml, cv=10).mean()
export_graphviz(clf, "tree.dot", feature_names=v.feature_names_)


nr = 0
for patient in patient_data['patients'].values():
    if patient.has_risk([(32, None)]):
        (case, risk_date) = patient.get_relevant_case_and_date()
        case_id = case.case_id if case is not None else ""
        risk_date_str = str(risk_date) if risk_date is not None else ""
        nr += 1
        print(str(nr) + "," + patient.patient_id + "," + case_id + "," + risk_date_str)