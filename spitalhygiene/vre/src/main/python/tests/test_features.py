from sklearn.feature_extraction import DictVectorizer

def test_features(patient_data):
    assert patient_data["patients"].get("00004348346").get_features() is None

    nr_not_none = 0
    for patient in patient_data["patients"].values():
        if patient.get_features() is not None:
            nr_not_none += 1

            features = patient.get_features()

            assert type(features.get("age", None)).__name__=="int"
            assert 0 <= features.get("age") < 150

            assert type(features.get("length_of_stay")).__name__=="int"
            assert 0 <= features.get("length_of_stay")

            assert type(features.get("surgery")).__name__ == "bool"

            assert type(features.get("icu")).__name__ == "bool"

            assert type(features.get("plz")).__name__ == "str"
            assert type(features.get("kanton")).__name__ == "str"

            assert type(features.get("language")).__name__ == "str"

    assert nr_not_none == 2

def test_features_antibiotic_exposure(patient_data):
    features = patient_data["patients"].get("00008301433").get_features()
    assert features["antibiotic=J01DC02"] == 2

def test_features_chop_codes(patient_data):
    features = patient_data["patients"].get("00008301433").get_features()
    assert features["chop=Z99"]

def test_features_rooms(patient_data):
    features = patient_data["patients"].get("00008301433").get_features()
    assert features["room=BH N 125"]
    assert features["room=KARR EKG"]

def test_features_devices(patient_data):
    features = patient_data["patients"].get("00008301433").get_features()
    assert features["device=ECC"]

def test_features_employees(patient_data):
    features = patient_data["patients"].get("00008301433").get_features()
    assert features["employee=0030236"] == 203 # from RAP
    assert features["employee=0324009"] == 43 # from TACS

def test_dict_vectorizer(patient_data):
    risk_factors = []
    for patient in patient_data["patients"].values():
        p_risk_factors = patient.get_features()
        if p_risk_factors is not None:
            risk_factors.append(p_risk_factors)
    v = DictVectorizer(sparse=False)
    features = v.fit_transform(risk_factors)

    assert len(features) == 2
    assert len(features[0]) == 203

    assert "device=ECC" in v.vocabulary_
