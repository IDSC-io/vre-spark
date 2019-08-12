import datetime


def test_risk_codes(patient_data):
    nr_risk = 0
    for patient in patient_data["patients"].values():
        if patient.has_risk([(32, None), (42, None), (22, None)]):
            nr_risk += 1
    assert nr_risk == 1


def test_load_risk_deleted(patient_data):
    assert patient_data["patients"].get("00003067149").has_risk([(142, None)])


def test_risk_date(patient_data):
    # screening date
    assert patient_data["patients"].get("00008301433").get_risk_date(
        [(42, None)]
    ) == datetime.datetime(2018, 7, 9, 16, 32, 10)

    # screening date of deleted screening
    assert patient_data["patients"].get("00003067149").get_risk_date(
        [(142, None)]
    ) == datetime.datetime(2018, 3, 16, 0, 0, 0)


def test_risk_case(patient_data):
    # do we get the relevant case for patients with risk factors (32: positive, 42: screening, 142: screening deleted)?
    assert patient_data["patients"].get("00003067149").get_relevant_case() is not None

    # patient with no relevant risk and only old cases
    assert patient_data["patients"].get("00004348346").get_relevant_case() is None
