def test_label_patient(patient_data):
    # no relevant case
    assert patient_data["patients"].get("00004348346").get_label() == -1
    # screening removed
    assert patient_data["patients"].get("00003067149").get_label() == 2
    # screening unconclusive: 00008301433
    assert patient_data["patients"].get("00008301433").get_label() == 1
