def test_age(patient_data):
    assert patient_data["patients"].get("00008301433").get_age() == 79
    assert patient_data["patients"].get("00003067149").get_age() == 65