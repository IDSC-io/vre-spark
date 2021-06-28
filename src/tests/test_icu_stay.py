
def test_has_icu_stay(patient_data):

    for patient_id, patient in patient_data["patients"].items():
        if patient.has_icu_stay():
            print(patient)
    assert(patient_data["patients"].get("00008301433").has_icu_stay())
    assert(not patient_data["patients"].get("00003067149").has_icu_stay())
