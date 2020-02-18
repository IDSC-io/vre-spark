
def test_has_icu_stay(patient_data):
    icu_orgs = ["N NORD"]
    assert(patient_data["patients"].get("00008301433").has_icu_stay(icu_orgs))
    assert ( not patient_data["patients"].get("00003067149").has_icu_stay(icu_orgs))