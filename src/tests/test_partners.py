def test_partner_dict(patient_data):
    assert len(patient_data["partners"]) == 1

def test_partner_case(patient_data):
    assert len(patient_data["patients"].get("00008301433").get_referring_partners()) == 1
    assert len(patient_data["patients"].get("00003067149").get_referring_partners()) == 0
    assert patient_data["patients"].get("00004348346").get_referring_partners() is None