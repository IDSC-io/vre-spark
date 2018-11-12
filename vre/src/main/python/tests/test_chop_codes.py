def test_chop_codes_load(patient_data):
    assert len(patient_data["chops"]) == 249


def test_chop_code_description(patient_data):
    print(patient_data["chops"].get("Z39.64_18").get_detailed_chop())
    assert (
        patient_data["chops"].get("Z39.64_18").get_detailed_chop()
        == "VorlÃ¤ufiger Schrittmacher eingesetzt wÃ¤hrend und unmittelbar nach herzchirurgischem Eingriff" # Note weird encoding of german "Umlaut" strings
    )


def test_lowest_level_code(patient_data):
    assert patient_data["chops"].get("Z39.64_18").get_lowest_level_code() == "Z39"
    assert patient_data["chops"].get("Z89.07.24_16").get_lowest_level_code() == "Z89"


def test_surgeries_per_chop(patient_data):
    assert len(patient_data["chops"].get("Z39.61.10_18").cases) == 1


def test_relevant_chops(patient_data):
    # 99.04.10
    assert len(patient_data["patients"].get("00008301433").get_chop_codes()) == 12

def test_has_surgery(patient_data):
    assert patient_data["patients"].get("00008301433").has_surgery()
    assert not patient_data["patients"].get("00004348346").has_surgery()