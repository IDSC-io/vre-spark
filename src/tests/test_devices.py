def test_device_load(patient_data):
    assert len(patient_data["devices"]) == 10


def test_relevant_devices(patient_data):
    assert len(patient_data["patients"].get("00008301433").get_devices()) == 1
    assert "ECC" in patient_data["patients"].get("00008301433").get_devices()
