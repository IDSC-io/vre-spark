import datetime

def test_stays_before_dt(patient_data):
    assert (
        len(
            patient_data["cases"]
            .get("0006594482")
            .get_stays_before_dt(datetime.datetime(2018, 7, 6, 0, 0))
        )
        == 3
    )

def test_relevant_rooms(patient_data):
    # From SAP IS-H: BH N 125, INE GE06
    # From RAP: KARR EKG, INO OP 08
    room_names = patient_data["patients"].get("00008301433").get_relevant_rooms()
    assert(len(room_names) == 4)
    assert("BH N 125" in room_names)
    assert("INE GE06" in room_names)
    assert("INO OP 08" in room_names)
    assert("KARR EKG" in room_names)
