import datetime


def test_case_timing(patient_data):
    # beg_dt and end_dt are not set in the source data
    assert patient_data["cases"].get("0006314210").begin_date is None
    assert patient_data["cases"].get("0006314210").end_date is None

    # stays_start and stays_end are set to the beginning of the first stay and end of the last stay respectively
    assert patient_data["cases"].get("0006314210").stays_start == datetime.datetime(
        2018, 1, 5, 11, 0
    )
    assert patient_data["cases"].get("0006314210").stays_end == datetime.datetime(
        2018, 1, 16, 13, 0
    )

    # length of stay is the timedelta between first and last stay
    assert patient_data["cases"].get(
        "0006314210"
    ).get_length_of_stay() == datetime.timedelta(11, 7200)

    # length of stay for non-stationary cases must be None
    assert patient_data["cases"].get("0006334066").stays_start == datetime.datetime(
        2018, 4, 9, 11, 7
    )
    assert patient_data["cases"].get("0006334066").stays_end == datetime.datetime(
        2018, 8, 17, 10, 56
    )
    assert patient_data["cases"].get("0006334066").get_length_of_stay() is None

    # case 0006594482 is almost 8 days
    assert patient_data["cases"].get(
        "0006594482"
    ).get_length_of_stay() == datetime.timedelta(7, 84600)

    # case 0006594482 is relevant case of patient with risk date during the case. thus, length of relevant stay is shorter
    assert patient_data["patients"].get(
        "00008301433"
    ).get_length_of_relevant_case() == datetime.timedelta(4, 50400)


def test_case_stationary(patient_data):
    assert patient_data["cases"].get("0005976205").is_inpatient_case()
    assert not patient_data["cases"].get("0005965462").is_inpatient_case()


def test_case_open_before(patient_data):
    # 0005976205 starts at 2017-6-16 and ends at 2017-6-17
    assert (
        patient_data["cases"]
        .get("0005976205")
        .open_before_or_at_date(datetime.datetime(2017, 6, 16).date())
    )
    assert (
        patient_data["cases"]
        .get("0005976205")
        .open_before_or_at_date(datetime.datetime(2017, 7, 1).date())
    )
    assert (
        not patient_data["cases"]
        .get("0005976205")
        .open_before_or_at_date(datetime.datetime(2017, 6, 15).date())
    )


def test_case_closed_after(patient_data):
    assert (
        patient_data["cases"]
        .get("0005976205")
        .closed_after_or_at_date(datetime.datetime(2017, 6, 17).date())
    )
    assert (
        patient_data["cases"]
        .get("0005976205")
        .closed_after_or_at_date(datetime.datetime(2017, 5, 1).date())
    )
    assert (
        not patient_data["cases"]
        .get("0005976205")
        .closed_after_or_at_date(datetime.datetime(2017, 7, 1).date())
    )


def test_relevant_date(patient_data):
    # for a patient without risk factor, the relevant date is now
    assert (
        patient_data["patients"].get("00004348346").get_relevant_date()
        == datetime.datetime.now().date()
    )

    # for a patient with risk factor, the relevant date is the date of the risk factor
    assert (
        patient_data["patients"].get("00008301433").get_relevant_date()
        == datetime.datetime(2018, 7, 9).date()
    )


def test_relevant_case(patient_data):
    # patient has no stationary case -> no relevant case
    assert patient_data["patients"].get("00004348346").get_relevant_case() is None

    # case 0006314210: 5.1.2018 - 16.1.2018
    assert (
        patient_data["patients"].get("00003067149").get_relevant_case().case_id
        == "0006314210"
    )
    assert (
        patient_data["patients"]
        .get("00003067149")
        .get_relevant_case(since=datetime.datetime(2018, 2, 1).date())
        is None
    )

    assert (
        patient_data["patients"].get("00008301433").get_relevant_case().case_id
        == "0006594482"
    )
