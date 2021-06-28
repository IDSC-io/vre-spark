# def test_add_care_to_case(patient_data):
#     sum = 0
#     for case in patient_data["cases"].values():
#         sum += len(case.cares)
#     assert sum == 409

def test_care_in_employees(patient_data):
    for case in patient_data["cases"].values():
        for care in case.cares:
            assert care.employee.id in list(patient_data["employees"].keys())
