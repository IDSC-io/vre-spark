#
# def test_icd_code(patient_data):
#     """
#     Test ICD codes for case "0006148746"
#
#     :param patient_data: Dictionary containing all VRE-relevant information (see @pytest.fixture patient_data() for details)
#     """
#     icd_list = [each_icd.icd_code for each_icd in patient_data['cases'].get('0006148746').icd_codes]
#
#     # This patient should have 5 ICD codes: C22.0, B16.9, G47.31, J45.9 and Z00.6
#
#     assert 'C22.0' in icd_list
#     assert 'B16.9' in icd_list
#     assert 'G47.31' in icd_list
#     assert 'J45.9' in icd_list
#     assert 'Z00.6' in icd_list
#
