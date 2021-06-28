# import datetime
#
#
# def test_antibiotic_exposure(patient_data):
#     assert (
#         len(patient_data["patients"].get("00003067149").get_antibiotic_exposure()) == 2
#     )
#     assert (
#         patient_data["patients"]
#         .get("00008301433")
#         .get_antibiotic_exposure()
#         .get("J01DC02")
#     )
#
#
# def test_antibiotic_exposure_dates(patient_data):
#     assert (
#         len(
#             patient_data["patients"]
#             .get("00008301433")
#             .get_antibiotic_exposure()
#             .get("J01DC02")
#         )
#         == 2
#     )
#     assert datetime.datetime(2018, 7, 7).date() in patient_data["patients"].get(
#         "00008301433"
#     ).get_antibiotic_exposure().get("J01DC02")
#     assert datetime.datetime(2018, 7, 5).date() not in patient_data["patients"].get(
#         "00008301433"
#     ).get_antibiotic_exposure().get("J01DC02")
#
#
# def test_dispforms(patient_data):
#     assert len(patient_data["patients"].get("00008301433").get_dispform()) == 7
#     assert "p.o." in patient_data["patients"].get("00008301433").get_dispform()
