# -*- coding: utf-8 -*-
"""This script contains all functions for compiling data from CSV or HDFS, and controls the creation of all
objects required for the VRE model. This process has multiple steps and is structured as follows:

- Reading the ``BasicConfig.ini`` file in this directory
- Loading all VRE-relevant data using the ``HDFS_data_loader`` class
- Creation of the feature vector using the ``feature_extractor`` class
- Export of this feature vector to CSV
- Creation of the surface model using the ``surface_model`` class
- Export of various results from the surface model using its built-in functions

**This script is called in the cronjob and triggers the build of the VRE model!**

Please refer to the script code for details.

-----
"""

from HDFS_data_loader import HDFS_data_loader
from feature_extractor import feature_extractor
from networkx_graph import surface_model, create_model_snapshots
import preprocessor
import logging
import os
import datetime
import configparser
import calendar
import pathlib


if __name__ == "__main__":
    #####################################
    # ### Load configuration file
    this_filepath = pathlib.Path(os.path.realpath(__file__)).parent

    config_reader = configparser.ConfigParser()
    config_reader.read(pathlib.Path(this_filepath, 'BasicConfig.ini'))

    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO,
                        datefmt='%d.%m.%Y %H:%M:%S')
    #####################################

    #####################################
    # ### Initiate data loader
    logging.info("Initiating HDFS_data_loader")

    # --> Load all data:
    loader = HDFS_data_loader(hdfs_pipe=False)  # hdfs_pipe = False --> files will be loaded directly from CSV
    patient_data = loader.patient_data()
    #####################################

    #####################################
    # ### Create and export feature vector
    logging.info("creating feature vector")
    model_creator = feature_extractor()
    (features, labels, dates, v) = model_creator.prepare_features_and_labels(patient_data["patients"])

    # Export feature vector
    logging.info("exporting feature vector")
    model_creator.export_csv(features, labels, dates, v, config_reader['PATHS']['csv_export_path'])
    #####################################

    #####################################
    # ### Add contact patients to patient_data (CURRENTLY NOT USED, IMPLEMENTED IN NETWORK)
    # logging.info('Retrieving patient contacts')
    # patient_data['contact_pats'] = model_creator.get_contact_patients(patients=patient_data['patients'])
    #####################################

    #####################################
    # ### Create graph of the CURRENT model in networkX
    surface_graph = surface_model(data_dir='/home/i0308559/vre_wd')
    surface_graph.add_network_data(patient_dict=patient_data, subset='relevant_case')
    surface_graph.remove_isolated_nodes()
    surface_graph.add_edge_infection()

    # Extract positive patient nodes
    positive_patient_nodes = [node for node, nodedata in surface_graph.S_GRAPH.nodes(data=True)
                              if nodedata['type'] == 'Patient' and nodedata['vre_status'] == 'pos']

    # Write node files
    surface_graph.write_node_files()

    # Export Patient Degree Ratio
    surface_graph.export_patient_degree_ratio(export_path='/home/i0308559/vre_output')

    # Export Total Degree Ratio
    surface_graph.export_total_degree_ratio(export_path='/home/i0308559/vre_output')

    # Export node betweenness
    surface_graph.update_shortest_path_statistics()
    surface_graph.export_node_betweenness(export_path='/home/i0308559/vre_output')
    #####################################

    # Create a monthly TIMESERIES of pdr values starting in January 2018
    # month_snapshots = [datetime.datetime(2018, i, calendar.monthrange(2018, i)[1]) for i in range(1, 13)] # calendar.monthrange() returns a tuple of length 2 of (day_of_week, day_of_month)
    # month_snapshots += [datetime.datetime(2019, 1, 31)]  # Add values from 2019
    # all_snapshot_models = create_model_snapshots(orig_model = surface_graph, snapshot_dt_list = month_snapshots)
    # for model in all_snapshot_models:
    #     model.export_patient_degree_ratio(export_path='/home/i0308559/vre_output')
    #####################################

    logging.info("Data processed successfully !")

    #####################################
    # ### OBSOLETE SECTION


    ### Export data in Neo4J-compatible format - REIMPLEMENT THIS PART
    # logging.info('Exporting data for Neo4J')
    # exporter = Neo4JExporter()

    # Export to csv files for Neo4J - REIMPLEMENT THIS PART
    # exporter.write_patient(patient_data["patients"])
    # exporter.write_patient_patient(patient_data['contact_pats'], patient_data['patients']) # patient_data['contact_pats'] is really only a list of tuples of patient contacts
    # exporter.write_room(patient_data["rooms"])
    # exporter.write_patient_room(patient_data["patients"])
    # exporter.write_bed(patient_data["rooms"])
    # exporter.write_room_bed(patient_data["rooms"])
    # exporter.write_device(patient_data["devices"])
    # exporter.write_patient_device(patient_data["patients"])
    # exporter.write_drug(patient_data["drugs"])
    # exporter.write_patient_medication(patient_data["patients"])
    # exporter.write_employees(patient_data["employees"])
    # exporter.write_patient_employee(patient_data["patients"])
    # exporter.write_referrer(patient_data["partners"])
    # exporter.write_referrer_patient(patient_data["patients"])
    # exporter.write_chop_code(patient_data["chops"])
    # exporter.write_chop_patient(patient_data["patients"])

    ##########################################################################
    # Export data using the data_purification.py script for manual inspection and purging
    ##########################################################################
    # relcase_count = 0
    # employee_list = []
    # room_list = []
    # geraet_list = []
    #
    # for patient in patient_data['patients'].values():
    #     rel_case = patient.get_relevant_case()
    #     patient_has_risk = patient.has_risk()
    #     if rel_case is not None: # This filter is important !
    #         # Write relevant case and associated patient ID
    #         dpf.write_patient_case(case = rel_case, has_risk = patient_has_risk)
    #         relcase_count += 1
    #
    #         # Note employees associated to the relevant case --> employee_list
    #         for care in rel_case.cares:
    #             employee_list.append(care.employee.mitarbeiter_id)
    #
    #         # Note rooms associated to the relevant case --> room_list
    #         for move in rel_case.moves.values():
    #             room_list.append(move.zimmr)
    #
    #         # Note all geraete associated to relevant case --> geraet_list
    #         for appmnt in rel_case.appointments:
    #             for each_geraet in appmnt.devices:
    #                 geraet_list.append((str(each_geraet.geraet_id), str(each_geraet.geraet_name)))
    # print(f"Wrote {relcase_count} relevant cases and associated patients.")
    #
    # # Remove duplicate entries from the list and write them to file:
    # unique_employees = list(set(employee_list))
    # for each_entry in unique_employees:
    #     dpf.write_employee(each_entry)
    # print(f"Wrote {len(unique_employees)} employees.")
    #
    # unique_rooms = list(set(room_list))
    # for each_entry in unique_rooms:
    #     dpf.write_room(each_entry)
    # print(f"Wrote {len(unique_rooms)} rooms.")
    #
    # unique_geraete = list(set(geraet_list))
    # for each_tuple in unique_geraete:
    #     dpf.write_geraet(each_tuple)
    # print(f"Wrote {len(unique_geraete)} devices.")
    ##########################################################################


    ##########################################################################
    ### For overview purposes (works only on test data)
    ##########################################################################

    # Room object
    # print('\nRoom object')
    # for attribute in ['name', 'moves', 'appointments', 'beds']:
    #     print(getattr(patient_data['rooms']['BH N 125'], attribute), type(getattr(patient_data['rooms']['BH N 125'], attribute)))
    #
    # # Bed object
    # print('\nBed object')
    # for attribute in ['name', 'moves']:
    #     print(getattr(patient_data['rooms']['BH N 125'].beds['BHN125F'], attribute), type(getattr(patient_data['rooms']['BH N 125'].beds['BHN125F'], attribute)))
    #
    # # Moves object
    # print('\nMoves object')
    # for attribute in ['fal_nr', 'lfd_nr','bew_ty','bw_art','bwi_dt','statu','bwe_dt','ldf_ref','kz_txt','org_fa','org_pf','org_au','zimmr','bett','storn','ext_kh', 'room', 'ward', 'case']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0], attribute)))
    #
    # # Ward object
    # print('\nWard object')
    # for attribute in ['name', 'moves', 'appointments']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].ward, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].ward, attribute)))
    #
    # # Case object
    # print('\nCase object')
    # for attribute in ['patient_id','case_id','case_typ','case_status','fal_ar','beg_dt','end_dt','patient_typ','patient_status','appointments','cares','surgeries','moves','moves_start','moves_end','referrers','patient','medications']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case, attribute)))
    #
    # # Patient object
    # print('\nPatient object')
    # for attribute in ['patient_id','geschlecht','geburtsdatum','plz','wohnort','kanton','sprache','cases','risks']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.patient, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.patient, attribute)))
    #
    # # Care object
    # print('\nCare object')
    # for attribute in ['patient_id','case_id','dt','duration_in_minutes','employee_nr','employee']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.cares[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.cares[0], attribute)))
    #
    # # Employee object
    # print('\nEmployee object')
    # for attribute in ['mitarbeiter_id']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.cares[0].employee, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.cares[0].employee, attribute)))
    #
    # # Appointment object
    # print('\nAppointment object')
    # for attribute in ['termin_id','is_deleted','termin_bezeichnung','termin_art','termin_typ','termin_datum','dauer_in_min','devices','employees','rooms']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.appointments[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.appointments[0], attribute)))
    #
    # # Surgery object
    # print('\nSurgery object')
    # for attribute in ['bgd_op','lfd_bew','icpmk','icpml','anzop','lslok','fall_nr','storn','org_pf','chop']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.surgeries[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.surgeries[0], attribute)))
    #
    # # Chop object
    # print('\nChop object')
    # for attribute in ['chop_code','chop_verwendungsjahr','chop','chop_code_level1','chop_level1','chop_code_level2','chop_level2','chop_code_level3','chop_level3','chop_code_level4','chop_level4','chop_code_level5','chop_level5','chop_code_level6','chop_level6','chop_status','chop_sap_katalog_id','cases']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.surgeries[0].chop, attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.surgeries[0].chop, attribute)))
    #
    # # Medication object
    # print('\nMedication object')
    # for attribute in ['patient_id','case_id','drug_text','drug_atc','drug_quantity','drug_unit','drug_dispform','drug_submission']:
    #     print(getattr(patient_data['rooms']['BH N 125'].moves[0].case.medications[0], attribute), type(getattr(patient_data['rooms']['BH N 125'].moves[0].case.medications[0], attribute)))
    #
    # # Partner object - these are found in the 'referrers' set attribute, which is why the attribute is converted to a list()
    # print('\nPartner object')
    # for attribute in ['gp_art','name1','name2','name3','land','pstlz','ort','ort2','stras','krkhs','referred_cases']:
    #     print(getattr(list(patient_data['rooms']['BH N 125'].moves[0].case.referrers)[0], attribute), type(getattr(list(patient_data['rooms']['BH N 125'].moves[0].case.referrers)[0], attribute)))
    #
    # # Device object
    # print('\nDevice object')
    # for attribute in ['geraet_id','geraet_name']:
    #     print(getattr(patient_data['devices']['64174'], attribute), type(getattr(patient_data['devices']['64174'], attribute)))
    #
    # # Risk object
    # print('\nRisk object --> see class definition')
    ##########################################################################
    # [END OF FILE]






