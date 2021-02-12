# -*- coding: utf-8 -*-
"""This script contains all functions for compiling data from CSV or HDFS, and controls the creation of all
objects required for the VRE model. This process has multiple steps and is structured as follows:

- Loading all VRE-relevant data using the ``data_loader`` class
- Creation of the surface model using the ``surface_model`` class
- Export of various results from the surface model using its built-in functions

**This script is called in the cronjob and triggers the build of the VRE model!**

Please refer to the script code for details.

-----
"""
import sys

sys.path.append("../..")

import logging

from datetime import datetime
import pathlib

import click

from src.features.dataloader import DataLoader
from src.models.networkx_graph import SurfaceModel


@click.command()
#@click.argument('input_filepath', type=click.Path(exists=True))
#@click.argument('output_filepath', type=click.Path())
def compose_model():
    #####################################
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO,
                        datefmt='%d.%m.%Y %H:%M:%S')
    #####################################

    now_str = datetime.now().strftime("%Y%m%d%H%M%S")

    #####################################
    # Initiate data loader
    logging.info("Initiating data_loader")

    # --> Load all data:
    loader = DataLoader(hdfs_pipe=False)  # hdfs_pipe = False --> files will be loaded directly from CSV
    patient_data = loader.prepare_dataset(load_medications=False,
                                          load_icd_codes=False,
                                          load_chop_codes=False,
                                          load_surgeries=False,
                                          load_partners=False)
    #####################################

    #####################################
    # TODO: Add contact patients to patient_data (CURRENTLY NOT USED, IMPLEMENTED IN NETWORK)
    # logging.info('Retrieving patient contacts')
    # patient_data['contact_pats'] = model_creator.get_contact_patients(patients=patient_data['patients'])
    #####################################

    #####################################
    # Create graph of the CURRENT model in networkX
    surface_graph = SurfaceModel(data_dir='./data/processed/networkx')
    surface_graph.add_network_data(patient_dict=patient_data, case_subset='relevant_case')
    surface_graph.remove_isolated_nodes()
    surface_graph.add_edge_infection()

    patient_data = None  # free up memory before graph processing!

    # Extract positive patient nodes
    # positive_patient_nodes = [node for node, nodedata in surface_graph.S_GRAPH.nodes(data=True)
    #                           if nodedata['type'] == 'Patient' and nodedata['vre_status'] == 'pos']

    # Write node files
    # surface_graph.write_node_files()
    #
    # export_path = './data/processed'

    # calculate scores

    pathlib.Path("./data/processed/metrics").mkdir(parents=True, exist_ok=True)

    patient_degree_ratio_df = surface_graph.calculate_patient_degree_ratio()
    print(patient_degree_ratio_df.head(50))
    patient_degree_ratio_df.to_csv(f"./data/processed/metrics/{now_str}_patient_degree_ratio.csv", index=False)

    total_degree_ratio_df = surface_graph.calculate_total_degree_ratio()
    print(total_degree_ratio_df.head(50))
    total_degree_ratio_df.to_csv(f"./data/processed/metrics/{now_str}_total_degree_ratio.csv", index=False)

    # TODO: Reenable node betweenness statistics. Deactivated as it uses a lot of resources!
    # node_betweenness_df = surface_graph.calculate_node_betweenness()
    # print(node_betweenness_df.head(50))
    # node_betweenness_df.to_csv(f"./data/processed/metrics/{now_str}_node_betweenness.csv", index=False)
    #####################################

    logging.info("Data processed successfully!")


if __name__ == "__main__":
    compose_model()

