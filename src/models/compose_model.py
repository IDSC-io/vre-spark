# -*- coding: utf-8 -*-
"""This script contains all functions for compiling data from CSV or HDFS, and controls the creation of all
objects required for the VRE model. This process has multiple steps and is structured as follows:

- Reading the ``basic_config.ini`` file in this directory
- Loading all VRE-relevant data using the ``data_loader`` class
- Creation of the surface model using the ``surface_model`` class
- Export of various results from the surface model using its built-in functions

**This script is called in the cronjob and triggers the build of the VRE model!**

Please refer to the script code for details.

-----
"""
import click

import configparser
import logging
import os
import pathlib

from src.features.dataloader import DataLoader
from src.models.networkx_graph import SurfaceModel

@click.command()
#@click.argument('input_filepath', type=click.Path(exists=True))
#@click.argument('output_filepath', type=click.Path())
def compose_model():
    #####################################
    # Load configuration file
    this_filepath = pathlib.Path(os.path.realpath(__file__)).parent

    config_reader = configparser.ConfigParser()
    config_reader.read(pathlib.Path(this_filepath, '../../configuration/basic_config.ini'))

    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.INFO,
                        datefmt='%d.%m.%Y %H:%M:%S')
    #####################################

    #####################################
    # Initiate data loader
    logging.info("Initiating data_loader")

    # --> Load all data:
    loader = DataLoader(hdfs_pipe=False)  # hdfs_pipe = False --> files will be loaded directly from CSV
    patient_data = loader.patient_data()
    #####################################

    #####################################
    # TODO: Add contact patients to patient_data (CURRENTLY NOT USED, IMPLEMENTED IN NETWORK)
    # logging.info('Retrieving patient contacts')
    # patient_data['contact_pats'] = model_creator.get_contact_patients(patients=patient_data['patients'])
    #####################################

    #####################################
    # Create graph of the CURRENT model in networkX
    surface_graph = SurfaceModel(data_dir='./data/processed/networkx')
    surface_graph.add_network_data(patient_dict=patient_data, subset='relevant_case')
    surface_graph.remove_isolated_nodes()
    surface_graph.add_edge_infection()

    # Extract positive patient nodes
    # positive_patient_nodes = [node for node, nodedata in surface_graph.S_GRAPH.nodes(data=True)
    #                           if nodedata['type'] == 'Patient' and nodedata['vre_status'] == 'pos']

    # Write node files
    surface_graph.write_node_files()

    export_path = './data/processed'

    # calculate scores

    # Export Patient Degree Ratio
    surface_graph.export_patient_degree_ratio(export_path=export_path)

    # Export Total Degree Ratio
    surface_graph.export_total_degree_ratio(export_path=export_path)

    # Export node betweenness
    surface_graph.update_shortest_path_statistics()
    surface_graph.export_node_betweenness(export_path=export_path)
    #####################################

    logging.info("Data processed successfully!")


if __name__ == "__main__":
    compose_model()

