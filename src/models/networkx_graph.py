# -*- coding: utf-8 -*-
"""This script contains the code to run statistics on various types of network models that will be used as proposed by
Theus (see README.md for more details).

**Surface Model**: This model assumes that VRE is transmitted on "surfaces", and contains all potential transmitting
surfaces as nodes. These currently include:

- Patients
- Employees
- Rooms
- Devices

Edges are based on contact between two nodes if the contact (e.g. a patient being in a particular room) has occurred
during a patient's relevant stay.

**Handshake Model**: This model assumes that VRE is transmitted via "handshakes", and contains only Patients as nodes.
In contrast to the Surface model, edges in this model correspond to the transmission vectors, and represent common rooms
or employees via which two patients may have been (indirectly) in contact with each other.

-----
"""

import copy
import os
import networkx as nx
import logging
import datetime
import itertools
import json
from collections import Counter
import pathlib
import pandas as pd

from tqdm import tqdm


def create_model_snapshots(orig_model, snapshot_dt_list):
    """Creates model snapshots based on the datetime.datetime() values provided in snapshot_dt_list.

    Just to remember how the edges are composed:
    - patient -> cases, risks
    - case -> appointments, stays
    - appointment -> devices, employees, rooms
    - stay -> room

    Note:
        For obvious reasons, all of the values provided must be smaller (earlier than) orig_model.snapshot_dt (i.e.
        the snapshot date of the model used as a basis for the other models).

    Args:
        orig_model (SurfaceModel): Original surface_model() object that will be used as starting point for all
                                    subsequent snapshots
        snapshot_dt_list (list):    List of dt.dt() objects, all smaller than self.snapshot_dt

    Returns:
        list: List of independent surface_model() objects corresponding to the various model snapshots in increasing
        order (i.e. the oldest snapshot is first in the list). The last entry in the returned list contains orig_model,
        meaning the list has length `len(snapshot_dt_list) + 1`. If no snapshot creation is possible, ``None`` is
        returned instead.
    """
    if orig_model.snapshot_dt is None:
        logging.error('Please add data to the model before taking snapshots !')
        return None
    if True in [dt_value > orig_model.snapshot_dt for dt_value in snapshot_dt_list]:
        logging.error('All snapshot values must be smaller than the snapshot time of the current model !')
        return None
    sorted_snapshots = sorted(snapshot_dt_list, reverse=True)
    model_list = [copy.deepcopy(orig_model)]
    for dt_value in sorted_snapshots:
        logging.info(f"Creating snapshot for {dt_value.strftime('%d.%m.%Y %H:%M:%S')}...")
        temp_model = copy.deepcopy(orig_model)
        temp_model.trim_model(snapshot_dt=dt_value)
        temp_model.remove_isolated_nodes(silent=True)
        model_list.append(temp_model)
        logging.info(f'--> Success ! Snapshot contains {len(temp_model.S_GRAPH.nodes())} nodes '
                     f'and {len(temp_model.S_GRAPH.edges())} edges')
    # Reverse order of models (oldest snapshot should be the first entry) and return the list of snapshot models
    model_list.reverse()
    return model_list


class SurfaceModel:
    """Represents the `Surface Model` graph in networkx.

    Nodes can be one of:

    - Patients :math:`\\longrightarrow` added with attribute dict ``{'type' : "Patient" }``
    - Employees :math:`\\longrightarrow` added with attribute dict ``{'type' : "Employee" }``
    - Rooms :math:`\\longrightarrow` added with attribute dict ``{'type' : "Room" }``
    - Devices :math:`\\longrightarrow` added with attribute dict ``{'type' : "Device" }``

    A few details on how the model graph is set up:

    - All node objects will be represented in the form of string objects, where unique identifiers are as follows:

        - Patients :math:`\\longrightarrow` ``patient ID``
        - Rooms: :math:`\\longrightarrow` ``room name``
        - Employees: :math:`\\longrightarrow` ``employee ID``
        - Devices: :math:`\\longrightarrow` ``Device ID``

    - All node objects will have at least two attributes:

        - ``type`` (see docstring above)
        - ``cluster`` (for visual representation with Alchemy) --> added via the function `add_node_clustering()`

    - All edge objects will have at least the following attributes:

        - ``from`` :math:`\\longrightarrow` indicates start of the interaction (a dt.dt() object)
        - ``to`` :math:`\\longrightarrow` indicates end of the interaction (a dt.dt() object)
        - ``type`` :math:`\\longrightarrow` indicates what node types are linked by this edge, e.g. "Patient-Room",
          "Patient-Employee", etc.
        - ``origin`` :math:`\\longrightarrow` indicates the source that was used to add this edge (e.g. "Stay",
          'Appointment", etc.)

    - Edge descriptions always contain the nodes in alphabetical order, i.e. Device-Room (NOT Room-Device),
      Employee-Patient (NOT Patient-Employee), etc.
    - Each node is only present once
    - Edges from a node to itself are not supported
    - Multiple parallel edges between two nodes are supported (:math:`\\longrightarrow` MultiGraph)
    - Edges do not have associated weights, but have at least one attribute "type" (e.g. Patient-Room, Employee-Patient)
    - The edge attribute "type" is **not** sorted alphabetically, and instead features the following 6 variations:

        - Patient-Room
        - Device-Patient
        - Employee-Patient
        - Device-Room
        - Employee-Room
        - Device-Employee

    - The graph is undirected
    - The python built-in ``None``-type object should not be used for attributes according to the networkx docs.
      Instead, unknown values will be set to "NULL" or ""

    """

    def __init__(self, data_dir='.', edge_types=None):
        """Initiates the graph in networkx (see class docstring for details).

        Args:
            data_dir (str):     Data directory in which to store output files from various data extraction functions.
            edge_types (tuple): Tuple containing all edge types to include in the model (can be any combination of the 4
                                node types). This value defaults to None, resulting in the inclusion of all edge types.
                                See class docstring for details.
        """
        self.S_GRAPH = nx.MultiGraph()
        # Flag indicating whether or not the self.add_edge_infection() function has been called on the graph
        # -> introduces the "infected" attribute for edges
        self.edges_infected = False
        self.Nodes = {
            'Patient': set(),
            'Room': set(),
            'Device': set(),
            'Employee': set(),
        }  # Dictionary mapping to sets of respective node string identifiers
        # Initiate various counters:
        self.edge_add_warnings = 0  # Number of warnings encountered during the addition of edges to the network
        self.room_add_warnings = 0  # Number of warnings encountered during the addition of room nodes
        self.patient_add_warnings = 0  # Number of warnings encountered during the addition of patient nodes
        self.employee_add_warnings = 0  # Number of warnings encountered during the addition of employee nodes
        self.device_add_warnings = 0  # Number of warnings encountered during the addition of device nodes

        self.betweenness_centrality = None
        # Changed to a dictionary mapping nodes to betweenness centrality scores once the
        # self.export_node_betweenness() function is called

        self.data_dir = data_dir

        self.shortest_path_stats = False
        # indicates whether shortest path statistics have been added to nodes via update_shortest_path_statistics()

        self.node_files_written = False  # indicates whether node files (in JSON format) are present in self.data_dir

        self.snapshot_dt = None  # time at which "snapshot" of the model is taken (important for "sub-snapshots")

        self.edge_types = ('Patient-Room', 'Device-Patient', 'Employee-Patient', 'Device-Room', 'Employee-Room',
                           'Device-Employee') if edge_types is None else edge_types
        # tuple containing types of edges to include in the network (or None --> includes edge types)

    ##########################################################################
    # Class-specific Exceptions
    ##########################################################################
    class NodeBetweennessException(Exception):
        """ Class-specific exception used for betweenness calculation functions.
        """
        pass

    ##########################################################################
    # Base Functions
    ##########################################################################
    @staticmethod
    def parse_filename(filename, replace_char='@'):
        """Parses filename and replaces problematic characters with replace_char.

        Args:
            filename (str):     Name of file to parse.
            replace_char (str): Replacement problematic characters.

        Returns:
            str: parsed (unproblematic) filename
        """
        return filename.replace('/', replace_char)

    @staticmethod
    def save_to_json(path_to_file, saved_object):
        """Saves object to `path_to_file` in JSON format.

        Args:
            path_to_file (str): Path to file to be saved (must include ``.json`` suffix)
            saved_object:       Object to be saved to file in JSON format.
        """
        json.dump(saved_object, open(path_to_file, 'w'))

    @staticmethod
    def load_from_json(path_to_file):
        """Loads the .json file specified in `path_to_file`.

        Args:
            path_to_file (str): Path to `.json` file to be loaded.

        Returns:
            The object loaded from `path_to_file`.
        """
        loaded_object = json.load(open(path_to_file))  # default mode for open(...) is 'r'
        return loaded_object

    def identify_id(self, string_id):
        """Checks whether a node with string_id exists in the network.

        Returns the type (e.g. 'Patient', 'Employee', etc.) of `string_id` in the network. If string_id does not exist
        in the network, ``None`` is returned instead.

        Args:
            string_id (str):    string identifier of the node to be identified.

        Returns:
            str or None: The type of node of string_id, or ``None`` if string_id is not found in the network.
        """
        for key in self.Nodes.keys():
            if string_id in self.Nodes[key]:  # self.Nodes[key] will be a set --> in operator performs very well
                return key
        return None

    def identify_node(self, node_id, node_type):
        """Checks whether node_id is found in self.Nodes[node_type].

        This function is more performant than identify_id(), since it already assumes that the node type of the string
        to be identified is known.

        Args:
            node_id (str):      String identifier of the node to be identified.
            node_type (str):    Type of node to be identified (e.g. 'Patient')

        Returns:
            bool: `True` if node_id is found in self.Nodes[node_type], `False` otherwise.
        """
        if node_id in self.Nodes[node_type]:
            return True
        return False

    ##########################################################################
    # Functions for expanding or reducing the graph
    ##########################################################################
    def new_generic_node(self, string_id, attribute_dict):
        """Adds a new generic node to the graph.

        String_id will be used as the unique identifier, and all key-value pairs in attribute_dict as additional
        information to add to the node. If a node already exists, only new entries in attribute_dict will be added to
        it, but it will otherwise be left unchanged.

        Args:
            string_id (str):        string identifier for node
            attribute_dict (dict):  dictionary of key-value pairs containing additional information
        """
        self.S_GRAPH.add_node(str(string_id), **attribute_dict)

    def new_patient_node(self, string_id, risk_dict, warn_log=False):
        """Add a patient node to the network.

        Automatically sets the 'type' attribute to "Patient". Also adds risk_dict to the 'risk' attribute as defined
        for patient nodes. It will also add an attribute 'vre_status' ('pos' or 'neg') depending on whether or not code
        32 is found in risk_dict. Note that if string_id is empty (''), no node will be added to the network and a
        warning will be logged if warn_log is `True`.

        Args:
            string_id (str):    string identifier of patient to be added.
            risk_dict (dict):   dictionary mapping dt.dt() to Risk() objects corresponding to a patient's VRE screening
                                history.
            warn_log (bool):    flag indicating whether or not to log warning messages.
        """
        if string_id == '':
            if warn_log:
                logging.warning('Empty patient identifier - node is skipped')
            self.patient_add_warnings += 1
            return
        risk_codes = [each_risk.result for each_risk in risk_dict.values() if each_risk.result != "nn"]
        self.S_GRAPH.add_node(str(string_id), type='Patient', risk=risk_dict, vre_status='pos' if len(risk_codes) != 0 else 'neg')
        self.Nodes['Patient'].add(string_id)

    def new_room_node(self, string_id, building_id=None, ward_id=None, room_id=None, warn_log=False):
        """Add a room node to the network.

        Automatically sets the 'type' attribute to "Room" and ward to the "ward" attribute, and sets room_id to either
        the specified value or "NULL". Note that if string_id is empty (''), no node will be added to the network and a
        warning will be logged if warn_log is `True`.

        Args:
            string_id (str):    string identifier of room to be added.
            building_id (str): id of ward of this room
            ward_id (str):         id of ward of this room
            room_id (str):      room id (in string form) of this room
            warn_log (bool):    flag indicating whether or not to log warning messages.
        """
        if string_id == '':
            if warn_log:
                logging.warning('Empty room identifier - node is skipped')
            self.room_add_warnings += 1
            return
        attribute_dict = {'building_id': 'NULL' if building_id is None else str(building_id),
                          'ward': 'NULL' if ward_id is None else str(ward_id),
                          'room_id': 'NULL' if room_id is None else str(room_id), 'type': 'Room'
                          }
        self.S_GRAPH.add_node(str(string_id), **attribute_dict)
        self.Nodes['Room'].add(str(string_id))

    def new_device_node(self, string_id, name, warn_log=False):
        """Add a device node to the network.

        Automatically sets the 'type' attribute to "Device". Note that if string_id is empty (''), no node will be
        added to the network and a warning will be logged if warn_log is `True`.

        Args:
            string_id (str):    string identifier of device to be added.
            name (str):         name of device.
            warn_log (bool):    flag indicating whether or not to log warning messages.
        """
        if string_id == '':
            if warn_log:
                logging.warning('Empty device identifier - node is skipped')
            self.device_add_warnings += 1
            return
        self.S_GRAPH.add_node(str(string_id), type='Device', name=name)
        self.Nodes['Device'].add(string_id)

    def new_employee_node(self, string_id, warn_log=False):
        """Add an employee node to the network.

        Automatically sets the 'type' attribute to "employee". Note that if string_id is empty (''), no node will be
        added to the network and a warning will be logged if warn_log is `True`.

        Args:
            string_id (str):    string identifier of employee to be added.
            warn_log (bool):    flag indicating whether or not to log warning messages.
        """
        if string_id == '':
            if warn_log:
                logging.warning('Empty employee identifier - node is skipped')
            self.employee_add_warnings += 1
            return
        self.S_GRAPH.add_node(str(string_id), type='Employee')
        self.Nodes['Employee'].add(string_id)

    def new_edge(self, source_id, source_type, target_id, target_type, att_dict, log_warning=False):
        """Adds a new edge to the network.

        The added edge will link source_id of source_type to target_id of target_type. Note that the edge will ONLY be
        added if both source_id and target_id are found in the self.Nodes attribute dictionary. In addition, all
        key-value pairs in att_dict will be added to the newly created edge.

        Args:
            source_id (str):    String identifying the source node
            source_type (str):  source_id type, which must be one of ['Patient', 'Room', 'Device', 'Employee']
            target_id (str):    String identifying the target node
            target_type (str):  target_id type, which must be one of ['Patient', 'Room', 'Device', 'Employee']
            att_dict (dict):    dictionary containing attribute key-value pairs for the new edge.
            log_warning (bool): flag indicating whether or not to log a warning each time a faulty edge is encountered
        """
        if self.identify_id(source_id) is None:
            if log_warning:
                logging.warning(f'Did not find node {source_id} of type {source_type} - no edge added')
            self.edge_add_warnings += 1
            return
        if self.identify_id(target_id) is None:
            if log_warning:
                logging.warning(f'Did not find node {target_id} of type {target_type} - no edge added')
            self.edge_add_warnings += 1
            return
        self.S_GRAPH.add_edge(source_id, target_id, **att_dict)

    def remove_isolated_nodes(self, silent=False):
        """Restays all isolated nodes from the network.

        Isolated nodes are identified as having degree 0.

        Args:
            silent (bool):  Flag indicating whether or not to log progress (defaults to ``False``)
        """
        node_degrees = [self.S_GRAPH.degree(each_node) for each_node in self.S_GRAPH.nodes]
        if not silent:
            logging.info(f"##################################################################################")
            logging.info('Removing isolated nodes:')
            logging.info(f'--> Before processing, network contains {len(node_degrees)} total nodes, out of which '
                         f'{node_degrees.count(0)} are isolated.')
        remove_count = 0
        delete_list = []
        for each_node in self.S_GRAPH.nodes:
            if self.S_GRAPH.degree(each_node) == 0:  # degree of 0 indicates an isolated node
                delete_list.append(each_node)
        for node in delete_list:
            self.S_GRAPH.remove_node(node)
            remove_count += 1
        node_degrees_after = [self.S_GRAPH.degree(each_node) for each_node in self.S_GRAPH.nodes]
        if not silent:
            logging.info(f'-->  After processing, network contains {len(node_degrees_after)} total nodes, out of which '
                         f'{node_degrees_after.count(0)} are isolated.')

    def trim_model(self, snapshot_dt):
        """Trims the current model.

        Restays all edges for which the ``to`` attribute is larger than snapshot_dt, and updates the self.snapshot_dt
        attribute. However, this function does NOT remove isolated nodes.

        Args:
            snapshot_dt (dt.dt()): dt.dt() object specifying to which timepoint the model should be trimmed
        """
        deleted_edges = [edge_tuple for edge_tuple in self.S_GRAPH.edges(data=True, keys=True)
                         if edge_tuple[3]['to'] > snapshot_dt]
        # S_GRAPH.edges() returns a list of tuples of length 4 --> ('source_id', 'target_id', key, attr_dict)
        self.S_GRAPH.remove_edges_from(deleted_edges)
        self.snapshot_dt = snapshot_dt

    ################################################################################################################
    # Functions for updating attributes
    ################################################################################################################
    def update_edge_attributes(self, edge_tuple, attribute_dict):
        """Updates the edge identified in edge_tuple.

        Add all key-value pairs in attribute_dict. Existing attributes will be overwritten.

        Args:
            edge_tuple (tuple):     Tuple of length 3 identifying the edge :math:`\\longrightarrow` (source_id,
                                    target_id, key) (`key` is required to uniquely identify MultiGraph() edges)
            attribute_dict (dict):  dictionary of key-value pairs with which to update the edge
        """
        attrs = {edge_tuple: attribute_dict}
        # to update a specific edge, the dictionary passed to set_edge_attributes() must be formatted
        # as --> { ('bla', 'doodel', 0) : {'newattr' : 'somevalue'} }
        nx.set_edge_attributes(self.S_GRAPH, attrs)

    def update_node_attributes(self, node_id, attribute_dict):
        """Updates the node identified in node_id.

        The node will be updated with all key-value pairs in attribute_dict. Note that existing attributes will be
        overwritten with the values in attribute_dict.

        Args:
            node_id (str):          string identifier for the node
            attribute_dict (dict):  dictionary of key-value pairs with which the node will be updated
        """
        attrs = {node_id: attribute_dict}
        # to update a specific edge, the dictionary passed to set_node_attributes() must be formatted
        # as --> { 'node_id' : {'newattr' : 'somevalue'} }
        nx.set_node_attributes(self.S_GRAPH, attrs)

    def add_edge_infection(self):
        """Sets "infected" attribute to all edges.

        This function will iterate over all edges in the network and set an additional attribute ``infected``, which
        will be set to ``True`` if it connects to a patient node for which the ``vre_status`` attribute is set to
        ``pos``. For all other edges, this attribute will be set to ``False``.
        """
        logging.info(f"##################################################################################")
        logging.info('Adding infection data to network edges...')
        neg_infect_count = 0
        pos_infect_count = 0
        error_count = 0
        for each_edge in self.S_GRAPH.edges(data=True, keys=True):
            # will yield a list of tuples of length 4 --> (source_id, target_id, key, attribute_dict)
            try:
                edge_types = [self.identify_id(each_edge[i]) for i in range(0, 2)]
                if None in edge_types:
                    # will yield a list of length 2 --> [source_id_type, target_id_type] (e.g. ['Patient', 'Room'] )
                    logging.warning(f"Encountered an edge for which at least one node could not be identified")
                    self.update_edge_attributes(edge_tuple=(each_edge[0], each_edge[1], each_edge[2]),
                                                attribute_dict={'infected': False})
                    continue
                if 'Patient' not in edge_types:  # indicates an edge between two non-patient nodes
                    self.update_edge_attributes(edge_tuple=(each_edge[0], each_edge[1], each_edge[2]),
                                                attribute_dict={'infected': False})
                    neg_infect_count += 1
                else:
                    # indicates an edge of one of the following types:
                    # Patient-Room, Patient-Device or Patient-Employee (patient id is always in each_edge[0])
                    pat_index = edge_types.index('Patient')
                    # returns the index (0 or 1) containing the patient node (can either be source_id or target_id)
                    if self.S_GRAPH.node[each_edge[pat_index]]['vre_status'] == 'neg':
                        self.update_edge_attributes(edge_tuple=(each_edge[0], each_edge[1], each_edge[2]),
                                                    attribute_dict={'infected': False})
                        neg_infect_count += 1
                    else:
                        self.update_edge_attributes(edge_tuple=(each_edge[0], each_edge[1], each_edge[2]),
                                                    attribute_dict={'infected': True})
                        pos_infect_count += 1
            except Exception as e:  # e is currently not used
                self.update_edge_attributes(edge_tuple=(each_edge[0], each_edge[1], each_edge[2]),
                                            attribute_dict={'infected': False})
                error_count += 1
        logging.warning(f'Encountered {error_count} errors during the identification of infected patient nodes')
        logging.info(f"Successfully added 'infected' status to network edges ({pos_infect_count} infected, "
                     f"{neg_infect_count} uninfected edges)")
        self.edges_infected = True

    def update_shortest_path_statistics(self, focus_nodes=None, approximate=False, max_path_length=None):
        """Prerequisite function for calculating betweenness centrality.

        Adds new attributes to all nodes in focus_nodes, where each attribute is a pair of nodes (sorted alphabetically)
        with a ``SP-`` prefix to tuples of length 2 containing (shortest_paths_through_this_node, total_shortest_paths)
        For example:

        ``{ 'SP-Node1-Node2': (2, 5) , 'SP-Node1-Node3': (1, 8), ... } }``

        Note:
             This may result in a lot of additional attributes for nodes which are integral to the network. This ap-
            proach is chosen because the networkx module does not allow updates to a dict-of-a-dict type
            of attributes - i.e. if these attributes were to be combined in a 1st-level key 'shortest-paths', the entire
            content would have to be copied every time a new node-pair attribute is added, which would make the function
            extremely inefficient.

        This is an important prerequisite function for the calculation of betweenness centrality.

        Args:
            focus_nodes (list):     list of node IDs. If set to ``None`` (the default), all nodes in the network will be
                                    considered. **WARNING: this may be extremely ressource-intensive !**
            approximate (bool):     Flag indicating whether to consider all shortest paths in the network
                                    (``False``, default) or approximate the betweenness statistic using the
                                    `max_path_length` argument. Note that if this is set to `False`, attributes of all
                                    nodes will be written **to file** so as to avoid memory overflows. This requires a
                                    preceding call to `self.write_node_files()`.
            max_path_length (int):  Maximum path length to consider for pairs of nodes when `approximate` == ``True``.
                                    If set to ``None`` (default), all possible shortest paths will be considered.
        """
        logging.info("Update betweenness statistics...")
        target_nodes = focus_nodes if focus_nodes is not None else self.S_GRAPH.nodes
        node_combinations = list(itertools.combinations(target_nodes, 2))
        # Returns a list of tuples containing all unique pairs of nodes in considered_nodes

        logging.info(f'--> Adding shortest path statistics considering {len(target_nodes)} nodes yielding '
                     f'{len(node_combinations)} combinations.')
        logging.info(f"Approximate set to {approximate}, maximum path length set to {max_path_length}")
        for count, combo_tuple in enumerate(tqdm(node_combinations, total=len(node_combinations))):
            if count % 100 == 0:
                logging.info(f" <> Processed {count} combinations")
            if nx.has_path(self.S_GRAPH, combo_tuple[0], combo_tuple[1]) is False:
                continue  # indicates a node pair in disconnected network parts
            # measure = datetime.datetime.now()
            if approximate is False:  # This will write required prerequisites to the node files in self.data_dir
                if self.node_files_written is False:
                    raise self.NodeBetweennessException('Missing a required call to self.write_node_files() !')
                all_shortest_paths = list(nx.all_shortest_paths(self.S_GRAPH, source=combo_tuple[0],
                                                                target=combo_tuple[1]))
                # Restay the first and last node (i.e. source and target) of all shortest paths
                trim_short_paths = [path_list[1:(len(path_list) - 1)] for path_list in all_shortest_paths]
                involved_nodes = [node for sublist in trim_short_paths for node in sublist]
                node_counts = Counter(involved_nodes)
                for each_key in node_counts:
                    json_filepath = os.path.join(self.data_dir, self.parse_filename(each_key) + '.json')
                    node_dict = self.load_from_json(path_to_file=json_filepath)  # Returns type dictionary
                    if 'BW-Stats' not in node_dict.keys():
                        node_dict['BW-Stats'] = [(node_counts[each_key], len(all_shortest_paths))]  # -> list of tuples
                    else:
                        node_dict['BW-Stats'].append((node_counts[each_key], len(all_shortest_paths)))
                    # Save node_dict back to file
                    self.save_to_json(path_to_file=json_filepath, saved_object=node_dict)
            else:
                shortest_pair_path = nx.shortest_path(self.S_GRAPH, source=combo_tuple[0], target=combo_tuple[1])
                if max_path_length is not None and len(shortest_pair_path) > max_path_length:
                    continue  # indicates a path too long to be considered relevant for transmission
                # If shortest paths are "short enough", calculate the exact measure
                all_shortest_paths = list(nx.all_shortest_paths(self.S_GRAPH, source=combo_tuple[0],
                                                                target=combo_tuple[1]))
                # Restay the first and last node (i.e. source and target) of all shortest paths
                trim_short_paths = [path_list[1:(len(path_list) - 1)] for path_list in all_shortest_paths]
                involved_nodes = [node for sublist in trim_short_paths for node in sublist]
                node_counts = Counter(involved_nodes)
                sorted_pair = sorted([combo_tuple[0], combo_tuple[1]])
                update_attr_dict = {each_key: {'SP-' + sorted_pair[0] + '-' + sorted_pair[1]: (node_counts[each_key],
                                                                                               len(all_shortest_paths))}
                                    for each_key in node_counts if each_key not in sorted_pair}
                # Then update node attributes
                nx.set_node_attributes(self.S_GRAPH, update_attr_dict)
        # Write it all to log
        logging.info(f"Successfully added betweenness statistics to the network !")
        # Adjust the self.shortest_path_stats
        self.shortest_path_stats = True

    ##########################################################################
    # Customized Network Functions
    ##########################################################################
    def inspect_network(self):
        """Important inspect function for the graph.

        An important function that will inspect all properties of the network and return diagnostic measures on the
        "quality". This includes:

        - Total number of nodes in the network
        - Number of isolated nodes in the network
        - Number of nodes in the network of type:

            - Patient
            - Device
            - Employee
            - Room

        - Total number of edges in the network
        - Number of edges in the network of type:

            - Patient-Device
            - Patient-Room
            - Patient-Employee
            - Employee-Device
            - Employee-Room
            - Device-Room

        - Number of improperly formatted edges. These include:

            - Edges for which at least one node is empty, i.e. ""
            - Edges for which any one of the ``from``, ``to``, ``type``, ``origin``, and ``infected``
              (if self.edge_infected == ``True``) attributes are not present

        All result statistics are printed to log.
        """
        logging.info(f"###############################################################")
        logging.info(f"Running network statistics...")

        all_nodes = self.S_GRAPH.nodes(data=True)  # list of tuples of ('source_id', key, {attr_dict } )
        node_degrees = [self.S_GRAPH.degree(each_node) for each_node in self.S_GRAPH.nodes]
        all_edges = self.S_GRAPH.edges(data=True, keys=True)  # tuple list -> ('source', 'target', key, {attr_dict } )

        # Overall network statistics
        logging.info(f'--> Model Snapshot date: {self.snapshot_dt.strftime("%d.%m.%Y %H:%M:%S")}')
        logging.info(f"--> Total {len(all_nodes)} nodes, out of which {node_degrees.count(0)} are isolated")
        logging.info(f"--> Total {len(all_edges)} edges")
        logging.info('------------------------------')

        # Extract specific node statistics
        nbr_pat_nodes = len(['_' for node_data_tuple in all_nodes if not pd.isna(node_data_tuple[0]) and node_data_tuple[1]['type'] == 'Patient'])
        nbr_dev_nodes = len(['_' for node_data_tuple in all_nodes if not pd.isna(node_data_tuple[0]) and node_data_tuple[1]['type'] == 'Device'])
        nbr_emp_nodes = len(['_' for node_data_tuple in all_nodes if not pd.isna(node_data_tuple[0]) and node_data_tuple[1]['type'] == 'Employee'])
        nbr_room_nodes = len(['_' for node_data_tuple in all_nodes if not pd.isna(node_data_tuple[0]) and node_data_tuple[1]['type'] == 'Room'])
        accounted_for = nbr_pat_nodes + nbr_dev_nodes + nbr_emp_nodes + nbr_room_nodes
        logging.info('Node overview:')
        logging.info(f"--> {nbr_pat_nodes} Patient nodes")
        logging.info(f"--> {nbr_dev_nodes} Device nodes")
        logging.info(f"--> {nbr_emp_nodes} Employee nodes")
        logging.info(f"--> {nbr_room_nodes} Room nodes")
        logging.info(f"--> TOTAL: {accounted_for} nodes ({len(all_nodes) - accounted_for} out of "
                     f"{len(all_nodes)} nodes not accounted for)")
        logging.info('------------------------------')

        # Extract specific edge statistics
        type_count_dict = {}

        faulty_sourceordest_id = 0  # Counts edges for which the source or target id are wrongly formatted
        nbr_missing_attr = 0  # Counts edges which are missing at least one of the attributes
        nbr_ok = 0  # Counts edges passing all tests

        test_attribute_keys = ['type', 'from', 'to', 'origin'] if self.edges_infected == False \
            else ['type', 'from', 'to', 'origin', 'infected']

        for each_edge in all_edges:
            # Check source and target id
            if each_edge[0] == '' or each_edge[1] == '':
                faulty_sourceordest_id += 1
                continue
            attr_keys = each_edge[3].keys()
            if len([entry for entry in test_attribute_keys if entry not in attr_keys]) > 0:
                # indicates at least one missing attribute
                nbr_missing_attr += 1
                continue
            # If everything is ok, update type_count_dict
            nbr_ok += 1
            if each_edge[3]['type'] not in type_count_dict:
                type_count_dict[each_edge[3]['type']] = 1
            else:
                type_count_dict[each_edge[3]['type']] += 1
        type_count_keys = sorted(list(type_count_dict.keys()))
        # Write all remaining results to log
        logging.info(f"--> {faulty_sourceordest_id} edges with a faulty source or target id")
        logging.info(f"--> {nbr_missing_attr} edges missing at least one attribute")
        logging.info(f"--> {nbr_ok} edges ok:")
        for each_key in type_count_keys:
            logging.info(f"    >> {type_count_dict[each_key]} edges of type {each_key}")
        accounted_for = sum([value for value in type_count_dict.values()])
        logging.info(f"--> TOTAL: {accounted_for} edges ({len(all_edges) - accounted_for} out of {len(all_edges)} "
                     f"edges not accounted for)")
        logging.info('------------------------------')

        # Number of positive patients in the network
        nbr_pos_pat = len(['_' for node_data_tuple in all_nodes if not pd.isna(node_data_tuple[0]) and node_data_tuple[1]['type'] == 'Patient' and
                           node_data_tuple[1]['vre_status'] == 'pos'])
        logging.info(f"--> {nbr_pos_pat} VRE-positive Patients in the network")
        logging.info('------------------------------')

        # Graph connectivity
        #logging.info(f"--> Graph connected: {nx.is_connected(self.S_GRAPH)}")
        logging.info(f"###############################################################")

    def add_network_data(self, patient_dict, case_subset='relevant_case', patient_subset=None, snapshot=datetime.datetime.now()):
        """Adds nodes and edges data to the network.

        Nodes and edges are added based on the data in patient_dict according to the subset specified (see description
        of parameters below).

        Args:
            patient_dict (dict):    Dictionary containing all data required to build the graph. Please refer to
                                    "Patient_Data_Overview.dov" for details on this argument.
            case_subset (str):           Subset of data to be used, can be one of:

                                    - ``relevant_case`` :math:`\\longrightarrow` includes patients with a relevant case
                                      (regardless of involvement in VRE screenings) and the data of relevant cases
                                    - ``risk`` :math:`\\longrightarrow` includes patients with an associated risk (i.e.
                                      at least one VRE screening) and data of relevant cases

            patient_subset (list)   A subset of patients to model in network.

            snapshot (dt.dt()):     datetime.datetime() object specifying to which point in time data are to be
                                    imported. Defaulting to the time of execution, this parameter can be used to create
                                    a "snapshot" of the model, and will *ignore* (i.e. not add) edges in patient_dict
                                    for which the 'to' attribute is larger than this parameter. Note that all nodes from
                                    patient_dict will be added, but most "new" nodes will be created in isolation. And
                                    since a call to this function is usually followed by a call to
                                    *remove_isolated_nodes()*, these isolated nodes will then be stripped from the
                                    network.
        """
        logging.info(f"Filter set to: {case_subset}")
        logging.info(f"Snapshot created at: {snapshot.strftime('%d.%m.%Y %H:%M:%S')}")
        self.snapshot_dt = snapshot
        #############################################################
        # --> Measures for the created network
        #############################################################
        # General measures
        nbr_pat_no_rel_case = 0  # Counts patients without...
        nbr_pat_rel_case = 0  # ... and with a relevant case
        nbr_room_no_id = 0  # Counts number of unidentifyable...
        nbr_room_id = 0  # ... and identifyable rooms
        nbr_app = 0  # number of appointments parsed

        # Edge measures
        nbr_pat_emp = 0  # number of Patient-Employee edges
        nbr_pat_room = 0  # number of Patient-Room edges
        nbr_pat_device = 0  # number of Patient-Device edges
        nbr_room_device = 0  # number of Room-Device edges
        nbr_device_emp = 0  # number of Device-Employee edges
        nbr_emp_room = 0  # number of Employee-Room edges

        for patient in tqdm(patient_dict['patients'].values(), total=len(patient_dict['patients'].values())):
            # Apply subset filter here --> relevant_case
            if case_subset == 'relevant_case':
                if patient_subset is not None and patient.patient_id not in patient_subset:
                    continue

                # TODO: Rethink relevant case for patient
                # pat_relevant_case = patient.get_relevant_case()  # Returns a Case() object or None
                # if pat_relevant_case is None:
                #     nbr_pat_no_rel_case += 1
                #     continue
                nbr_pat_rel_case += 1

                this_pat_id = patient.patient_id
                if this_pat_id == '':
                    logging.warning('Encountered empty patient ID !')
                    continue
                # Add patient node
                self.new_patient_node(str(this_pat_id), risk_dict=patient.risks)
                #########################################
                # --> Step 1: Add rooms based on Stay() objects to the network
                #########################################
                for stay in patient.get_stays():  # iterate over all stays of a Patient
                    room_id = stay.room_id  # will either be the room's name or None
                    ward_name = stay.ward.name  # will either be the ward's name or None
                    if room_id is None:  # --> If room is not identified, add it to the 'generic' Room node "Room_Unknown"
                        if "Room_Unknown" not in self.S_GRAPH.nodes:
                            self.new_room_node('Room_Unknown')
                        this_room = 'Room_Unknown'
                        nbr_room_no_id += 1
                    else:  # --> room is identified
                        this_room = stay.room_id
                        # Add room node - this will only overwrite attributes if node is already present
                        # --> does not matter since room_id and ward are the same
                        try:
                            room = patient_dict["rooms"][this_room]
                            building_id = room.ww_building_id
                        except:
                            building_id = None

                        self.new_room_node(stay.room_id, building_id=building_id, ward_id=ward_name, room_id=stay.room.get_ids()
                                           if stay.room is not None else None)
                        # .get_ids() will return a '@'-delimited list of [room_id]_[system] entries, or None
                        nbr_room_id += 1
                    # Add Patient-Room edge if it is within scope of the current snapshot
                    edge_dict = {'from': stay.from_datetime, 'to': stay.to_datetime,
                                 'type': 'Patient-Room', 'origin': 'Stay'}
                    if edge_dict['to'] < snapshot:
                        self.new_edge(str(this_pat_id), 'Patient', this_room, 'Room', att_dict=edge_dict)
                #########################################
                # --> Step 2: Add Rooms, devices and employees based on the relevant Case().appointments
                #         This will add various edges in the network, since all Appointments contain information on the
                #         patient, employees, devices and rooms:
                #         --> Device-Patient
                #         --> Patient-Room
                #         --> Employee-Patient
                #         --> Device-Employee
                #         --> Employee-Room
                #         --> Device-Room
                #         (Remember: nodes in edge specifications are sorted alphabetically)
                #########################################
                for each_app in patient.get_appointments():
                    nbr_app += 1
                    duration_from = each_app.date
                    duration_to = each_app.date + datetime.timedelta(hours=each_app.duration_in_mins / 60.0)
                    edge_attributes = {'from': duration_from, 'to': duration_to, 'origin': 'Appointment'}
                    # 'type' key will be added during the creation of edges, see below
                    device_list = []
                    employee_list = []
                    room_list = []
                    ####################################
                    # --> ADD NODES
                    ####################################
                    # --> Add device nodes
                    for each_device in each_app.devices:
                        self.new_device_node(str(each_device.id), name=str(each_device.name))
                        device_list.append(str(each_device.id))
                    # --> Add employee nodes
                    for each_emp in each_app.employees:
                        self.new_employee_node(str(each_emp.id))
                        employee_list.append(str(each_emp.id))
                    # --> Add Room nodes
                    for each_room in each_app.rooms:
                        try:
                            room = patient_dict["rooms"][each_room.room_id]
                            building_id = room.ww_building_id
                        except:
                            building_id = None
                        self.new_room_node(string_id=each_room.room_id, building_id=building_id, ward_id=each_room.ward.name if each_room.ward is not None else None, room_id=each_room.get_ids())
                        room_list.append(each_room.room_id)
                    ####################################
                    # --> ADD EDGES based on specifications in self.edge_types
                    ####################################
                    # --> Device-Patient
                    if 'Device-Patient' in self.edge_types:
                        edge_attributes['type'] = 'Device-Patient'
                        for device in device_list:
                            if edge_attributes['to'] < snapshot:
                                self.new_edge(this_pat_id, 'Patient', device, 'Device', att_dict=edge_attributes)
                                nbr_pat_device += 1
                    # --> Patient-Room
                    if 'Patient-Room' in self.edge_types:
                        edge_attributes['type'] = 'Patient-Room'
                        for room in room_list:
                            if edge_attributes['to'] < snapshot:
                                self.new_edge(this_pat_id, 'Patient', room, 'Room', att_dict=edge_attributes)
                                nbr_pat_room += 1
                    # --> Employee-Patient
                    if 'Employee-Patient' in self.edge_types:
                        edge_attributes['type'] = 'Employee-Patient'
                        for emp in employee_list:
                            if edge_attributes['to'] < snapshot:
                                self.new_edge(this_pat_id, 'Patient', emp, 'Employee', att_dict=edge_attributes)
                                nbr_pat_emp += 1
                    # --> Device-Employee
                    if 'Device-Employee' in self.edge_types:
                        edge_attributes['type'] = 'Device-Employee'
                        for emp in employee_list:
                            for device in device_list:
                                if edge_attributes['to'] < snapshot:
                                    self.new_edge(emp, 'Employee', device, 'Device', att_dict=edge_attributes)
                                    nbr_device_emp += 1
                    # --> Employee-Room
                    if 'Employee-Room' in self.edge_types:
                        edge_attributes['type'] = 'Employee-Room'
                        for emp in employee_list:
                            for room in room_list:
                                if edge_attributes['to'] < snapshot:
                                    self.new_edge(emp, 'Employee', room, 'Room', att_dict=edge_attributes)
                                    nbr_emp_room += 1
                    # --> Device-Room
                    if 'Device-Room' in self.edge_types:
                        edge_attributes['type'] = 'Device-Room'
                        for device in device_list:
                            for room in room_list:
                                if edge_attributes['to'] < snapshot:
                                    self.new_edge(device, 'Device', room, 'Room', att_dict=edge_attributes)
                                    nbr_room_device += 1
        #########################################
        logging.info(f"##################################################################################")
        logging.info(f"Encountered {nbr_room_no_id} stays without associated room, {nbr_room_id} rooms identified.")
        logging.info('------------------------------------------------------------------')
        total_warnings = self.room_add_warnings + self.employee_add_warnings + self.device_add_warnings + self.patient_add_warnings
        if total_warnings > 0:
            logging.warning(f'Encountered the following errors during the addition of nodes to the network:')
            logging.warning(f'--> {self.room_add_warnings} errors during the addition of room nodes to the network')
            logging.warning(f'--> {self.employee_add_warnings} errors during the addition of employee nodes to the '
                            f'network')
            logging.warning(f'--> {self.patient_add_warnings} errors during the addition of patient nodes to the '
                            f'network')
            logging.warning(f'--> {self.device_add_warnings} errors during the addition of device nodes to the network')
        if self.edge_add_warnings > 0:
            logging.info('------------------------------------------------------------------')
            logging.warning(f'Encountered {self.edge_add_warnings} warnings while adding edges to the network.')
        logging.info(f"##################################################################################")
        # Log "global" statistics
        self.inspect_network()

    ################################################################################################################
    # Data Export Functions
    ################################################################################################################
    def calculate_patient_degree_ratio(self):
        if not self.edges_infected:
            logging.error('This operation requires infection data on edges !')
            return None

        logging.info('Calculating patient degree ratio...')
        patient_degree_rows = []

        for each_node in tqdm(self.S_GRAPH.nodes(data=True)):
            if pd.isna(each_node[0]):
                continue

            # each_node will be a tuple of length 2 --> ( 'node_id', {'att_1' : 'att_value1', ... } )
            this_node_edges = self.S_GRAPH.edges(each_node[0], data=True, keys=True)
            # will return a list of tuples
            # --> [ ( 'node_id', 'target_id1', key, {attr_dict} ), ('node_id', 'target_id2', key, {attr_dict} ),...]
            pat_edges = [edge for edge in this_node_edges if 'Patient' in edge[3]['type']]
            # indicates a type of "Patient-XXX" node
            infected_pat_edges = [True if edge[3]['infected'] is True else False for edge in pat_edges]

            # Write to output file:
            really_intected_pat_edges = [entry for entry in infected_pat_edges if entry]
            patient_degree_rows.append([each_node[0], each_node[1]['type'], sum(infected_pat_edges) / len(infected_pat_edges),
                                        len(really_intected_pat_edges), len(pat_edges), len(this_node_edges)])

        patient_degree_df = pd.DataFrame.from_records(patient_degree_rows)
        patient_degree_df.columns = ["Node ID", "Node Type", "Degree Ratio", "Number of Infected Edges", "Total Patient Edges", "Total Edges"]
        patient_degree_df.sort_values(by="Degree Ratio", ascending=False, inplace=True)

        logging.info(f"Successfully calculated patient degree ratios for {len(patient_degree_rows)} nodes.")

        return patient_degree_df

    def export_patient_degree_ratio(self, csv_sep=';', export_path=None):
        """Calculates and exports patient degree ratio for all nodes in the network.

        The patient degree ratio is defined for a single node_x as:

        number of infected edges between node_x and patients / number of total edges between node_x and patients

        Args:
            csv_sep (str):      Separator for created csv file.
            export_path (str):  Path to which export file will be written. If set to `None` (the default), the
                                exported file will be written to `self.data_dir`.

        The result file will be written to the path [self.data_dir]/[self.snapshot_dt]_pdr.txt, and contains the
        following columns:
        - Node ID
        - Node type
        - Degree ratio
        - Number of infected edges (always patient-related)
        - Total number of patient-related edges
        - Total number of edges (i.e. degree of node_x)
        """

        df = self.calculate_patient_degree_ratio()

        exact_path = self.data_dir if export_path is None else export_path
        export_path = os.path.join(exact_path, self.snapshot_dt.strftime('%Y_%m_%d') + '_pdr.txt')
        df.to_csv(export_path, sep=csv_sep)

        # Write to log
        logging.info(f"Successfully exported patient degree ratios to {os.path.join(exact_path, self.snapshot_dt.strftime('%Y_%m_%d') + '_pdr.txt')}")

    def calculate_total_degree_ratio(self):
        logging.info('Calculating total degree ratio...')
        patient_degree_ratio_rows = []

        for each_node in tqdm(self.S_GRAPH.nodes(data=True)):
            if pd.isna(each_node[0]):
                continue

            # each_node will be a tuple of length 2 --> ( 'node_id', {'att_1' : 'att_value1', ... } )
            this_node_edges = self.S_GRAPH.edges(each_node[0], data=True, keys=True)
            # will return a list of tuples
            # --> [ ( 'node_id', 'target_id1', key, {attr_dict} ), ('node_id', 'target_id2', key, {attr_dict} ),...]
            infected_edges = [True if edge[3]['infected'] else False for edge in this_node_edges]
            # Write to output file:
            patient_degree_ratio_rows.append([each_node[0],
                                              each_node[1]['type'],
                                              sum(infected_edges)/len(this_node_edges),
                                              len([entry for entry in infected_edges if entry]),
                                              len(this_node_edges)])
        patient_degree_ratio_df = pd.DataFrame.from_records(patient_degree_ratio_rows)
        patient_degree_ratio_df.columns = ["Node ID", "Node Type", "Total Degree Ratio", "Number of Infected Edges", "Total Edges"]
        patient_degree_ratio_df.sort_values(by="Total Degree Ratio", ascending=False, inplace=True)

        # Write to log
        logging.info(f"Successfully calculated total degree ratios for {len(patient_degree_ratio_rows)} nodes")

        return patient_degree_ratio_df

    def export_total_degree_ratio(self, csv_sep=';', export_path=None):
        """Exports total degree ratio (TDR) for all nodes in the network.

        Will calculate and export the total degree ratio for all nodes in the network, which is defined for a
        single *node_x* as:

        :math:`TDR = \\frac{Number\_of\_infected\_edges\_between\_node
        \\_x\_and\_patients}{Total\_number\_of\_edges\_leading\_to\_node\\_x}`

        The result file will be written to a file [self.data_dir]/[self.snapshot_dt]_tdr.txt, and contains the
        following columns:

        - Node ID
        - Node type
        - Degree ratio
        - Number of infected edges (always patient-related)
        - Total number of edges for node_x (also includes non-patient-related edges)

        Args:
            csv_sep (str):      Separator for created csv file.
            export_path (str):  Path to which node files will be written. If set to `None` (the default), the
                                exported file will be written to `self.data_dir`.
        """
        exact_path = self.data_dir if export_path is None else export_path
        export_path = os.path.join(exact_path, self.snapshot_dt.strftime('%Y_%m_%d') + '_tdr.txt')
        df = self.calculate_total_degree_ratio()

        df.to_csv(export_path, sep=csv_sep)

        # Write to log
        logging.info(f"Successfully wrote total degree ratios to "
                     f"{os.path.join(exact_path, self.snapshot_dt.strftime('%Y_%m_%d') + '_tdr.txt')}")

    def export_shortest_path_length_overview(self, focus_nodes=None, csv_sep=';', export_path=None):
        """Exports an overview of shortest path lengths in the network to self.data_dir.

        Exports an overview of shortest path lengths of all nodes in the network (if *focus_nodes* is `None`,
        default). If a list of node identifiers is provided in *focus_nodes* instead, only these nodes will be
        considered for the data export. Data are exported to the self.data_dir directory.

        Note:
            This overview only considers *one* possible shortest path between any given pair of nodes, not all paths.

        Args:
            focus_nodes (list or None): List of nodes considered for distribution.
            csv_sep (str):              Separator used in export file.
            export_path (str):          Path to which node files will be written. If set to `None` (the default), the
                                        exported file will be written to `self.data_dir`.

        """
        exact_path = self.data_dir if export_path is None else export_path
        target_nodes = focus_nodes if focus_nodes is not None else self.S_GRAPH.nodes
        node_combinations = list(itertools.combinations(target_nodes, 2))
        logging.info(f'Exporting shortest path length overview for {len(node_combinations)} node combinations...')
        shortest_path_lengths = []  # list holding all encountered shortest path lengths
        for count, combo_tuple in enumerate(node_combinations):
            if count % 1000 == 0:
                logging.info(f" <> Processed {count} combinations")
            if nx.has_path(self.S_GRAPH, combo_tuple[0], combo_tuple[1]) is False:
                continue  # indicates a node pair in disconnected parts of network
            shortest_path_lengths.append(len(nx.shortest_path(self.S_GRAPH, source=combo_tuple[0],
                                                              target=combo_tuple[1])))
        # Then export results to file
        node_counts = Counter(shortest_path_lengths)
        with open(os.path.join(exact_path, self.snapshot_dt.strftime('%Y_%m_%d') + '_plov.txt'), 'w') as outfile:
            outfile.write(f"Path Length{csv_sep}Count\n")
            for each_key in sorted(node_counts.keys()):
                outfile.write(f"{each_key}{csv_sep}{node_counts[each_key]}\n")
        # Write to log and exit
        logging.info(f'Successfully exported shortest path length overview to'
                     f"{os.path.join(exact_path, self.snapshot_dt.strftime('%Y_%m_%d') + '_plov.txt')}")

    def calculate_node_betweenness(self):
        """Calculate node betweenness.

        This function will calculate node betweenness for all nodes of the network. This is done by calculating the sum
        of all fractions of shortest paths a particular node is in, which is found in the attribute keys starting with
        the "SP-" prefix (e.g. "SP-Node1-Node4"). These tuples contain 2 entries, the first being the fraction of
        shortest path this node is found in (between the given pair of nodes), and the second one being the total
        number of shortest paths. This function can only be executed once the function update_shortest_path_statistics()
        has been called. Nodes without an attribute key starting with the "SP-" prefix will have node betweenness of 0.
        """
        logging.info('Calculating node betweenness...')
        # self.update_shortest_path_statistics()
        node_betweenness_rows = []

        for node_tuple in tqdm(self.S_GRAPH.nodes(data=True)):
            # Returns tuples of length 2 --> (node_id, attribute_dict)
            write_string = [node_tuple[0], self.identify_id(node_tuple[0])]
            target_keys = [each_key for each_key in node_tuple[1].keys() if each_key.startswith('SP-')]
            betweenness_score = sum([node_tuple[1][each_key][0] / node_tuple[1][each_key][1]
                                     for each_key in target_keys])
            write_string.append(betweenness_score)

            node_betweenness_rows.append(write_string)

        node_betweenness_df = pd.DataFrame.from_records(node_betweenness_rows)
        node_betweenness_df.columns = ['Node ID', 'Node Type', 'Betweenness Score']
        node_betweenness_df.sort_values(by="Betweenness Score", ascending=False, inplace=True)

        # Write to log
        logging.info(f"Successfully calculated betweenness centralities for {len(node_betweenness_rows)} nodes")

        return node_betweenness_df

    def export_node_betweenness(self, csv_sep=';', export_path=None):
        """Exports node betweenness.

        This function will export node betweenness for all nodes of the network. This is done by exporting the sum
        of all fractions of shortest paths a particular node is in, which is found in the attribute keys starting with
        the "SP-" prefix (e.g. "SP-Node1-Node4"). These tuples contain 2 entries, the first being the fraction of
        shortest path this node is found in (between the given pair of nodes), and the second one being the total
        number of shortest paths. This function can only be executed once the function update_shortest_path_statistics()
        has been called. Nodes without an attribute key starting with the "SP-" prefix will have node betweenness of 0.

        The exported file is written into self.data_dir and contains 3 columns:

        - Node ID
        - Node Type
        - Betweenness Score

        Args:
            csv_sep (str):      separator to be used in export file.
            export_path (str):  Path to which node files will be written. If set to `None` (the default), the
                                exported file will be written to `self.data_dir`.
        """

        df = self.calculate_node_betweenness()

        exact_path = self.data_dir if export_path is None else export_path
        export_path = os.path.join(exact_path, self.snapshot_dt.strftime('%Y_%m_%d') + '_nbc.txt')
        df.to_csv(export_path, sep=csv_sep)

        # Write to log
        logging.info(f"Successfully exported betweenness centrality scores to {os.path.join(exact_path, self.snapshot_dt.strftime('%Y_%m_%d') + '_nbc.txt')}")

    def write_node_files(self, attribute_subset=None, export_path=None):
        """Writes a JSON representation of all nodes in the network to file.

        For each node in the network, this function will write a JSON representation containing a dictionary of
        node_data_dict[keys], where keys are all entries in node_attributes if that key is actually found in the node
        data dictionary. The file is named [node_id].json and written to the self.data_dir directory.

        This function is mainly used to "hard-store" values for highly resource intensive calculations such as
        betweenness centrality and avoid memory overflows.

        Note:
            This function will also set the self.node_files_written flag to ``True``.

        Args:
            attribute_subset (list):    List of keys in a node's attribute_dict to be included in the written JSON
                                        representation of the node. If set to `None` (the default), all attributes will
                                        be included.
            export_path (str):          Path to which node files will be written. If set to `None` (the default), all
                                        files will be written to `self.data_dir`.
        """
        exact_path = self.data_dir if export_path is None else export_path

        # make all folders along the path
        # TODO: Does this also work on HDFS?
        if isinstance(exact_path, str):
            export_path = pathlib.Path(exact_path)

        export_path.mkdir(parents=True, exist_ok=True)

        logging.info(f"Writing node files to {os.path.abspath(exact_path)}...")
        write_count = 0
        for node_tuple in self.S_GRAPH.nodes(data=True):
            # Returns tuples of length 2 --> (node_id, attribute_dict)
            if write_count % 5000 == 0:
                logging.info(f"Wrote {write_count} nodes to file.")
            if attribute_subset is None:
                write_keys = [key for key in node_tuple[1]]
            else:
                write_keys = [key for key in node_tuple[1] if key in attribute_subset]
            write_dict = {node_attr: node_tuple[1][node_attr] for node_attr in write_keys}
            try:
                json.dump(write_dict, open(os.path.join(exact_path, self.parse_filename(node_tuple[0]) + '.json'), 'w'))
            except TypeError as e:
                print(e)
                print(str(write_dict))  # TODO: the json dumper can not write risk objects
            write_count += 1
        # Write to log and exit
        logging.info(f"Successfully wrote all nodes to file!")
        self.node_files_written = True
