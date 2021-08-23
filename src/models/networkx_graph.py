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
    if orig_model.from_range is None or orig_model.to_range is None:
        logging.error('Please add data to the model before taking snapshots !')
        return None
    if True in [dt_value > orig_model.to_range for dt_value in snapshot_dt_list]:
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

    def __init__(self, edge_types=None):
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
        self.nodes = {
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

        self.shortest_path_stats = False
        # indicates whether shortest path statistics have been added to nodes via update_shortest_path_statistics()

        self.node_files_written = False  # indicates whether node files (in JSON format) are present in self.data_dir

        self.from_range = None  # time at which "snapshot" of the model is taken (important for "sub-snapshots")
        self.to_range = None

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
        for key in self.nodes.keys():
            if string_id in self.nodes[key]:  # self.nodes[key] will be a set --> in operator performs very well
                return key
        return None

    def identify_node(self, node_id, node_type):
        """Checks whether node_id is found in self.nodes[node_type].

        This function is more performant than identify_id(), since it already assumes that the node type of the string
        to be identified is known.

        Args:
            node_id (str):      String identifier of the node to be identified.
            node_type (str):    Type of node to be identified (e.g. 'Patient')

        Returns:
            bool: `True` if node_id is found in self.nodes[node_type], `False` otherwise.
        """
        if node_id in self.nodes[node_type]:
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
        infection_date = datetime.date.max
        for date, risk in risk_dict.items():
            if risk.result != "nn":
                infection_date = date
                break

        self.S_GRAPH.add_node(str(string_id), type='Patient', risk=risk_dict, infection_date=infection_date, vre_status='pos' if len(risk_codes) != 0 else 'neg')
        self.nodes['Patient'].add(string_id)

    def new_room_node(self, string_id, building_id=None, ward_id=None, room_id=None, room_description=None, warn_log=False):
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
                          'room_id': 'NULL' if room_id is None else str(room_id),
                          'room_description': 'NULL' if room_description is None else str(room_description),
                          'type': 'Room'
                          }
        self.S_GRAPH.add_node(str(string_id), **attribute_dict)
        self.nodes['Room'].add(str(string_id))

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
        self.nodes['Device'].add(string_id)

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
        self.nodes['Employee'].add(string_id)

    def new_edge(self, source_id, source_type, target_id, target_type, att_dict, log_warning=False):
        """Adds a new edge to the network.

        The added edge will link source_id of source_type to target_id of target_type. Note that the edge will ONLY be
        added if both source_id and target_id are found in the self.nodes attribute dictionary. In addition, all
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

    def trim_model(self, snapshot_dt_from=None, snapshot_dt_to=None):
        """Trims the current model by removing edges.

        Removes all edges for which the ``to`` attribute is larger than snapshot_dt, and updates the self.snapshot_dt
        attribute. However, this function does NOT remove isolated nodes.

        Args:
            snapshot_dt_from (dt.dt()): dt.dt() object specifying from which timepoint the model should be trimmed
            snapshot_dt_to (dt.dt()): dt.dt() object specifying to which timepoint the model should be trimmed
        """
        if snapshot_dt_from is None or snapshot_dt_to is None:
            raise Exception("Incorrect snapshot dt range provided:", (snapshot_dt_from, snapshot_dt_to))

        if not (self.from_range <= snapshot_dt_from and snapshot_dt_to < self.to_range):
            raise Exception("Snapshot from and to range exceeds range of graph:", (self.from_range, self.to_range))

        deleted_edges = [edge_tuple for edge_tuple in self.S_GRAPH.edges(data=True, keys=True)
                         if edge_tuple[3]['to'] > snapshot_dt_to]

        if snapshot_dt_from is not None:
            deleted_edges += [edge_tuple for edge_tuple in self.S_GRAPH.edges(data=True, keys=True)
                              if edge_tuple[3]['from'] < snapshot_dt_from]
        # S_GRAPH.edges() returns a list of tuples of length 4 --> ('source_id', 'target_id', key, attr_dict)
        self.S_GRAPH.remove_edges_from(deleted_edges)
        self.from_range = snapshot_dt_from
        self.to_range = snapshot_dt_to

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
        # as --> { ('bla', 'doodle', 0) : {'newattr' : 'somevalue'} }
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

    def add_edge_infection(self, infection_distance=1, forward_in_time=False, colonialization_timedelta=datetime.timedelta(days=0), is_verbose=False):
        """Sets "infected" attribute to all edges.

        This function will iterate over all edges in the network and set an additional attribute ``infected``, which
        will be set to ``True`` if it connects to a patient node for which the ``vre_status`` attribute is set to
        ``pos``. For all other edges, this attribute will be set to ``False``.
        """
        logging.info(f"##################################################################################")
        logging.info('Propagate infection through interaction edges...')
        s_infection_date = "infection_date"
        s_infected = "infected"
        error_count = 0
        pos_devs, pos_emps, pos_rooms, pos_pats = set(), set(), set(), set()

        for distance in tqdm(range(infection_distance), desc="Infection distance", position=0):
            neg_edges_count = 0
            pos_edges_count = 0
            total_edges_count = 0
            nodes_to_infect = []
            for each_edge in tqdm(self.S_GRAPH.edges(data=True, keys=True), desc="Edge", position=1):
                node_0_negative = self.S_GRAPH.nodes[each_edge[0]].get('vre_status', 'neg') == 'neg' and not self.S_GRAPH.nodes[each_edge[0]].get(s_infected, False)
                node_1_negative = self.S_GRAPH.nodes[each_edge[1]].get('vre_status', 'neg') == 'neg' and not self.S_GRAPH.nodes[each_edge[1]].get(s_infected, False)
                # infect edges based on node state (neither nodes of edge is positive or infected, then the edge is not infected)
                if node_0_negative and node_1_negative:
                    self.update_edge_attributes(edge_tuple=(each_edge[0], each_edge[1], each_edge[2]), attribute_dict={s_infected: False})
                    neg_edges_count += 1
                else:
                    if forward_in_time:
                        # get interaction end datetime
                        edge_to = each_edge[3]["to"]

                        # get infection dates of nodes, datetime.max means never infected
                        node_0_infection_date = self.S_GRAPH.nodes[each_edge[0]].get(s_infection_date, datetime.date.max)
                        node_1_infection_date = self.S_GRAPH.nodes[each_edge[1]].get(s_infection_date, datetime.date.max)

                        # calculate the time of infection by assuming a colonialization prior to the official measurement
                        node_0_infection_date -= colonialization_timedelta
                        node_1_infection_date -= colonialization_timedelta

                        if is_verbose and node_0_infection_date < datetime.date.max:
                            print(f"{self.S_GRAPH.nodes[each_edge[0]]['type']} {each_edge[0]}, infected {node_0_infection_date}, interacted {edge_to} with {self.S_GRAPH.nodes[each_edge[1]]['type']} {each_edge[1]}")

                        if is_verbose and node_1_infection_date < datetime.date.max:
                            print(f"{self.S_GRAPH.nodes[each_edge[1]]['type']} {each_edge[1]}, infected {node_1_infection_date}, interacted {edge_to} with {self.S_GRAPH.nodes[each_edge[0]]['type']} {each_edge[0]}")

                        # if node 0 was infected before the interaction ended, infect node 1 and vice versa
                        if node_0_infection_date < edge_to:
                            nodes_to_infect.append(each_edge[1])
                            if node_1_infection_date > edge_to:  # update infection date if it is earlier than the prior infection date
                                self.S_GRAPH.nodes[each_edge[1]][s_infection_date] = edge_to.date()
                                # TODO: In principle, infection date could be set to edge_from here, but this could cause larger jumps back in time depending on length

                        if node_1_infection_date < edge_to:
                            nodes_to_infect.append(each_edge[0])
                            if node_0_infection_date > edge_to:  # update infection date if it is earlier than the prior infection date
                                self.S_GRAPH.nodes[each_edge[0]][s_infection_date] = edge_to.date()
                    else:
                        nodes_to_infect.append(each_edge[0])
                        nodes_to_infect.append(each_edge[1])

                    self.update_edge_attributes(edge_tuple=(each_edge[0], each_edge[1], each_edge[2]), attribute_dict={s_infected: True})

                    pos_edges_count += 1
                total_edges_count += 1

            # update infected nodes (in the loop, this has runaway effects, transferring infection further that infection_distance)
            for node_id in nodes_to_infect:
                if is_verbose:
                    print(f"{node_id} infected at distance {distance}")

                self.S_GRAPH.nodes[node_id][s_infected] = True

                # update statistics
                if self.S_GRAPH.nodes[node_id]["type"] == "Device":
                    pos_devs.add(node_id)
                elif self.S_GRAPH.nodes[node_id]["type"] == "Employee":
                    pos_emps.add(node_id)
                elif self.S_GRAPH.nodes[node_id]["type"] == "Room":
                    pos_rooms.add(node_id)
                elif self.S_GRAPH.nodes[node_id]["type"] == "Patient":
                    pos_pats.add(node_id)

            logging.warning(f'Encountered {error_count} errors during the identification of infected patient nodes')
            logging.info(f"Infected network edges ({pos_edges_count} infected, "
                         f"{neg_edges_count} uninfected edges of total {total_edges_count} edges) "
                         f"infecting {len(pos_pats)} patients, {len(pos_rooms)} rooms, {len(pos_emps)} employees, {len(pos_devs)} devices (total {len(pos_pats) + len(pos_rooms) + len(pos_emps) + len(pos_devs)}).")
        self.edges_infected = True

    def update_shortest_path_statistics(self, focus_nodes=None, max_path_length=None):
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
        # Returns a list of tuples containing all unique pairs of nodes in target_nodes

        logging.info(f'--> Adding shortest path statistics considering {len(target_nodes)} nodes yielding {len(node_combinations)} combinations.')
        logging.info(f"Maximum path length set to {max_path_length}")
        for count, combo_tuple in enumerate(tqdm(node_combinations, total=len(node_combinations))):
            if nx.has_path(self.S_GRAPH, combo_tuple[0], combo_tuple[1]) is False:
                continue  # indicates a node pair in disconnected network parts
            # measure = datetime.datetime.now()
            shortest_pair_path = nx.shortest_path(self.S_GRAPH, source=combo_tuple[0], target=combo_tuple[1])
            if max_path_length is not None and len(shortest_pair_path) > max_path_length:
                continue  # indicates a path too long to be considered relevant for transmission
            # If shortest paths are "short enough", calculate the exact measure
            all_shortest_paths = list(nx.all_shortest_paths(self.S_GRAPH, source=combo_tuple[0],
                                                            target=combo_tuple[1]))
            # Remove the first and last node (i.e. source and target) of all shortest paths
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
        logging.info(f'--> Model Snapshot date: from {self.from_range.strftime("%d.%m.%Y %H:%M:%S")} to {self.to_range.strftime("%d.%m.%Y %H:%M:%S")}')
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
        # logging.info(f"--> Graph connected: {nx.is_connected(self.S_GRAPH)}")
        logging.info(f"###############################################################")

    def add_network_data(self, patient_dict, case_subset='relevant_case', patient_subset=None,
                         from_range=datetime.datetime.min, to_range=datetime.datetime.now()):
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

            from_range(dt.dt()):    datetime.datetime() object specifying from which point in time data are to be imported.
                                    Defaulting to a time far in the past, this parameter can be used to create a "snapshot"
                                    of the model starting at a certain point in time.
            to_range (dt.dt()):     datetime.datetime() object specifying to which point in time data are to be
                                    imported. Defaulting to the time of execution, this parameter can be used to create
                                    a "snapshot" of the model, and will *ignore* (i.e. not add) edges in patient_dict
                                    for which the 'to' attribute is larger than this parameter. Note that all nodes from
                                    patient_dict will be added, but most "new" nodes will be created in isolation. And
                                    since a call to this function is usually followed by a call to
                                    *remove_isolated_nodes()*, these isolated nodes will then be stripped from the
                                    network.
        """
        logging.info(f"Filter set to: {case_subset}")
        logging.info(f"Snapshot created from {from_range.strftime('%d.%m.%Y %H:%M:%S')} to {to_range.strftime('%d.%m.%Y %H:%M:%S')}")
        self.from_range = from_range
        self.to_range = to_range
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

                        self.new_room_node(stay.room_id, building_id=building_id, ward_id=ward_name,
                                           room_id=stay.room.get_ids() if stay.room is not None else None,
                                           room_description=stay.room.room_description if stay.room is not None else None)
                        # .get_ids() will return a '@'-delimited list of [room_id]_[system] entries, or None
                        nbr_room_id += 1
                    # Add Patient-Room edge if it is within scope of the current snapshot
                    edge_dict = {'from': stay.from_datetime, 'to': stay.to_datetime,
                                 'type': 'Patient-Room', 'origin': 'Stay'}
                    if edge_dict['from'] > self.from_range and edge_dict['to'] < self.to_range:
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

                        self.new_room_node(string_id=each_room.room_id, building_id=building_id, ward_id=each_room.ward_name ,
                                           room_id=each_room.get_ids(),
                                           room_description=each_room.room_description if each_room is not None else None)
                        room_list.append(each_room.room_id)
                    ####################################
                    # --> ADD EDGES based on specifications in self.edge_types
                    ####################################
                    # --> Device-Patient
                    if 'Device-Patient' in self.edge_types:
                        edge_attributes['type'] = 'Device-Patient'
                        for device in device_list:
                            if edge_attributes['from'] >= self.from_range and edge_attributes['to'] < self.to_range:
                                self.new_edge(this_pat_id, 'Patient', device, 'Device', att_dict=edge_attributes)
                                nbr_pat_device += 1
                    # --> Patient-Room
                    if 'Patient-Room' in self.edge_types:
                        edge_attributes['type'] = 'Patient-Room'
                        for room in room_list:
                            if edge_attributes['from'] >= self.from_range and edge_attributes['to'] < self.to_range:
                                self.new_edge(this_pat_id, 'Patient', room, 'Room', att_dict=edge_attributes)
                                nbr_pat_room += 1
                    # --> Employee-Patient
                    if 'Employee-Patient' in self.edge_types:
                        edge_attributes['type'] = 'Employee-Patient'
                        for emp in employee_list:
                            if edge_attributes['from'] >= self.from_range and edge_attributes['to'] < self.to_range:
                                self.new_edge(this_pat_id, 'Patient', emp, 'Employee', att_dict=edge_attributes)
                                nbr_pat_emp += 1
                    # --> Device-Employee
                    if 'Device-Employee' in self.edge_types:
                        edge_attributes['type'] = 'Device-Employee'
                        for emp in employee_list:
                            for device in device_list:
                                if edge_attributes['from'] >= self.from_range and edge_attributes['to'] < self.to_range:
                                    self.new_edge(emp, 'Employee', device, 'Device', att_dict=edge_attributes)
                                    nbr_device_emp += 1
                    # --> Employee-Room
                    if 'Employee-Room' in self.edge_types:
                        edge_attributes['type'] = 'Employee-Room'
                        for emp in employee_list:
                            for room in room_list:
                                if edge_attributes['from'] >= self.from_range and edge_attributes['to'] < self.to_range:
                                    self.new_edge(emp, 'Employee', room, 'Room', att_dict=edge_attributes)
                                    nbr_emp_room += 1
                    # --> Device-Room
                    if 'Device-Room' in self.edge_types:
                        edge_attributes['type'] = 'Device-Room'
                        for device in device_list:
                            for room in room_list:
                                if edge_attributes['from'] >= self.from_range and edge_attributes['to'] < self.to_range:
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

    def get_positive_patients(self):
        """
        Get positive patients currently in graph.
        :return:
        """
        all_nodes = self.S_GRAPH.nodes(data=True)  # list of tuples of ('source_id', key, {attr_dict } )

        # return positive patients in the network
        pos_pats = [node_data_tuple[0] for node_data_tuple in all_nodes if not pd.isna(node_data_tuple[0]) and node_data_tuple[1]['type'] == 'Patient' and
                    node_data_tuple[1]['vre_status'] == 'pos']

        return pos_pats

    def get_patients(self):

        all_nodes = self.S_GRAPH.nodes(data=True)  # list of tuples of ('source_id', key, {attr_dict } )

        # return patients in the network
        pats = [node_data_tuple[0] for node_data_tuple in all_nodes if not pd.isna(node_data_tuple[0]) and node_data_tuple[1]['type'] == 'Patient']

        return pats


    ################################################################################################################
    # Centrality Functions
    ################################################################################################################
    def calculate_infection_degree(self):
        """Calculates infection degree for all nodes in the network.

        The infection degree is defined for a single node_x as the number of infected edges between node_x and patients (connection of nth degree as set by

        The result will be a dataframe and contains the following columns:
        - Node ID
        - Node type
        - Degree ratio
        - Number of infected edges (always patient-related)
        - Total number of edges (i.e. degree of node_x)
        """

        if not self.edges_infected:
            logging.error('This operation requires infection data on edges!')
            return None

        logging.info('Calculating infection degrees...')
        infection_degree_rows = []

        for each_node in tqdm(self.S_GRAPH.nodes(data=True)):  # each_node will be a tuple of length 2 --> ( 'node_id', {'att_1' : 'att_value1', ... } )
            if pd.isna(each_node[0]):
                continue

            this_node_edges = self.S_GRAPH.edges(each_node[0], data=True, keys=True)  # will return a list of tuples
            # --> [ ( 'node_id', 'target_id1', key, {attr_dict} ), ('node_id', 'target_id2', key, {attr_dict} ),...]

            intected_edges = [edge for edge in this_node_edges if edge[3]['infected']]  # get all infected edges of this node
            # they get infectected by the spread of the disease with a certain distance, see @

            risk_status = each_node[1]["vre_status"] if "vre_status" in each_node[1] else 'neg'  # get status of node

            infection_degree_rows.append([each_node[0], each_node[1]['type'], risk_status, len(intected_edges) / len(this_node_edges), len(intected_edges), len(this_node_edges)])

        infection_degree_df = pd.DataFrame.from_records(infection_degree_rows)
        infection_degree_df.columns = ["Node ID", "Node Type", "Risk Status", "Degree Ratio", "Number of Infected Edges", "Total Edges"]
        infection_degree_df.sort_values(by="Number of Infected Edges", ascending=False, inplace=True)  # sort by how many contacts the patient had with infected entities

        logging.info(f"Successfully calculated infection degrees for {len(infection_degree_rows)} nodes.")

        return infection_degree_df

    def calculate_patient_degree_ratio(self):
        """Calculates and exports patient degree ratio for all nodes in the network.

        This will expose non-patient nodes that have high interaction with positive patients.

        The patient degree ratio is defined for a single node_x as:

        number of infected edges between node_x and patients / number of total edges between node_x and patients

        The result will be a dataframe and contains the following columns:
        - Node ID
        - Node type
        - Degree ratio
        - Number of infected edges (always patient-related)
        - Total number of patient-related edges
        - Total number of edges (i.e. degree of node_x)
        """
        if not self.edges_infected:
            logging.error('This operation requires infection data on edges !')
            return None

        logging.info('Calculating patient degree ratio...')
        patient_degree_rows = []

        for each_node in tqdm(self.S_GRAPH.nodes(data=True)):
            # each_node will be a tuple of length 2 --> ( 'node_id', {'att_1' : 'att_value1', ... } )

            if pd.isna(each_node[0]):
                continue

            this_node_edges = self.S_GRAPH.edges(each_node[0], data=True, keys=True)  # will return a list of tuples
            # --> [ ( 'node_id', 'target_id1', key, {attr_dict} ), ('node_id', 'target_id2', key, {attr_dict} ),...]

            pat_edges = [edge for edge in this_node_edges if 'Patient' in edge[3]['type']]  # indicates a type of "Patient-XXX" node

            infected_pat_edges = [edge for edge in pat_edges if edge[3]['infected']]

            risk_status = each_node[1]["vre_status"] if "vre_status" in each_node[1] else 'neg'
            patient_degree_rows.append([each_node[0], each_node[1]['type'], risk_status, len(infected_pat_edges) / len(pat_edges),
                                        len(infected_pat_edges), len(pat_edges), len(this_node_edges)])

        patient_degree_df = pd.DataFrame.from_records(patient_degree_rows)
        patient_degree_df.columns = ["Node ID", "Node Type", "Risk Status", "Degree Ratio", "Number of Infected Edges", "Total Patient Edges", "Total Edges"]
        patient_degree_df.sort_values(by="Degree Ratio", ascending=False, inplace=True)   # sort by the ratio of positive contacts of total contacts (TODO: Does that really influence the chance to be infected? Not really, right?)

        logging.info(f"Successfully calculated patient degree ratios for {len(patient_degree_rows)} nodes.")

        return patient_degree_df

    def calculate_total_degree_ratio(self):
        """Calculates total degree ratio (TDR) for all nodes in the network.

        Will calculate and export the total degree ratio for all nodes in the network, which is defined for a
        single *node_x* as:

        :math:`TDR = \\frac{Number\_of\_infected\_edges\_between\_node
        \\_x\_and\_patients}{Total\_number\_of\_edges\_leading\_to\_node\\_x}`

        The result will be a dataframe and contains the following columns:

        - Node ID
        - Node type
        - Degree ratio
        - Number of infected edges (always patient-related)
        - Total number of edges for node_x (also includes non-patient-related edges)
        """
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
                                              sum(infected_edges) / len(this_node_edges),
                                              len([entry for entry in infected_edges if entry]),
                                              len(this_node_edges)])
        patient_degree_ratio_df = pd.DataFrame.from_records(patient_degree_ratio_rows)
        patient_degree_ratio_df.columns = ["Node ID", "Node Type", "Total Degree Ratio", "Number of Infected Edges", "Total Edges"]
        patient_degree_ratio_df.sort_values(by="Total Degree Ratio", ascending=False, inplace=True)

        # Write to log
        logging.info(f"Successfully calculated total degree ratios for {len(patient_degree_ratio_rows)} nodes")

        return patient_degree_ratio_df

    def calculate_shortest_path_length_overview(self, focus_nodes=None):
        """Calculates  an overview of shortest path lengths in the network to self.data_dir.

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
        target_nodes = focus_nodes if focus_nodes is not None else self.S_GRAPH.nodes
        node_combinations = list(itertools.combinations(target_nodes, 2))
        logging.info(f'Exporting shortest path length overview for {len(node_combinations)} node combinations...')
        shortest_path_lengths_rows = []  # list holding all encountered shortest path lengths
        for count, combo_tuple in enumerate(node_combinations):
            if count % 1000 == 0:
                logging.info(f" <> Processed {count} combinations")
            if nx.has_path(self.S_GRAPH, combo_tuple[0], combo_tuple[1]) is False:
                continue  # indicates a node pair in disconnected parts of network
            shortest_path_lengths_rows.append([combo_tuple[0], combo_tuple[1], len(nx.shortest_path(self.S_GRAPH, source=combo_tuple[0], target=combo_tuple[1]))])

        node_distances_df = pd.DataFrame.from_records(shortest_path_lengths_rows)
        node_distances_df.columns = ['Node 1 ID', 'Node 2 ID', 'Path lengths']

        # write to log
        logging.info(f"Successfully calculated node distances for {len(shortest_path_lengths_rows)} nodes")

        return node_distances_df

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

        for each_node in tqdm(self.S_GRAPH.nodes(data=True)):
            # Returns tuples of length 2 --> (node_id, attribute_dict)
            risk_status = each_node[1]["vre_status"] if "vre_status" in each_node[1] else 'neg'  # get status of node
            target_keys = [each_key for each_key in each_node[1].keys() if each_key.startswith('SP-')]
            betweenness_score = sum([each_node[1][each_key][0] / each_node[1][each_key][1] for each_key in target_keys])
            write_string = [each_node[0], each_node[1]['type'], risk_status, betweenness_score]

            node_betweenness_rows.append(write_string)

        node_betweenness_df = pd.DataFrame.from_records(node_betweenness_rows)
        node_betweenness_df.columns = ['Node ID', 'Node Type', 'Risk Status', 'Centrality']
        node_betweenness_df.sort_values(by="Centrality", ascending=False, inplace=True)

        # Write to log
        logging.info(f"Successfully calculated betweenness centralities for {len(node_betweenness_rows)} nodes")

        return node_betweenness_df

    def calculate_subset_betweenness(self):
        """
        Calculate subset betweenness on the graph to find central nodes and use the initially known positive patients as personalization.
        :return:
        """
        all_nodes = self.S_GRAPH.nodes(data=True)  # list of tuples of ('source_id', key, {attr_dict } )

        # get positive patients in the network
        pos_pats = {node_data_tuple[0]: 1 if (not pd.isna(node_data_tuple[0]) and node_data_tuple[1]['type'] == 'Patient' and node_data_tuple[1]['vre_status'] == 'pos') else 0 for node_data_tuple in all_nodes}

        c = nx.betweenness_centrality_subset(self.S_GRAPH,
                                              sources=self.get_positive_patients(),
                                              targets=self.get_patients())
        betweenness_rows = []
        for each_node in tqdm(self.S_GRAPH.nodes(data=True)):
            # each_node will be a tuple of length 2 --> ( 'node_id', {'att_1' : 'att_value1', ... } )

            if pd.isna(each_node[0]):
                continue

            risk_status = each_node[1]["vre_status"] if "vre_status" in each_node[1] else 'neg'
            betweenness_rows.append([each_node[0], each_node[1]['type'], risk_status, c[each_node[0]]])

        betweenness_df = pd.DataFrame.from_records(betweenness_rows)
        betweenness_df.columns = ["Node ID", "Node Type", "Risk Status", "Centrality"]
        betweenness_df.sort_values(by="Centrality", ascending=False, inplace=True)

        logging.info(f"Successfully calculated subset betweenness centrality for {len(betweenness_rows)} nodes.")

        return betweenness_df

    def calculate_pagerank_centrality(self):
        """
        Calculate pagerank on the graph to find central nodes and use the initially known positive patients as personalization.
        :return:
        """
        all_nodes = self.S_GRAPH.nodes(data=True)  # list of tuples of ('source_id', key, {attr_dict } )

        # get positive patients in the network
        pos_pats = {node_data_tuple[0]: 1 if (not pd.isna(node_data_tuple[0]) and node_data_tuple[1]['type'] == 'Patient' and node_data_tuple[1]['vre_status'] == 'pos') else 0 for node_data_tuple in all_nodes}

        pr = nx.pagerank_numpy(self.S_GRAPH, personalization=pos_pats)
        pagerank_rows = []
        for each_node in tqdm(self.S_GRAPH.nodes(data=True)):
            # each_node will be a tuple of length 2 --> ( 'node_id', {'att_1' : 'att_value1', ... } )

            if pd.isna(each_node[0]):
                continue

            risk_status = each_node[1]["vre_status"] if "vre_status" in each_node[1] else 'neg'
            pagerank_rows.append([each_node[0], each_node[1]['type'], risk_status, pr[each_node[0]]])

        pagerank_df = pd.DataFrame.from_records(pagerank_rows)
        pagerank_df.columns = ["Node ID", "Node Type", "Risk Status", "Centrality"]
        pagerank_df.sort_values(by="Centrality", ascending=False, inplace=True)

        logging.info(f"Successfully calculated pagerank centrality for {len(pagerank_rows)} nodes.")

        return pagerank_df
