import calendar
import networkx as nx
import logging
import itertools
from collections import Counter
import os


def update_shortest_path_statistics(graph, focus_nodes=None):
    """ Prerequisite function for calculating betweenness centrality.

    Adds new attributes to all nodes in focus_nodes, where each attribute
    is a pair of nodes (sorted alphabetically) with a "SP-" prefix to tuples
    of length 2 containing
    (shortest_paths_through_this_node, total_shortest_paths)
    For example:
    { 'SP-Node1-Node2': (2, 5) , 'SP-Node1-Node3': ... } }

    Note that this may result in a lot of additional attributes for nodes
    which are integral to the network. This approach is chosen because of
    the fact that the networkx module does not allow updates to a
    dict-of-a-dict type of attributes (i.e. if these attributes were to
    be combined in a 1st-level key 'shortest-paths', the entire content
    would have to be copied every time a new node-pair attribute is added,
    which would make the function extremely inefficient.

    This is an important prerequisite function for the calculation of
    betweenness centrality.

    Args:
        focus_nodes (list): list of node IDs. If set to None (the default),
                            all nodes in the network will be considered.
                            WARNING: this may be ressource-intensive !
    """
    target_nodes = focus_nodes if focus_nodes is not None else graph.nodes
    node_combinations = list(itertools.combinations(target_nodes, 2))
    # Returns a list of tuples containing all unique pairs of nodes
    #   in considered_nodes

    logging.info(f'--> Adding shortest path statistics considering '
                 f'{len(target_nodes)} nodes yielding '
                 f'{len(node_combinations)} combinations')
    for count, combo_tuple in enumerate(node_combinations):
        if count % 100 == 0:
            logging.info(f" <> Processed {count} combinations")
        all_shortest_paths = list(nx.all_shortest_paths(graph,
                                                        source=combo_tuple[0],
                                                        target=combo_tuple[1]))  # Yields an iterator of lists of shortest paths for the given combination of nodes
        involved_nodes = [node for sublist in all_shortest_paths
                          for node in
                          sublist]  # Contains all nodes involved in all combinations of shortest paths for the given combination of nodes
        node_counts = Counter(involved_nodes)
        sorted_pair = sorted([combo_tuple[0], combo_tuple[1]])
        update_attr_dict = {
        each_key: {'SP-' + sorted_pair[0] + '-' + sorted_pair[1]:
                       (node_counts[each_key], len(all_shortest_paths))}
        for each_key in node_counts
        if each_key not in sorted_pair}
        # Then update all involved nodes in the network
        nx.set_node_attributes(graph, update_attr_dict)
    # Write it all to log
    logging.info(f"Successfully added betweenness statistics to the network !")


def export_node_betweenness(graph, data_dir, csv_sep=';'):
    """ Function to export node betweenness.

    This function will export node betweenness for all nodes from the
    network. This is done by exporting the sum of all fractions of shortest
    paths a particular node is in, which is found in the attribute keys
    starting with the "SP-" prefix (e.g. "SP-Node1-Node4"). These tuples
    contain 2 entries, the first being the fraction of shortest path this
    node is found in (between the given pair of nodes), and the second one
    being the total number of shortest paths. This function can only be
    executed once the function update_shortest_path_statistics() has been
    called. Nodes without an attribute key starting with the "SP-" prefix
    will have node betweenness of 0.

    The exported file is written into self.data_dir and contains 3 columns:
    - Node ID
    - Node Type
    - Betweenness Score

    Args:
        csv_sep (str): separator to be used in export file.
    """
    # if self.shortest_path_stats is False:
    #     logging.error(f"This function requires specific node attributes "
    #                   f"which can be added with the function "
    #                   f"update_shortest_path_statitics()")
    #     raise self.NodeBetweennessException('Missing a required call to'
    #                                         'update_shortest_path_statitics()')
    with open(os.path.join(data_dir,
                           '_node_bwn.txt'), 'w') as outfile:
        outfile.write(csv_sep.join(['Node ID', 'Node Type',
                                    'Betweenness_Score']) + '\n')
        write_counter = 0
        for node_tuple in graph.nodes(data=True):
            # Returns tuples of length 2 --> (node_id, attribute_dict)
            write_string = [node_tuple[0], 'hello']
            target_keys = [each_key for each_key in node_tuple[1].keys()
                           if each_key.startswith('SP-')]
            print(target_keys)
            betweenness_score = sum([node_tuple[1][each_key][0] / node_tuple[1][each_key][1]
                                     for each_key in target_keys])
            write_string.append(str(betweenness_score))
            # Then write to outfile
            outfile.write(csv_sep.join(write_string) + '\n')
            write_counter += 1
    # Log progress
    logging.info(f"Successfully wrote betweenness scores for "
                 f"{write_counter} nodes")
#######################################

gra = nx.MultiGraph()


gra.add_edge('B', 'A')
gra.add_edge('F', 'A')
gra.add_edge('E', 'A')
gra.add_edge('D', 'A')

gra.add_edge('F', 'C')
gra.add_edge('B', 'C')
gra.add_edge('D', 'B')
gra.add_edge('G', 'J')


print(nx.shortest_path(gra, 'B', 'F'))
print(nx.shortest_path(gra, 'B', 'F'))
print(nx.shortest_path(gra, 'B', 'F'))
print(nx.shortest_path(gra, 'B', 'F'))

update_shortest_path_statistics(gra, focus_nodes=['B', 'C', 'E'])

export_node_betweenness(gra, data_dir='.')

print([node for node in gra.nodes(data=True)])

sys.exit()

# gra.add_edge('P1', 'X')

# print(nx.shortest_path(gra, 'P1', 'P3'))
#
# print(nx.is_connected(gra))
#
# sys.exit()

gra.add_edge('E', 'P2')
gra.add_edge('P1', 'P2')

print(calculate_node_betweenness(gra))
print(calculate_node_betweenness(gra, considered_nodes=['P1', 'P3']))


print(calendar.monthrange(2018, 1))


print('hello') if True == True else ''
