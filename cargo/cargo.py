################################################################################
# Copyright IBM Corporate 2023, 2024
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from pathlib import Path
from statistics import mode
from typing import Optional, Union
import networkx as nx
import random
import json
from collections import defaultdict
from .utils import TransformGraph
import itertools

class Cargo:
    """Context Sensitive Label Propagation for partitioning a monolith application into microservices."""

    def __init__(
        self,
        json_sdg_path: str,
    ):
        random.seed(42)  # Seed random number generator for reproducibility
        self.json2nx(json_sdg_path)

    def json2nx(self, path_to_sdg_json: str) -> nx.MultiDiGraph:
        """Consume a JSON SDG to build a networkx graph out of it.

        Args:
            path_to_sdg_json (str): Path to the SDG

        Returns:
            nx.MultiDiGraph: A networkx graph
        """
        with open(path_to_sdg_json, "r") as sdg_json:
            self.json_graph = json.load(sdg_json)

        self.json_graph["links"] = self.json_graph.pop("edges")
        self.nodes_dict = defaultdict()
        self.SDG = nx.MultiDiGraph()

        # Populate the nodes
        for key, group in itertools.groupby(self.json_graph["nodes"], key=lambda x: x["id"]):
            node_attr = group.__next__()
            self.nodes_dict[key] = node_attr
            node_id = node_attr["id"]
            self.SDG.add_node(node_id, partition=None, **node_attr)

        # Populate the edges
        for edge in self.json_graph["links"]:
            node1 = edge["source"]
            node2 = edge["target"]

            if node1 == node2:
                continue

            self.SDG.add_edge(node1, node2, weight=edge["weight"], type=edge["type"])

    def _assign_init_labels(self, init_labels, max_part, labels_file):
        """Assign initial labels to the nodes in the graph.

        Parameters
        ----------
        init_labels : str
            The initial seeding strategy for the partitioning. It can be a path to a JSON file or one of the following: random_methods, random_classes, package_names (default).
        max_part : int
            The maximum number of partitions.
        labels_file : Union[str, Path, None]
            Path to the file containing the initial partitioning. This is only used if init_labels is set to "file".
        """
        if init_labels == "random_methods":
            self._assign_init_labels_via_random_methods(max_part)
        elif init_labels == "random_classes":
            self._assign_init_labels_via_random_classes(max_part)
        elif init_labels == "file":
            self._assign_init_labels_via_file(max_part, labels_file)
        else:
            # "package_names" is the default
            self._assign_init_labels_via_package_names(max_part)

    def _assign_init_labels_via_file(self, max_part, labels_file):
        # TODO: This assumes that the JSON has method partitions.
        if labels_file is None:
            raise Exception("File name must be provided if init_labels='file'")

        with open(labels_file, "r") as f:
            class_partition_map = json.load(f)

        for i, (node, data) in enumerate(self.SDG.nodes(data=True)):
            class_attr = data.get("class")
            if class_attr not in class_partition_map:
                if max_part < 2:
                    partition_value = i
                else:
                    partition_value = random.randint(1, max_part)

                self.SDG.nodes[node]["partition"] = partition_value
            else:
                self.SDG.nodes[node]["partition"] = class_partition_map[class_attr]

    def _assign_init_labels_via_random_classes(self, max_part):
        # Step 1: Collect unique classes and assign them a random partition ID
        class_partition_map = {}
        for i, (node, data) in enumerate(self.SDG.nodes(data=True)):
            class_attr = data.get("class")
            if max_part < 2:
                # We would need at least 2 partitions. Anything less than that will be treated aribitrarily.
                partition_value = i
            else:
                # Generate a random value between 0 and K for the partition
                partition_value = random.randint(1, max_part)
            if class_attr not in class_partition_map:
                class_partition_map[class_attr] = partition_value

        # Step 2: Update each node with the partition ID corresponding to its class
        for node, data in self.SDG.nodes(data=True):
            class_attr = data.get("class")
            self.SDG.nodes[node]["partition"] = class_partition_map[class_attr]

    def _assign_init_labels_via_random_methods(self, max_part):
        for i, node in enumerate(self.SDG.nodes):
            if max_part < 2:
                # We would need at least 2 partitions. Anything less than that will be treated aribitrarily.
                partition_value = i
            else:
                # Generate a random value between 0 and K for the partition
                partition_value = random.randint(1, max_part)

            # Assign the partition value to the node
            self.SDG.nodes[node]["partition"] = partition_value

    def _assign_init_labels_via_package_names(self, max_part):
        try: 
            # Here it is using package name to set the initial partition distribution
            packages_depth = {}
            level_depth_frequency = {}

            for node in self.SDG.nodes:
                package_name = ".".join(node.split(".")[:-2])

                if package_name not in packages_depth:
                    level_depth = len(package_name.split("."))
                    packages_depth[package_name] = level_depth
                    level_depth_frequency[level_depth] = 1 if level_depth not in level_depth_frequency else level_depth_frequency[level_depth]+1

            if len(packages_depth) < max_part:
                # when the number of packages are lesser than max_part, it is better to assign the labels randomly
                self._assign_init_labels_via_random_methods(max_part)
            else:
                initial_level_to_labels = -1
                level_depth_frequency = dict(sorted(level_depth_frequency.items(), key=lambda item: item[0])) # sorted by key value
                for level in level_depth_frequency:
                    if level_depth_frequency[level] >= max_part-1:
                        initial_level_to_labels = level
                        break

                if initial_level_to_labels == -1:
                    # when none of the package levels have at least the max_part number of packages, it is better to assign the labels randomly
                    self._assign_init_labels_via_random_methods(max_part)
                else:
                    packages = {}
                    counter = 0
                    packages_depth = dict(sorted(packages_depth.items(), key=lambda item: item[1])) # sorted by value
                    for pack in packages_depth:
                        if packages_depth[pack] < initial_level_to_labels:
                            packages[pack] = max_part-1 # default label for the upper level packages in the package hierarchy
                        elif packages_depth[pack] == initial_level_to_labels:
                            # assign partition label using round robin algorithm
                            packages[pack] = counter % (max_part-1) 
                            counter += 1
                            if counter >= (max_part-1): 
                                counter = 0
                        else:
                            # this case need to find what is the package in the hierarchy that has this package prefix
                            prefix = ".".join(pack.split(".")[:-1])
                            if prefix in packages:
                                packages[pack] = packages[prefix]
                            else:
                                pack_init_root_level_prefix = ".".join(pack.split('.')[:initial_level_to_labels])
                                if pack_init_root_level_prefix in packages:
                                    packages[pack] = packages[pack_init_root_level_prefix]
                                else:
                                    # assign partition label using round robin algorithm
                                    packages[pack] = counter % (max_part-1)
                                    packages[pack_init_root_level_prefix] = packages[pack]
                                    counter += 1
                                    if counter >= (max_part-1): 
                                        counter = 0

                    for node in self.SDG.nodes:
                        package_name = ".".join(node.split(".")[:-2])
                        if package_name in packages:
                            self.SDG.nodes[node]["partition"] = packages[package_name]
                        else:
                            self.SDG.nodes[node]["partition"] = max_part-1 # we do not expect it to happen, but it is just a safe approach
        except:
            # if any major issue happens, we go with the random methods assignment
            self._assign_init_labels_via_random_methods(max_part)

    def _propogate_labels(self):
        """This implements the weighted label propagation algorithm for CARGO."""
        label_changed = True
        while label_changed:
            changes = 0
            nodes = list(self.SDG.nodes())
            random.shuffle(nodes)

            for node in nodes:
                labels_list = []

                for neighbor in self.SDG.neighbors(node):
                    weight = self.SDG[node][neighbor].get(0)["weight"]
                    neighbor_label = self.SDG.nodes[neighbor]["partition"]
                    labels_list.extend([neighbor_label] * int(weight))

                if labels_list:
                    new_label = mode(labels_list)  # TODO: check if mode works on string labels.
                    if new_label != self.SDG.nodes[node]["partition"]:
                        self.SDG.nodes[node]["partition"] = new_label
                        changes += 1

            if changes == 0:
                label_changed = False

    def execute(
        self,
        init_labels,
        max_part: Optional[int] = None,
        labels_file: Union[str, Path, None] = None,
    ):
        """Execute the CARGO algorithm.

        Parameters
        ----------
        init_labels : str
            The initial seeding stratreagy for the partitioning. It can be a path to a JSON file or one of the following: random_methods, random_classes.
        max_part : int, optional
            The maximum number of partitions.
        labels_file : Union[str, Path, None], optional
            Path to the file containing the initial partitioning. This is only used if init_labels is set to "file".
        """

        self._assign_init_labels(init_labels, max_part, labels_file)
        self._propogate_labels()
        assignments = nx.get_node_attributes(self.SDG, "partition")

        # Compute the centrality of the nodes
        nx.set_node_attributes(self.SDG, nx.degree_centrality(self.SDG), "data_centrality")

        method_graph_view = self.json_graph

        for method_node in method_graph_view["nodes"]:
            # TODO: for daytrader7 there is 208 assignments, however, the sdg contains 308 nodes
            method_node["partition"] = assignments[method_node["id"]]

            try:
                method_node["centrality"] = self.SDG.nodes[method_node["id"]]["data_centrality"]
            except Exception as e:
                method_node["centrality"] = 0

        class_graph_view = TransformGraph.from_method_graph_to_class_graph(method_sdg_as_dict=method_graph_view)

        return class_graph_view
