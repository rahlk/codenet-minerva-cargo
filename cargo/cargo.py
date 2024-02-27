################################################################################
# Copyright IBM Corporate 2023
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

import numpy as np

from collections import defaultdict

from .utils import TransformGraph

import itertools


class Cargo:
    """Context Sensitive Label Propagation for partitioning a monolith application into microservices."""

    def __init__(
        self,
        json_sdg_path: str,
    ):
        self.graph = self.json2nx(json_sdg_path)

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
        if init_labels == "random_methods":
            for i, node in enumerate(self.SDG.nodes):
                if max_part < 2:
                    # We would need at least 2 partitions. Anything less than that will be treated aribitrarily.
                    partition_value = i
                else:
                    # Generate a random value between 0 and K for the partition
                    partition_value = random.randint(1, max_part)

                # Assign the partition value to the node
                self.SDG.nodes[node]["partition"] = partition_value

        if init_labels == "random_classes":
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

        elif init_labels == "file":
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
                    new_label = mode(labels_list)
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

        partition_sizes = np.unique(list(assignments.values()), return_counts=True)[1]

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
