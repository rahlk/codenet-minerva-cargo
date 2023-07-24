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
import click
import json
from py2neo import Graph
from cargo import Cargo
from .utils.json_to_neo4j import to_neo4j

@click.command()
@click.option(
    "--max-partitions",
    "-k",
    default=-1,
    type=int,
    help="The maximum number of partitions",
)
@click.option(
    "--app-dependency-graph",
    "-i",
    type=click.Path(exists=True),
    help="Path to the input JSON file. This is a System Dependency Graph, "
    "you can use the tool from https://github.com/konveyor/dgi-code-analyzer "
    "to get the system dependency graph of an application.",
)
@click.option(
    "--transactions",
    "-t",
    type=click.Path(exists=True),
    default=None,
    help="Path to the discovered transactions JSON file",
)
@click.option(
    "--seed-partitions",
    "-s",
    type=click.Path(exists=True),
    default=None,
    help="Path to the initial seed partitions JSON file",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path.cwd(),
    help="Path to save the output JSON file",
)
@click.option(
    "--neo4j",
    type=bool,
    default=False,
    help="URI of the neo4j database to save the output in.",
)
@click.option(
    "--neo4j-uri",
    type=str,
    default="neo4j:password@localhost:7474",
    help="URI of the neo4j database to save the output in.",
)
def minerva_cargo(
    max_partitions: int, 
    app_dependency_graph: Path, 
    transactions: Path, 
    seed_partitions: Path, 
    output: Path, 
    neo4j: bool,
    neo4j_uri
    ):
    """
    CLI version of CARGO a un-/semi-supervised partition refinement technique that uses a system dependence
    graph built using context and flow-sensitive static analysis of a monolithic application.
    """

    if seed_partitions is None:
        seed_partitions = "auto"

    cargo = Cargo(json_sdg_path=app_dependency_graph, transactions_json=transactions)
    partitions = cargo.run(init_labels=seed_partitions, max_part=max_partitions)

    with open(output.joinpath("partitions.json"), "w") as partitions_file:
        json.dump(partitions, partitions_file, indent=4, sort_keys=False)
    
    if neo4j:
        auth_str, uri = neo4j_uri.split("@")
        auth_tuple = tuple(auth_str.split(":"))
        neo4j_graph = Graph("http://"+uri, auth=auth_tuple)
        neo4j_graph.delete_all()
        to_neo4j(partitions, graph=neo4j_graph)

def main():
    minerva_cargo()


if __name__ == "__main__":
    main()
