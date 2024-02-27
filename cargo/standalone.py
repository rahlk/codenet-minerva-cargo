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
from typing_extensions import Annotated
from typer import Option, Typer
import json
from cargo import Cargo


app = Typer(
    help="MKDATA: A tool to process Method2Test data to generate instruction tuning dataset for test generation."
)


@app.command()
def minerva_cargo(
    app_dependency_graph: Annotated[
        Path,
        Option(
            "--app-dependency-graph",
            "-i",
            help="Path to the input JSON file. This is a System Dependency Graph.",
        ),
    ],
    output: Annotated[
        Path,
        Option(
            "--output",
            "-o",
            help="Path to save the output JSON file.",
        ),
    ],
    seed_partitions: Annotated[
        Path,
        Option(
            "--seed-partitions",
            "-s",
            help="Path to the initial seed partitions JSON file.",
        ),
    ] = None,
    transactions: Annotated[
        Path,
        Option(
            "--transactions",
            "-t",
            help="Path to the discovered transactions JSON file.",
        ),
    ] = None,
    max_partitions: Annotated[
        int,
        Option("--max-partitions", "-k", help="The maximum number of partitions."),
    ] = -1,
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


if __name__ == "__main__":
    app()
