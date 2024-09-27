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
from typing import Union
from typing_extensions import Annotated
from typer import Option, Typer
import json
from cargo import Cargo


app = Typer(help="")


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
        str,
        Option(
            "--seed-partitions",
            "-s",
            help="Initial seeding stratreagy for the partitioning. It can be a path to a JSON file or one of the following: random_methods, random_classes, package_names.",
        ),
    ] = "package_names",
    max_partitions: Annotated[
        int,
        Option("--max-partitions", "-k", help="The maximum number of partitions."),
    ] = 0,
):
    """
    CLI version of CARGO a un-/semi-supervised partition refinement technique that uses a system dependence
    graph built using context and flow-sensitive static analysis of a monolithic application.
    """

    if seed_partitions == "random_methods":
        seed_partitions = "random_methods"
    if seed_partitions == "random_classes":
        seed_partitions = "random_classes"
    elif Path(seed_partitions).exists():
        seed_partitions = json.load(open(seed_partitions))
    else:
        seed_partitions = "package_names"

    cargo = Cargo(json_sdg_path=app_dependency_graph)
    partitions = cargo.execute(init_labels=seed_partitions, max_part=max_partitions)

    with open(output.joinpath("partitions.json"), "w") as partitions_file:
        json.dump(partitions, partitions_file, indent=4, sort_keys=False)


if __name__ == "__main__":
    app()
