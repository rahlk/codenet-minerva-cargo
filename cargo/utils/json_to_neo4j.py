from py2neo import Graph, Node, Relationship
import json
import argparse

# Parse the command line arguments
parser = argparse.ArgumentParser(description="Load a JSON file into Neo4j.")
parser.add_argument('input_json', type=str, help="The JSON file to load.")
args = parser.parse_args()

# Initialize the Neo4j database connection
graph = Graph("http://localhost:7474", auth=("neo4j", "password"))  # Replace with your actual username and password

def _purge_graph():
    graph.delete_all()

def to_neo4j(input_json: str):
    # Load your JSON data
    with open(input_json, 'r') as f:
        data = json.load(f)

    # Parse the JSON and add the nodes and edges
    for node in data['nodes']:
        if node['type'] == "SQLTable":
            continue
        # Add the parent node
        class_node = Node("ClassNode", name=node['name'], centrality=node['centrality'], type=node['type'], 
                          class_partition=node['class_partition'], uncertainity=node['uncertainity'])
        graph.create(class_node)

        # Add the child nodes
        for child in node['children']:
            child_node = Node("MethodNode", name=child['name'], centrality=child['centrality'], type=child['type'], 
                              partition=child['partition'], uncertainity=node['uncertainity'])
            graph.create(child_node)

            # Add relationship from parent node to child node
            parent_to_child = Relationship(class_node, 'MEMBER', child_node)
            graph.create(parent_to_child)

    # Parse the links and add the relationships
    for link in data['links']:
        # Get nodes by name
        source_node = graph.nodes.match(name=link['source']).first()
        target_node = graph.nodes.match(name=link['target']).first()

        # Create the relationship
        if source_node and target_node:
            rel = Relationship(source_node, link['type'], target_node, weight=link['weight'])
            graph.create(rel)

        # Add relationships between child nodes
        for child_link in link['children']:
            child_source_node = graph.nodes.match(name=child_link['source']).first()
            child_target_node = graph.nodes.match(name=child_link['target']).first()
            if child_source_node and child_target_node:
                child_rel = Relationship(child_source_node, link['type'], child_target_node)
                graph.create(child_rel)

if __name__ == "__main__":
    _purge_graph()
    to_neo4j(args.input_json)