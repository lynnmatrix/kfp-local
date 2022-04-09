from collections import deque
from typing import List, Dict


class Dag:
    """DAG stands for Direct Acyclic Graph.

    DAG here is used to decide the order to execute pipeline ops.

    For more information on DAG, please refer to
    `wiki <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_.
    """

    def __init__(self, nodes: List[str]) -> None:
        """

        Args::
          nodes: List of DAG nodes, each node is identified by an unique name.
        """
        self._graph: Dict[str, List[str]] = {node: [] for node in nodes}
        self._reverse_graph: Dict[str, List[str]] = {node: [] for node in nodes}

    @property
    def graph(self) -> Dict[str, List[str]]:
        return self._graph

    @property
    def reverse_graph(self) -> Dict[str, List[str]]:
        return self._reverse_graph

    def add_edge(self, edge_source: str, edge_target: str) -> None:
        """Add an edge between DAG nodes.

        Args::
          edge_source: the source node of the edge
          edge_target: the target node of the edge
        """
        self._graph[edge_source].append(edge_target)
        self._reverse_graph[edge_target].append(edge_source)

    def get_follows(self, source_node: str) -> List[str]:
        """Get all target nodes start from the specified source node.

        Args::
          source_node: the source node
        """
        return self._graph.get(source_node, [])

    def get_dependencies(self, target_node: str) -> List[str]:
        """Get all source nodes end with the specified target node.

        Args::
          target_node: the target node
        """
        return self._reverse_graph.get(target_node, [])

    def topological_sort(self) -> List[str]:
        """List DAG nodes in topological order."""

        in_degree = {node: 0 for node in self._graph.keys()}

        for i in self._graph:
            for j in self._graph[i]:
                in_degree[j] += 1

        queue: deque = deque()
        for node, degree in in_degree.items():
            if degree == 0:
                queue.append(node)

        sorted_nodes = []

        while queue:
            u = queue.popleft()
            sorted_nodes.append(u)

            for node in self._graph[u]:
                in_degree[node] -= 1

                if in_degree[node] == 0:
                    queue.append(node)

        return sorted_nodes
