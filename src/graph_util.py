from typing import Optional, Dict, List, Set

import igraph


def add_typical_measures(graph: igraph.Graph, prefix: str):
    graph.vs[f"{prefix}page_rank"] = graph.pagerank()
    graph.vs[f"{prefix}hub"] = graph.hub_score()
    graph.vs[f"{prefix}authority"] = graph.authority_score()
    graph.vs[f"{prefix}hub_authority"] = [max(a, b) for a, b in zip(
        graph.vs[f"{prefix}hub"],
        graph.vs[f"{prefix}authority"],
    )]
    graph.vs[f"{prefix}dollar_hub"] = graph.hub_score(weights="shares_dollar")
    graph.vs[f"{prefix}dollar_authority"] = graph.authority_score(weights="shares_dollar")
    graph.vs[f"{prefix}dollar_hub_authority"] = [max(a, b) for a, b in zip(
        graph.vs[f"{prefix}dollar_hub"],
        graph.vs[f"{prefix}dollar_authority"],
    )]
    graph.vs[f"{prefix}degree"] = graph.degree()
    graph.vs[f"{prefix}degree_in"] = graph.indegree()
    graph.vs[f"{prefix}degree_out"] = graph.outdegree()

    graph.vs[f"{prefix}weighted_degree"] = graph.strength(mode="all", weights="weight")
    graph.vs[f"{prefix}weighted_degree_in"] = graph.strength(mode="in", weights="weight")
    graph.vs[f"{prefix}weighted_degree_out"] = graph.strength(mode="out", weights="weight")

    graph.vs[f"{prefix}dollar_degree"] = graph.strength(mode="all", weights="shares_dollar")
    graph.vs[f"{prefix}dollar_degree_in"] = graph.strength(mode="in", weights="shares_dollar")
    graph.vs[f"{prefix}dollar_degree_out"] = graph.strength(mode="out", weights="shares_dollar")


def filter_graph(
        graph: igraph.Graph,
        edge_filters: Optional[dict] = None,
        vertex_filters: Optional[dict] = None,
) -> None:

    def _ids_to_delete(seq, attributes: dict) -> Set[int]:
        ids_to_delete = set()
        for key, value in attributes.items():
            key_split = key.split("__")
            if len(key_split) == 1:
                name = key_split[0]
                operator = "equal"
            elif len(key_split) == 2:
                name, operator = key_split
            else:
                raise ValueError(f"Invalid filter '{key}'")

            if name == "degree":
                seq = seq.degree()
            else:
                try:
                    seq = seq[name]
                except KeyError:
                    raise ValueError(f"Unknown attribute '{name}'")

            func = getattr(_FilterFuncs, operator, None)
            if not func or not callable(func):
                raise ValueError(f"Invalid operator in '{key}'")
            
            ids_to_delete |= func(seq, value)

        return ids_to_delete

    if edge_filters:
        ids = _ids_to_delete(graph.es, edge_filters)
        if ids:
            graph.delete_edges(sorted(ids))

    if vertex_filters:
        ids = _ids_to_delete(graph.vs, vertex_filters)
        if ids:
            graph.delete_vertices(sorted(ids))


class _FilterFuncs:
    """
    Some operators to filter the IDs (indices) of
    `igraph.VertexSeq` or `igraph.EdgeSeq` and
    return the IDs to delete.
    """
    @staticmethod
    def equal(seq, value) -> Set[int]:
        return {
            i for i, v in enumerate(seq)
            if not v == value
        }

    @staticmethod
    def lt(seq, value) -> Set[int]:
        return {
            i for i, v in enumerate(seq)
            if not v < value
        }

    @staticmethod
    def lte(seq, value) -> Set[int]:
        return {
            i for i, v in enumerate(seq)
            if not v <= value
        }

    @staticmethod
    def gt(seq, value) -> Set[int]:
        return {
            i for i, v in enumerate(seq)
            if not v > value
        }

    @staticmethod
    def gte(seq, value) -> Set[int]:
        return {
            i for i, v in enumerate(seq)
            if not v >= value
        }
