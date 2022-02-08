import json
import argparse
import datetime
from pathlib import Path
from typing import Optional, List, Dict, Callable

import igraph


def parse_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input", type=str,
        help=f"graph input filename",
    )
    parser.add_argument(
        "output", type=str, nargs="?", default=None,
        help=f"graph output filename",
    )
    return vars(parser.parse_args())


def main(
        input: str,
        output: Optional[str],
):
    graph = igraph.read(input)
    add_typical_measures(graph, "full_")

    dump_graph(graph)


def add_typical_measures(graph: igraph.Graph, prefix: str):
    graph.vs[f"{prefix}page_rank"] = graph.pagerank()
    graph.vs[f"{prefix}hub"] = graph.hub_score()
    graph.vs[f"{prefix}authority"] = graph.authority_score()
    graph.vs[f"{prefix}dollar_hub"] = graph.hub_score(weights="shares_dollar")
    graph.vs[f"{prefix}dollar_authority"] = graph.authority_score(weights="shares_dollar")
    graph.vs[f"{prefix}degree"] = graph.degree()
    graph.vs[f"{prefix}degree_in"] = graph.indegree()
    graph.vs[f"{prefix}degree_out"] = graph.outdegree()

    graph.vs[f"{prefix}weighted_degree"] = graph.strength(mode="all", weights="weight")
    graph.vs[f"{prefix}weighted_degree_in"] = graph.strength(mode="in", weights="weight")
    graph.vs[f"{prefix}weighted_degree_out"] = graph.strength(mode="out", weights="weight")

    graph.vs[f"{prefix}dollar_degree"] = graph.strength(mode="all", weights="shares_dollar")
    graph.vs[f"{prefix}dollar_degree_in"] = graph.strength(mode="in", weights="shares_dollar")
    graph.vs[f"{prefix}dollar_degree_out"] = graph.strength(mode="out", weights="shares_dollar")


def dump_graph(graph: igraph.Graph):
    report = dict()

    def _attributes(attrs, name: str):
        if len(attrs):
            for key in attrs.attributes():
                if isinstance(attrs[key][0], (int, float)):
                    min_ = min(*attrs[key])
                    max_ = max(*attrs[key])
                    mean = round(sum(attrs[key]) / len(attrs), 4)
                    report[f"{name}.{key}"] = (min_, max_, mean)

    _attributes(graph.vs, "vertex")
    _attributes(graph.es, "edge")

    for key, (min_, max_, mean)  in report.items():
        print(f"{key:40}: {min_:24,.6f} {max_:24,.6f} {mean:24,.6f}")



def filter_graph(
        graph: igraph.Graph,
        edge_weight_gte: float = 0.,
        edge_attribute: Optional[Dict[str, Callable]] = None,
        degree_gte: int = 0,
) -> None:
    num_nodes, num_edges = len(graph.vs), len(graph.es)
    if edge_weight_gte:
        graph.delete_edges([
            i for i, w in enumerate(graph.es["weight"])
            if w < edge_weight_gte
        ])

    if edge_attribute:
        edges_to_delete = set()
        for key, func in edge_attribute.items():
            for i, value in enumerate(graph.es[key]):
                if not func(value):
                    edges_to_delete.add(i)
        if edges_to_delete:
            graph.delete_edges(sorted(edges_to_delete))

    if degree_gte:
        graph.delete_vertices([
            i for i, (d_in, d_out) in enumerate(zip(graph.indegree(), graph.outdegree()))
            if d_in + d_out < degree_gte
        ])

    f_num_nodes, f_num_edges = len(graph.vs), len(graph.es)
    print(f"filtered {num_nodes}x{num_edges} -> {f_num_nodes}x{f_num_edges}")


if __name__ == "__main__":
    main(**parse_args())
