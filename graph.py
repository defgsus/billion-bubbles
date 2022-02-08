import json
import argparse
import datetime
from pathlib import Path
from typing import Optional, List, Dict, Callable

import igraph

from src.graph_util import filter_graph


def parse_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input", type=str,
        help=f"graph input filename",
    )
    parser.add_argument(
        "-o", "--output", type=str, nargs="?", default=None,
        help=f"graph output filename",
    )
    parser.add_argument(
        "-ef", "--edge-filter", type=str, nargs="*", default=[],
        help=f"Filter for edges, e.g. 'weight__gt=.2'",
    )
    parser.add_argument(
        "-vf", "--vertex-filter", type=str, nargs="*", default=[],
        help=f"Filter for vertices, e.g. 'degree_in__gt=3'",
    )
    return vars(parser.parse_args())


def main(
        input: str,
        output: Optional[str],
        vertex_filter: List[str],
        edge_filter: List[str],
):
    graph: igraph.Graph = igraph.read(input)

    num_nodes, num_edges = len(graph.vs), len(graph.es)
    print(f"graph size: {num_nodes:,} x {num_edges:,}")

    add_typical_measures(graph, "full_")

    if vertex_filter or edge_filter:
        apply_filters(graph, vertex_filter, edge_filter)
        f_num_nodes, f_num_edges = len(graph.vs), len(graph.es)
        print(f"filtered graph size: {f_num_nodes:,} x {f_num_edges:,}")

        add_typical_measures(graph, "final_")

    dump_graph(graph)

    if output:
        graph.write(output)


def apply_filters(graph, vertex_filter: List[str], edge_filter: [str]):
    def _parse_filter(filter_list):
        filter_dict = {}
        for filter_arg in filter_list:
            try:
                key, value = filter_arg.split("=")
                try:
                    value = float(value)
                except ValueError:
                    pass
                filter_dict[key] = value
            except:
                print(f"Invalid filter argument '{filter_arg}'")
                exit(1)
        return filter_dict

    vertex_filter = _parse_filter(vertex_filter)
    edge_filter = _parse_filter(edge_filter)
    filter_graph(
        graph,
        vertex_filters=vertex_filter,
        edge_filters=edge_filter,
    )


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



if __name__ == "__main__":
    main(**parse_args())
