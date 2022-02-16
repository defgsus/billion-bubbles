import json
import argparse
import datetime
from pathlib import Path
from typing import Optional, List, Dict, Callable, Set

from tqdm import tqdm

import igraph

from src.graph_util import filter_graph, add_typical_measures


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
    parser.add_argument(
        "-i", "--include", type=str, nargs="*", default=[],
        help=f"Vertex IDs to include. Connected nodes will be kept as well."
             " If one contains a dot (.) it's treated as new-line separated text file",
    )
    parser.add_argument(
        "-x", "--exclude", type=str, nargs="*", default=[],
        help=f"Vertex IDs to exclude."
             " If one contains a dot (.) it's treated as new-line separated text file",
    )
    return vars(parser.parse_args())


def _get_id_set(ids: List[str]) -> Set[str]:
    all_ids = set()
    for id in ids:
        if "." not in id:
            all_ids.add(id)
        else:
            for file_id in Path(id).read_text().splitlines():
                if file_id.strip():
                    all_ids.add(file_id.strip())
    return all_ids


def main(
        input: str,
        output: Optional[str],
        vertex_filter: List[str],
        edge_filter: List[str],
        include: List[str],
        exclude: List[str],
):
    graph: igraph.Graph = igraph.read(input)

    num_nodes, num_edges = len(graph.vs), len(graph.es)
    print(f"graph size: {num_nodes:,} x {num_edges:,}")

    add_typical_measures(graph, "")
    calc_final_measures = False

    if include:
        include_ids = _get_id_set(include)
        id_map = {
            i: id
            for i, id in enumerate(graph.vs["name"])
        }
        connect_map = {}
        for from_, to_ in tqdm(graph.get_edgelist(), "build connection map"):
            from_, to_ = id_map[from_], id_map[to_]
            connect_map.setdefault(from_, []).append(to_)
            connect_map.setdefault(to_, []).append(from_)

        ids_to_delete = []
        for i, id in tqdm(enumerate(graph.vs["name"]), desc="filtering"):
            include_id = id in include_ids
            if not include_id:
                for con_id in connect_map.get(id, []):
                    if con_id in include_ids:
                        include_id = True
                        break
            if not include_id:
                ids_to_delete.append(id)

        if ids_to_delete:
            graph.delete_vertices(ids_to_delete)
            calc_final_measures = True

    if exclude:
        exclude_ids = _get_id_set(exclude)

        ids_to_delete = [
            i for i, id in enumerate(graph.vs["name"])
            if str(id) in exclude_ids
        ]
        if ids_to_delete:
            graph.delete_vertices(ids_to_delete)
            calc_final_measures = True

    if vertex_filter or edge_filter:
        apply_filters(graph, vertex_filter, edge_filter)
        calc_final_measures = True

    if calc_final_measures:
        f_num_nodes, f_num_edges = len(graph.vs), len(graph.es)
        print(f"filtered graph size: {f_num_nodes:,} x {f_num_edges:,}")
        add_typical_measures(graph, "final_")

    dump_graph(graph)
    print(f"graph size: {len(graph.vs):,} x {len(graph.es):,}")

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


def dump_graph(graph: igraph.Graph):
    report = dict()
    report_str = dict()

    def _attributes(attrs, name: str):
        if len(attrs):
            for key in attrs.attributes():
                if isinstance(attrs[key][0], (int, float)):
                    min_ = min(*attrs[key])
                    max_ = max(*attrs[key])
                    mean = round(sum(attrs[key]) / len(attrs), 4)
                    report[f"{name}.{key}"] = (min_, max_, mean)

                elif isinstance(attrs[key][0], str):
                    counter = dict()
                    for a in attrs[key]:
                        counter[a] = counter.get(a, 0) + 1
                    report_str[f"{name}.{key}"] = ", ".join(
                        f"{a}: {counter[a]}"
                        for a in sorted(counter, key=lambda key: -counter[key])[:5]
                    )

    _attributes(graph.vs, "vertex")
    _attributes(graph.es, "edge")

    for key, (min_, max_, mean)  in report.items():
        print(f"{key:40}: {min_:24,.6f} {max_:24,.6f} {mean:24,.6f}")
    for key, value in report_str.items():
        print(f"{key:40}: {value}")


if __name__ == "__main__":
    main(**parse_args())
