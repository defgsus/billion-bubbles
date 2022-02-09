import json
import argparse
from pathlib import Path
from typing import Optional, List, Union

from src.nasdaq_db import NasdaqDatabase
from src.nasdaq_walker import NasdaqWalker
from src.nasdaq_graph import NasdaqGraphBuilder
from src.config import DEFAULT_DB_NAME


def parse_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--company", type=str, nargs="*", default=[],
        help="Company trading symbols to start with."
             " If one contains a dot (.) it's treated as new-line separated text file",
    )
    parser.add_argument(
        "-i", "--institution", type=str, nargs="*", default=[],
        help="Institution IDs to start with."
             " If one contains a dot (.) it's treated as new-line separated text file",
    )
    parser.add_argument(
        "-in", "--insider", type=str, nargs="*", default=[],
        help="Insider IDs to start with."
             " If one contains a dot (.) it's treated as new-line separated text file",
    )
    parser.add_argument(
        "-db", "--database", type=str, nargs="?", default=str(DEFAULT_DB_NAME),
        help=f"Name of database file, defaults to {DEFAULT_DB_NAME}",
    )
    parser.add_argument(
        "-d", "--depth", type=int, nargs="?", default=0,
        help=f"Maximum traversal depth",
    )
    parser.add_argument(
        "-dh", "--depth-holder", type=int, nargs="?", default=None,
        help=f"Maximum traversal depth for holder relationships. Overrides --depth",
    )
    parser.add_argument(
        "-di", "--depth-insider", type=int, nargs="?", default=None,
        help=f"Maximum traversal depth for insider relationships. Overrides --depth",
    )
    parser.add_argument(
        "-ms", "--min-share-value", type=int, nargs="?", default=100_000_000,
        help=f"Minimum holder/position share value in dollars to follow",
    )
    parser.add_argument(
        "-so", "--sort-order", type=str, nargs="?", default=None,
        help=f"Any string fixes the order of symbol/ID traversal to a specific 'random' sequence",
    )
    parser.add_argument(
        "-o", "--output", type=str, nargs="?", default=None,
        help=f"If provided, the resulting graph is written to this file",
    )
    parser.add_argument(
        "--all-db", type=bool, nargs="?", default=False, const="True",
        help=f"Build a graph from the whole database. Skips any tree traversal.",
    )
    parser.add_argument(
        "-v", "--verbose", type=bool, nargs="?", default=False, const=True,
        help=f"Log all web-requests and such",
    )
    return vars(parser.parse_args())


def walk(
        company: List[str],
        institution: List[str],
        insider: List[str],
        depth: int,
        depth_holder: Optional[int],
        depth_insider: Optional[int],
        min_share_value: int,
        sort_order: str,
        database: str,
        output: str,
        all_db: bool,
        verbose: bool,
):
    db = NasdaqDatabase(
        database_filename=database,
        verbose=verbose,
    )

    graph_builder = None
    if output:
        graph_builder = NasdaqGraphBuilder()

    if not all_db:
        walker = NasdaqWalker(
            db=db,
            max_depth_holder=depth if depth_holder is None else depth_holder,
            max_depth_insider=depth if depth_insider is None else depth_insider,
            share_market_value_gte=min_share_value,
            sort_order=sort_order,
            interface=graph_builder,
        )

        def _get_id_list(ids: List[str]) -> List[str]:
            all_ids = set()
            for id in ids:
                if "." not in id:
                    all_ids.add(id)
                else:
                    for file_id in Path(id).read_text().splitlines():
                        if file_id.strip():
                            all_ids.add(file_id.strip())
            return sorted(all_ids)

        for i in _get_id_list(company):
            walker.add_company(i)
        for i in _get_id_list(institution):
            walker.add_institution(i)
        for i in _get_id_list(insider):
            walker.add_insider(i)

        walker.run()
        print(walker.status_string())

    else:
        if not graph_builder:
            print("--all-db must be specified together with --output")
            exit(1)

        iterable = db.iter_objects(
            stock_chart=False,
        )
        graph_builder.from_objects(iterable)
        graph_builder.finalize()

    if graph_builder:
        graph = graph_builder.to_igraph()
        print(f"graph {len(graph.vs)}x{len(graph.es)}")
        graph.write(output)


if __name__ == "__main__":
    walk(**parse_args())
