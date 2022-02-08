import json
import argparse
import datetime
from pathlib import Path
from typing import Optional, List

from src.nasdaq_db import NasdaqDatabase
from src.nasdaq_walker import NasdaqWalker
from src.nasdaq_graph import NasdaqGraphBuilder


PROJECT_DIR = Path(__file__).resolve().parent

DEFAULT_DB_NAME = PROJECT_DIR / datetime.date.today().strftime("nasdaq-%Y-%m.sqlite3")


def parse_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--company", type=str, nargs="*", default=[],
        help="Company trading symbols to start with",
    )
    parser.add_argument(
        "-i", "--institution", type=int, nargs="*", default=[],
        help="Institution IDs to start with",
    )
    parser.add_argument(
        "-in", "--insider", type=int, nargs="*", default=[],
        help="Insider IDs to start with",
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
        "-so", "--sort-order", type=str, nargs="?", default="",
        help=f"Any string (default "") changes the 'random' sort order of symbol/ID traversal",
    )
    parser.add_argument(
        "-o", "--output", type=str, nargs="?", default=None,
        help=f"If provided, the resulting graph is written to this file",
    )
    parser.add_argument(
        "-v", "--verbose", type=bool, nargs="?", default=False, const=True,
        help=f"Log all web-requests and such",
    )
    return vars(parser.parse_args())


def walk(
        company: List[str],
        institution: List[int],
        insider: List[int],
        depth: int,
        depth_holder: Optional[int],
        depth_insider: Optional[int],
        min_share_value: int,
        sort_order: str,
        database: str,
        output: str,
        verbose: bool,
):
    db = NasdaqDatabase(
        database_filename=database,
        verbose=verbose,
    )

    graph_builder = None
    if output:
        graph_builder = NasdaqGraphBuilder()

    walker = NasdaqWalker(
        db=db,
        max_depth_holder=depth if depth_holder is None else depth_holder,
        max_depth_insider=depth if depth_insider is None else depth_insider,
        share_market_value_gte=min_share_value,
        sort_order=sort_order,
        interface=graph_builder,
    )
    for i in company:
        walker.add_company(i)
    for i in institution:
        walker.add_institution(i)
    for i in insider:
        walker.add_insider(i)

    walker.run()
    print(walker.status_string())

    if graph_builder:
        graph = graph_builder.to_igraph()
        print(f"graph {len(graph.vs)}x{len(graph.es)}")
        graph.write(output)


if __name__ == "__main__":
    walk(**parse_args())
