import json
import argparse
import datetime
from pathlib import Path
from typing import Optional, List

from src.nasdaq_db import NasdaqDatabase
from src.nasdaq_walker import NasdaqWalker


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
        "-d", "--depth", type=int, nargs="?", default=1,
        help=f"Maximum traversal depth",
    )
    parser.add_argument(
        "-ms", "--min-share-value", type=int, nargs="?", default=100_000_000,
        help=f"Minimum holder/position share value in dollars to follow",
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
        min_share_value: int,
        database: str,
        verbose: bool,
):
    db = NasdaqDatabase(
        database_filename=database,
        verbose=verbose,
    )

    walker = NasdaqWalker(
        db=db,
        max_depth=depth,
        share_market_value_gte=min_share_value,
    )
    for i in company:
        walker.add_company(i)
    for i in institution:
        walker.add_institution(i)
    for i in insider:
        walker.add_insider(i)

    walker.run()
    print(walker.status_string())


if __name__ == "__main__":
    walk(**parse_args())
