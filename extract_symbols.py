import json
import argparse
import datetime
import gzip
import io
import codecs
import sys
from pathlib import Path
from typing import Optional, List, Set, Callable

from tqdm import tqdm
import pandas as pd

from src.nasdaq_db import *
from src.util import iter_ndjson, write_ndjson, to_int, to_float, JsonEncoder
from src.config import DEFAULT_DB_NAME


def parse_args() -> dict:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-db", "--databases", type=str, nargs="+", default=[str(DEFAULT_DB_NAME)],
        help=f"Name of database files, defaults to {DEFAULT_DB_NAME}",
    )

    parser.add_argument(
        "-o", "--output", type=str, default="symbols.txt",
        help=f"Output filename (a newline-separated text file)",
    )

    return vars(parser.parse_args())


class Main:

    def __init__(
            self,
            databases: List[str],
            output: str,
            verbose: bool = True,
    ):
        self.databases = sorted(databases)
        self.output_filename = output
        self.verbose = verbose
        self.symbols = set()

    def _log(self, *args):
        if self.verbose:
            print(*args, file=sys.stderr, flush=True)

    def run(self):
        for database_name in self.databases:
            self._log(f"\n----- Database: {database_name} ------" )

            db = NasdaqDatabase(
                database_filename=database_name,
                verbose=self.verbose,
            )

            self._extract_symbols(db)

        self._log("num symbols:", len(self.symbols))

        with open(self.output_filename, "wt") as fp:
            for s in sorted(self.symbols):
                print(s, file=fp)

    def _extract_symbols(self, db: NasdaqDatabase):
        for obj in db.iter_objects(
            company_profile=True,
            company_holders=True,
            company_insiders=False,
            stock_chart=False,
            institution_positions=False,
            insider_positions=False,
        ):
            if obj["data"]:
                self.symbols.add(obj["data"]["symbol"])


def dump(data: dict):
    print(json.dumps(data, indent=2, ensure_ascii=False, cls=JsonEncoder))


if __name__ == "__main__":
    Main(**parse_args()).run()
