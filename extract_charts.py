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
        "-o", "--output", type=str, default="charts.df",
        help=f"Output filename (a pickled pandas DataFrame)",
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
        self.rows = {}

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

            self._extract_charts(db)

        df = pd.DataFrame(self.rows).T
        df.index.set_names("date", inplace=True)
        df.columns.set_names(("company", "value"), inplace=True)
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df.to_pickle(self.output_filename)
        print(df)

    def _extract_charts(self, db: NasdaqDatabase):
        for obj in db.iter_objects(
            company_profile=False,
            company_holders=False,
            company_insiders=False,
            stock_chart=True,
            institution_positions=False,
            insider_positions=False,
        ):
            if obj["data"]["data"]["data"]:
                data = obj["data"]["data"]["data"]
                symbol = data["symbol"]
                company = data["company"]
                name = f"{symbol} ({company})"

                for chart_row in data["chart"]:
                    date = chart_row["z"]["dateTime"]
                    date = "{2:04}-{0:02}-{1:02}".format(*map(int, date.split("/")))
                    self.rows.setdefault(date, {})
                    for key in ("value", "open", "close", "high", "low", "volume"):
                        if key == "volume":
                            val = to_int(chart_row["z"][key])
                        else:
                            val = to_float(chart_row["z"][key])
                        self.rows[date][(name, key)] = val

                #break


def dump(data: dict):
    print(json.dumps(data, indent=2, ensure_ascii=False, cls=JsonEncoder))


if __name__ == "__main__":
    Main(**parse_args()).run()
