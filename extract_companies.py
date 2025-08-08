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

    return vars(parser.parse_args())


class Main:

    def __init__(
            self,
            databases: List[str],
            verbose: bool = True,
    ):
        self.databases = sorted(databases)
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

            self._extract_company_holders(db)

        df = pd.DataFrame(self.rows).T
        df.to_pickle("extract-companies.df")
        print(df)

    def _extract_company_holders(self, db: NasdaqDatabase):
        db_date = None
        for obj in db.iter_objects(
            company_profile=False,
            company_holders=True,
            company_insiders=True,
            stock_chart=False,
            institution_positions=False,
            insider_positions=False,
        ):
            is_insider = obj["type"] == "company_insiders"

            symbol = obj["data"]["symbol"]
            if db_date is None:  # pick first date and use for whole DB
                db_date = str(obj["data"]["timestamp"])[:10]
            key = (db_date, symbol)

            data = obj["data"]["data"]["data"]
            if not data or not data.get("ownershipSummary"):
                continue
            if not is_insider:
                obj = {
                    "institutional_ownership%": to_float(data["ownershipSummary"]["SharesOutstandingPCT"]["value"][:-1]),
                    "total_shares_outstanding_mill": to_int(data["ownershipSummary"]["ShareoutstandingTotal"]["value"]),
                    "total_value_holdings_mill": to_int(data["ownershipSummary"]["TotalHoldingsValue"]["value"][1:]),
                }
                for row in data["activePositions"]["rows"]:
                    for what in ("holders", "shares"):
                        if row["positions"] == "Increased Positions":
                            obj[f"increased_positions_{what}"] = to_int(row[what])
                        elif row["positions"] == "Decreased Positions":
                            obj[f"decreased_positions_{what}"] = to_int(row[what])
                        elif row["positions"] == "Held Positions":
                            obj[f"held_positions_{what}"] = to_int(row[what])
                        elif row["positions"] == "Total Institutional Shares":
                            obj[f"total_positions_{what}"] = to_int(row[what])
                for row in data["newSoldOutPositions"]["rows"]:
                    for what in ("holders", "shares"):
                        if row["positions"] == "New Positions":
                            obj[f"new_positions_{what}"] = to_int(row[what])
                        elif row["positions"] == "Sold Out Positions":
                            obj[f"sold_out_positions_{what}"] = to_int(row[what])
            else:
                obj = {}
                for row in data["numberOfTrades"]["rows"]:
                    for span in ("months3", "months12"):
                        if row["insiderTrade"] == "Number of Open Market Buys":
                            obj[f"insider_open_market_buys_{span}"] = to_int(row[span])
                        elif row["insiderTrade"] == "Number of Sells":
                            obj[f"insider_sells_{span}"] = to_int(row[span])
                        elif row["insiderTrade"] == "Total Insider Trades":
                            obj[f"insider_total_trades_{span}"] = to_int(row[span])
                for row in data["numberOfSharesTraded"]["rows"]:
                    for span in ("months3", "months12"):
                        if row["insiderTrade"] == "Number of Shares Bought":
                            obj[f"insider_shares_bought_{span}"] = to_int(row[span])
                        elif row["insiderTrade"] == "Number of Shares Sold":
                            obj[f"insider_shares_sold_{span}"] = to_int(row[span])
                        elif row["insiderTrade"] == "Net Activity":
                            obj[f"insider_shares_net_activity_{span}"] = to_int(row[span].strip("()"))
            self.rows.setdefault(key, {}).update(obj)



def dump(data: dict):
    print(json.dumps(data, indent=2, ensure_ascii=False, cls=JsonEncoder))


if __name__ == "__main__":
    Main(**parse_args()).run()
