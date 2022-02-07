import json
import argparse
import datetime
from pathlib import Path
from typing import Optional, List

from tqdm import tqdm

from src.nasdaq_db import NasdaqDatabase, NasdaqDBBase


PROJECT_DIR = Path(__file__).resolve().parent

DEFAULT_DB_NAME = PROJECT_DIR / datetime.date.today().strftime("nasdaq-%Y-%m.sqlite3")


def parse_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", type=str,
        choices=["show", "export"],
        help=f"Commands",
    )
    parser.add_argument(
        "-db", "--database", type=str, nargs="?", default=str(DEFAULT_DB_NAME),
        help=f"Name of database file, defaults to {DEFAULT_DB_NAME}",
    )
    parser.add_argument(
        "-o", "--output", type=str, nargs="?", default="export.ndjson",
        help=f"Filename for the nd-json export, defaults to 'export.ndjson'",
    )
    parser.add_argument(
        "-v", "--verbose", type=bool, nargs="?", default=False, const=True,
        help=f"Log all web-requests and such",
    )
    return vars(parser.parse_args())


def main(
        command: str,
        database: str,
        output: str,
        verbose: bool,
):
    db = NasdaqDatabase(
        database_filename=database,
        verbose=verbose,
    )

    if command == "show":
        print(dir(NasdaqDBBase.metadata))
        for name, table in NasdaqDBBase.metadata.tables.items():
            query = db.db_session.query(table)
            print(f"{name:25}: {query.count()}")

    elif command == "export":
        export(db, output)


def export(db: NasdaqDatabase, filename: str):
    num_objects = 0
    for name, table in NasdaqDBBase.metadata.tables.items():
        num_objects += db.db_session.query(table).count()

    with open(filename, "wt") as fp:
        for obj in tqdm(db.iter_objects(), total=num_objects, desc="exporting"):
            fp.write(json.dumps(obj, separators=(',', ':'), ensure_ascii=False, cls=JsonEncoder))
            fp.write("\n")


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        return super().default(o)


if __name__ == "__main__":
    main(**parse_args())
