import json
import argparse
import datetime
import gzip
import io
import codecs
from pathlib import Path
from typing import Optional, List

from tqdm import tqdm

from src.nasdaq_db import NasdaqDatabase, NasdaqDBBase
from src.util import iter_ndjson


PROJECT_DIR = Path(__file__).resolve().parent

DEFAULT_DB_NAME = PROJECT_DIR / datetime.date.today().strftime("nasdaq-%Y-%m.sqlite3")


def parse_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", type=str,
        choices=["show", "export", "import"],
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
        "-i", "--input", type=str, nargs="?", default="export.ndjson",
        help=f"nd-json filename to import, defaults to 'export.ndjson'",
    )
    parser.add_argument(
        "-v", "--verbose", type=bool, nargs="?", default=False, const=True,
        help=f"Log export progress",
    )
    return vars(parser.parse_args())


def main(
        command: str,
        database: str,
        output: str,
        input: str,
        verbose: bool,
):
    db = NasdaqDatabase(
        database_filename=database,
        verbose=verbose,
    )

    if command == "show":
        for name, table in NasdaqDBBase.metadata.tables.items():
            query = db.db_session.query(table)
            print(f"{name:25}: {query.count()}")

    elif command == "export":
        export_ndjson(db, output)

    elif command == "import":
        import_ndjson(db, input)


def export_ndjson(db: NasdaqDatabase, filename: str):
    iterable = db.iter_objects()

    if db.verbose:
        num_objects = 0
        for name, table in NasdaqDBBase.metadata.tables.items():
            num_objects += db.db_session.query(table).count()

        iterable = tqdm(iterable, total=num_objects, desc="exporting")

    def _export(fp, iterable):
        for obj in iterable:
            fp.write(json.dumps(obj, separators=(',', ':'), ensure_ascii=False, cls=JsonEncoder))
            fp.write("\n")

    if filename.lower().endswith(".gz"):
        with io.TextIOWrapper(io.BufferedWriter(gzip.open(filename, "wb"))) as fp:
            _export(fp, iterable)
    else:
        with open(filename, "wt") as fp:
            _export(fp, iterable)


def import_ndjson(db: NasdaqDatabase, filename: str):
    report = db.import_objects(iter_ndjson(filename))


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        return super().default(o)


if __name__ == "__main__":
    main(**parse_args())
