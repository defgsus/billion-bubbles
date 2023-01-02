import json
import argparse
import datetime
import gzip
import io
import codecs
from pathlib import Path
from typing import Optional, List

from tqdm import tqdm

from src.nasdaq_db import *
from src.util import iter_ndjson, write_ndjson, to_int, JsonEncoder
from src.config import DEFAULT_DB_NAME


def parse_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", type=str,
        choices=[
            "show", "export", "import", "search",
            "export-charts", "export-charts-es",
            "export-holders",
        ],
        help=f"Command",
    )
    parser.add_argument(
        "query", type=str, nargs="?",
        help=f"search term",
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
        query: str,
        verbose: bool,
):
    db = NasdaqDatabase(
        database_filename=database,
        verbose=verbose,
    )

    if command == "show":
        for name, table in NasdaqDBBase.metadata.tables.items():
            db_query = db.db_session.query(table)
            print(f"{name:25}: {db_query.count()}")

    if command == "search":
        search(db, query)

    elif command == "import":
        import_ndjson(db, input)

    elif command == "export":
        export_ndjson(db, output)

    elif command == "export-charts":
        export_charts(db, output)

    elif command == "export-charts-es":
        export_charts_elastic(db)

    elif command == "export-holders":
        export_holders(db, output)


def export_charts(db: NasdaqDatabase, filename: str):
    iterable = db.iter_objects(
        company_profile=False,
        company_holders=False,
        company_insiders=False,
        stock_chart=True,
        institution_positions=False,
        insider_positions=False,
        batch_size=200,
    )
    index = dict()

    def _generator():
        num_entries = 0
        last_time = time.time()
        for obj in iterable:
            #if num_entries > 100000:
            #    break
            data = obj["data"]["data"]["data"]
            if not data:
                continue
            try:
                index[data["symbol"]] = {
                    "name": data["company"],
                    "start_date": datetime.datetime.fromtimestamp(data["chart"][0]["x"] // 1000).date(),
                    "start_value": float(data["chart"][0]["z"]["value"]),
                    "start_volume": to_int(data["chart"][0]["z"]["volume"]),
                    "end_date": datetime.datetime.fromtimestamp(data["chart"][-1]["x"] // 1000).date(),
                    "end_value": float(data["chart"][-1]["z"]["value"]),
                    "end_volume": to_int(data["chart"][-1]["z"]["volume"]),
                }
                # print(index[data["symbol"]])
                for entry in data["chart"]:
                    yield {
                        "symbol": data["symbol"],
                        "date": datetime.datetime.fromtimestamp(entry["x"] // 1000).date(),
                        "value": float(entry["z"]["value"]),
                        "volume": to_int(entry["z"]["volume"]),
                        "open": float(entry["z"]["open"]),
                        "close": float(entry["z"]["close"]),
                        "high": float(entry["z"]["high"]),
                        "low": float(entry["z"]["low"]),
                    }
                    num_entries += 1
            except KeyError:
                print(json.dumps(data, indent=2)[:2000])
                raise

            cur_time = time.time()
            if cur_time - last_time > 5:
                last_time = cur_time
                print(f"{num_entries:,} rows yielded")

    write_ndjson(filename, _generator())
    index_filename = filename.split(".")
    while index_filename[-1].lower() in ("gz", "ndjson"):
        index_filename = index_filename[:-1]
    index_filename = ".".join(index_filename) + "-index.json"
    Path(index_filename).write_text(json.dumps(index, indent=2, cls=JsonEncoder))


def export_charts_elastic(db: NasdaqDatabase):
    from src.elastic import StockChartExporter

    iterable = db.iter_objects(
        company_profile=False,
        company_holders=False,
        company_insiders=False,
        stock_chart=True,
        institution_positions=False,
        insider_positions=False,
        batch_size=200,
    )

    def _generator():
        company_map = dict()

        for obj in iterable:
            data = obj["data"]["data"]["data"]
            if not data:
                continue
            try:
                if data["symbol"] not in company_map:
                    company_map[data["symbol"]] = db.company_profile(data["symbol"])["data"]
                profile = company_map[data["symbol"]]
                first_timestamp = datetime.datetime.fromtimestamp(data["chart"][0]["x"] // 1000).date()
                last_timestamp = datetime.datetime.fromtimestamp(data["chart"][-1]["x"] // 1000).date()

                for entry in data["chart"]:
                    yield {
                        "symbol": data["symbol"],
                        "name": get_path(profile, "CompanyName.value"),
                        "region": get_path(profile, "Region.value"),
                        "sector": get_path(profile, "Sector.value"),
                        "industry": get_path(profile, "Industry.value"),

                        "timestamp": datetime.datetime.fromtimestamp(entry["x"] // 1000).date(),
                        "first_timestamp": first_timestamp,
                        "last_timestamp": last_timestamp,
                        "value": float(entry["z"]["value"]),
                        "volume": to_int(entry["z"]["volume"]),
                        "open": float(entry["z"]["open"]),
                        "close": float(entry["z"]["close"]),
                        "high": float(entry["z"]["high"]),
                        "low": float(entry["z"]["low"]),
                    }
            except KeyError:
                print(json.dumps(data, indent=2)[:2000])
                raise

    exporter = StockChartExporter()
    exporter.export_list(_generator(), chunk_size=2000)


def export_ndjson(db: NasdaqDatabase, filename: str):
    iterable = db.iter_objects(
        company_profile=True,
        company_holders=True,
        company_insiders=True,
        stock_chart=True,
        institution_positions=True,
        insider_positions=True,
    )

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


def search(db: NasdaqDatabase, query: str):
    for model in (
            CompanyProfile, CompanyHolders, CompanyInsiders,
            InstitutionPositions, InsiderPositions,
    ):
        field = model.__table__.c[0]
        q = (
            db.db_session.query(field)
            .filter(model.data.contains(query))
        )
        result = q.limit(10).all()
        if result:
            print(f"\n{model.__table__.name}:")
            for row in result:
                print(f"  {row[0]}")


def export_holders(db: NasdaqDatabase, filename: str):
    import pandas as pd

    iterable = db.iter_objects(
        company_profile=False,
        company_holders=True,
        company_insiders=False,
        stock_chart=False,
        institution_positions=False,
        insider_positions=False,
        batch_size=200,
    )
    holder_map = dict()
    unknown_ids = dict()
    for holders in iterable:
        holders = holders["data"]["data"]["data"]
        rows = get_path(holders, "holdingsTransactions.table.rows")
        if rows:
            try:
                company_shares = None
                if holders["activePositions"]:
                    company_shares = to_int(holders["activePositions"]["rows"][3]["shares"])

                for row in rows:
                    name = row["ownerName"]
                    shares = to_int(row["sharesHeld"])
                    dollar = to_int(row["marketValue"][1:]) / 1_000
                    if row["url"]:
                        id = str(int(row["url"].split("-")[-1]))
                    else:
                        if name not in unknown_ids:
                            unknown_ids[name] = f"UNK-f{len(unknown_ids)}"
                        id = unknown_ids[name]

                    if name not in holder_map:
                        holder_map[name] = {
                            "id": id,
                            "companies": 0,
                            "shares_mill": 0,
                            "shares_percent": 0,
                            "shares_dollar_mill": 0,
                        }
                    holder = holder_map[name]
                    holder["companies"] += 1
                    holder["shares_mill"] += shares / 1_000_000
                    holder["shares_percent"] += shares / company_shares * 100 if company_shares else 0
                    holder["shares_dollar_mill"] += dollar
            except:
                print(json.dumps(holders, indent=2)[:5000])
                raise

    df = pd.DataFrame(holder_map).T
    print(df)
    if filename:
        print("writing csv", filename)
        df.to_csv(filename)


if __name__ == "__main__":
    main(**parse_args())
