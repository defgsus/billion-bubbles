import json
import datetime
import argparse
from pathlib import Path
from src.nasdaq_db import NasdaqDatabase
from src.nasdaq_api import NasdaqApi
from src.config import DEFAULT_DB_NAME, PROJECT_DIR


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "what", type=str,
        choices=["profile", "positions", "holders", "insider-positions", "insiders", "stock-chart"],
    )
    parser.add_argument(
        "id", type=str,
        help="ID or trading symbol",
    )
    parser.add_argument(
        "--db", type=str, nargs="?", default=str(DEFAULT_DB_NAME),
        help=f"Name of database file, defaults to '{DEFAULT_DB_NAME}'",
    )
    parser.add_argument(
        "--api", type=bool, nargs="?", default=False, const=True,
        help="Do not use database, scrape live API",
    )
    args = parser.parse_args()

    if args.api:
        api = NasdaqApi(verbose=True)

        if args.what == "profile":
            data = api.company_profile(args.id)
        elif args.what in ("positions", "holders"):
            data = api.institutional_holdings(args.id, is_company=args.what == "holders")
        elif args.what in ("insider-positions", "insiders"):
            data = api.insider_trades(args.id)
        else:
            raise ValueError(f"Unknown thing '{args.what}'")

    else:
        db_name = PROJECT_DIR / "nasdaq.sqlite3"
        if args.db:
            db_name = args.db

        db = NasdaqDatabase(
            database_filename=db_name,
            verbose=True,
        )

        if args.what == "profile":
            data = db.company_profile(args.id)
        elif args.what == "positions":
            data = db.institution_positions(args.id)
        elif args.what == "holders":
            data = db.company_holders(args.id)
        elif args.what == "insiders":
            data = db.company_insiders(args.id)
        elif args.what == "insider-positions":
            data = db.insider_positions(args.id)
        elif args.what == "stock-chart":
            data = db.stock_chart(args.id)
        else:
            raise ValueError(f"Unknown thing '{args.what}'")

    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()


