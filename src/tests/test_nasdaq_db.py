import unittest
import tempfile
import secrets
import os
import sqlite3
from pathlib import Path
from typing import Union, Optional

from src.nasdaq_db import NasdaqDatabase


class FakeApi:

    def __init__(self, verbose: bool = False):
        self.num_calls = 0

    def company_profile(self, symbol: str):
        self.num_calls += 1
        return {"symbol": symbol}

    def institutional_holdings(
            self,
            id: Union[int, str],
            limit: int = 100,
            offset: int = 0,
            type: str = "TOTAL",
            is_company: Optional[bool] = None,
    ):
        self.num_calls += 1
        if is_company:
            return {
                "data": {
                    "holdingsTransactions": {
                        "totalRecords": "150",
                        "table": {
                            "rows": [
                                {"index": i}
                                for i in range(offset, min(offset + limit, 150))
                            ]
                        }
                    }
                }
            }


class TestNasdaqDatabase(unittest.TestCase):

    def test_profile(self):
        db_filename = Path(tempfile.gettempdir()) / f"billion-bubbles-{secrets.token_hex(10)}.sqlite3"
        try:
            nasdaq = NasdaqDatabase(db_filename, verbose=True)
            nasdaq.api = FakeApi()

            profile = nasdaq.company_profile("BOLD")
            self.assertEqual({"symbol": "BOLD"}, profile)
            self.assertEqual(1, nasdaq.api.num_calls)

            profile = nasdaq.company_profile("BOLD")
            self.assertEqual({"symbol": "BOLD"}, profile)
            self.assertEqual(1, nasdaq.api.num_calls)  # still one web-request

            for i, symbol in enumerate(("BLA", "BLUB", "BONG")):
                profile = nasdaq.company_profile(symbol)
                self.assertEqual({"symbol": symbol}, profile)
                self.assertEqual(i + 2, nasdaq.api.num_calls)

            for i, symbol in enumerate(("BLA", "BLUB", "BONG")):
                profile = nasdaq.company_profile(symbol)
                self.assertEqual({"symbol": symbol}, profile)
                self.assertEqual(4, nasdaq.api.num_calls)

        finally:
            if db_filename.exists():
                os.remove(db_filename)

    def test_holders(self):
        db_filename = Path(tempfile.gettempdir()) / f"billion-bubbles-{secrets.token_hex(10)}.sqlite3"
        try:
            nasdaq = NasdaqDatabase(db_filename, verbose=True)
            nasdaq.api = FakeApi()

            data = nasdaq.company_holders("BOLD", page_size=100)
            self.assertEqual(
                [{"index": i} for i in range(150)],
                data["data"]["holdingsTransactions"]["table"]["rows"],
            )
            # needs two requests because page_size is too small
            self.assertEqual(2, nasdaq.api.num_calls)

            # repeat the whole thing

            nasdaq = NasdaqDatabase(db_filename, verbose=True)
            nasdaq.api = FakeApi()

            for i in range(2):
                data = nasdaq.company_holders("BOLD", page_size=100)
                self.assertEqual(
                    [{"index": i} for i in range(150)],
                    data["data"]["holdingsTransactions"]["table"]["rows"],
                )
                # no requests thistime
                self.assertEqual(0, nasdaq.api.num_calls)

        finally:
            if db_filename.exists():
                os.remove(db_filename)

    def test_iter_objects(self):
        db_filename = Path(tempfile.gettempdir()) / f"billion-bubbles-{secrets.token_hex(10)}.sqlite3"
        try:
            nasdaq = NasdaqDatabase(db_filename)
            nasdaq.api = FakeApi()

            all_symbols = set()
            for i in range(100):
                profile = nasdaq.company_profile(f"S{i}")
                all_symbols.add(f"S{i}")

            self.assertEqual(100, nasdaq.api.num_calls)

            for obj in nasdaq.iter_objects(batch_size=30):
                obj["data"].pop("timestamp")
                symbol = obj["data"]["symbol"]
                self.assertEqual(
                    {"type": "company_profile", "data": {"symbol": symbol, "data": {"symbol": symbol}}},
                    obj
                )
                all_symbols.remove(symbol)

            self.assertFalse(all_symbols)

        finally:
            if db_filename.exists():
                os.remove(db_filename)
