import json
from typing import Optional, Union

from .nasdaq_db import NasdaqDatabase


class NasdaqWalker:
    """
    A tree-like walker through NasdaqDatabase objects.

    """
    def __init__(
            self,
            db: NasdaqDatabase,
            max_depth: int = 0,
            share_markt_value_gte: int = 0,
    ):
        self.db = db
        self._share_markt_value_gte = share_markt_value_gte
        self._max_depth = max_depth
        self._todo_company = dict()
        self._todo_institution = dict()
        self._seen = set()

    def add_company(self, symbol: str, depth: int = 0):
        symbol = symbol.upper()
        if symbol not in self._seen:
            if symbol not in self._todo_company:
                self._todo_company[symbol] = depth
            else:
                self._todo_company[symbol] = min(self._todo_company[symbol], depth)
        self._seen.add(symbol)

    def add_institution(self, id: Union[int, str], depth: int = 0):
        id = int(id)
        if id not in self._seen:
            if id not in self._todo_institution:
                self._todo_institution[id] = depth
            else:
                self._todo_institution[id] = min(self._todo_institution[id], depth)
        self._seen.add(id)

    def run(self):
        while self._todo_company or self._todo_institution:
            self._follow_company()
            self._follow_institution()

    def _follow_company(self):
        if not self._todo_company:
            return

        symbol = sorted(self._todo_company)[0]
        depth = self._todo_company[symbol]
        del self._todo_company[symbol]

        profile = self.db.company_profile(symbol)["data"]
        self.on_company_profile(symbol, profile)

        chart = self.db.stock_chart(symbol)["data"]
        self.on_stock_chart(symbol, chart)

        holders = self.db.company_holders(symbol)["data"]
        self.on_company_holders(symbol, holders)

        if depth >= self._max_depth:
            return

        try:
            value_title = holders["holdingsTransactions"]["table"]["headers"]["marketValue"]
            assert value_title == "VALUE (IN 1,000S)"

            for row in holders["holdingsTransactions"]["table"]["rows"]:
                if not row["url"]:
                    continue

                value = to_int(row["marketValue"][1:]) * 1_000

                if value >= self._share_markt_value_gte:
                    id = int(row["url"].split("-")[-1])
                    self.add_institution(id, depth + 1)
        except:
            print(json.dumps(holders, indent=2)[:10000])
            raise

    def _follow_institution(self):
        if not self._todo_institution:
            return

        id = sorted(self._todo_institution)[0]
        depth = self._todo_institution[id]
        del self._todo_institution[id]

        holdings = self.db.institution_positions(id)["data"]
        self.on_institution_positions(id, holdings)

        if depth >= self._max_depth:
            return

        try:
            value_title = holdings["institutionPositions"]["table"]["headers"]["value"]
            assert value_title == "Value ($1,000s)"

            for row in holdings["institutionPositions"]["table"]["rows"]:
                if not row["url"]:
                    continue

                value = to_int(row["value"]) * 1_000
                if value >= self._share_markt_value_gte:
                    symbol = row["url"].split("/")[3]
                    self.add_company(symbol, depth + 1)
        except:
            print(json.dumps(holdings, indent=2)[:10000])
            raise

    def on_company_profile(self, symbol: str, data: dict):
        pass

    def on_stock_chart(self, symbol: str, data: dict):
        pass

    def on_company_holders(self, symbol: str, data: dict):
        pass

    def on_institution_positions(self, id: str, data: dict):
        pass


def get_path(data: Optional[dict], path: str):
    path = path.split(".")
    while path:
        if data is None:
            return None
        key = path.pop(0)
        data = data.get(key)
    return data


def to_int(x: Union[int, str]) -> int:
    if isinstance(x, str):
        if not x:
            return 0
        return int(x.replace(",", ""))
    elif isinstance(x, int):
        return x
    raise TypeError(f"Got '{type(x).__name__}'")
