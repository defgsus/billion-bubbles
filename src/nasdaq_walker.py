import json
import time
import hashlib
from typing import Optional, Union, Iterable

from tqdm import tqdm

from .nasdaq_db import NasdaqDatabase
from .util import get_path, to_int


class NasdaqWalkerInterface:
    """
    Override and pass to NasdaqWalker constructor to receive each *walked* object.

    All the API and database caching trouble finally leads
    to this nice tidy place.
    """
    def on_company_profile(self, symbol: str, data: dict):
        pass

    def on_company_holders(self, symbol: str, data: Optional[dict]):
        pass

    def on_company_insiders(self, symbol: str, data: Optional[dict]):
        pass

    def on_stock_chart(self, symbol: str, data: dict):
        pass

    def on_institution_positions(self, id: int, data: Optional[dict]):
        pass

    def on_insider_positions(self, id: int, data: Optional[dict]):
        pass

    def finalize(self):
        """
        Called before the `NasdaqWalker.run` method returns.
        All objects are sent at this time.
        """
        pass

    def from_objects(self, iterable: Iterable[dict]):
        """
        Helper function to pass the NasdaqDatabase.iter_objects() generator
        and build a graph from that.
        """
        for obj in iterable:
            if obj["type"] == "company_profile":
                self.on_company_profile(obj["data"]["symbol"], obj["data"]["data"]["data"])
            elif obj["type"] == "company_holders":
                self.on_company_holders(obj["data"]["symbol"], obj["data"]["data"]["data"])
            elif obj["type"] == "company_insiders":
                self.on_company_insiders(obj["data"]["symbol"], obj["data"]["data"]["data"])
            elif obj["type"] == "stock_chart":
                self.on_stock_chart(obj["data"]["symbol"], obj["data"]["data"]["data"])
            elif obj["type"] == "institutional_positions":
                self.on_institution_positions(obj["data"]["id"], obj["data"]["data"]["data"])
            elif obj["type"] == "insider_positions":
                self.on_insider_positions(obj["data"]["id"], obj["data"]["data"]["data"])
            else:
                raise ValueError(f"Unknown object type '{obj['type']}'")


class NasdaqWalker:
    """
    A tree-like walker through NasdaqDatabase objects.

    """
    def __init__(
            self,
            db: NasdaqDatabase,
            interface: Optional[NasdaqWalkerInterface] = None,
            stock_charts: bool = True,
            follow_holders: bool = True,
            follow_insiders: bool = True,
            max_depth_holder: int = 0,
            max_depth_insider: int = 0,
            share_market_value_gte: int = 0,
            sort_order: Optional[str] = None,
    ):
        self.db = db
        self._interface = interface
        self._do_stock_charts = stock_charts
        self._do_follow_holders = follow_holders
        self._do_follow_insiders = follow_insiders
        self._share_market_value_gte = share_market_value_gte
        self._max_depth_holder = max_depth_holder
        self._max_depth_insider = max_depth_insider
        self._sort_order = sort_order
        self._todo_company = dict()
        self._todo_institution = dict()
        self._todo_insiders = dict()
        self._seen = set()
        self._num_companies = 0
        self._num_institutions = 0
        self._num_insiders = 0
        self._num_duplicate_companies = 0
        self._num_duplicate_institutions = 0
        self._num_duplicate_insiders = 0
        self._start_time = time.time()
        self._last_message_time = self._start_time

    def add_company(self, symbol: str, depth: int = 0):
        symbol = symbol.upper()
        if symbol in self._seen:
            self._num_duplicate_companies += 1
        else:
            self._seen.add(symbol)
            if symbol not in self._todo_company:
                self._todo_company[symbol] = depth
            else:
                self._todo_company[symbol] = min(self._todo_company[symbol], depth)

    def add_institution(self, id: Union[int, str], depth: int = 0):
        id = int(id)
        seen_id = f"inst-{id}"
        if seen_id in self._seen:
            self._num_duplicate_institutions += 1
        else:
            self._seen.add(seen_id)
            if id not in self._todo_institution:
                self._todo_institution[id] = depth
            else:
                self._todo_institution[id] = min(self._todo_institution[id], depth)

    def add_insider(self, id: Union[int, str], depth: int = 0):
        id = int(id)
        seen_id = f"insi-{id}"
        if seen_id in self._seen:
            self._num_duplicate_insiders += 1
        else:
            self._seen.add(seen_id)
            if id not in self._todo_insiders:
                self._todo_insiders[id] = depth
            else:
                self._todo_insiders[id] = min(self._todo_insiders[id], depth)

    def run(self):
        while self._todo_company or self._todo_institution or self._todo_insiders:
            self._dump_status()
            self._follow_company()
            self._dump_status()
            self._follow_institution()
            self._dump_status()
            self._follow_insider()

        if self._interface:
            self._interface.finalize()

    def status_string(self) -> str:
        return (
            f"todo: (company/institution/insider)"
            f" {len(self._todo_company):,}/{len(self._todo_institution):,}/{len(self._todo_insiders):,}"
            f", done: {self._num_companies:,}/{self._num_institutions:,}/{self._num_insiders:,}"
            f", duplicates: {self._num_duplicate_companies:,}/{self._num_duplicate_institutions:,}"
            f"/{self._num_duplicate_insiders:,}"
        )

    # --- private ---

    def _dump_status(self):
        cur_time = time.time()
        if cur_time - self._last_message_time >= 1.:
            self._last_message_time = cur_time
            print(
                f"@ {cur_time - self._start_time:.0f} sec"
                f", {self.status_string()}"
            )

    def _unsorted_sort_key(self, x: Union[int, str]) -> str:
        return hashlib.sha256(f"{x}{self._sort_order}".encode()).hexdigest()

    def _next_unsorted(self, iterable) -> Union[int, str]:
        if self._sort_order:
            return sorted(iterable, key=self._unsorted_sort_key)[0]
        else:
            return next(iter(iterable))

    def _follow_company(self):
        if not self._todo_company:
            return

        symbol = self._next_unsorted(self._todo_company)
        depth = self._todo_company[symbol]
        del self._todo_company[symbol]
        self._num_companies += 1

        profile = self.db.company_profile(symbol)
        if not profile:
            return
        profile = profile["data"]

        if self._interface:
            try:
                self._interface.on_company_profile(symbol, profile)
            except:
                print(json.dumps(profile, indent=2)[:10000])
                raise

        if self._do_stock_charts:
            chart = self.db.stock_chart(symbol)
            if chart:
                chart = chart["data"]
                if self._interface:
                    self._interface.on_stock_chart(symbol, chart)

        if self._do_follow_holders and depth < self._max_depth_holder:
            self._follow_company_holders(symbol, depth + 1)

        if self._do_follow_insiders and depth < self._max_depth_insider:
            self._follow_company_insiders(symbol, depth + 1)

    def _follow_institution(self):
        if not self._todo_institution:
            return

        id = self._next_unsorted(self._todo_institution)
        depth = self._todo_institution[id]
        del self._todo_institution[id]
        self._num_institutions += 1

        if self._do_follow_holders and depth < self._max_depth_holder:
            self._follow_institution_positions(id, depth + 1)

    def _follow_insider(self):
        if not self._todo_insiders:
            return

        id = self._next_unsorted(self._todo_insiders)
        depth = self._todo_insiders[id]
        del self._todo_insiders[id]
        self._num_insiders += 1

        if self._do_follow_insiders and depth < self._max_depth_insider:
            self._follow_insider_positions(id, depth + 1)

    def _follow_company_holders(self, symbol: str, depth: int):
        holders = self.db.company_holders(symbol)
        if not holders:
            return
        holders = holders["data"]
        if self._interface:
            try:
                self._interface.on_company_holders(symbol, holders)
            except:
                print(json.dumps(holders, indent=2)[:10000])
                raise

        if depth < self._max_depth_holder and get_path(holders, "holdingsTransactions.table.rows"):
            try:
                value_title = get_path(holders, "holdingsTransactions.table.headers.marketValue")
                assert value_title.lower() == "value (in 1,000s)", value_title

                for row in holders["holdingsTransactions"]["table"]["rows"]:
                    if not row["url"]:
                        continue

                    value = to_int(row["marketValue"][1:]) * 1_000

                    if value >= self._share_market_value_gte:
                        id = int(row["url"].split("-")[-1])
                        self.add_institution(id, depth + 1)
            except:
                print(json.dumps(holders, indent=2)[:10000])
                raise

    def _follow_company_insiders(self, symbol: str, depth: int):
        insiders = self.db.company_insiders(symbol)
        if not insiders:
            return
        insiders = insiders["data"]
        if self._interface:
            try:
                self._interface.on_company_insiders(symbol, insiders)
            except:
                print(json.dumps(insiders, indent=2)[:10000])
                raise

        if depth < self._max_depth_insider and get_path(insiders, "transactionTable.table.rows"):
            try:
                for row in get_path(insiders, "transactionTable.table.rows"):
                    if not row["url"]:
                        continue
                    id = int(row["url"].split("-")[-1])
                    self.add_insider(id, depth + 1)
            except:
                print(json.dumps(insiders, indent=2)[:10000])
                raise

    def _follow_institution_positions(self, id: int, depth: int):
        holdings = self.db.institution_positions(id)
        if not holdings:
            return
        holdings = holdings["data"]
        if self._interface:
            try:
                self._interface.on_institution_positions(id, holdings)
            except:
                print(json.dumps(holdings, indent=2)[:10000])
                raise

        if depth < self._max_depth_holder and get_path(holdings, "institutionPositions.table.rows"):
            try:
                value_title = get_path(holdings, "institutionPositions.table.headers.value")
                assert value_title.lower() == "value ($1,000s)", value_title

                for row in get_path(holdings, "institutionPositions.table.rows"):
                    if not row["url"]:
                        continue

                    value = to_int(row["value"]) * 1_000
                    if value >= self._share_market_value_gte:
                        symbol = row["url"].split("/")[3]
                        self.add_company(symbol, depth + 1)
            except:
                print(json.dumps(holdings, indent=2)[:10000])
                raise

    def _follow_insider_positions(self, id: int, depth: int):
        data = self.db.insider_positions(id)
        if not data:
            return
        data = data["data"]
        if self._interface:
            try:
                self._interface.on_insider_positions(id, data)
            except:
                print(json.dumps(data, indent=2)[:10000])
                raise

        if depth < self._max_depth_insider and get_path(data, "filerTransactionTable.rows"):
            try:
                for row in get_path(data, "filerTransactionTable.rows"):
                    if not row["url"]:
                        continue

                    symbol = row["url"].split("/")[3]
                    self.add_company(symbol, depth + 1)

                # the response for positions of an insider additionally lists
                #   the major insiders of all companies of that insider
                if data.get("companyInsiders"):
                    for company in data["companyInsiders"]:
                        for rel in company["relationInsider"]:
                            for name, url in rel["nameURL"].items():
                                if not url:
                                    continue
                                insider_id = int(url.split("-")[-1])
                                self.add_insider(insider_id, depth + 1)

            except:
                print(json.dumps(data, indent=2)[:10000])
                raise

