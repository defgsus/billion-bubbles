import sys
import time
import json
import datetime
from pathlib import Path
from typing import Union, Generator, Tuple, Optional

import requests


class NasdaqApi:
    """
    This is wrapping up the website API (not the official developers API)
    """

    REQUEST_TIMEOUT = 30.
    REQUEST_RETRIES = 4

    def __init__(
            self,
            verbose: bool = True,
    ):
        self.verbose = verbose
        self.session = requests.Session()
        self.session.headers = {
            "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0",
            "referer":  "https://www.nasdaq.com/",
            "origin":  "https://www.nasdaq.com/",
            "host": "api.nasdaq.com",
            "connection": "keep-alive",
            "accept": "Accept: application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.5",
            "accept-encoding": "gzip, deflate, br",
        }

    def request(
            self,
            url: str,
            as_json: bool = True,
            clear_cookies: bool = False,
            **kwargs,
    ) -> Union[str, list, dict]:
        kwargs.setdefault("timeout", self.REQUEST_TIMEOUT)
        for i in range(self.REQUEST_RETRIES):
            try:
                if self.verbose:
                    if i == 0:
                        print(f"requesting {url}", file=sys.stderr)
                    else:
                        print(f"\nretry {i} request {url}", file=sys.stderr)

                response = self.session.get(url, **kwargs)
                if clear_cookies:
                    self.session.cookies.clear()
                if as_json:
                    return response.json()
                return response.text
            except (requests.RequestException, ValueError) as e:
                if self.verbose:
                    print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
                if i + 1 == self.REQUEST_RETRIES:
                    raise
                kwargs["timeout"] += 5
                time.sleep(i)

    def search(self, query: str):
        url = f"https://api.nasdaq.com/api/autocomplete/slookup/10"
        return self.request(url, params={"search": query})

    def company_profile(self, symbol: str) -> dict:
        url = f"https://api.nasdaq.com/api/company/{symbol}/company-profile"
        return self.request(url)

    def company_summary(self, symbol: str) -> dict:
        url = f"https://api.nasdaq.com/api/quote/{symbol}/summary?assetclass=stocks"
        return self.request(url)

    def company_financials(self, symbol: str) -> dict:
        url = f"https://api.nasdaq.com/api/company/{symbol}/financials?frequency=1"
        return self.request(url)

    def stock_chart(
            self,
            symbol: str,
            asset_class: str = "stocks",
            date_from: Union[str, datetime.date, datetime.datetime] = "2000-01-01",
            date_to: Optional[Union[str, datetime.date, datetime.datetime]] = None,
    ) -> dict:
        symbol = symbol.upper()
        if isinstance(date_from, (datetime.date, datetime.datetime)):
            date_from = date_from.strftime("%Y-%m-%d")
        if date_to is None:
            date_to = datetime.date.today()
        if isinstance(date_to, (datetime.date, datetime.datetime)):
            date_to = date_to.strftime("%Y-%m-%d")
        url = f"https://api.nasdaq.com/api/quote/{symbol}/chart" \
              f"?assetclass={asset_class}&fromdate={date_from}&todate={date_to}"
        return self.request(url, json=True, clear_cookies=True)

    def institutional_holdings(
            self,
            id: Union[int, str],
            limit: int = 100,
            offset: int = 0,
            type: str = "TOTAL",
            is_company: Optional[bool] = None,
    ):
        if is_company is None:
            is_company = isinstance(id, str)

        # sort_column = "marketValue" if is_company else "value"
        sort_column = "ownerName"  # this seems to be faster than value/marketValue

        url = f"https://api.nasdaq.com/api/company/{id}/institutional-holdings" \
              f"?limit={limit}&offset={offset}&type={type}&sortColumn={sort_column}&sortOrder=DESC"

        return self.request(url)

    def insider_trades(
            self,
            id: Union[int, str],
            limit: int = 100,
            offset: int = 0,
            sort_date: bool = False,
    ) -> dict:
        url = f"https://api.nasdaq.com/api/company/{id}/insider-trades" \
              f"?limit={limit}&offset={offset}"
        if sort_date:
            url += "&sortColumn=lastDate&sortOrder=DESC"
        return self.request(url)
