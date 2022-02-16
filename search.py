import sys
import json
import argparse
from typing import List

import tabulate

from src.nasdaq_api import NasdaqApi


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "query", type=str, nargs="+",
        help="The search query",
    )
    parser.add_argument(
        "-l", "--limit", type=int, default=10,
        help="The maximum number of results (default 10)",
    )
    return vars(parser.parse_args())


def search(
        query: List[str],
        limit: int,
):
    api = NasdaqApi(verbose=False)
    result = api.search(
        query=" ".join(query),
        limit=limit,
    )
    for i, row in enumerate(result["data"]):
        result["data"][i] = {
            "index": i,
            **row,
        }

    print(tabulate.tabulate(
        result["data"],
        tablefmt="presto"),
    )

    # print(json.dumps(result, indent=2))


if __name__ == "__main__":
    search(**parse_args())
