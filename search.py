import sys
import json

import tabulate

from src.nasdaq_api import NasdaqApi


def search(query: str):
    api = NasdaqApi(verbose=False)
    result = api.search(query)

    print(tabulate.tabulate(result["data"], tablefmt="presto"))

    # print(json.dumps(result, indent=2))


if __name__ == "__main__":
    search(" ".join(sys.argv[1:]))