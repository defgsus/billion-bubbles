import json
import io
import gzip
from pathlib import Path
from typing import Optional, Union, Generator, IO


def to_id(name: str) -> str:
    """
    Convert a name to ID, for entities that don't have an ID
    """
    return "".join(
        "-" if c.isspace() else c
        for c in name.strip().lower()
        if c.isalnum() or c.isspace()
    )


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


def to_float(x: Union[int, str]) -> float:
    if isinstance(x, str):
        if not x:
            return 0
        return float(x.replace(",", ""))
    elif isinstance(x, (int, float)):
        return float(x)
    raise TypeError(f"Got '{type(x).__name__}'")


def iter_ndjson(file: Union[str, Path, IO], raise_error: bool = True, skip: int = 0) -> Generator[dict, None, None]:
    for line in iter_lines(file, skip=skip):
        try:
            yield json.loads(line)
        except json.JSONDecodeError as e:
            if raise_error:
                raise
            print(f"\n\nJSON ERROR '{e}' for line '{line}'\n")


def iter_lines(file: Union[str, Path, IO], skip: int = 0, keep_first: bool = False) -> Generator[dict, None, None]:
    if isinstance(file, (str, Path)):
        filename = str(file)

        if filename.lower().endswith(".gz"):
            with io.TextIOWrapper(io.BufferedReader(gzip.open(filename))) as fp:
                count = 0
                for line in fp:
                    if skip and count < skip:
                        count += 1
                        if keep_first and count == 1:
                            yield line
                        continue

                    yield line

        else:
            with open(file, "rt") as fp:
                yield from iter_lines(fp, skip=skip)

    else:
        count = 0
        for line in file.readlines():
            if skip and count and count < skip:
                count += 1
                if keep_first and count == 1:
                    yield line
                continue

            yield line
