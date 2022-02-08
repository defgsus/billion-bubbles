import hashlib
from typing import Optional, Union


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


def unsorted_sort_key(x: Union[int, str]):
    return hashlib.sha256(str(x).encode()).hexdigest()
