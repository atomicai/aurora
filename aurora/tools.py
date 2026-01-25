from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def normalize_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_str_list(values: Iterable[Any] | None) -> list[str]:
    if not values:
        return []

    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = normalize_str(value)
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def normalize_str_list_keep_duplicates(values: Iterable[Any] | None) -> list[str]:
    if not values:
        return []

    result: list[str] = []
    for value in values:
        text = normalize_str(value)
        if not text:
            continue
        result.append(text)
    return result
