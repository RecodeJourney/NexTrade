import re
from typing import Any, Optional


MISSING_VALUES = {"", "-", "--", "na", "n/a", "none", "null"}


def clean_number(value: Any) -> Optional[float]:
    """Convert Screener number strings to floats; return None for blanks."""
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if text.lower() in MISSING_VALUES:
        return None

    # Avoid silently turning a range like "1,612 / 1,227" into one number.
    if "/" in text:
        return None

    is_negative = text.startswith("(") and text.endswith(")")
    text = text.replace("\xa0", " ")
    text = text.replace("−", "-")
    text = text.replace(",", "")
    text = text.replace("%", "")
    text = text.replace("₹", "")
    text = re.sub(r"\b(cr|crore|crores|rs|inr|x)\.?\b", "", text, flags=re.I)
    text = text.strip(" ()")

    numbers = re.findall(r"[-+]?\d*\.?\d+", text)
    if len(numbers) != 1:
        return None

    number = float(numbers[0])
    return -number if is_negative else number


def normalize_key(value: str) -> str:
    """Convert Screener labels and DB column labels into comparable snake_case."""
    text = value.strip().lower()
    text = text.replace("%", " pct ")
    text = text.replace("&", " and ")
    text = text.replace("+", " ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        return "value"
    if text[0].isdigit():
        return "col_" + text
    return text


def normalize_period(value: str) -> str:
    """Collapse whitespace in table period headers such as 'Mar 2025'."""
    return re.sub(r"\s+", " ", value.strip())
