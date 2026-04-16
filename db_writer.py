import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from utils import normalize_key


DEFAULT_DB_PATH = os.environ.get("SCREENER_DB_PATH", "screener.db")

FIELD_ALIASES = {
    "source_url": ("screener_url",),
    "scraped_at": ("last_scraped", "updated_at", "scrape_time"),
    "stock_p_e": ("pe",),
    "eps_in_rs": ("eps",),
    "opm_pct": ("opm",),
    "dividend_payout_pct": ("dividend_payout",),
    "fiis": ("fii",),
    "diis": ("dii",),
    "current_price": ("close_price",),
}

MONTH_END_DAYS = {
    "Jan": "31",
    "Feb": "28",
    "Mar": "31",
    "Apr": "30",
    "May": "31",
    "Jun": "30",
    "Jul": "31",
    "Aug": "31",
    "Sep": "30",
    "Oct": "31",
    "Nov": "30",
    "Dec": "31",
}


def connect_db(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    if db_path != ":memory:" and not Path(db_path).exists():
        raise FileNotFoundError(
            f"Database file '{db_path}' does not exist. Create it once from "
            "schema.sql before running the scraper."
        )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    columns = [row["name"] for row in rows]
    if not columns:
        raise RuntimeError(
            f"SQLite table '{table}' was not found. Create the database once "
            "from schema.sql before running the scraper."
        )
    return columns


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _scrape_timestamp(data: Mapping[str, Any]) -> str:
    return str(data.get("scraped_at") or datetime.now(timezone.utc).isoformat())


def _scrape_date(data: Mapping[str, Any]) -> str:
    return _scrape_timestamp(data).split("T", 1)[0]


def _period_label(period: str) -> str:
    return period.split(":", 1)[-1].strip()


def _year_end(period: str) -> Optional[int]:
    match = re.search(r"\b(19|20)\d{2}\b", _period_label(period))
    return int(match.group(0)) if match else None


def _period_end_date(period: str) -> Optional[str]:
    label = _period_label(period)
    match = re.search(r"\b([A-Z][a-z]{2})\s+((?:19|20)\d{2})\b", label)
    if not match:
        return None
    month, year = match.groups()
    day = MONTH_END_DAYS.get(month)
    if not day:
        return None
    return f"{year}-{datetime.strptime(month, '%b').month:02d}-{day}"


def _latest_period(data: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    if not data:
        return None
    return next(reversed(data.values()))


def _expand_aliases(candidates: Mapping[str, Any]) -> Dict[str, Any]:
    normalized_candidates = {
        normalize_key(key): value for key, value in candidates.items()
    }
    for key, aliases in FIELD_ALIASES.items():
        if key not in normalized_candidates:
            continue
        for alias in aliases:
            normalized_candidates.setdefault(alias, normalized_candidates[key])
    return normalized_candidates


def _select_existing_columns(
    conn: sqlite3.Connection,
    table: str,
    candidates: Mapping[str, Any],
) -> Dict[str, Any]:
    columns = _table_columns(conn, table)
    normalized_candidates = _expand_aliases(candidates)

    row: Dict[str, Any] = {}
    for column in columns:
        normalized_column = normalize_key(column)
        if normalized_column in normalized_candidates:
            row[column] = normalized_candidates[normalized_column]
    return row


def _insert_or_upsert_row(
    conn: sqlite3.Connection,
    table: str,
    candidates: Mapping[str, Any],
    conflict_columns: Optional[Sequence[str]] = None,
) -> None:
    row = _select_existing_columns(conn, table, candidates)
    if not row:
        return

    columns = list(row.keys())
    placeholders = ", ".join("?" for _ in columns)
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)

    sql = (
        f"INSERT INTO {_quote_identifier(table)} ({quoted_columns}) "
        f"VALUES ({placeholders})"
    )

    if conflict_columns:
        update_columns = [column for column in columns if column not in conflict_columns]
        quoted_conflict = ", ".join(_quote_identifier(column) for column in conflict_columns)

        if update_columns:
            assignments = ", ".join(
                f"{_quote_identifier(column)} = excluded.{_quote_identifier(column)}"
                for column in update_columns
            )
            sql += f" ON CONFLICT({quoted_conflict}) DO UPDATE SET {assignments}"
        else:
            sql += f" ON CONFLICT({quoted_conflict}) DO NOTHING"

    conn.execute(sql, [row[column] for column in columns])


def upsert_company(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    company_info = data.get("company_info", {})
    top_ratios = data.get("top_ratios", {})

    # companies: identity fields come from company_info; Screener URL maps to
    # screener_url; face_value/market_cap come from the top ratios block.
    candidates: Dict[str, Any] = {
        "symbol": symbol,
        "company_name": company_info.get("company_name"),
        "screener_url": company_info.get("source_url"),
        "source_url": company_info.get("source_url"),
        "last_scraped": _scrape_timestamp(data),
        "scraped_at": _scrape_timestamp(data),
    }
    candidates.update(top_ratios)

    _insert_or_upsert_row(conn, "companies", candidates, ("symbol",))


def upsert_ratios(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    top_ratios = data.get("top_ratios", {})
    profit_loss = data.get("profit_loss", {})
    latest_results = _latest_period(profit_loss) or {}

    # ratios: current ratio fields mostly come from top_ratios. EPS is not in
    # top_ratios, so use the latest profit/loss EPS when available.
    candidates: Dict[str, Any] = {
        "symbol": symbol,
        "updated_at": _scrape_timestamp(data),
        "eps": latest_results.get("eps_in_rs"),
    }
    candidates.update(top_ratios)

    _insert_or_upsert_row(conn, "ratios", candidates, ("symbol",))


def insert_yearly_results(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    profit_loss = data.get("profit_loss", {})

    # yearly_results: Screener profit_loss is period-keyed. Each annual period
    # becomes one row keyed by (symbol, year_end). TTM is skipped because it is
    # not a fiscal year end.
    for period, metrics in profit_loss.items():
        year_end = _year_end(period)
        if year_end is None or "ttm" in period.lower():
            continue

        candidates: Dict[str, Any] = {
            "symbol": symbol,
            "year_end": year_end,
        }
        candidates.update(metrics)

        _insert_or_upsert_row(
            conn,
            "yearly_results",
            candidates,
            ("symbol", "year_end"),
        )


def insert_balance_sheet(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    balance_sheet = data.get("balance_sheet", {})

    # balance_sheet: each Screener annual balance-sheet column becomes one row
    # keyed by (symbol, year_end).
    for period, metrics in balance_sheet.items():
        year_end = _year_end(period)
        if year_end is None:
            continue

        candidates: Dict[str, Any] = {
            "symbol": symbol,
            "year_end": year_end,
        }
        candidates.update(metrics)

        _insert_or_upsert_row(
            conn,
            "balance_sheet",
            candidates,
            ("symbol", "year_end"),
        )


def insert_shareholding(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    shareholding = data.get("shareholding", {})
    quarterly_items = [
        (period, metrics)
        for period, metrics in shareholding.items()
        if period.startswith("quarterly:")
    ]
    items = quarterly_items or list(shareholding.items())

    # shareholding_pattern: quarterly Screener shareholding periods map to
    # quarter_end. This schema has no government column, so government is mapped
    # into others when present.
    for period, metrics in items:
        quarter_end = _period_end_date(period)
        if quarter_end is None:
            continue

        candidates: Dict[str, Any] = {
            "symbol": symbol,
            "quarter_end": quarter_end,
            "others": metrics.get("others", metrics.get("government")),
        }
        candidates.update(metrics)

        _insert_or_upsert_row(
            conn,
            "shareholding_pattern",
            candidates,
            ("symbol", "quarter_end"),
        )


def upsert_price_history(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    top_ratios = data.get("top_ratios", {})

    # price_history: Screener page gives current price, not OHLC history. Store
    # current_price as close_price for the scrape date; OHLC/volume remain NULL.
    candidates: Dict[str, Any] = {
        "symbol": symbol,
        "price_date": _scrape_date(data),
        "close_price": top_ratios.get("current_price"),
    }

    _insert_or_upsert_row(
        conn,
        "price_history",
        candidates,
        ("symbol", "price_date"),
    )


def insert_scrape_log(
    conn: sqlite3.Connection,
    symbol: str,
    status: str,
    duration_seconds: Optional[float] = None,
    error_message: Optional[str] = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()

    # scrape_log: id is AUTOINCREMENT, so each scraper attempt is inserted as a
    # new log row rather than upserted.
    candidates: Dict[str, Any] = {
        "symbol": symbol,
        "scrape_time": now,
        "status": status,
        "error_message": error_message,
        "duration_seconds": duration_seconds,
    }

    _insert_or_upsert_row(conn, "scrape_log", candidates)


def insert_raw_json(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    scrape_time = _scrape_timestamp(data)

    # screener_raw_json: complete scrape snapshot keyed by (symbol, scrape_time).
    candidates: Dict[str, Any] = {
        "symbol": symbol,
        "scrape_time": scrape_time,
        "raw_json": _json_dumps(data),
    }

    _insert_or_upsert_row(
        conn,
        "screener_raw_json",
        candidates,
        ("symbol", "scrape_time"),
    )
