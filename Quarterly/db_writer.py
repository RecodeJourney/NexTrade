import json
import os
import re
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
import sys

# Add parent directory to path for direct execution
sys_path_insert = Path(__file__).resolve().parent.parent
if str(sys_path_insert) not in sys.path:
    sys.path.insert(0, str(sys_path_insert))

from Quarterly.utils import normalize_key
from Utils import get_logger

logger = get_logger(__name__)


DEFAULT_DB_PATH = Path(__file__).parent.parent/"Database" / "screener.db"

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


def connect_db(db_path = None) -> sqlite3.Connection:
    if db_path is None:
        db_path = str(DEFAULT_DB_PATH)
    else:
        db_path = str(db_path)
    
    if db_path != ":memory:" and not Path(db_path).exists():
        raise FileNotFoundError(
            f"Database file '{db_path}' does not exist. Create it once from "
            "schema.sql before running the scraper."
        )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    logger.debug(f"Connected to database: {db_path}")
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


def _scrape_date(data: Mapping[str, Any]) -> str:
    data = str(data.get("scraped_at") or datetime.now(timezone.utc).isoformat())
    return data.split("T", 1)[0]


def _period_label(period: str) -> str:
    return period.split(":", 1)[-1].strip()


def _year_end(period: str) -> Optional[int]:
    match = re.search(r"\b(19|20)\d{2}\b", _period_label(period))
    return int(match.group(0)) if match else None


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

    # companies: Map company_info and top_ratios to schema columns
    # category, sector remain NULL unless provided
    candidates: Dict[str, Any] = {
        "symbol": symbol,
        "name": company_info.get("company_name"),
        "category": None,  # To be filled by V40 classification
        "sector": None,
        "url": company_info.get("source_url"),
    }

    _insert_or_upsert_row(conn, "companies", candidates, ("symbol",))


def upsert_snapshot_metrics(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    """Insert/update snapshot metrics from top_ratios."""
    symbol = str(data["symbol"])
    top_ratios = data.get("top_ratios", {})

    # snapshot_metrics: current snapshot from top_ratios
    candidates: Dict[str, Any] = {
        "symbol": symbol,
        "market_cap": top_ratios.get("market_cap"),
        "current_price": top_ratios.get("current_price"),
        "high_low": top_ratios.get("high_low"),
        "pe": top_ratios.get("stock_p_e"),
        "book_value": top_ratios.get("book_value"),
        "dividend_yield": top_ratios.get("dividend_yield"),
        "roce": top_ratios.get("roce"),
        "roe": top_ratios.get("roe"),
        "face_value": top_ratios.get("face_value"),
        "profit_growth": top_ratios.get("profit_growth"),
        "promoter_holding": top_ratios.get("promoter_holding"),
        "sales_growth": top_ratios.get("sales_growth"),
        "debt_to_equity": top_ratios.get("debt_to_equity"),
    }

    _insert_or_upsert_row(conn, "snapshot_metrics", candidates, ("symbol",))


def insert_ratios(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    """Insert ratios from ratios_history (period-based data)."""
    symbol = str(data["symbol"])
    ratios_history = data.get("ratios_history", {})

    # ratios: Screener ratios_history is period-keyed. Each annual period
    # becomes one row keyed by (symbol, year).
    for period, metrics in ratios_history.items():
        year = _year_end(period)
        if year is None or "ttm" in period.lower():
            continue

        candidates: Dict[str, Any] = {
            "symbol": symbol,
            "year": year,
            "debtor_days": metrics.get("debtor_days"),
            "inventory_days": metrics.get("inventory_days"),
            "days_payable": metrics.get("days_payable"),
            "cash_conversion_cycle": metrics.get("cash_conversion_cycle"),
            "working_capital_days": metrics.get("working_capital_days"),
            "roce": metrics.get("roce"),
        }

        _insert_or_upsert_row(
            conn,
            "ratios",
            candidates,
            ("symbol", "year"),
        )


def insert_profit_loss(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    profit_loss = data.get("profit_loss", {})

    # profit_and_loss: Screener profit_loss is period-keyed. Each annual period
    # becomes one row keyed by (symbol, year). TTM is skipped because it is
    # not a fiscal year end.
    for period, metrics in profit_loss.items():
        year = _year_end(period)
        if year is None or "ttm" in period.lower():
            continue

        candidates: Dict[str, Any] = {
            "symbol": symbol,
            "year": year,
            "sales": metrics.get("sales"),
            "expenses": None,  # Not available in scraper data
            "operating_profit": metrics.get("operating_profit"),
            "opm": metrics.get("opm"),
            "other_income": metrics.get("other_income"),
            "interest": metrics.get("interest"),
            "depreciation": metrics.get("depreciation"),
            "profit_before_tax": metrics.get("profit_before_tax"),
            "tax": metrics.get("tax"),
            "net_profit": metrics.get("net_profit"),
            "eps": metrics.get("eps_in_rs"),
            "dividend_payout": metrics.get("dividend_payout"),
        }

        _insert_or_upsert_row(
            conn,
            "profit_and_loss",
            candidates,
            ("symbol", "year"),
        )


def insert_balance_sheet(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    balance_sheet = data.get("balance_sheet", {})

    # balance_sheet: each Screener annual balance-sheet column becomes one row
    # keyed by (symbol, year).
    for period, metrics in balance_sheet.items():
        year = _year_end(period)
        if year is None:
            continue

        candidates: Dict[str, Any] = {
            "symbol": symbol,
            "year": year,
            "equity_capital": metrics.get("equity_capital"),
            "reserves": metrics.get("reserves"),
            "borrowings": metrics.get("borrowings"),
            "other_liabilities": None,  # Not available in scraper data
            "total_liabilities": metrics.get("total_liabilities"),
            "fixed_assets": metrics.get("fixed_assets"),
            "cwip": metrics.get("cwip"),
            "investments": metrics.get("investments"),
            "other_assets": None,  # Not available in scraper data
            "total_assets": metrics.get("total_assets"),
        }

        _insert_or_upsert_row(
            conn,
            "balance_sheet",
            candidates,
            ("symbol", "year"),
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

    # shareholding_pattern: quarterly Screener shareholding periods map to year.
    # Extract year from period date.
    for period, metrics in items:
        year = _year_end(period)
        if year is None:
            continue

        candidates: Dict[str, Any] = {
            "symbol": symbol,
            "year": year,
            "promoters": metrics.get("promoters"),
            "fiis": metrics.get("fiis"),
            "diis": metrics.get("diis"),
            "government": metrics.get("government"),
            "public": metrics.get("public"),
        }

        _insert_or_upsert_row(
            conn,
            "shareholder_pattern",
            candidates,
            ("symbol", "year"),
        )


def upsert_price_history(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    symbol = str(data["symbol"])
    top_ratios = data.get("top_ratios", {})

    # price_history: Screener page gives current price, not OHLC history. Store
    # current_price as close_price for the scrape date; OHLC/volume remain NULL.
    candidates: Dict[str, Any] = {
        "symbol": symbol,
        "date": _scrape_date(data),
        "open_price": None,
        "close_price": top_ratios.get("current_price"),
        "high_price": None,
        "low_price": None,
        "volume": None,
    }

    _insert_or_upsert_row(
        conn,
        "price_history",
        candidates,
        ("symbol", "date"),
    )


def save_company_to_db(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    """
    Main function to save all company data to database.
    
    Inserts/updates company information across all tables:
    - companies: basic company info
    - profit_and_loss: financial metrics
    - balance_sheet: balance sheet data
    - shareholder_pattern: shareholding data
    - price_history: current price snapshot
    
    Args:
        conn: SQLite database connection
        data: Scraped company data dictionary
    """
    try:
        symbol = str(data.get("symbol", "UNKNOWN"))
        
        logger.info(f"Saving {symbol} to database...")
        
        upsert_company(conn, data)
        logger.debug(f"✓ Saved company info for {symbol}")
        
        upsert_snapshot_metrics(conn, data)
        logger.debug(f"✓ Saved snapshot metrics for {symbol}")
        
        insert_ratios(conn, data)
        logger.debug(f"✓ Saved ratios history for {symbol}")
        
        insert_profit_loss(conn, data)
        logger.debug(f"✓ Saved profit & loss for {symbol}")
        
        insert_balance_sheet(conn, data)
        logger.debug(f"✓ Saved balance sheet for {symbol}")
        
        insert_shareholding(conn, data)
        logger.debug(f"✓ Saved shareholding for {symbol}")
        
        upsert_price_history(conn, data)
        logger.debug(f"✓ Saved price history for {symbol}")
        
        logger.info(f"✓ Successfully saved {symbol} to database")
        
    except Exception as e:
        logger.error(f"✗ Failed to save company data: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Quick test to verify DB connection and table access
    conn = connect_db()
    try:
        tables = ["companies", "profit_and_loss", "balance_sheet", "shareholder_pattern", "price_history"]
        for table in tables:
            columns = _table_columns(conn, table)
            logger.info(f"Table '{table}' columns: {columns}")
    finally:
        conn.close()
