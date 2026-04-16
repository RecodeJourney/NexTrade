import contextlib
import sqlite3
import sys
import time
from typing import Any, Iterable, Mapping

from db_writer import (
    connect_db,
    insert_balance_sheet,
    insert_raw_json,
    insert_scrape_log,
    insert_shareholding,
    insert_yearly_results,
    upsert_company,
    upsert_price_history,
    upsert_ratios,
)
from scraper import scrape_company


symbols = ["RELIANCE", "TCS", "INFY"]


def save_company_data(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    upsert_company(conn, data)
    upsert_ratios(conn, data)
    insert_yearly_results(conn, data)
    insert_balance_sheet(conn, data)
    insert_shareholding(conn, data)
    upsert_price_history(conn, data)
    insert_raw_json(conn, data)


def run(symbols_to_scrape: Iterable[str] = symbols) -> None:
    conn = connect_db()
    try:
        for symbol in symbols_to_scrape:
            start = time.perf_counter()
            try:
                data = scrape_company(symbol)
                duration_seconds = time.perf_counter() - start

                with conn:
                    save_company_data(conn, data)
                    insert_scrape_log(
                        conn,
                        symbol=data["symbol"],
                        status="success",
                        duration_seconds=duration_seconds,
                    )

                print(f"{data['symbol']}: success in {duration_seconds:.2f}s")
            except Exception as exc:
                duration_seconds = time.perf_counter() - start
                with contextlib.suppress(Exception):
                    with conn:
                        insert_scrape_log(
                            conn,
                            symbol=symbol,
                            status="failure",
                            duration_seconds=duration_seconds,
                            error_message=str(exc),
                        )
                print(f"{symbol}: failure in {duration_seconds:.2f}s: {exc}")
    finally:
        conn.close()


if __name__ == "__main__":
    run(sys.argv[1:] or symbols)
