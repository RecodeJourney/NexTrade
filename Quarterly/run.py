import sqlite3
import sys
import time
from typing import Any, Iterable, Mapping
import logging
import json
from pathlib import Path


# Add parent directory to path for direct execution
sys_path_insert = Path(__file__).resolve().parent.parent
if str(sys_path_insert) not in sys.path:
    sys.path.insert(0, str(sys_path_insert))

from Quarterly.db_writer import (
    connect_db,
    save_company_to_db,
)
from Quarterly.scraper import scrape_company
from Utils import get_logger

logger = get_logger()


symbols = ["RELIANCE", "TCS", "INFY"]


def save_company_data(conn: sqlite3.Connection, data: Mapping[str, Any]) -> None:
    logger.debug(f"Preparing to save {data['symbol']} to database...")
    save_company_to_db(conn, data)


def run(symbols_to_scrape: Iterable[str] = symbols) -> None:
    logger.info(f"Starting scrape for symbols: {list(symbols_to_scrape)}")
    
    conn = connect_db()
    try:
        for symbol in symbols_to_scrape:
            start = time.perf_counter()
            logger.info(f"🔄 Processing {symbol}...")
            
            try:
                logger.debug(f"Scraping data for {symbol}...")
                data = scrape_company(symbol)
                duration_seconds = time.perf_counter() - start
                logger.debug(f"✓ Scrape completed in {duration_seconds:.2f}s")

                logger.debug(f"Starting database transaction for {symbol}...")
                with conn:
                    save_company_data(conn, data)
                    conn.commit()
                
                logger.info(f"✓ {symbol}: success in {duration_seconds:.2f}s")
                
            except Exception as exc:
                duration_seconds = time.perf_counter() - start
                logger.error(f"✗ {symbol}: failure in {duration_seconds:.2f}s", exc_info=True)
                logger.error(f"Error details: {exc}")
    
    finally:
        logger.info("Closing database connection...")
        conn.close()
        logger.info("✓ Database connection closed")


if __name__ == "__main__":
    logger.info("="*60)
    logger.info("NexTrade Quarterly Scraper Starting")
    logger.info("="*60)
    
    DEFAULT_SYMBOLS = ("RELIANCE",)
    symbols_to_run = tuple(sys.argv[1:]) or DEFAULT_SYMBOLS
    
    logger.info(f"Symbols to process: {symbols_to_run}")
    logger.info("Running scraper with database save...")
    logger.info("-"*60)
    
    try:
        run(symbols_to_run)
        logger.info("="*60)
        logger.info("✓ Scraper completed successfully")
        logger.info("="*60)
    except Exception as e:
        logger.error("="*60)
        logger.error(f"✗ Scraper failed: {e}", exc_info=True)
        logger.error("="*60)
        sys.exit(1)
