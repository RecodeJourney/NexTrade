import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, Optional
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup, Tag

from utils import clean_number, normalize_key, normalize_period


BASE_URL = "https://www.screener.in/company/{symbol}/consolidated/"
DEFAULT_SYMBOLS = ("RELIANCE",)
RUNTIME_JSON_DIR = "json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
}


def normalize_symbol(symbol: str) -> str:
    clean_symbol = symbol.strip().upper()
    if not clean_symbol:
        raise ValueError("symbol cannot be empty")
    return clean_symbol


def build_company_url(symbol: str) -> str:
    return BASE_URL.format(symbol=quote(normalize_symbol(symbol), safe=""))


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def _text_or_none(element: Optional[Tag]) -> Optional[str]:
    if not element:
        return None
    text = element.get_text(" ", strip=True)
    return text or None


def extract_company_info(html: str, symbol: str, url: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    profile = soup.select_one(".company-profile")

    info: Dict[str, Optional[str]] = {
        "symbol": normalize_symbol(symbol),
        "company_name": _text_or_none(soup.select_one("h1")),
        "description": _text_or_none(soup.select_one(".company-profile .about"))
        or _text_or_none(soup.select_one(".about")),
        "source_url": url,
    }

    if profile:
        for link in profile.select("a[href]"):
            label = link.get_text(" ", strip=True).lower()
            href = link.get("href")
            if not href:
                continue
            if label == "website":
                info["website"] = href
            elif label == "bse":
                info["bse_url"] = href
            elif label == "nse":
                info["nse_url"] = href

    return info


def extract_top_ratios(html: str) -> Dict[str, Optional[float]]:
    soup = BeautifulSoup(html, "html.parser")
    ratios: Dict[str, Optional[float]] = {}

    for item in soup.select("#top-ratios li"):
        name_elem = item.select_one(".name")
        value_elem = item.select_one(".value")
        if not name_elem or not value_elem:
            continue

        name = normalize_key(name_elem.get_text(" ", strip=True))
        value = value_elem.get_text(" ", strip=True)
        ratios[name] = clean_number(value)

    return ratios


def _transpose_metric_table(table: Optional[Tag]) -> Dict[str, Dict[str, Optional[float]]]:
    if not table:
        return {}

    headers = [
        normalize_period(th.get_text(" ", strip=True))
        for th in table.select("thead th")[1:]
    ]
    table_data: Dict[str, Dict[str, Optional[float]]] = {
        header: {} for header in headers if header
    }

    for row in table.select("tbody tr"):
        cols = row.select("td")
        if not cols:
            continue

        metric = normalize_key(cols[0].get_text(" ", strip=True))
        values = [td.get_text(" ", strip=True) for td in cols[1:]]

        for period, value in zip(headers, values):
            if not period:
                continue
            table_data.setdefault(period, {})[metric] = clean_number(value)

    return table_data


def extract_profit_loss_table(html: str) -> Dict[str, Dict[str, Optional[float]]]:
    soup = BeautifulSoup(html, "html.parser")
    return _transpose_metric_table(soup.select_one("#profit-loss table.data-table"))


def extract_balance_sheet_table(html: str) -> Dict[str, Dict[str, Optional[float]]]:
    soup = BeautifulSoup(html, "html.parser")
    return _transpose_metric_table(soup.select_one("#balance-sheet table.data-table"))


def extract_ratios_table(html: str) -> Dict[str, Dict[str, Optional[float]]]:
    soup = BeautifulSoup(html, "html.parser")
    return _transpose_metric_table(soup.select_one("#ratios table.data-table"))


def extract_shareholding(html: str) -> Dict[str, Dict[str, Optional[float]]]:
    soup = BeautifulSoup(html, "html.parser")
    result: Dict[str, Dict[str, Optional[float]]] = {}

    sources = (
        ("quarterly", soup.select_one("#quarterly-shp table")),
        ("yearly", soup.select_one("#yearly-shp table")),
    )

    for source, table in sources:
        table_data = _transpose_metric_table(table)
        for period, metrics in table_data.items():
            # Screener has duplicate labels across quarterly and yearly tables.
            # Prefixing the source preserves both tables in one period-keyed dict.
            result[f"{source}:{period}"] = metrics

    return result


def scrape_company(symbol: str) -> Dict[str, Any]:
    clean_symbol = normalize_symbol(symbol)
    url = build_company_url(clean_symbol)
    html = fetch_html(url)

    return {
        "symbol": clean_symbol,
        "company_info": extract_company_info(html, clean_symbol, url),
        "top_ratios": extract_top_ratios(html),
        "profit_loss": extract_profit_loss_table(html),
        "balance_sheet": extract_balance_sheet_table(html),
        "ratios_history": extract_ratios_table(html),
        "shareholding": extract_shareholding(html),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def save_runtime_json(data: Dict[str, Any], output_dir: str = RUNTIME_JSON_DIR) -> Dict[str, Path]:
    symbol = normalize_symbol(str(data["symbol"]))
    symbol_dir = Path(output_dir) / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)

    # latest.json is the full scrape snapshot for quick inspection.
    written_files = {
        "latest": symbol_dir / "latest.json",
        "company_info": symbol_dir / "company_info.json",
        "top_ratios": symbol_dir / "top_ratios.json",
        "profit_loss": symbol_dir / "profit_loss.json",
        "balance_sheet": symbol_dir / "balance_sheet.json",
        "ratios_history": symbol_dir / "ratios_history.json",
        "shareholding": symbol_dir / "shareholding.json",
    }

    written_files["latest"].write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    for section in (
        "company_info",
        "top_ratios",
        "profit_loss",
        "balance_sheet",
        "ratios_history",
        "shareholding",
    ):
        written_files[section].write_text(
            json.dumps(data.get(section, {}), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    return written_files


def _print_summary(data: Dict[str, Any], sections: Iterable[str]) -> None:
    print(json.dumps({key: data.get(key) for key in sections}, indent=2))


def main() -> None:
    symbols = tuple(sys.argv[1:]) or DEFAULT_SYMBOLS

    for symbol in symbols:
        data = scrape_company(symbol)
        written_files = save_runtime_json(data)
        _print_summary(
            data,
            (
                "symbol",
                "company_info",
                "top_ratios",
                "profit_loss",
                "balance_sheet",
                "ratios_history",
                "shareholding",
            ),
        )
        print(f"\nSaved JSON files under: {written_files['latest'].parent}")


if __name__ == "__main__":
    main()
