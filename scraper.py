import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, Optional
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright

from utils import clean_number, normalize_key, normalize_period


BASE_URL = "https://www.screener.in/company/{symbol}/consolidated/"
DEFAULT_SYMBOLS = ("RELIANCE",)
RUNTIME_JSON_DIR = "json"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

QUICK_RATIOS = [
    "Profit growth",
    "Promoter holding",
    "Sales growth",
    "Debt to equity"
]


# ---------------- PLAYWRIGHT BLOCK ---------------- #

def login(page):
    page.goto("https://www.screener.in/login/")

    page.fill('input[name="username"]', "angelscur@gmail.com")
    page.fill('input[name="password"]', "Nava@#1352")

    page.click('button[type="submit"]')
    page.wait_for_timeout(2000)

    if "login" in page.url:
        print("❌ Login failed")
        return False
    else:
        print("✅ Login successful")
        return True


def add_quick_ratio(page, text):
    search = page.locator("#quick-ratio-search")

    search.click()
    search.fill("")
    search.type(text, delay=100)

    page.wait_for_timeout(500)

    options = page.locator(".dropdown-content li")

    for i in range(options.count()):
        opt = options.nth(i)
        if text.lower() in opt.inner_text().lower():
            opt.click()
            break

    try:
        page.wait_for_selector("li[data-source='quick-ratio']", timeout=3000)
    except:
        print(f"⚠️ Failed to add: {text}")

    page.wait_for_timeout(500)


def fetch_html_playwright(symbol: str) -> str:
    url = BASE_URL.format(symbol=symbol)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        success = login(page)
        if not success:
            browser.close()
            raise Exception("Login failed")

        page.goto(url)
        page.wait_for_selector("#top-ratios")

        for ratio in QUICK_RATIOS:
            add_quick_ratio(page, ratio)

        html = page.content()
        browser.close()
        return html


# ---------------- REQUESTS FALLBACK ---------------- #

def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


# ---------------- EXISTING CODE ---------------- #

def normalize_symbol(symbol: str) -> str:
    clean_symbol = symbol.strip().upper()
    if not clean_symbol:
        raise ValueError("symbol cannot be empty")
    return clean_symbol


def _text_or_none(element: Optional[Tag]) -> Optional[str]:
    if not element:
        return None
    return element.get_text(" ", strip=True) or None


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


def extract_company_info(html: str, symbol: str, url: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    profile = soup.select_one(".company-profile")

    info: Dict[str, Optional[str]] = {
        "symbol": normalize_symbol(symbol),
        "company_name": _text_or_none(soup.select_one("h1")),
        "description": _text_or_none(soup.select_one(".company-profile .about")),
        "source_url": url,
    }

    return info


def extract_top_ratios(html: str) -> Dict[str, Optional[float]]:
    soup = BeautifulSoup(html, "html.parser")
    ratios: Dict[str, Optional[float]] = {}

    for item in soup.select("#top-ratios li"):
        name = normalize_key(item.select_one(".name").get_text(" ", strip=True))
        value = item.select_one(".value").get_text(" ", strip=True)
        ratios[name] = clean_number(value)

    return ratios


def extract_profit_loss_table(html: str):
    soup = BeautifulSoup(html, "html.parser")
    return _transpose_metric_table(soup.select_one("#profit-loss table.data-table"))


def extract_balance_sheet_table(html: str):
    soup = BeautifulSoup(html, "html.parser")
    return _transpose_metric_table(soup.select_one("#balance-sheet table.data-table"))


def extract_ratios_table(html: str):
    soup = BeautifulSoup(html, "html.parser")
    return _transpose_metric_table(soup.select_one("#ratios table.data-table"))


def extract_shareholding(html: str):
    soup = BeautifulSoup(html, "html.parser")
    return _transpose_metric_table(soup.select_one("#quarterly-shp table"))


# ---------------- MAIN ---------------- #

def scrape_company(symbol: str) -> Dict[str, Any]:
    clean_symbol = normalize_symbol(symbol)
    url = BASE_URL.format(symbol=quote(clean_symbol))

    try:
        html = fetch_html_playwright(clean_symbol)
    except Exception as e:
        print("⚠️ Playwright failed, falling back:", e)
        html = fetch_html(url)

    data = {
        "symbol": clean_symbol,
        "company_info": extract_company_info(html, clean_symbol, url),
        "top_ratios": extract_top_ratios(html),
        "profit_loss": extract_profit_loss_table(html),
        "balance_sheet": extract_balance_sheet_table(html),
        "ratios_history": extract_ratios_table(html),
        "shareholding": extract_shareholding(html),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }

    return data


def main():
    symbols = tuple(sys.argv[1:]) or DEFAULT_SYMBOLS

    for symbol in symbols:
        data = scrape_company(symbol)
        print(json.dumps(data["top_ratios"], indent=2))


if __name__ == "__main__":
    main()