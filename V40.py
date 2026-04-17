"""Quarterly V40 category classifier.

Flow:
1. Read companies from SQLite.
2. Read manual V40 research fields from data/v40_research.json.
3. Fill missing research fields from local scraped data and public web APIs.
4. Check government filter, business age, future growth, and debt condition.
5. Print/write a report for audit.
6. Update companies.category.
"""

import argparse
import json
import os
import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = Path(os.environ.get("SCREENER_DB_PATH", BASE_DIR / "screener.db"))
DEFAULT_RESEARCH_PATH = BASE_DIR / "data" / "v40_research.json"
DEFAULT_REPORT_DIR = BASE_DIR / "reports"

V40_CATEGORY = "V40"
PASS = "PASS"
FAIL = "FAIL"
REVIEW = "REVIEW"

# These values are intentionally near the top so quarterly tuning is simple.
MIN_BUSINESS_AGE_YEARS = 15
MIN_FUTURE_GROWTH_YEARS = 15
MAX_BORROWINGS_TO_RESERVES = 0.25
MIN_SALES_CAGR_FOR_AUTO_GROWTH = 5.0
MIN_SALES_HISTORY_YEARS = 7
USER_AGENT = "NexTrade-V40/1.0"


@dataclass
class CompanyEvaluation:
    symbol: str
    company_name: Optional[str]
    current_category: Optional[str]
    status: str
    reasons: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    planned_db_action: str = "no_change"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate V40 criteria and update companies.category."
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        help="Optional symbols to evaluate. If omitted, all companies are evaluated.",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="SQLite database path. Defaults to NexTrade/screener.db or SCREENER_DB_PATH.",
    )
    parser.add_argument(
        "--research-path",
        default=str(DEFAULT_RESEARCH_PATH),
        help="JSON file containing manual V40 research fields.",
    )
    parser.add_argument(
        "--as-of-year",
        type=int,
        default=datetime.now().year,
        help="Year used for business-age calculation.",
    )
    parser.add_argument(
        "--max-borrowings-to-reserves",
        type=float,
        default=MAX_BORROWINGS_TO_RESERVES,
        help="Maximum allowed borrowings / reserves ratio.",
    )
    parser.add_argument(
        "--report-dir",
        default=str(DEFAULT_REPORT_DIR),
        help="Directory where the run report JSON is written.",
    )
    return parser.parse_args()


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_research(path: Path) -> Dict[str, Mapping[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Research JSON file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)

    if not isinstance(raw, dict):
        raise ValueError("Research JSON must contain an object at the top level.")

    # Support either {"SYMBOL": {...}} or {"companies": {"SYMBOL": {...}}}.
    company_block = raw.get("companies", raw)
    if not isinstance(company_block, dict):
        raise ValueError("Research JSON 'companies' field must be an object.")

    research: Dict[str, Mapping[str, Any]] = {}
    for symbol, payload in company_block.items():
        if str(symbol).startswith("_"):
            continue
        if not isinstance(payload, dict):
            continue
        research[str(symbol).upper()] = payload
    return research


def has_value(value: Any) -> bool:
    return value is not None and value != ""


def set_if_missing(target: Dict[str, Any], key: str, value: Any) -> None:
    if has_value(target.get(key)) or not has_value(value):
        return
    target[key] = value


def http_get_json(url: str, params: Optional[Mapping[str, Any]] = None) -> Optional[Any]:
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            return json.load(response)
    except (OSError, ValueError, urllib.error.HTTPError, urllib.error.URLError):
        return None


def load_project_json(path: Path) -> Optional[Mapping[str, Any]]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def fetch_latest_raw_json(
    conn: sqlite3.Connection,
    symbol: str,
) -> Optional[Mapping[str, Any]]:
    row = conn.execute(
        """
        SELECT raw_json
        FROM screener_raw_json
        WHERE symbol = ?
        ORDER BY scrape_time DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()
    if row is None or not row["raw_json"]:
        return None
    try:
        payload = json.loads(row["raw_json"])
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def company_info_from_local_data(
    conn: sqlite3.Connection,
    symbol: str,
) -> Mapping[str, Any]:
    file_payload = load_project_json(BASE_DIR / "json" / symbol / "company_info.json")
    if file_payload:
        return file_payload

    raw_payload = fetch_latest_raw_json(conn, symbol)
    company_info = (raw_payload or {}).get("company_info", {})
    return company_info if isinstance(company_info, dict) else {}


def latest_period_payload(data: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    if not data:
        return None
    latest_key = next(reversed(data.keys()))
    latest_value = data.get(latest_key)
    return latest_value if isinstance(latest_value, dict) else None


def latest_government_ownership_from_local_json(symbol: str) -> Optional[float]:
    shareholding = load_project_json(BASE_DIR / "json" / symbol / "shareholding.json")
    latest = latest_period_payload(shareholding or {})
    if not latest:
        return None
    return to_float(latest.get("government"))


def business_year_from_text(text: str) -> Optional[int]:
    if not text:
        return None

    patterns = (
        r"\b(?:founded|established|incorporated|started|set up)\D{0,80}"
        r"((?:18|19|20)\d{2})\b",
        r"\b((?:18|19|20)\d{2})\D{0,80}"
        r"(?:founded|established|incorporated|started|set up)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def clean_company_search_name(company_name: str) -> str:
    cleaned = re.sub(r"\b(ltd|limited)\.?\b", "", company_name, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or company_name


def wikidata_search_terms(symbol: str, company_name: Optional[str]) -> List[str]:
    terms: List[str] = []
    if company_name:
        terms.append(company_name)
        cleaned = clean_company_search_name(company_name)
        if cleaned != company_name:
            terms.append(cleaned)
    terms.append(symbol)
    return list(dict.fromkeys(term for term in terms if term))


def search_wikidata_entity(symbol: str, company_name: Optional[str]) -> Optional[str]:
    for term in wikidata_search_terms(symbol, company_name):
        payload = http_get_json(
            "https://www.wikidata.org/w/api.php",
            {
                "action": "wbsearchentities",
                "search": term,
                "language": "en",
                "format": "json",
                "limit": 3,
            },
        )
        if not isinstance(payload, dict):
            continue
        for result in payload.get("search", []):
            label = str(result.get("label", "")).lower()
            description = str(result.get("description", "")).lower()
            if symbol.lower() in label or "company" in description:
                return result.get("id")
    return None


def wikidata_year_from_time(value: Mapping[str, Any]) -> Optional[int]:
    time_value = value.get("time")
    if not isinstance(time_value, str):
        return None
    match = re.search(r"([+-]?\d{4})", time_value)
    if not match:
        return None
    return abs(int(match.group(1)))


def business_start_year_from_wikidata(
    symbol: str,
    company_name: Optional[str],
) -> Tuple[Optional[int], Optional[str]]:
    entity_id = search_wikidata_entity(symbol, company_name)
    if not entity_id:
        return None, None

    payload = http_get_json(
        f"https://www.wikidata.org/wiki/Special:EntityData/{entity_id}.json"
    )
    if not isinstance(payload, dict):
        return None, None

    entity = payload.get("entities", {}).get(entity_id, {})
    claims = entity.get("claims", {}).get("P571", [])
    for claim in claims:
        value = (
            claim.get("mainsnak", {})
            .get("datavalue", {})
            .get("value")
        )
        if isinstance(value, dict):
            year = wikidata_year_from_time(value)
            if year:
                return year, f"wikidata:{entity_id}:P571"
    return None, None


def fetch_sales_history(
    conn: sqlite3.Connection,
    symbol: str,
) -> List[Tuple[int, float]]:
    rows = conn.execute(
        """
        SELECT year_end, sales
        FROM yearly_results
        WHERE symbol = ? AND sales IS NOT NULL AND sales > 0
        ORDER BY year_end
        """,
        (symbol,),
    ).fetchall()
    return [(int(row["year_end"]), float(row["sales"])) for row in rows]


def estimate_future_growth_from_sales(
    conn: sqlite3.Connection,
    symbol: str,
) -> Tuple[Optional[float], Optional[str], Dict[str, Any]]:
    history = fetch_sales_history(conn, symbol)
    if len(history) < MIN_SALES_HISTORY_YEARS:
        return None, None, {"sales_history_years": len(history)}

    start_year, start_sales = history[0]
    end_year, end_sales = history[-1]
    years = end_year - start_year
    metrics: Dict[str, Any] = {
        "sales_start_year": start_year,
        "sales_end_year": end_year,
        "sales_start": start_sales,
        "sales_end": end_sales,
        "sales_history_years": years,
    }

    if years <= 0 or start_sales <= 0:
        return None, None, metrics

    cagr = ((end_sales / start_sales) ** (1 / years) - 1) * 100
    metrics["sales_cagr_percent"] = cagr

    if cagr < MIN_SALES_CAGR_FOR_AUTO_GROWTH:
        reason = (
            f"Historical sales CAGR is {cagr:.2f}% from {start_year} to {end_year}, "
            f"below the {MIN_SALES_CAGR_FOR_AUTO_GROWTH:.2f}% auto-growth threshold."
        )
        return 0, reason, metrics

    reason = (
        f"Auto-estimated {MIN_FUTURE_GROWTH_YEARS} years from "
        f"{cagr:.2f}% sales CAGR between {start_year} and {end_year}; "
        "manual long-term thesis should still be reviewed."
    )
    return float(MIN_FUTURE_GROWTH_YEARS), reason, metrics


def auto_enrich_research(
    conn: sqlite3.Connection,
    company: sqlite3.Row,
) -> Dict[str, Any]:
    symbol = str(company["symbol"]).upper()
    company_name = company["company_name"]
    enriched: Dict[str, Any] = {"sources": {}, "auto_metrics": {}}

    government_ownership = latest_government_ownership_from_local_json(symbol)
    if government_ownership is not None:
        enriched["government_ownership_percent"] = government_ownership
        enriched["is_government_company"] = government_ownership >= 50
        enriched["sources"]["government_ownership_percent"] = "local_shareholding_json"
        enriched["sources"]["is_government_company"] = "derived_from_government_ownership"

    company_info = company_info_from_local_data(conn, symbol)
    description = str(company_info.get("description") or "")
    year = business_year_from_text(description)
    if year:
        enriched["business_start_year"] = year
        enriched["sources"]["business_start_year"] = "local_company_description"
    else:
        year, source = business_start_year_from_wikidata(symbol, company_name)
        if year:
            enriched["business_start_year"] = year
            enriched["sources"]["business_start_year"] = source

    growth_years, growth_reason, growth_metrics = estimate_future_growth_from_sales(
        conn, symbol
    )
    enriched["auto_metrics"].update(growth_metrics)
    if growth_years is not None:
        enriched["future_growth_years"] = growth_years
        enriched["growth_reason"] = growth_reason
        enriched["sources"]["future_growth_years"] = "derived_from_sales_cagr"
        enriched["sources"]["growth_reason"] = "derived_from_sales_cagr"

    return enriched


def merge_research(
    manual: Mapping[str, Any],
    enriched: Mapping[str, Any],
) -> Dict[str, Any]:
    merged = dict(manual)
    sources = dict(manual.get("sources") or {})
    auto_sources = dict(enriched.get("sources") or {})

    for key, value in enriched.items():
        if key in {"sources", "auto_metrics"}:
            continue
        set_if_missing(merged, key, value)

    for key, value in auto_sources.items():
        sources.setdefault(key, value)
    if sources:
        merged["sources"] = sources

    auto_metrics = dict(enriched.get("auto_metrics") or {})
    if auto_metrics:
        merged["auto_metrics"] = auto_metrics

    return merged


def build_effective_research(
    conn: sqlite3.Connection,
    companies: Sequence[sqlite3.Row],
    research_by_symbol: Mapping[str, Mapping[str, Any]],
    auto_enrich: bool,
) -> Dict[str, Mapping[str, Any]]:
    effective: Dict[str, Mapping[str, Any]] = {}
    for company in companies:
        symbol = str(company["symbol"]).upper()
        manual = dict(research_by_symbol.get(symbol, {}))
        if auto_enrich:
            enriched = auto_enrich_research(conn, company)
            effective[symbol] = merge_research(manual, enriched)
        else:
            effective[symbol] = manual
    return effective


def update_research_file(
    path: Path,
    effective_research: Mapping[str, Mapping[str, Any]],
) -> None:
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)

    company_block = raw.setdefault("companies", {}) if "companies" in raw else raw
    for symbol, effective in effective_research.items():
        row = company_block.setdefault(symbol, {})
        if not isinstance(row, dict):
            row = {}
            company_block[symbol] = row

        for key in (
            "business_start_year",
            "future_growth_years",
            "government_ownership_percent",
            "growth_reason",
            "is_government_company",
        ):
            if not has_value(row.get(key)) and has_value(effective.get(key)):
                row[key] = effective[key]

        if isinstance(effective.get("sources"), dict):
            sources = row.setdefault("sources", {})
            if isinstance(sources, dict):
                for key, value in effective["sources"].items():
                    sources.setdefault(key, value)

    with path.open("w", encoding="utf-8") as handle:
        json.dump(raw, handle, indent=2, sort_keys=True)
        handle.write("\n")


def fetch_companies(
    conn: sqlite3.Connection,
    symbols: Sequence[str],
) -> List[sqlite3.Row]:
    if symbols:
        placeholders = ", ".join("?" for _ in symbols)
        return conn.execute(
            f"""
            SELECT symbol, company_name, category
            FROM companies
            WHERE upper(symbol) IN ({placeholders})
            ORDER BY symbol
            """,
            [symbol.upper() for symbol in symbols],
        ).fetchall()

    return conn.execute(
        """
        SELECT symbol, company_name, category
        FROM companies
        ORDER BY symbol
        """
    ).fetchall()


def fetch_latest_balance_sheet(
    conn: sqlite3.Connection,
    symbol: str,
) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT year_end, borrowings, reserves
        FROM balance_sheet
        WHERE symbol = ?
        ORDER BY year_end DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_int(value: Any) -> Optional[int]:
    number = to_float(value)
    if number is None:
        return None
    return int(number)


def get_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "yes", "y", "1"}:
        return True
    if text in {"false", "no", "n", "0"}:
        return False
    return None


def merge_status(current: str, candidate: str) -> str:
    # FAIL is strongest, REVIEW means data is incomplete, PASS is clean.
    priority = {PASS: 0, REVIEW: 1, FAIL: 2}
    return candidate if priority[candidate] > priority[current] else current


def evaluate_government_filter(
    research: Mapping[str, Any],
) -> Tuple[str, List[str], Dict[str, Any]]:
    reasons: List[str] = []
    metrics: Dict[str, Any] = {}

    is_government_company = get_bool(research.get("is_government_company"))
    government_ownership = to_float(research.get("government_ownership_percent"))

    metrics["is_government_company"] = is_government_company
    metrics["government_ownership_percent"] = government_ownership

    if is_government_company is True:
        reasons.append("Fail: government/PSU controlled company.")
        return FAIL, reasons, metrics

    if government_ownership is not None and government_ownership >= 50:
        reasons.append(
            f"Fail: government ownership is {government_ownership:.2f}%."
        )
        return FAIL, reasons, metrics

    if is_government_company is False:
        reasons.append("Pass: marked as non-government company.")
        return PASS, reasons, metrics

    if government_ownership is not None and government_ownership < 50:
        reasons.append(
            f"Pass: government ownership is {government_ownership:.2f}%."
        )
        return PASS, reasons, metrics

    reasons.append("Review: government/company ownership classification is missing.")
    return REVIEW, reasons, metrics


def evaluate_business_age(
    research: Mapping[str, Any],
    as_of_year: int,
) -> Tuple[str, List[str], Dict[str, Any]]:
    reasons: List[str] = []
    metrics: Dict[str, Any] = {}

    business_start_year = to_int(research.get("business_start_year"))
    metrics["business_start_year"] = business_start_year

    if business_start_year is None:
        reasons.append("Review: business_start_year is missing.")
        return REVIEW, reasons, metrics

    business_age = as_of_year - business_start_year
    metrics["business_age_years"] = business_age

    if business_age >= MIN_BUSINESS_AGE_YEARS:
        reasons.append(f"Pass: business age is {business_age} years.")
        return PASS, reasons, metrics

    reasons.append(
        f"Fail: business age is {business_age} years; "
        f"minimum is {MIN_BUSINESS_AGE_YEARS}."
    )
    return FAIL, reasons, metrics


def evaluate_future_growth(
    research: Mapping[str, Any],
) -> Tuple[str, List[str], Dict[str, Any]]:
    reasons: List[str] = []
    metrics: Dict[str, Any] = {}

    future_growth_years = to_float(research.get("future_growth_years"))
    metrics["future_growth_years"] = future_growth_years
    metrics["growth_reason"] = research.get("growth_reason")

    if future_growth_years is None:
        reasons.append("Review: future_growth_years is missing.")
        return REVIEW, reasons, metrics

    if future_growth_years >= MIN_FUTURE_GROWTH_YEARS:
        reasons.append(
            f"Pass: future growth visibility is {future_growth_years:g} years."
        )
        return PASS, reasons, metrics

    reasons.append(
        f"Fail: future growth visibility is {future_growth_years:g} years; "
        f"minimum is {MIN_FUTURE_GROWTH_YEARS}."
    )
    return FAIL, reasons, metrics


def evaluate_debt_filter(
    conn: sqlite3.Connection,
    symbol: str,
    max_borrowings_to_reserves: float,
) -> Tuple[str, List[str], Dict[str, Any]]:
    reasons: List[str] = []
    metrics: Dict[str, Any] = {}
    row = fetch_latest_balance_sheet(conn, symbol)

    if row is None:
        reasons.append("Review: no balance_sheet row found.")
        return REVIEW, reasons, metrics

    borrowings = to_float(row["borrowings"])
    reserves = to_float(row["reserves"])
    metrics.update(
        {
            "balance_sheet_year_end": row["year_end"],
            "borrowings": borrowings,
            "reserves": reserves,
        }
    )

    if borrowings is None or reserves is None:
        reasons.append("Review: borrowings or reserves are missing.")
        return REVIEW, reasons, metrics

    if borrowings <= 0:
        reasons.append("Pass: borrowings are zero or negative.")
        metrics["borrowings_to_reserves"] = 0
        return PASS, reasons, metrics

    if reserves <= 0:
        reasons.append("Fail: borrowings exist but reserves are zero or negative.")
        return FAIL, reasons, metrics

    ratio = borrowings / reserves
    metrics["borrowings_to_reserves"] = ratio

    if ratio <= max_borrowings_to_reserves:
        reasons.append(
            "Pass: borrowings/reserves is "
            f"{ratio:.2f}, within {max_borrowings_to_reserves:.2f}."
        )
        return PASS, reasons, metrics

    reasons.append(
        "Fail: borrowings/reserves is "
        f"{ratio:.2f}, above {max_borrowings_to_reserves:.2f}."
    )
    return FAIL, reasons, metrics


def planned_db_action(status: str, current_category: Optional[str]) -> str:
    if status == PASS:
        if current_category == V40_CATEGORY:
            return "keep_V40"
        return "set_V40"

    if status in {FAIL, REVIEW} and current_category == V40_CATEGORY:
        return "remove_V40"

    return "no_change"


def evaluate_company(
    conn: sqlite3.Connection,
    company: sqlite3.Row,
    research_by_symbol: Mapping[str, Mapping[str, Any]],
    as_of_year: int,
    max_borrowings_to_reserves: float,
) -> CompanyEvaluation:
    symbol = str(company["symbol"]).upper()
    research = research_by_symbol.get(symbol, {})
    status = PASS
    reasons: List[str] = []
    metrics: Dict[str, Any] = {}

    if isinstance(research.get("sources"), dict):
        metrics["sources"] = dict(research["sources"])
    if isinstance(research.get("auto_metrics"), dict):
        metrics["auto_metrics"] = dict(research["auto_metrics"])

    # Each check returns PASS/FAIL/REVIEW plus reason text. The final status is
    # the strongest result: FAIL beats REVIEW, REVIEW beats PASS.
    for check_status, check_reasons, check_metrics in (
        evaluate_government_filter(research),
        evaluate_business_age(research, as_of_year),
        evaluate_future_growth(research),
        evaluate_debt_filter(conn, symbol, max_borrowings_to_reserves),
    ):
        status = merge_status(status, check_status)
        reasons.extend(check_reasons)
        metrics.update(check_metrics)

    current_category = company["category"]
    action = planned_db_action(status, current_category)

    return CompanyEvaluation(
        symbol=symbol,
        company_name=company["company_name"],
        current_category=current_category,
        status=status,
        reasons=reasons,
        metrics=metrics,
        planned_db_action=action,
    )


def apply_category_changes(
    conn: sqlite3.Connection,
    evaluations: Iterable[CompanyEvaluation],
) -> Dict[str, int]:
    counts = {"set_V40": 0, "keep_V40": 0, "remove_V40": 0, "no_change": 0}

    for evaluation in evaluations:
        action = evaluation.planned_db_action

        if action == "set_V40":
            # Setting one text category automatically replaces V40next/V200/NULL.
            conn.execute(
                "UPDATE companies SET category = ? WHERE symbol = ?",
                (V40_CATEGORY, evaluation.symbol),
            )
            counts["set_V40"] += 1
        elif action == "keep_V40":
            counts["keep_V40"] += 1
        elif action == "remove_V40":
            # V40.py only removes V40. It does not remove V40next or V200.
            conn.execute(
                "UPDATE companies SET category = NULL WHERE symbol = ? AND category = ?",
                (evaluation.symbol, V40_CATEGORY),
            )
            counts["remove_V40"] += 1
        else:
            counts["no_change"] += 1

    return counts


def write_report(
    report_dir: Path,
    evaluations: Sequence[CompanyEvaluation],
    args: argparse.Namespace,
    applied_counts: Optional[Mapping[str, int]],
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = report_dir / f"v40_{stamp}.json"

    status_counts = {
        PASS: sum(1 for item in evaluations if item.status == PASS),
        FAIL: sum(1 for item in evaluations if item.status == FAIL),
        REVIEW: sum(1 for item in evaluations if item.status == REVIEW),
    }

    payload = {
        "generated_at": generated_at,
        "mode": "update",
        "settings": {
            "as_of_year": args.as_of_year,
            "min_business_age_years": MIN_BUSINESS_AGE_YEARS,
            "min_future_growth_years": MIN_FUTURE_GROWTH_YEARS,
            "max_borrowings_to_reserves": args.max_borrowings_to_reserves,
            "auto_enrich": True,
            "update_research": True,
            "db_path": str(Path(args.db_path)),
            "research_path": str(Path(args.research_path)),
        },
        "status_counts": status_counts,
        "db_action_counts": applied_counts,
        "companies": [asdict(item) for item in evaluations],
    }

    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return path


def print_summary(
    evaluations: Sequence[CompanyEvaluation],
    applied_counts: Mapping[str, int],
    report_path: Path,
) -> None:
    print("Mode: update")
    print(f"Report: {report_path}")
    print()

    for evaluation in evaluations:
        print(
            f"{evaluation.symbol}: {evaluation.status} "
            f"({evaluation.planned_db_action})"
        )
        for reason in evaluation.reasons:
            print(f"  - {reason}")
        print()

    print("Database actions:")
    for action, count in applied_counts.items():
        print(f"  {action}: {count}")


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    research_path = Path(args.research_path)
    report_dir = Path(args.report_dir)
    symbols = [symbol.upper() for symbol in args.symbols]

    research_by_symbol = load_research(research_path)
    conn = connect_db(db_path)

    try:
        companies = fetch_companies(conn, symbols)
        if not companies:
            raise RuntimeError("No matching companies found in the database.")

        effective_research = build_effective_research(
            conn=conn,
            companies=companies,
            research_by_symbol=research_by_symbol,
            auto_enrich=True,
        )
        update_research_file(research_path, effective_research)

        evaluations = [
            evaluate_company(
                conn=conn,
                company=company,
                research_by_symbol=effective_research,
                as_of_year=args.as_of_year,
                max_borrowings_to_reserves=args.max_borrowings_to_reserves,
            )
            for company in companies
        ]

        with conn:
            applied_counts = apply_category_changes(conn, evaluations)

        report_path = write_report(report_dir, evaluations, args, applied_counts)
        print_summary(evaluations, applied_counts, report_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
