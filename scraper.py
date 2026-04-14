import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import easyocr
import cv2
import numpy as np
from PIL import Image
from io import BytesIO
import re
from rsl_logging import log

# CONSTANTS

HEADERS = {"User-Agent": "Mozilla/5.0"}

OCR_READER = easyocr.Reader(["en"], gpu=False)

STAT_RANGES = {
    "hp": (1000, 100000),
    "atk": (100, 5000),
    "def": (100, 5000),
    "spd": (70, 150),
    "acc": (0, 500),
    "res": (0, 500),
}
STAT_FIXUPS = {
    "hp": (10000, 40000),
    "atk": (500, 3000),
    "def": (500, 3000),
    "spd": (80, 130),
    "res": (0, 500),
    "acc": (0, 500),
}
# HELPER FUNCTIONS


# Use rsl_logging.log which prefixes messages with the caller filename.

def fetch_html(url: str) -> str:
    log(f"Fetching URL: {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=60000)
        page.wait_for_selector(".raid-skill", timeout=20000)
        html = page.content()
        browser.close()
    log(f"Fetched HTML ({len(html)} bytes) for {url}")
    return html

def extract_meta(soup: BeautifulSoup) -> dict:
    name = soup.select_one("h1").text.strip()
    log(f"Extracting meta for: {name}")
    faction = ""
    affinity = ""

    for img in soup.select("img"):
        src = img.get("data-orig-src") or img.get("src") or ""
        if "/factions/" in src:
            faction = src.split("/")[-1].replace(".png", "").replace("-", " ").title()
        if "/affinity/" in src:
            affinity = src.split("/")[-1].replace(".png", "").title()

    base = soup.find(["h2", "h3"], string=lambda t: t and "base form" in t.lower())
    alt = soup.find(["h2", "h3"], string=lambda t: t and "alternate form" in t.lower())

    meta = {
        "name": name,
        "faction": faction,
        "affinity": affinity,
        "is_mythical": bool(base and alt),
    }
    log(f"Meta extracted: faction={meta['faction']}, affinity={meta['affinity']}, mythical={meta['is_mythical']}")
    return meta

def normalize_stat(stat, value):
    if value is None:
        return None

    lo, hi = STAT_FIXUPS[stat]

    if value < lo:
        for m in (10, 100):
            fixed = value * m
            if lo <= fixed <= hi:
                return fixed

    return value

def extract_base_stats_from_ocr_text(text: str) -> dict:
    text = text.upper().replace(",", "").replace(".", "")
    tokens = text.split()

    stats = {
        "hp": None,
        "atk": None,
        "def": None,
        "spd": None,
        "res": None,
        "acc": None,
        "crit_rate": "15%",
        "crit_dmg": "50%"
    }

    type_map = {
        "ATTACK": "Attack",
        "DEFENSE": "Defense",
        "HP": "Hp",
        "SUPPORT": "Support"
    }

    champ_type = None
    for k, v in type_map.items():
        if k in text:
            champ_type = v
            break

    stat_aliases = {
        "hp": ["HP"],
        "atk": ["ATK", "ATT"],
        "def": ["DEF", "OFF"],
        "spd": ["SPD", "SPO"],
        "res": ["RES", "RESIST", "RIST"],
        "acc": ["ACC"]
    }

    for stat, keys in stat_aliases.items():
        for key in keys:
            match = re.search(rf"{key}\s*(\d{{2,6}})", text)
            if match:
                stats[stat] = int(match.group(1))
                break

    for stat in stats:
        if(stat != "crit_rate" and stat != "crit_dmg"):
            stats[stat] = normalize_stat(stat, stats[stat])
    return {
        "type": champ_type,
        "stats": stats
    }

# 3 MAIN EXTRACTION FUNCTIONS

def extract_base_stats(soup: BeautifulSoup) -> dict:
    img = soup.select_one("img#raid-splash")
    if not img:
        log("No splash image found for base stats")
        return {"type": None, "stats": {}}

    img_url = img.get("data-orig-src") or img.get("src")
    log(f"Found splash image, downloading for OCR: {img_url}")
    img_bytes = requests.get(img_url, timeout=30).content
    image = Image.open(BytesIO(img_bytes)).convert("RGB")
    img_np = np.array(image)

    h, w, _ = img_np.shape
    right_half = img_np[:, int(w * 0.55):]

    gray = cv2.cvtColor(right_half, cv2.COLOR_RGB2GRAY)
    gray = cv2.resize(gray, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)

    log("Running OCR on splash image (this can take a moment)")
    ocr_text = " ".join(OCR_READER.readtext(gray, detail=0))
    if ocr_text:
        log(f"OCR text (truncated): {ocr_text[:200]}")
    else:
        log("OCR returned no text")
    return extract_base_stats_from_ocr_text(ocr_text)

def extract_skills(soup: BeautifulSoup) -> dict:
    log("Extracting skills from page")
    skills = {"actives": {}, "passives": []}
    active_count = 0

    for block in soup.select(".raid-skill"):
        title = block.select_one("h4")
        desc = block.select_one(".raid-skill-description")
        if not title or not desc:
            continue

        raw = title.text.strip()
        name = raw.replace("[P]", "").strip()
        description = " ".join(desc.stripped_strings)
        is_passive = "[P]" in raw or block.select_one(".raid-skill-passive")

        cooldown = None
        cd = block.select_one(".raid-skill-cooldown span")
        if cd:
            try:
                cooldown = int(cd.text.split()[0])
            except Exception:
                cooldown = None

        multipliers = [
            s.text.strip()
            for s in block.select(".raid-skill-multipliers span")
            if "*" in s.text
        ]

        books = [r.text.strip() for r in block.select(".raid-skill-book-row")]

        data = {
            "name": name,
            "description": description,
            "cooldown": cooldown,
            "multipliers": multipliers,
            "books": books,
        }

        if is_passive:
            skills["passives"].append(data)
        else:
            active_count += 1
            skills["actives"][f"a{active_count}"] = data

    log(f"Extracted skills: {len(skills['actives'])} active(s), {len(skills['passives'])} passive(s)")
    return skills

def extract_area_ratings(url: str) -> dict:
    log(f"Extracting area ratings from: {url}")
    soup = BeautifulSoup(fetch_html(url), "html.parser")

    ratings = {
        "core_areas": {},
        "dungeons": {},
        "hard_mode_dungeons": {},
        "doom_tower": {},
    }

    sections = {
        "key-areas": "core_areas",
        "dungeons": "dungeons",
        "hard-mode": "hard_mode_dungeons",
        "doom-tower": "doom_tower",
    }

    for sid, key in sections.items():
        sec = soup.select_one(f".raid-ratings-content#{sid}")
        if not sec:
            log(f"No ratings section found for {sid}")
            continue

        for row in sec.select(".raid-rating"):
            label_el = row.select_one(".raid-rating-label")
            if not label_el:
                continue

            label = label_el.text.replace(":", "").strip().lower().replace(" ", "_")
            stars = row.select(".star-ratings i")

            if stars:
                score = 0.0
                for s in stars:
                    cls = " ".join(s.get("class", []))
                    score += 0.5 if "fa-star-half" in cls else 1.0
            else:
                meta = row.select_one(".raid-meta-value")
                try:
                    score = float(meta.text.strip()) if meta else 0
                except Exception:
                    score = 0

            ratings[key][label] = score
        log(f"Found ratings for section {sid}: {len(ratings[key])} items")

    return ratings

def scrape_champion_full(url: str) -> dict:
    log(f"Starting full scrape for: {url}")
    soup = BeautifulSoup(fetch_html(url), "html.parser")

    meta = extract_meta(soup)

    log("Extracting base stats")
    base_stats = extract_base_stats(soup)

    log("Extracting skills")
    skills = extract_skills(soup)

    log("Extracting area ratings")
    ratings = extract_area_ratings(url)

    log(f"Scrape complete for {meta.get('name')}")
    return {
        "name": meta["name"],
        "url": url,
        "meta": meta,
        "base_stats": base_stats,
        "skills": skills,
        "ratings": ratings,
    }

if __name__ == "__main__":
    url = "https://hellhades.com/raid/champions/rae/"
    result = scrape_champion_full(url)
    log(f"Scrape finished: {result.get('name')} ({result.get('url')})")
    if result.get("base_stats"):
        log(f"  base stats present: {bool(result['base_stats'].get('stats'))}")
    log(f"  active skills: {len(result.get('skills', {}).get('actives', {}))}")
