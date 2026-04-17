from playwright.sync_api import sync_playwright


URL = "https://www.screener.in/company/RELIANCE/consolidated/"

QUICK_RATIOS = [
    "Profit growth",
    "Promoter holding",
    "Sales growth",
    "Debt to equity"
]


def extract_ratios(page):
    ratios = {}
    items = page.query_selector_all("#top-ratios li")

    for item in items:
        name = item.query_selector(".name").inner_text().strip()
        value = item.query_selector(".value").inner_text().strip()
        ratios[name] = value

    return ratios


def add_quick_ratio(page, text):
    search = page.query_selector("#quick-ratio-search")
    search.fill(text)

    page.wait_for_selector(".dropdown-content li", state="attached")

    options = page.query_selector_all(".dropdown-content li")
    for opt in options:
        if text.lower() in opt.inner_text().lower():
            opt.click()
            break

    page.wait_for_timeout(500)  # allow DOM update


def scrape():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(URL)
        page.wait_for_selector("#top-ratios")

        for ratio in QUICK_RATIOS:
            add_quick_ratio(page, ratio)

        data = extract_ratios(page)

        browser.close()
        return data


if __name__ == "__main__":
    result = scrape()
    for k, v in result.items():
        print(f"{k}: {v}")