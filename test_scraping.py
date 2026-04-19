from playwright.sync_api import sync_playwright


URL = "https://www.screener.in/company/RELIANCE/consolidated/"

QUICK_RATIOS = [
    "Profit growth",
    "Promoter holding",
    "Sales growth",
    "Debt to equity"
]


def login(page):
    page.goto("https://www.screener.in/login/")

    page.fill('input[name="username"]', "angelscur@gmail.com")
    page.fill('input[name="password"]', "Nava@#1352")

    page.click('button[type="submit"]')
    page.wait_for_timeout(2000)

    # ✅ Correct login check
    if "login" in page.url:
        print("❌ Login failed")
        error = page.query_selector(".errorlist, .alert")
        if error:
            print("Error:", error.inner_text())
        return False
    else:
        print("✅ Login successful:", page.url)
        return True


def extract_ratios(page):
    ratios = {}
    items = page.query_selector_all("#top-ratios li")

    for item in items:
        name = item.query_selector(".name").inner_text().strip()
        value = item.query_selector(".value").inner_text().strip()
        ratios[name] = value

    return ratios


def add_quick_ratio(page, text):
    print(f"➡️ Adding: {text}")

    search = page.locator("#quick-ratio-search")

    search.click()
    search.fill("")  # clear
    search.type(text, delay=100)

    page.wait_for_timeout(500)

    options = page.locator(".dropdown-content li")

    found = False
    for i in range(options.count()):
        opt = options.nth(i)
        if text.lower() in opt.inner_text().lower():
            opt.click()
            found = True
            break

    if not found:
        print(f"⚠️ Option not found: {text}")
        return

    # ✅ Wait for DOM update
    try:
        page.wait_for_selector("li[data-source='quick-ratio']", timeout=3000)
        print(f"✅ Added: {text}")
    except:
        print(f"⚠️ UI did not update for: {text}")

    page.wait_for_timeout(500)


def scrape():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        success = login(page)
        if not success:
            browser.close()
            return {}

        # 🔥 FORCE NAVIGATION AFTER LOGIN
        page.goto(URL)

        try:
            page.wait_for_selector("#top-ratios", timeout=10000)
            print("✅ Landed on company page")
        except:
            print("❌ Failed to load company page")
            print("Current URL:", page.url)
            browser.close()
            return {}

        for ratio in QUICK_RATIOS:
            add_quick_ratio(page, ratio)

        data = extract_ratios(page)

        browser.close()
        return data

if __name__ == "__main__":
    result = scrape()
    for k, v in result.items():
        print(f"{k}: {v}")