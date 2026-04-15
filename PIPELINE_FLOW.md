# NexTrade Screener Pipeline Flow

This project scrapes financial data from Screener and stores it in two places:

1. Runtime JSON files under `json/<SYMBOL>/`
2. SQLite database tables inside `screener.db`

The current default symbols are:

```python
["RELIANCE", "TCS", "INFY"]
```

You can pass other symbols from the command line.

## File Overview

### `scraper.py`

This file is responsible for fetching and parsing Screener pages.

Main function:

```python
scrape_company(symbol: str) -> dict
```

For a symbol like `RELIANCE`, it builds this URL:

```text
https://www.screener.in/company/RELIANCE/consolidated/
```

Then it downloads the HTML page and extracts:

- Company information
- Top ratios
- Profit and loss history
- Balance sheet history
- Ratios history
- Shareholding pattern

The final output of `scrape_company("RELIANCE")` looks like:

```python
{
    "symbol": "RELIANCE",
    "company_info": {...},
    "top_ratios": {...},
    "profit_loss": {...},
    "balance_sheet": {...},
    "ratios_history": {...},
    "shareholding": {...},
    "scraped_at": "2026-04-16T..."
}
```

The scraper also has:

```python
save_runtime_json(data)
```

This saves the scraped dictionary into JSON files under:

```text
json/<SYMBOL>/
```

Example:

```text
json/RELIANCE/latest.json
json/RELIANCE/company_info.json
json/RELIANCE/top_ratios.json
json/RELIANCE/profit_loss.json
json/RELIANCE/balance_sheet.json
json/RELIANCE/ratios_history.json
json/RELIANCE/shareholding.json
```

`latest.json` contains the full scrape snapshot. The other files contain one specific section each.

When you run:

```bash
python scraper.py RELIANCE TCS INFY
```

the flow is:

```text
command line symbols
        |
        v
scrape_company(symbol)
        |
        v
parse Screener HTML into dict
        |
        v
save_runtime_json(data)
        |
        v
json/<SYMBOL>/*.json files
```

This command does not write to SQLite. It is useful when you only want local JSON output for inspection or debugging.

### `utils.py`

This file contains helper functions used by the scraper and database writer.

Important function:

```python
clean_number(value)
```

It converts Screener text values into Python floats or `None`.

Examples:

```text
"1,234"            -> 1234.0
"23%"              -> 23.0
"12.5"             -> 12.5
"--"               -> None
"₹ 18,19,660 Cr."  -> 1819660.0
```

Another important function:

```python
normalize_key(value)
```

It converts Screener labels into Python/database-friendly keys.

Examples:

```text
"Stock P/E"          -> "stock_p_e"
"Net Profit"         -> "net_profit"
"OPM %"              -> "opm_pct"
"Dividend Payout %"  -> "dividend_payout_pct"
```

This normalization is important because Screener field names are human-readable, while the database uses snake_case column names.

### `schema.sql`

This file contains the database table definitions.

It creates these tables:

```text
companies
ratios
yearly_results
balance_sheet
shareholding_pattern
price_history
scrape_log
screener_raw_json
```

Current `companies` table includes:

```sql
category TEXT
```

For now, `category` is `NULL`. Later it can be filled with:

```text
V40
V40next
V200
```

The schema is used only once to create the database:

```bash
sqlite3 screener.db < schema.sql
```

After the database has been created, do not keep recreating it unless you intentionally want to reset all data.

### `screener.db`

This is the SQLite database file created from `schema.sql`.

It currently stores scraped data for:

```text
RELIANCE
TCS
INFY
```

You can inspect it from terminal:

```bash
sqlite3 screener.db ".tables"
```

Check companies:

```bash
sqlite3 screener.db "SELECT symbol, company_name, market_cap, category FROM companies;"
```

Check scrape logs:

```bash
sqlite3 screener.db "SELECT symbol, status, scrape_time, duration_seconds FROM scrape_log;"
```

### `db_writer.py`

This file writes scraped Python dictionaries into SQLite.

It uses:

```python
sqlite3
```

No ORM is used.

Important function:

```python
connect_db()
```

By default, it uses:

```text
screener.db
```

You can override this with:

```bash
SCREENER_DB_PATH=/path/to/database.db
```

The writer functions are:

```python
upsert_company()
upsert_ratios()
insert_yearly_results()
insert_balance_sheet()
insert_shareholding()
upsert_price_history()
insert_scrape_log()
insert_raw_json()
```

#### Company Mapping

`upsert_company()` writes into:

```text
companies
```

Mapping:

```text
symbol                         -> companies.symbol
company_info.company_name      -> companies.company_name
company_info.source_url        -> companies.screener_url
top_ratios.face_value          -> companies.face_value
top_ratios.market_cap          -> companies.market_cap
scraped_at                     -> companies.last_scraped
```

`category` is not touched by the scraper. This is intentional so manual or future classification values are not overwritten.

#### Ratios Mapping

`upsert_ratios()` writes into:

```text
ratios
```

Mapping:

```text
top_ratios.stock_p_e      -> ratios.pe
top_ratios.roce           -> ratios.roce
top_ratios.roe            -> ratios.roe
top_ratios.book_value     -> ratios.book_value
top_ratios.dividend_yield -> ratios.dividend_yield
top_ratios.face_value     -> ratios.face_value
latest profit_loss EPS    -> ratios.eps
scraped_at                -> ratios.updated_at
```

Some ratio columns may remain `NULL` because the current Screener page parsing does not extract them directly yet:

```text
pb
debt
debt_to_equity
interest_coverage
current_ratio
quick_ratio
sales_growth
profit_growth
```

Those can be added later if the criteria and source fields are finalized.

#### Yearly Results Mapping

`insert_yearly_results()` writes into:

```text
yearly_results
```

This table has composite primary key:

```sql
PRIMARY KEY(symbol, year_end)
```

That means each company has one row per financial year.

Example:

```text
RELIANCE, 2023
RELIANCE, 2024
RELIANCE, 2025
```

Mapping:

```text
profit_loss period year       -> yearly_results.year_end
profit_loss.sales             -> yearly_results.sales
profit_loss.expenses          -> yearly_results.expenses
profit_loss.operating_profit  -> yearly_results.operating_profit
profit_loss.opm_pct           -> yearly_results.opm
profit_loss.net_profit        -> yearly_results.net_profit
profit_loss.eps_in_rs         -> yearly_results.eps
profit_loss.dividend_payout_pct -> yearly_results.dividend_payout
```

The scraper skips `TTM` for this table because `TTM` is not a fiscal year end.

#### Balance Sheet Mapping

`insert_balance_sheet()` writes into:

```text
balance_sheet
```

This table has composite primary key:

```sql
PRIMARY KEY(symbol, year_end)
```

Mapping:

```text
balance_sheet period year        -> balance_sheet.year_end
balance_sheet.equity_capital     -> balance_sheet.equity_capital
balance_sheet.reserves           -> balance_sheet.reserves
balance_sheet.borrowings         -> balance_sheet.borrowings
balance_sheet.other_liabilities  -> balance_sheet.other_liabilities
balance_sheet.total_liabilities  -> balance_sheet.total_liabilities
balance_sheet.fixed_assets       -> balance_sheet.fixed_assets
balance_sheet.cwip               -> balance_sheet.cwip
balance_sheet.investments        -> balance_sheet.investments
balance_sheet.other_assets       -> balance_sheet.other_assets
balance_sheet.total_assets       -> balance_sheet.total_assets
```

#### Shareholding Mapping

`insert_shareholding()` writes into:

```text
shareholding_pattern
```

This table has composite primary key:

```sql
PRIMARY KEY(symbol, quarter_end)
```

The scraper prefers quarterly shareholding data. If quarterly data is missing, it can fall back to available shareholding periods.

Mapping:

```text
shareholding period date  -> shareholding_pattern.quarter_end
promoters                 -> shareholding_pattern.promoters
fiis                      -> shareholding_pattern.fiis
diis                      -> shareholding_pattern.diis
public                    -> shareholding_pattern.public
government/others         -> shareholding_pattern.others
```

`pledged_percent` is currently `NULL` because the scraper does not extract pledged share data yet.

#### Price History Mapping

`upsert_price_history()` writes into:

```text
price_history
```

This table has composite primary key:

```sql
PRIMARY KEY(symbol, price_date)
```

The current Screener page gives a current price but not full OHLC history.

So the current implementation stores:

```text
top_ratios.current_price -> price_history.close_price
scrape date              -> price_history.price_date
```

These columns currently remain `NULL`:

```text
open_price
high_price
low_price
volume
```

Later, if we add a separate historical price source, this table can be populated more completely.

#### Scrape Log Mapping

`insert_scrape_log()` writes into:

```text
scrape_log
```

It inserts one new row per scrape attempt.

Mapping:

```text
symbol           -> scrape_log.symbol
current time     -> scrape_log.scrape_time
success/failure  -> scrape_log.status
error message    -> scrape_log.error_message
duration         -> scrape_log.duration_seconds
```

This is useful for checking which symbols succeeded or failed.

#### Raw JSON Mapping

`insert_raw_json()` writes into:

```text
screener_raw_json
```

This table stores the full scrape dictionary as JSON.

Primary key:

```sql
PRIMARY KEY(symbol, scrape_time)
```

This is important because even if some parsed columns do not fit the schema yet, the full source snapshot is still preserved.

### `run_scraper.py`

This is the main database pipeline runner.

It imports:

```python
scrape_company
db_writer functions
```

The flow is:

```text
symbols list
    |
    v
for each symbol
    |
    v
start timer
    |
    v
scrape_company(symbol)
    |
    v
save company, ratios, yearly results, balance sheet,
shareholding, price history, raw JSON
    |
    v
insert scrape_log success
```

If there is an error:

```text
exception happens
    |
    v
insert scrape_log failure
    |
    v
print failure message
```

You can run it with default symbols:

```bash
SCREENER_DB_PATH=screener.db python run_scraper.py
```

Or pass symbols manually:

```bash
SCREENER_DB_PATH=screener.db python run_scraper.py RELIANCE TCS INFY
```

## Setup And Run Commands

Go to the project:

```bash
cd /Users/gauravsingh/Trading_project/NexTrade
```

Activate the virtual environment:

```bash
source .venv/bin/activate
```

If creating the database from scratch:

```bash
sqlite3 screener.db < schema.sql
```

Scrape only JSON files:

```bash
python scraper.py RELIANCE TCS INFY
```

Scrape and save into SQLite:

```bash
SCREENER_DB_PATH=screener.db python run_scraper.py RELIANCE TCS INFY
```

Check tables:

```bash
sqlite3 screener.db ".tables"
```

Check company rows:

```bash
sqlite3 screener.db "SELECT symbol, company_name, market_cap, category FROM companies;"
```

Check table counts:

```bash
sqlite3 screener.db "SELECT 'companies', count(*) FROM companies UNION ALL SELECT 'ratios', count(*) FROM ratios UNION ALL SELECT 'yearly_results', count(*) FROM yearly_results UNION ALL SELECT 'balance_sheet', count(*) FROM balance_sheet UNION ALL SELECT 'shareholding_pattern', count(*) FROM shareholding_pattern UNION ALL SELECT 'price_history', count(*) FROM price_history UNION ALL SELECT 'scrape_log', count(*) FROM scrape_log UNION ALL SELECT 'screener_raw_json', count(*) FROM screener_raw_json;"
```

Check latest scrape logs:

```bash
sqlite3 screener.db "SELECT symbol, status, scrape_time, duration_seconds, error_message FROM scrape_log ORDER BY id DESC LIMIT 10;"
```

## Current Database State

The database file is:

```text
screener.db
```

It has already been created from:

```text
schema.sql
```

It has been populated for:

```text
RELIANCE
TCS
INFY
```

The `companies.category` column exists and is currently `NULL`.

Later, categories can be updated manually:

```bash
sqlite3 screener.db "UPDATE companies SET category = 'V40' WHERE symbol = 'RELIANCE';"
sqlite3 screener.db "UPDATE companies SET category = 'V40next' WHERE symbol = 'TCS';"
sqlite3 screener.db "UPDATE companies SET category = 'V200' WHERE symbol = 'INFY';"
```

## Important Notes

### `scraper.py` Versus `run_scraper.py`

Use `scraper.py` when you want JSON files only:

```bash
python scraper.py RELIANCE TCS INFY
```

Use `run_scraper.py` when you want SQLite database writes:

```bash
SCREENER_DB_PATH=screener.db python run_scraper.py RELIANCE TCS INFY
```

`run_scraper.py` also scrapes fresh data. So for normal database updates, running `run_scraper.py` is enough.

### Upsert Behavior

The writer updates existing rows when the primary key already exists.

Examples:

```text
companies: same symbol gets updated
ratios: same symbol gets updated
yearly_results: same symbol + year_end gets updated
balance_sheet: same symbol + year_end gets updated
shareholding_pattern: same symbol + quarter_end gets updated
price_history: same symbol + price_date gets updated
screener_raw_json: same symbol + scrape_time is stored
```

`scrape_log` is different. It inserts a new row every time because it is a log table.

### Category Column

The scraper does not write `category`.

This is intentional.

Reason:

```text
category will later depend on custom business criteria:
V40, V40next, V200
```

So the scraper should not overwrite classification decisions.

### Foreign Key Note

The `companies` table has:

```sql
FOREIGN KEY(symbol) REFERENCES stock_universe(symbol)
```

But the current schema does not define `stock_universe`.

SQLite does not enforce foreign keys unless `PRAGMA foreign_keys = ON` is enabled. The current pipeline runs successfully because it does not enable foreign key enforcement.

If foreign key enforcement is enabled later, one of these should be done:

1. Add a `stock_universe` table.
2. Remove that foreign key.
3. Insert symbols into `stock_universe` before inserting into `companies`.

## Recommended Next Steps

1. Finalize the full symbol universe.
2. Decide how categories `V40`, `V40next`, and `V200` should be assigned.
3. Add category assignment logic after the criteria are finalized.
4. Add a real historical price data source if OHLC and volume history are required.
5. Add missing ratio extraction for fields like `pb`, `debt_to_equity`, `current_ratio`, and growth metrics.
6. Add automated validation checks after each scrape run.

## Team Update Summary

The Screener pipeline is now functional end to end. It can scrape symbols like `RELIANCE`, `TCS`, and `INFY`, save structured JSON snapshots under `json/<SYMBOL>/`, and write normalized data into SQLite tables in `screener.db`. The database schema is stored in `schema.sql`, and the `companies` table now includes a nullable `category` column for future labels such as `V40`, `V40next`, and `V200`. The main command for database updates is `SCREENER_DB_PATH=screener.db python run_scraper.py RELIANCE TCS INFY`. Current limitations are that category assignment criteria are not implemented yet, and some ratio/price fields remain `NULL` until we add extra extraction logic or another data source.
