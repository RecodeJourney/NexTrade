CREATE TABLE companies (
symbol TEXT PRIMARY KEY,
company_name TEXT,
screener_url TEXT,
face_value REAL,
market_cap REAL,
category TEXT,
listing_date DATE,
last_scraped TIMESTAMP,

FOREIGN KEY(symbol) REFERENCES stock_universe(symbol)
);

CREATE TABLE ratios (
symbol TEXT PRIMARY KEY,

pe REAL,
pb REAL,
roce REAL,
roe REAL,
eps REAL,
book_value REAL,

debt REAL,
debt_to_equity REAL,
interest_coverage REAL,

dividend_yield REAL,
face_value REAL,

current_ratio REAL,
quick_ratio REAL,

sales_growth REAL,
profit_growth REAL,

updated_at TIMESTAMP,

FOREIGN KEY(symbol) REFERENCES companies(symbol)
);

CREATE TABLE yearly_results (
symbol TEXT,

year_end INTEGER,

sales REAL,
expenses REAL,
operating_profit REAL,
opm REAL,

net_profit REAL,
eps REAL,

dividend_payout REAL,

PRIMARY KEY(symbol, year_end),

FOREIGN KEY(symbol) REFERENCES companies(symbol)
);

CREATE TABLE balance_sheet (
symbol TEXT,

year_end INTEGER,

equity_capital REAL,
reserves REAL,

borrowings REAL,
other_liabilities REAL,

total_liabilities REAL,

fixed_assets REAL,
cwip REAL,

investments REAL,

other_assets REAL,
total_assets REAL,

PRIMARY KEY(symbol, year_end),

FOREIGN KEY(symbol) REFERENCES companies(symbol)
);


CREATE TABLE shareholding_pattern (
symbol TEXT,

quarter_end DATE,

promoters REAL,
fiis REAL,
diis REAL,
public REAL,
others REAL,

pledged_percent REAL,

PRIMARY KEY(symbol, quarter_end),

FOREIGN KEY(symbol) REFERENCES companies(symbol)
);

CREATE TABLE price_history (
symbol TEXT,

price_date DATE,

open_price REAL,
high_price REAL,
low_price REAL,
close_price REAL,

volume INTEGER,

PRIMARY KEY(symbol, price_date),

FOREIGN KEY(symbol) REFERENCES companies(symbol)
);

CREATE TABLE scrape_log (
id INTEGER PRIMARY KEY AUTOINCREMENT,

symbol TEXT,

scrape_time TIMESTAMP,

status TEXT,

error_message TEXT,

duration_seconds REAL,

FOREIGN KEY(symbol) REFERENCES companies(symbol)
);

CREATE TABLE screener_raw_json (
symbol TEXT,

scrape_time TIMESTAMP,

raw_json TEXT,

PRIMARY KEY(symbol, scrape_time)
);
