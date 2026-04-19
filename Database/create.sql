PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS price_history;
DROP TABLE IF EXISTS shareholder_yearly;
DROP TABLE IF EXISTS ratios;
DROP TABLE IF EXISTS balance_sheet;
DROP TABLE IF EXISTS profit_and_loss;
DROP TABLE IF EXISTS snapshot_metrics;
DROP TABLE IF EXISTS scrape_logs;
DROP TABLE IF EXISTS screener_raw_json;
DROP TABLE IF EXISTS shareholder_pattern;
DROP TABLE IF EXISTS shareholder_quarterly;
DROP TABLE IF EXISTS yearly_results;
DROP TABLE IF EXISTS companies;

PRAGMA foreign_keys = ON;

CREATE TABLE companies (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    sector TEXT,
    url TEXT
);

CREATE TABLE snapshot_metrics (
    symbol TEXT PRIMARY KEY,
    market_cap REAL,
    current_price REAL,
    high_low TEXT,
    pe REAL,
    book_value REAL,
    dividend_yield REAL,
    roce REAL,
    roe REAL,
    face_value REAL,
    profit_growth REAL,
    promoter_holding REAL,
    sales_growth REAL,
    debt_to_equity REAL,
    FOREIGN KEY (symbol) REFERENCES companies(symbol)
);

CREATE TABLE profit_and_loss (
    symbol TEXT,
    year TEXT,
    sales REAL,
    expenses REAL,
    operating_profit REAL,
    opm REAL,
    other_income REAL,
    interest REAL,
    depreciation REAL,
    profit_before_tax REAL,
    tax REAL,
    net_profit REAL,
    eps REAL,
    dividend_payout REAL,
    PRIMARY KEY (symbol, year),
    FOREIGN KEY (symbol) REFERENCES companies(symbol)
);

CREATE TABLE balance_sheet (
    symbol TEXT,
    year TEXT,
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
    PRIMARY KEY (symbol, year),
    FOREIGN KEY (symbol) REFERENCES companies(symbol)
);

CREATE TABLE ratios (
    symbol TEXT,
    year TEXT,
    debtor_days REAL,
    inventory_days REAL,
    days_payable REAL,
    cash_conversion_cycle REAL,
    working_capital_days REAL,
    roce REAL,
    PRIMARY KEY (symbol, year),
    FOREIGN KEY (symbol) REFERENCES companies(symbol)
);

CREATE TABLE shareholder_pattern(
    symbol TEXT,
    year TEXT,
    promoters REAL,
    fiis REAL,
    diis REAL,
    government REAL,
    public REAL,
    PRIMARY KEY (symbol, year),
    FOREIGN KEY (symbol) REFERENCES companies(symbol)
);

CREATE TABLE price_history (
    symbol TEXT,
    date TEXT,
    open_price REAL,
    close_price REAL,
    high_price REAL,
    low_price REAL,
    volume REAL,
    PRIMARY KEY (symbol, date),
    FOREIGN KEY (symbol) REFERENCES companies(symbol)
);