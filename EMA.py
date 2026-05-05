import pandas as pd

INPUT_FILE = "synthetic_stock_data.csv"
OUTPUT_FILE = "stock_with_ma.csv"


def metrics_engine():
    trades = []

    normal_trade = None
    avg_trade = None

    def update(signal, price, index):
        nonlocal normal_trade, avg_trade

        if signal == "BUY":
            normal_trade = {
                "entry_price": price,
                "entry_index": index
            }

        elif signal == "AVERAGE":
            avg_trade = {
                "entry_price": price,
                "entry_index": index
            }

        elif signal == "SELL":
            if normal_trade:
                profit = price - normal_trade["entry_price"]
                profit_pct = (profit / normal_trade["entry_price"]) * 100

                trades.append({
                    "profit": profit,
                    "profit_pct": profit_pct,
                    "duration": index - normal_trade["entry_index"],
                    "type": "normal"
                })
                normal_trade = None

        elif signal == "EXIT":
            if avg_trade:
                profit = price - avg_trade["entry_price"]
                profit_pct = (profit / avg_trade["entry_price"]) * 100

                trades.append({
                    "profit": profit,
                    "profit_pct": profit_pct,
                    "duration": index - avg_trade["entry_index"],
                    "type": "avg"
                })
                avg_trade = None

    def results():
        total_profit = sum(t["profit"] for t in trades)

        normal = [t for t in trades if t["type"] == "normal"]
        avg = [t for t in trades if t["type"] == "avg"]

        avg_normal_time = sum(t["duration"] for t in normal)/len(normal) if normal else 0
        avg_avg_time = sum(t["duration"] for t in avg)/len(avg) if avg else 0

        avg_profit_pct = sum(t["profit_pct"] for t in trades)/len(trades) if trades else 0

        total_trades = len(trades)              # completed trades

        return (
            total_profit,
            avg_normal_time,
            avg_avg_time,
            avg_profit_pct,
            total_trades
        )

    return update, results

def EMA():
    df = pd.read_csv(OUTPUT_FILE)
    close_prices = df["close"]

    update, results = metrics_engine()

    system_state = 1
    last_buy_price = None

    for i in range(len(df)):
        ma20 = df["MA_20"].iloc[i]
        ma50 = df["MA_50"].iloc[i]
        ma200 = df["MA_200"].iloc[i]
        close = close_prices.iloc[i]

        if pd.notna(ma200):

            if (ma200 > ma50 > ma20 > close) and system_state == 1 > close:
                print(f"{i}: BUY =====> {close}")
                update("BUY", close, i)
                system_state = 2
                last_buy_price = close

            elif system_state == 2:
                if close < last_buy_price * 0.9:
                    print(f"{i}: AVERAGING BUY =====> {close}")
                    update("AVERAGE", close, i)
                    system_state = 3

                elif (ma200 < ma50 < ma20 < close):
                    print(f"{i}: SELL =====> {close}")
                    update("SELL", close, i)
                    system_state = 1
                    last_buy_price = None

            elif system_state == 3:
                if close >= last_buy_price:
                    print(f"{i}: EXIT AFTER AVERAGING =====> {close}")
                    update("EXIT", close, i)
                    system_state = 2

    total_profit, avg_normal_time, avg_avg_time, avg_profit_pct, total_trades = results()
    print("\n==== METRICS ====")
    print(f"Total Profit: {total_profit}")
    print(f"Avg Time (Normal Trades): {avg_normal_time}")
    print(f"Avg Time (Averaging Trades): {avg_avg_time}")
    print(f"Avg Profit %: {avg_profit_pct}")
    print(f"Total Trades: {total_trades}")


def MA_calculation():
    df = pd.read_csv(INPUT_FILE)
    close_prices = df["close"]

    df["MA_20"] = close_prices.rolling(window=20).mean()
    df["MA_50"] = close_prices.rolling(window=50).mean()
    df["MA_200"] = close_prices.rolling(window=200).mean()

    df.to_csv(OUTPUT_FILE, index=False)


if __name__ == "__main__":
    MA_calculation()
    EMA()