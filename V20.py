import csv


# -----------------------------
# DATA STRUCTURE
# -----------------------------
def make_candle(row):
    return {
        "open": float(row[0]),
        "high": float(row[1]),
        "low": float(row[2]),
        "close": float(row[3]),
        "volume": float(row[4])
    }


def is_green(candle):
    return candle["close"] > candle["open"]


def load_data(path):
    data = []
    with open(path, "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            data.append(make_candle(row))
    return data


# -----------------------------
# METRICS ENGINE (same idea)
# -----------------------------
def metrics_engine():
    trades = []
    normal_trade = None
    avg_trade = None

    def update(signal, price, index):
        nonlocal normal_trade, avg_trade

        if signal == "BUY":
            normal_trade = {"entry_price": price, "entry_index": index}

        elif signal == "AVERAGE":
            avg_trade = {"entry_price": price, "entry_index": index}

        elif signal == "SELL" and normal_trade:
            profit = price - normal_trade["entry_price"]
            trades.append({
                "profit": profit,
                "profit_pct": (profit / normal_trade["entry_price"]) * 100,
                "duration": index - normal_trade["entry_index"],
                "type": "normal"
            })
            normal_trade = None

        elif signal == "EXIT" and avg_trade:
            profit = price - avg_trade["entry_price"]
            trades.append({
                "profit": profit,
                "profit_pct": (profit / avg_trade["entry_price"]) * 100,
                "duration": index - avg_trade["entry_index"],
                "type": "avg"
            })
            avg_trade = None

    def results():
        total_profit = sum(t["profit"] for t in trades)
        total_trades = len(trades)

        normal = [t for t in trades if t["type"] == "normal"]
        avg = [t for t in trades if t["type"] == "avg"]

        avg_normal_time = sum(t["duration"] for t in normal)/len(normal) if normal else 0
        avg_avg_time = sum(t["duration"] for t in avg)/len(avg) if avg else 0
        avg_profit_pct = sum(t["profit_pct"] for t in trades)/total_trades if trades else 0

        return total_profit, avg_normal_time, avg_avg_time, avg_profit_pct, total_trades

    return update, results


# -----------------------------
# CORE STRATEGY LOGIC
# -----------------------------

def detect_20(data, start, n):
    j = start
    while j < n and is_green(data[j]):
        if j == start:
            growth = (data[j]["close"] - data[j]["open"]) / data[j]["open"]
        else:
            growth = (data[j]["close"] - data[start]["open"]) / data[start]["open"]

        if growth >= 0.2:
            return data[start]["open"], data[j]["close"]

        j += 1
    return None, None

def run_v20_strategy(data):
    state = 0
    signals = []

    update_metrics, get_metrics = metrics_engine()

    buy_signal = None
    sell_signal = None

    buy2_signal = None
    sell2_signal = None

    wait_counter = 0

    i = 0
    n = len(data)

    while i < n:

        if state == 0:
            buy, sell = detect_20(data, i, n)
            if buy:
                buy_signal = buy
                sell_signal = sell
                state = 1
                wait_counter = 0
            i += 1

        elif state == 1:
            if wait_counter > 500:
                state = 0
                buy_signal = None
                sell_signal = None
                continue

            if data[i]["low"] <= buy_signal <= data[i]["high"]:
                signals.append(f"{i+1}: BUY 1 =====> {buy_signal}")
                update_metrics("BUY", buy_signal, i+1)
                state = 3
            else:
                wait_counter += 1

            i += 1

        elif state == 3:

            if data[i]["low"] <= sell_signal <= data[i]["high"]:
                signals.append(f"{i+1}: SELL 1 =====> {sell_signal}")
                update_metrics("SELL", sell_signal, i+1)

                state = 0
                buy_signal = None
                sell_signal = None
                continue

            buy2, sell2 = detect_20(data, i, n)

            if buy2:
                buy2_signal = buy2
                sell2_signal = sell2
                state = 2
                continue

            i += 1

        elif state == 2:
            if data[i]["low"] <= buy2_signal <= data[i]["high"]:
                signals.append(f"{i+1}: BUY 2 =====> {buy2_signal}")
                update_metrics("2ND BUY", buy2_signal, i+1)
                state = 4
            i += 1

        elif state == 4:

            if data[i]["low"] <= sell2_signal <= data[i]["high"]:
                signals.append(f"{i+1}: 2ND SELL =====> {sell2_signal}")
                update_metrics("2ND SELL", sell2_signal, i+1)

                state = 3
                buy2_signal = None
                sell2_signal = None
                continue

            i += 1

    return signals, get_metrics()
# -----------------------------
# MAIN
# -----------------------------
def main():
    data = load_data("synthetic_stock_data.csv")

    signals, metrics = run_v20_strategy(data)

    for s in signals:
        print(s)

    total_profit, avg_normal_time, avg_avg_time, avg_profit_pct, total_trades = metrics

    print("\n==== METRICS ====")
    print("Total Profit:", total_profit)
    print("Avg Time (Normal Trades):", avg_normal_time)
    print("Avg Time (Averaging Trades):", avg_avg_time)
    print("Avg Profit %:", avg_profit_pct)
    print("Total Trades:", total_trades)


if __name__ == "__main__":
    main()