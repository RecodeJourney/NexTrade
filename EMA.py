import pandas as pd

INPUT_FILE = "synthetic_stock_data.csv"
OUTPUT_FILE = "stock_with_ma.csv"

def EMA():
    df = pd.read_csv(OUTPUT_FILE)
    close_prices = df["close"]
    system_state = 1

    for i in range(len(df)):
        ma20 = df["MA_20"].iloc[i]
        ma50 = df["MA_50"].iloc[i]
        ma200 = df["MA_200"].iloc[i]
        close = close_prices.iloc[i]

        if pd.notna(ma200):
            if (ma200 > ma50 > ma20 > close) and system_state == 1:
                print(f"{i}: Bullish trend detected : BUY   =======> {close}")
                system_state += 1

            elif (ma200 < ma50 < ma20 < close) and system_state == 2:
                print(f"{i}: Bearish trend detected : SELL  =======> {close}")
                system_state -= 1


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