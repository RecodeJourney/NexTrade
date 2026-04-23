import pandas as pd
import pandas_ta as ta
import json
import os

def run_knoxville_strategy(file_path):
    if not os.path.exists(file_path):
        print(f"--- [ERROR] File '{file_path}' not found! ---")
        return []

    print(f"--- [LOG] Loading data from {file_path} ---")
    df = pd.read_csv(file_path)
    df.columns = [x.lower() for x in df.columns]
    
    # Calculate Math
    df['momentum'] = df['close'] - df['close'].shift(20)
    df['rsi'] = ta.rsi(df['close'], length=14)
    df.dropna(inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    def is_p_low(idx):
        if idx < 2 or idx > len(df) - 3: return False
        return df['low'].iloc[idx] == df['low'].iloc[idx-2:idx+3].min()
        
    def is_p_high(idx):
        if idx < 2 or idx > len(df) - 3: return False
        return df['high'].iloc[idx] == df['high'].iloc[idx-2:idx+3].max()

    completed_cycles = []
    active_buys = []  
    lookback = 200

    print(f"--- [LOG] Scanning for Double-Buy Cycles with Reasoning ---")
    
    for i in range(len(df)):
        # 1. BUYING PHASE (Logic for two entries)
        if len(active_buys) < 2:
            if is_p_low(i):
                for p in range(i-1, max(0, i-lookback), -1):
                    if is_p_low(p):
                        if (df['low'].iloc[i] < df['low'].iloc[p] and 
                            df['momentum'].iloc[i] > df['momentum'].iloc[p] and 
                            df['rsi'].iloc[p:i+1].min() < 30):
                            
                            entry_reason = (f"Bullish KD: Price {df['low'].iloc[i]} is lower than previous pivot {df['low'].iloc[p]}, "
                                            f"but Momentum rose from {round(df['momentum'].iloc[p],2)} to {round(df['momentum'].iloc[i],2)} "
                                            f"while RSI was in oversold territory.")
                            
                            entry = {
                                "buy_number": len(active_buys) + 1,
                                "buy_row": int(i), 
                                "buy_price": float(df['low'].iloc[i]),
                                "reasoning": entry_reason
                            }
                            active_buys.append(entry)
                            print(f"  [+] BUY #{len(active_buys)} Executed at Row {i}")
                            break

        # 2. SELLING PHASE (Wait for both positions to fill, then sell together)
        if len(active_buys) == 2:
            if is_p_high(i):
                for p in range(i-1, max(0, i-lookback), -1):
                    if is_p_high(p):
                        if (df['high'].iloc[i] > df['high'].iloc[p] and 
                            df['momentum'].iloc[i] < df['momentum'].iloc[p] and 
                            df['rsi'].iloc[p:i+1].max() > 70):
                            
                            sell_price = float(df['high'].iloc[i])
                            sell_row = int(i)
                            combined_pl = 0
                            trade_summaries = []
                            
                            for b in active_buys:
                                pl = sell_price - b['buy_price']
                                combined_pl += pl
                                trade_summaries.append({
                                    "buy_id": b['buy_number'],
                                    "entry_row": b['buy_row'],
                                    "entry_price": b['buy_price'],
                                    "exit_price": sell_price,
                                    "p_l": round(pl, 2),
                                    "entry_reasoning": b['reasoning']
                                })

                            exit_reason = (f"Bearish KD: Price reached {sell_price} (higher than pivot {df['high'].iloc[p]}), "
                                           f"but Momentum slowed to {round(df['momentum'].iloc[i],2)}. RSI hit overbought levels, "
                                           f"triggering a simultaneous exit for all open positions.")

                            cycle = {
                                "cycle_id": len(completed_cycles) + 1,
                                "total_profit_loss": round(combined_pl, 2),
                                "sell_row": sell_row,
                                "sell_price": sell_price,
                                "exit_reasoning": exit_reason,
                                "individual_trades": trade_summaries
                            }
                            
                            completed_cycles.append(cycle)
                            print(f"  [-] DOUBLE SELL at Row {i} | Total Cycle P&L: {cycle['total_profit_loss']}")
                            active_buys = [] # Reset
                            break

    return completed_cycles

if __name__ == "__main__":
    results = run_knoxville_strategy('synthetic_stock_data.csv')
    with open('double_buy_reasoning_report.json', 'w') as f:
        json.dump(results, f, indent=4)
    print("\n[FINISH] Report generated: double_buy_reasoning_report.json")
