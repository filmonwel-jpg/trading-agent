import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import warnings
warnings.filterwarnings('ignore')

# --- CONFIGURATION: THE 4-MODEL PARAMETERS ---
CSV_FILE = "TSLA_5_Sec_Data.csv"

# Ambitious targets for Entries
ENTRY_PROFIT_PCT = 0.0015    # +0.15% (Looking for big moves)
ENTRY_RISK_PCT = 0.0040      # -0.40% (Wide stop to let it breathe)

# Paranoid targets for Exits
EXIT_DROP_PCT = 0.0008       # 0.08% (Highly sensitive to minor pullbacks)
EXIT_RISK_PCT = 0.0020       # 0.20% (Tighter invalidation)

FUTURE_WINDOW_BARS = 60      # Look ahead 5 minutes (60 bars * 5 seconds)

def calculate_features(df):
    print(">>> Calculating Java-equivalent features...")
    df = df.copy()
    
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Date'] = df['Timestamp'].dt.date
    
    # 1. VWAP
    df['TypicalPrice'] = (df['High'] + df['Low'] + df['Close']) / 3.0
    df['VolxTP'] = df['Volume'] * df['TypicalPrice']
    df['CumVol'] = df.groupby('Date')['Volume'].cumsum()
    df['CumPv'] = df.groupby('Date')['VolxTP'].cumsum()
    df['VWAP'] = df['CumPv'] / df['CumVol']
    df['VWAP'] = df['VWAP'].fillna(df['Close'])
    
    # 2. Daily High/Low
    df['DayHigh'] = df.groupby('Date')['High'].cummax()
    df['DayLow'] = df.groupby('Date')['Low'].cummin()
    
    # 3. Rolling Windows (Optimized for 5-sec bars: 12-bar = 1min, 60-bar = 5min)
    df['SMA_12'] = df['Close'].rolling(window=12).mean()
    df['STD_12'] = df['Close'].rolling(window=12).std()
    df['BB_Lower'] = df['SMA_12'] - (2.5 * df['STD_12'])
    df['BB_Upper'] = df['SMA_12'] + (2.5 * df['STD_12'])
    df['SMA_60'] = df['Close'].rolling(window=60).mean()
    
    # 4. MACD
    df['EMA_12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA_26'] = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_Diff'] = df['MACD'] - df['MACD_Signal']
    
    # 5. ATR (12-bar exponential moving average for 5-sec bars)
    df['PrevClose'] = df['Close'].shift(1)
    df['TR'] = np.maximum(df['High'] - df['Low'], 
               np.maximum(abs(df['High'] - df['PrevClose']), 
                          abs(df['Low'] - df['PrevClose'])))
    df['ATR_12'] = df['TR'].ewm(alpha=1/12, adjust=False).mean()
    
    # 6. RSI
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].fillna(50)

    # 7. Construct Final Features
    df['f_dist_vwap'] = (df['Close'] - df['VWAP']) / df['VWAP']
    df['f_bb_lower_dist'] = (df['Close'] - df['BB_Lower']) / df['Close']
    df['f_bb_upper_dist'] = (df['BB_Upper'] - df['Close']) / df['Close']
    df['f_macd_diff'] = df['MACD_Diff']
    df['f_body_size'] = abs(df['Close'] - df['Open']) / df['Close']
    df['f_lower_wick'] = (df[['Open', 'Close']].min(axis=1) - df['Low']) / df['Close']
    df['f_upper_wick'] = (df['High'] - df[['Open', 'Close']].max(axis=1)) / df['Close']
    df['f_atr_norm'] = df['ATR_12'] / df['Close']
    df['f_dist_sma'] = (df['Close'] - df['SMA_60']) / df['SMA_60']
    df['f_dist_high'] = (df['DayHigh'] - df['Close']) / df['Close']
    df['f_dist_low'] = (df['Close'] - df['DayLow']) / df['Close']
    df['f_rsi'] = df['RSI']
    
    if 'YesterdayClose' in df.columns:
        df['f_gap_from_prev_close'] = (df['Close'] - df['YesterdayClose']) / df['YesterdayClose']
    else:
        df['f_gap_from_prev_close'] = 0.0

    df = df.dropna()
    return df

def generate_labels(df):
    print(">>> Generating 4 distinct path-dependent labels...")
    
    # Look ahead array
    future_highs = df['High'].shift(-1).rolling(window=FUTURE_WINDOW_BARS, min_periods=1).max().shift(-(FUTURE_WINDOW_BARS - 1))
    future_lows = df['Low'].shift(-1).rolling(window=FUTURE_WINDOW_BARS, min_periods=1).min().shift(-(FUTURE_WINDOW_BARS - 1))

    # --- 1. LONG ENTRY (Ambitious: Wants price to go UP heavily) ---
    long_entry_tp = df['Close'] * (1 + ENTRY_PROFIT_PCT)
    long_entry_sl = df['Close'] * (1 - ENTRY_RISK_PCT)
    df['Label_Long_Entry'] = np.where((future_highs >= long_entry_tp) & (future_lows > long_entry_sl), 1, 0)

    # --- 2. SHORT ENTRY (Ambitious: Wants price to go DOWN heavily) ---
    short_entry_tp = df['Close'] * (1 - ENTRY_PROFIT_PCT)
    short_entry_sl = df['Close'] * (1 + ENTRY_RISK_PCT)
    df['Label_Short_Entry'] = np.where((future_lows <= short_entry_tp) & (future_highs < short_entry_sl), 1, 0)

    # --- 3. LONG EXIT (Paranoid: We are Long, looking to spot a minor DROP to secure profits) ---
    # Trigger if price drops 0.08% before it goes up another 0.20%
    long_exit_trigger = df['Close'] * (1 - EXIT_DROP_PCT)
    long_exit_invalid = df['Close'] * (1 + EXIT_RISK_PCT)
    df['Label_Long_Exit'] = np.where((future_lows <= long_exit_trigger) & (future_highs < long_exit_invalid), 1, 0)

    # --- 4. SHORT EXIT (Paranoid: We are Short, looking to spot a minor BOUNCE to cover) ---
    # Trigger if price bounces 0.08% before it drops another 0.20%
    short_exit_trigger = df['Close'] * (1 + EXIT_DROP_PCT)
    short_exit_invalid = df['Close'] * (1 - EXIT_RISK_PCT)
    df['Label_Short_Exit'] = np.where((future_highs >= short_exit_trigger) & (future_lows > short_exit_invalid), 1, 0)

    # Drop the final incomplete window
    df = df.iloc[:-FUTURE_WINDOW_BARS]
    return df

def export_to_onnx(model, feature_count, filename):
    initial_type = [('float_input', FloatTensorType([None, feature_count]))]
    onnx_model = convert_sklearn(model, initial_types=initial_type)
    with open(filename, "wb") as f:
        f.write(onnx_model.SerializeToString())
    print(f">>> Exported Model: {filename}")

def train_and_export(X, y, name, filename):
    print(f"\n--- Training {name} ---")
    print(f"Positive signals found: {sum(y)} / {len(y)}")
    
    if sum(y) == 0:
        print(f"WARNING: No positive labels found for {name}. Adjust your percentages.")
        return

    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    model = RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    
    print(classification_report(y_test, model.predict(X_test)))
    export_to_onnx(model, X.shape[1], filename)

def main():
    print(f">>> Loading historical data from {CSV_FILE}...")
    try:
        raw_df = pd.read_csv(CSV_FILE)
    except FileNotFoundError:
        print(f"ERROR: {CSV_FILE} not found. Please run the Java Historical Bulk Scraper first.")
        return

    df = calculate_features(raw_df)
    df = generate_labels(df)

    feature_cols = [
        'f_dist_vwap', 'f_bb_lower_dist', 'f_bb_upper_dist', 'f_macd_diff',
        'f_body_size', 'f_lower_wick', 'f_upper_wick', 'f_atr_norm',
        'f_dist_sma', 'f_dist_high', 'f_dist_low', 'f_rsi', 'f_gap_from_prev_close'
    ]
    X = df[feature_cols].values.astype(np.float32)

    # Train all 4 models
    train_and_export(X, df['Label_Long_Entry'].values, "LONG ENTRY (Dip Buyer)", "long_entry.onnx")
    train_and_export(X, df['Label_Short_Entry'].values, "SHORT ENTRY (Rip Seller)", "short_entry.onnx")
    train_and_export(X, df['Label_Long_Exit'].values, "LONG EXIT (Top Detector)", "long_exit.onnx")
    train_and_export(X, df['Label_Short_Exit'].values, "SHORT EXIT (Bottom Detector)", "short_exit.onnx")

    print("\n>>> SUCCESS! All 4 models have been generated. Move them to your Spring Boot resources folder.")

if __name__ == "__main__":
    main()