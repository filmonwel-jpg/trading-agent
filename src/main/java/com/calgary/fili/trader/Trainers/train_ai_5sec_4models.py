import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score, precision_score
from sklearn.model_selection import TimeSeriesSplit
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import warnings
warnings.filterwarnings('ignore')

# --- CONFIGURATION: THE 4-MODEL PARAMETERS ---
CSV_FILE = "TSLA_5Sec_Historical_Bulk_20260228_1558_clean.csv"

# Ambitious targets for Entries
ENTRY_PROFIT_PCT = 0.0015    # +0.15% (Looking for big moves)
ENTRY_RISK_PCT = 0.0040      # -0.40% (Wide stop to let it breathe)

# Paranoid targets for Exits
EXIT_DROP_PCT = 0.0008       # 0.08% (Highly sensitive to minor pullbacks)
EXIT_RISK_PCT = 0.0020       # 0.20% (Tighter invalidation)

FUTURE_WINDOW_BARS = 60      # Look ahead 5 minutes (60 bars * 5 seconds)

# Walk-Forward Settings
N_SPLITS = 5                 # Number of sliding windows to test

def calculate_features(df):
    print(">>> Calculating Java-equivalent features (12-bar & 60-bar aligned)...")
    df = df.copy()

    ts_str = df['Timestamp'].astype(str).str.strip()
    df['Timestamp'] = pd.to_datetime(
        ts_str,
        format='%Y%m%d %H:%M:%S America/New_York',
        errors='coerce'
    )
    if df['Timestamp'].isna().any():
        fallback = pd.to_datetime(ts_str, errors='coerce')
        df['Timestamp'] = df['Timestamp'].fillna(fallback)
    if df['Timestamp'].isna().any():
        bad_samples = ts_str[df['Timestamp'].isna()].head(3).tolist()
        raise ValueError(f"Unable to parse some Timestamp values. Examples: {bad_samples}")

    df['Date'] = df['Timestamp'].dt.date
    
    # Time of Day (normalized 0-1 for intraday patterns)
    df['Hour'] = df['Timestamp'].dt.hour
    df['Minute'] = df['Timestamp'].dt.minute
    df['f_time_of_day'] = (df['Hour'] * 60 + df['Minute']) / (24 * 60)
    
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
    
    # 3. Rolling Windows (12-bar = 1min, 60-bar = 5min)
    df['SMA_12'] = df['Close'].rolling(window=12).mean()
    df['STD_12'] = df['Close'].rolling(window=12).std(ddof=1) # ddof=1 matches Java's N-1 sample variance
    df['BB_Lower'] = df['SMA_12'] - (2.5 * df['STD_12'])
    df['BB_Upper'] = df['SMA_12'] + (2.5 * df['STD_12'])
    df['SMA_60'] = df['Close'].rolling(window=60).mean()
    
    # 4. MACD
    df['EMA_12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA_26'] = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_Diff'] = df['MACD'] - df['MACD_Signal']
    
    # 5. ATR (12-bar)
    df['PrevClose'] = df['Close'].shift(1)
    df['TR'] = np.maximum(df['High'] - df['Low'], 
               np.maximum(abs(df['High'] - df['PrevClose']), 
                          abs(df['Low'] - df['PrevClose'])))
    df['ATR_12'] = df['TR'].ewm(alpha=1/12, adjust=False).mean()
    
    # 6. RSI (14-bar Wilder)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].fillna(50)

    # --- NEW: Price Action & Support/Resistance ---
    # 60-bar (5-minute) local resistance and support
    df['SwingHigh_60'] = df['High'].rolling(window=60).max()
    df['SwingLow_60'] = df['Low'].rolling(window=60).min()

    # 7. Construct Final 23 Features
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

    # NEW: Time of Day feature
    # f_time_of_day already calculated above

    # NEW PA FEATURES
    df['f_dist_swing_high'] = (df['SwingHigh_60'] - df['Close']) / df['Close']
    df['f_dist_swing_low'] = (df['Close'] - df['SwingLow_60']) / df['Close']
    
    # 1.0 if we are making a new Day High (Breakout), else 0.0
    df['f_is_new_high'] = np.where(df['Close'] >= df['DayHigh'], 1.0, 0.0) 
    
    # 1.0 if we are making a new Day Low (Breakdown), else 0.0
    df['f_is_new_low'] = np.where(df['Close'] <= df['DayLow'], 1.0, 0.0)

    # WHOLE NUMBER & CANDLE DIRECTION
    df['f_dist_whole_num'] = np.abs(df['Close'] - np.round(df['Close']))
    df['f_is_green'] = np.where(df['Close'] >= df['Open'], 1.0, -1.0)
    
    # PLACEHOLDERS FOR OPTIONS & ORDER FLOW
    df['f_put_call_ratio'] = 1.0
    df['f_vol_ask_ratio'] = 0.33
    df['f_vol_bid_ratio'] = 0.33

    df = df.dropna()
    return df

def generate_labels(df):
    print(">>> Generating 4 distinct path-dependent labels...")
    
    # Shift by -1 to prevent lookahead bias (don't include current bar in future look)
    future_highs = df['High'].shift(-1).rolling(window=FUTURE_WINDOW_BARS, min_periods=1).max().shift(-(FUTURE_WINDOW_BARS - 1))
    future_lows = df['Low'].shift(-1).rolling(window=FUTURE_WINDOW_BARS, min_periods=1).min().shift(-(FUTURE_WINDOW_BARS - 1))

    # --- 1. LONG ENTRY ---
    long_entry_tp = df['Close'] * (1 + ENTRY_PROFIT_PCT)
    long_entry_sl = df['Close'] * (1 - ENTRY_RISK_PCT)
    df['Label_Long_Entry'] = np.where((future_highs >= long_entry_tp) & (future_lows > long_entry_sl), 1, 0)

    # --- 2. SHORT ENTRY ---
    short_entry_tp = df['Close'] * (1 - ENTRY_PROFIT_PCT)
    short_entry_sl = df['Close'] * (1 + ENTRY_RISK_PCT)
    df['Label_Short_Entry'] = np.where((future_lows <= short_entry_tp) & (future_highs < short_entry_sl), 1, 0)

    # --- 3. LONG EXIT ---
    long_exit_trigger = df['Close'] * (1 - EXIT_DROP_PCT)
    long_exit_invalid = df['Close'] * (1 + EXIT_RISK_PCT)
    df['Label_Long_Exit'] = np.where((future_lows <= long_exit_trigger) & (future_highs < long_exit_invalid), 1, 0)

    # --- 4. SHORT EXIT ---
    short_exit_trigger = df['Close'] * (1 + EXIT_DROP_PCT)
    short_exit_invalid = df['Close'] * (1 - EXIT_RISK_PCT)
    df['Label_Short_Exit'] = np.where((future_highs >= short_exit_trigger) & (future_lows > short_exit_invalid), 1, 0)

    df = df.iloc[:-FUTURE_WINDOW_BARS]
    return df

def export_to_onnx(model, feature_count, filename):
    initial_type = [('float_input', FloatTensorType([None, feature_count]))]
    onnx_model = convert_sklearn(model, initial_types=initial_type, target_opset=12)
    if getattr(onnx_model, 'ir_version', 0) > 9:
        onnx_model.ir_version = 9
    with open(filename, "wb") as f:
        f.write(onnx_model.SerializeToString())
    opsets = ",".join(str(imp.version) for imp in onnx_model.opset_import)
    print(f">>> Exported Production Model: {filename} (ir={onnx_model.ir_version}, opset={opsets})")

def perform_walk_forward_testing(X, y, name):
    print(f"\n=========================================")
    print(f"--- Walk-Forward Testing: {name} ---")
    print(f"Total signals found: {sum(y)} / {len(y)}")
    
    if sum(y) == 0:
        print(f"WARNING: No positive labels found. Adjust target percentages.")
        return None

    # TimeSeriesSplit creates expanding training windows and shifting test windows
    tscv = TimeSeriesSplit(n_splits=N_SPLITS)
    
    fold = 1
    precisions = []
    
    for train_index, test_index in tscv.split(X):
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]
        
        # If a fold has no positive labels, skip it
        if sum(y_train) == 0 or sum(y_test) == 0:
            print(f"Fold {fold}: Skipped (insufficient signal data in split)")
            fold += 1
            continue

        model = RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        
        # Calculate Precision (When the AI says "Trade", how often is it right?)
        prec = precision_score(y_test, y_pred, zero_division=0)
        precisions.append(prec)
        
        print(f"Fold {fold} | Train Size: {len(X_train)} | Test Size: {len(X_test)} | Out-of-Sample Precision: {prec:.2%}")
        fold += 1

    if precisions:
        avg_precision = np.mean(precisions)
        print(f">>> Average Walk-Forward Precision: {avg_precision:.2%}")
        if avg_precision < 0.40:
            print(f"!!! WARNING: This model shows poor out-of-sample prediction power.")
    
    # After proving it works in Walk-Forward, train the FINAL model on 100% of the data for live deployment
    print(f"\n>>> Training Final Production Model on 100% of data...")
    final_model = RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1)
    final_model.fit(X, y)
    return final_model

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
        'f_dist_sma', 'f_dist_high', 'f_dist_low', 'f_rsi', 'f_gap_from_prev_close',
        'f_time_of_day',
        # --- The 4 New Features ---
        'f_dist_swing_high', 'f_dist_swing_low', 'f_is_new_high', 'f_is_new_low',
        'f_dist_whole_num', 'f_is_green', 'f_put_call_ratio',
        'f_vol_ask_ratio', 'f_vol_bid_ratio'
    ]
    X = df[feature_cols].values.astype(np.float32)

    # Train and test all 4 models using Walk-Forward Optimization
    models = [
        ("LONG ENTRY (Dip Buyer)", df['Label_Long_Entry'].values, "long_entry.onnx"),
        ("SHORT ENTRY (Rip Seller)", df['Label_Short_Entry'].values, "short_entry.onnx"),
        ("LONG EXIT (Top Detector)", df['Label_Long_Exit'].values, "long_exit.onnx"),
        ("SHORT EXIT (Bottom Detector)", df['Label_Short_Exit'].values, "short_exit.onnx")
    ]

    for name, y_data, filename in models:
        trained_model = perform_walk_forward_testing(X, y_data, name)
        if trained_model:
            export_to_onnx(trained_model, len(feature_cols), filename)

    print("\n==================================================")
    print(">>> PIPELINE COMPLETE.")
    print(">>> All models have been walk-forward tested and exported.")
    print(">>> Move the .onnx files to your Spring Boot resources folder.")
    print("==================================================")

if __name__ == "__main__":
    main()