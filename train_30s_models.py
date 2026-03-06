import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score, recall_score
from sklearn.model_selection import TimeSeriesSplit
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import warnings
warnings.filterwarnings('ignore')

# --- CONFIGURATION: THE 4-MODEL PARAMETERS ---
# CHANGED: Now pointing to the new 30-second aggregated data
CSV_FILE = r"C:\data harvested\TSLA_30Sec_Historical_Bulk.csv"

# Hybrid targets for Entries (best overall from 3-way comparison)
ENTRY_PROFIT_PCT = 0.0014    # +0.14%
ENTRY_RISK_PCT = 0.0030      # -0.30%

# Hybrid targets for Exits
EXIT_DROP_PCT = 0.0009       # 0.09%
EXIT_RISK_PCT = 0.0017       # 0.17%

# CHANGED: 10 bars * 30 seconds = 5 minutes lookahead
FUTURE_WINDOW_BARS = 10      

# Walk-Forward Settings
N_SPLITS = 5                 # Number of sliding windows to test

# Keep Java compatibility by default (23 features). Turn on to train richer models.
USE_EXTENDED_FEATURES = False

# Threshold optimization on probabilities
MIN_RECALL = 0.05
MIN_PRED_POS_RATE = 0.02
THRESHOLD_GRID = np.arange(0.30, 0.91, 0.02)

# Volatility regime multipliers by hour bucket (TSLA 30s observations)
# open: 9-10 ET, midday: 11-14 ET, close: 15 ET
REGIME_MULTIPLIERS = {
    'open': {'entry_profit': 1.20, 'entry_risk': 1.10, 'exit_drop': 1.20, 'exit_risk': 1.10},
    'midday': {'entry_profit': 0.90, 'entry_risk': 0.90, 'exit_drop': 0.90, 'exit_risk': 0.90},
    'close': {'entry_profit': 1.00, 'entry_risk': 1.00, 'exit_drop': 1.00, 'exit_risk': 1.00},
}


def _hour_bucket(hour):
    if hour in (9, 10):
        return 'open'
    if hour in (11, 12, 13, 14):
        return 'midday'
    return 'close'

def calculate_features(df):
    print(">>> Calculating Java-equivalent features (30-second adjusted)...")
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
    df['MinuteOfDay'] = df['Hour'] * 60 + df['Minute']
    
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
    
    # 3. Rolling Windows (On a 30s chart: 12-bar = 6 mins, 60-bar = 30 mins)
    df['SMA_12'] = df['Close'].rolling(window=12).mean()
    df['STD_12'] = df['Close'].rolling(window=12).std(ddof=1) 
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

    # --- Extended intraday context indicators ---
    # 1) Relative volume vs same minute-of-day baseline
    minute_median_vol = df.groupby('MinuteOfDay')['Volume'].transform('median')
    df['f_rel_volume_30s'] = df['Volume'] / (minute_median_vol + 1.0)

    # 2) Realized volatility regime (20 bars ~= 10 minutes)
    df['Ret_30s'] = df['Close'].pct_change()
    df['f_realized_vol_20'] = df['Ret_30s'].rolling(window=20).std(ddof=1)
    vol_mean = df['f_realized_vol_20'].rolling(window=100).mean()
    vol_std = df['f_realized_vol_20'].rolling(window=100).std(ddof=1)
    df['f_realized_vol_z'] = (df['f_realized_vol_20'] - vol_mean) / (vol_std + 1e-9)

    # 3) Opening range distances (first 10 bars = first 5 minutes)
    first_10_high = (
        df.groupby('Date')['High']
        .transform(lambda s: s.iloc[:10].max() if len(s) > 0 else np.nan)
    )
    first_10_low = (
        df.groupby('Date')['Low']
        .transform(lambda s: s.iloc[:10].min() if len(s) > 0 else np.nan)
    )
    df['f_dist_or_high_atr'] = (first_10_high - df['Close']) / (df['ATR_12'] + 1e-9)
    df['f_dist_or_low_atr'] = (df['Close'] - first_10_low) / (df['ATR_12'] + 1e-9)

    # 4) Spread / L1 book imbalance / signed flow (only if columns exist)
    if {'Bid', 'Ask'}.issubset(df.columns):
        bid = pd.to_numeric(df['Bid'], errors='coerce').fillna(df['Close'])
        ask = pd.to_numeric(df['Ask'], errors='coerce').fillna(df['Close'])
        mid = (bid + ask) / 2.0
        df['f_spread_pct'] = (ask - bid) / (mid + 1e-9)
        spread_mean = df['f_spread_pct'].rolling(window=100).mean()
        spread_std = df['f_spread_pct'].rolling(window=100).std(ddof=1)
        df['f_spread_z'] = (df['f_spread_pct'] - spread_mean) / (spread_std + 1e-9)
    else:
        df['f_spread_pct'] = 0.0
        df['f_spread_z'] = 0.0

    if {'BidSize', 'AskSize'}.issubset(df.columns):
        bid_size = pd.to_numeric(df['BidSize'], errors='coerce').fillna(0.0)
        ask_size = pd.to_numeric(df['AskSize'], errors='coerce').fillna(0.0)
        df['f_l1_imbalance'] = (bid_size - ask_size) / (bid_size + ask_size + 1.0)
    else:
        df['f_l1_imbalance'] = 0.0

    # Accept common naming variants for aggressive buy/sell flow.
    buy_candidates = ['AggBuyVol', 'BuyVol', 'AtAskVol']
    sell_candidates = ['AggSellVol', 'SellVol', 'AtBidVol']
    buy_col = next((c for c in buy_candidates if c in df.columns), None)
    sell_col = next((c for c in sell_candidates if c in df.columns), None)
    if buy_col and sell_col:
        buy_v = pd.to_numeric(df[buy_col], errors='coerce').fillna(0.0)
        sell_v = pd.to_numeric(df[sell_col], errors='coerce').fillna(0.0)
        df['f_signed_flow_30s'] = (buy_v - sell_v) / (buy_v + sell_v + 1.0)
    else:
        df['f_signed_flow_30s'] = 0.0
    
    # 6. RSI (14-bar Wilder)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].fillna(50)

    # --- NEW: Price Action & Support/Resistance ---
    # 60-bar (30-minute) local resistance and support
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

    # NEW PA FEATURES
    df['f_dist_swing_high'] = (df['SwingHigh_60'] - df['Close']) / df['Close']
    df['f_dist_swing_low'] = (df['Close'] - df['SwingLow_60']) / df['Close']
    df['f_is_new_high'] = np.where(df['Close'] >= df['DayHigh'], 1.0, 0.0) 
    df['f_is_new_low'] = np.where(df['Close'] <= df['DayLow'], 1.0, 0.0)

    # WHOLE NUMBER & CANDLE DIRECTION
    df['f_dist_whole_num'] = np.abs(df['Close'] - np.round(df['Close']))
    is_green = df['Close'] >= df['Open']
    df['f_is_green'] = np.where(is_green, 1.0, -1.0)
    # Consecutive candle streaks (per day)
    df['is_red'] = (df['Close'] < df['Open']).astype(int)
    df['is_green'] = (df['Close'] >= df['Open']).astype(int)

    def _streak(series):
        streak = 0
        out = []
        for val in series:
            if val:
                streak += 1
            else:
                streak = 0
            out.append(streak)
        return pd.Series(out, index=series.index)

    df['f_red_streak'] = df.groupby('Date', sort=False)['is_red'].apply(_streak).reset_index(level=0, drop=True)
    df['f_green_streak'] = df.groupby('Date', sort=False)['is_green'].apply(_streak).reset_index(level=0, drop=True)
    
    # Use real order-flow/options fields when present, else safe defaults.
    if {'BidVol', 'Volume'}.issubset(df.columns):
        df['f_vol_bid_ratio'] = pd.to_numeric(df['BidVol'], errors='coerce').fillna(0.0) / (df['Volume'] + 1.0)
    else:
        df['f_vol_bid_ratio'] = 0.33

    if {'AskVol', 'Volume'}.issubset(df.columns):
        df['f_vol_ask_ratio'] = pd.to_numeric(df['AskVol'], errors='coerce').fillna(0.0) / (df['Volume'] + 1.0)
    else:
        df['f_vol_ask_ratio'] = 0.33

    if {'PutVol', 'CallVol'}.issubset(df.columns):
        put_vol = pd.to_numeric(df['PutVol'], errors='coerce').fillna(0.0)
        call_vol = pd.to_numeric(df['CallVol'], errors='coerce').fillna(0.0)
        df['f_put_call_ratio'] = put_vol / (call_vol + 1.0)
    else:
        df['f_put_call_ratio'] = 1.0

    df = df.dropna()
    return df


def _adaptive_thresholds(hour, atr_norm):
    bucket = _hour_bucket(int(hour))
    m = REGIME_MULTIPLIERS[bucket]

    # Light ATR scaling to avoid over-reacting to spikes.
    atr_scale = float(np.clip(atr_norm / 0.0010, 0.75, 1.50))

    entry_profit = ENTRY_PROFIT_PCT * m['entry_profit'] * atr_scale
    entry_risk = ENTRY_RISK_PCT * m['entry_risk'] * atr_scale
    exit_drop = EXIT_DROP_PCT * m['exit_drop'] * atr_scale
    exit_risk = EXIT_RISK_PCT * m['exit_risk'] * atr_scale

    return entry_profit, entry_risk, exit_drop, exit_risk


def generate_labels(df):
    print(">>> Generating 4 event-ordered path-dependent labels...")

    df = df.copy()
    n = len(df)
    closes = df['Close'].values
    highs = df['High'].values
    lows = df['Low'].values
    hours = df['Hour'].values
    atr_norm = (df['ATR_12'] / df['Close']).replace([np.inf, -np.inf], np.nan).fillna(0.0010).values

    y_long_entry = np.zeros(n, dtype=np.int8)
    y_short_entry = np.zeros(n, dtype=np.int8)
    y_long_exit = np.zeros(n, dtype=np.int8)
    y_short_exit = np.zeros(n, dtype=np.int8)

    usable = n - FUTURE_WINDOW_BARS
    for i in range(usable):
        ep, er, xd, xr = _adaptive_thresholds(hours[i], atr_norm[i])

        c = closes[i]
        le_tp = c * (1.0 + ep)
        le_sl = c * (1.0 - er)

        se_tp = c * (1.0 - ep)
        se_sl = c * (1.0 + er)

        lx_tp = c * (1.0 - xd)
        lx_sl = c * (1.0 + xr)

        sx_tp = c * (1.0 + xd)
        sx_sl = c * (1.0 - xr)

        for j in range(i + 1, i + FUTURE_WINDOW_BARS + 1):
            h = highs[j]
            l = lows[j]

            # LONG ENTRY: TP up, SL down.
            if y_long_entry[i] == 0:
                le_hit_tp = h >= le_tp
                le_hit_sl = l <= le_sl
                if le_hit_tp and le_hit_sl:
                    break
                if le_hit_tp:
                    y_long_entry[i] = 1
                    break
                if le_hit_sl:
                    break

        for j in range(i + 1, i + FUTURE_WINDOW_BARS + 1):
            h = highs[j]
            l = lows[j]

            # SHORT ENTRY: TP down, SL up.
            se_hit_tp = l <= se_tp
            se_hit_sl = h >= se_sl
            if se_hit_tp and se_hit_sl:
                break
            if se_hit_tp:
                y_short_entry[i] = 1
                break
            if se_hit_sl:
                break

        for j in range(i + 1, i + FUTURE_WINDOW_BARS + 1):
            h = highs[j]
            l = lows[j]

            # LONG EXIT: trigger down, invalidation up.
            lx_hit_tp = l <= lx_tp
            lx_hit_sl = h >= lx_sl
            if lx_hit_tp and lx_hit_sl:
                break
            if lx_hit_tp:
                y_long_exit[i] = 1
                break
            if lx_hit_sl:
                break

        for j in range(i + 1, i + FUTURE_WINDOW_BARS + 1):
            h = highs[j]
            l = lows[j]

            # SHORT EXIT: trigger up, invalidation down.
            sx_hit_tp = h >= sx_tp
            sx_hit_sl = l <= sx_sl
            if sx_hit_tp and sx_hit_sl:
                break
            if sx_hit_tp:
                y_short_exit[i] = 1
                break
            if sx_hit_sl:
                break

    df['Label_Long_Entry'] = y_long_entry
    df['Label_Short_Entry'] = y_short_entry
    df['Label_Long_Exit'] = y_long_exit
    df['Label_Short_Exit'] = y_short_exit

    df = df.iloc[:usable].copy()
    return df


def print_label_prevalence_by_hour(df):
    hour_stats = (
        df.groupby('Hour')[['Label_Long_Entry', 'Label_Short_Entry', 'Label_Long_Exit', 'Label_Short_Exit']]
        .mean()
        .round(3)
    )
    print("\n>>> Label prevalence by hour (ET):")
    print(hour_stats.to_string())

def export_to_onnx(model, feature_count, filename, alias_filename=None):
    initial_type = [('float_input', FloatTensorType([None, feature_count]))]
    onnx_model = convert_sklearn(model, initial_types=initial_type, target_opset=12)
    if getattr(onnx_model, 'ir_version', 0) > 9:
        onnx_model.ir_version = 9
    model_bytes = onnx_model.SerializeToString()

    out_path = Path(filename)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(model_bytes)

    if alias_filename:
        alias_path = Path(alias_filename)
        alias_path.parent.mkdir(parents=True, exist_ok=True)
        with open(alias_path, "wb") as f:
            f.write(model_bytes)

    opsets = ",".join(str(imp.version) for imp in onnx_model.opset_import)
    print(f">>> Exported Production Model: {out_path} (ir={onnx_model.ir_version}, opset={opsets})")
    if alias_filename:
        print(f">>> Updated Canonical Model: {alias_filename}")


def optimize_threshold(y_true, probas):
    best_thr = 0.50
    best_prec = -1.0
    best_recall = 0.0
    best_pos_rate = 0.0

    for thr in THRESHOLD_GRID:
        preds = (probas >= thr).astype(np.int8)
        pos_rate = preds.mean()
        if pos_rate <= 0:
            continue

        rec = recall_score(y_true, preds, zero_division=0)
        if rec < MIN_RECALL or pos_rate < MIN_PRED_POS_RATE:
            continue

        prec = precision_score(y_true, preds, zero_division=0)
        if prec > best_prec:
            best_prec = prec
            best_thr = float(thr)
            best_recall = float(rec)
            best_pos_rate = float(pos_rate)

    if best_prec < 0:
        # Fallback: keep default threshold
        return 0.50, 0.0, 0.0, 0.0
    return best_thr, best_prec, best_recall, best_pos_rate

def perform_walk_forward_testing(X, y, name):
    print(f"\n=========================================")
    print(f"--- Walk-Forward Testing: {name} ---")
    total_signals = int(np.sum(y, dtype=np.int64))
    print(f"Total signals found: {total_signals} / {len(y)}")
    
    if int(np.sum(y, dtype=np.int64)) == 0:
        print(f"WARNING: No positive labels found. Adjust target percentages.")
        return {
            'model': None,
            'total_signals': total_signals,
            'total_rows': len(y),
            'avg_precision': 0.0,
            'avg_threshold': 0.50,
            'folds_used': 0,
        }

    tscv = TimeSeriesSplit(n_splits=N_SPLITS, gap=FUTURE_WINDOW_BARS)
    
    fold = 1
    precisions = []
    thresholds = []
    
    for train_index, test_index in tscv.split(X):
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]
        
        if int(np.sum(y_train, dtype=np.int64)) == 0 or int(np.sum(y_test, dtype=np.int64)) == 0:
            print(f"Fold {fold}: Skipped (insufficient signal data in split)")
            fold += 1
            continue

        model = RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1)
        model.fit(X_train, y_train)

        # Calibrate threshold from the tail of train split (time-consistent).
        calib_size = max(200, int(len(X_train) * 0.2))
        calib_size = min(calib_size, len(X_train) - 1)
        if calib_size > 0:
            X_cal = X_train[-calib_size:]
            y_cal = y_train[-calib_size:]
            cal_proba = model.predict_proba(X_cal)[:, 1]
            thr, cal_prec, cal_rec, cal_pos = optimize_threshold(y_cal, cal_proba)
        else:
            thr, cal_prec, cal_rec, cal_pos = (0.5, 0.0, 0.0, 0.0)

        test_proba = model.predict_proba(X_test)[:, 1]
        y_pred = (test_proba >= thr).astype(np.int8)
        
        prec = precision_score(y_test, y_pred, zero_division=0)
        precisions.append(prec)
        thresholds.append(thr)
        
        print(
            f"Fold {fold} | Train Size: {len(X_train)} | Test Size: {len(X_test)} "
            f"| Thr: {thr:.2f} | CalP/R/Pos: {cal_prec:.2%}/{cal_rec:.2%}/{cal_pos:.2%} "
            f"| Out-of-Sample Precision: {prec:.2%}"
        )
        fold += 1

    avg_precision = np.mean(precisions) if precisions else 0.0
    avg_threshold = np.mean(thresholds) if thresholds else 0.5
    if precisions:
        print(f">>> Average Walk-Forward Precision: {avg_precision:.2%}")
        print(f">>> Average Tuned Threshold: {avg_threshold:.2f}")
        if avg_precision < 0.40:
            print(f"!!! WARNING: This model shows poor out-of-sample prediction power.")
    
    print(f"\n>>> Training Final Production Model on 100% of data...")
    final_model = RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1)
    final_model.fit(X, y)
    return {
        'model': final_model,
        'total_signals': total_signals,
        'total_rows': len(y),
        'avg_precision': float(avg_precision),
        'avg_threshold': float(avg_threshold),
        'folds_used': len(precisions),
    }

def main():
    run_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    versioned_out_dir = Path("model_exports") / run_tag

    print(f">>> Loading historical data from {CSV_FILE}...")
    try:
        raw_df = pd.read_csv(CSV_FILE)
    except FileNotFoundError:
        print(f"ERROR: {CSV_FILE} not found. Please run the Java Historical Bulk Scraper first.")
        return

    df = calculate_features(raw_df)
    df = generate_labels(df)
    print_label_prevalence_by_hour(df)

    # EXACT 23-FEATURE ARRAY TO MATCH JAVA
    feature_cols = [
        'f_dist_vwap', 'f_bb_lower_dist', 'f_bb_upper_dist', 'f_macd_diff',
        'f_body_size', 'f_lower_wick', 'f_upper_wick', 'f_atr_norm',
        'f_dist_sma', 'f_dist_high', 'f_dist_low', 'f_rsi', 'f_gap_from_prev_close',
        'f_time_of_day', 'f_dist_swing_high', 'f_dist_swing_low', 'f_is_new_high', 
        'f_is_new_low', 'f_dist_whole_num', 'f_is_green', 'f_green_streak',
        'f_red_streak', 'f_put_call_ratio', 'f_vol_ask_ratio', 'f_vol_bid_ratio'
    ]

    extended_feature_cols = [
        'f_rel_volume_30s', 'f_realized_vol_20', 'f_realized_vol_z',
        'f_dist_or_high_atr', 'f_dist_or_low_atr',
        'f_spread_pct', 'f_spread_z', 'f_l1_imbalance', 'f_signed_flow_30s'
    ]

    if USE_EXTENDED_FEATURES:
        feature_cols = feature_cols + extended_feature_cols
        print(">>> Using extended feature set (NOT Java-compatible without Java feature updates).")
    else:
        print(">>> Using base 23-feature set (Java-compatible).")

    print(f">>> Feature count used for training: {len(feature_cols)}")
    X = df[feature_cols].values.astype(np.float32)

    models = [
        ("LONG ENTRY (Dip Buyer)", df['Label_Long_Entry'].values, "long_entry.onnx"),
        ("SHORT ENTRY (Rip Seller)", df['Label_Short_Entry'].values, "short_entry.onnx"),
        ("LONG EXIT (Top Detector)", df['Label_Long_Exit'].values, "long_exit.onnx"),
        ("SHORT EXIT (Bottom Detector)", df['Label_Short_Exit'].values, "short_exit.onnx")
    ]

    score_rows = []
    for name, y_data, filename in models:
        result = perform_walk_forward_testing(X, y_data, name)
        exported_path = "-"
        if result['model'] is not None:
            versioned_path = versioned_out_dir / filename
            resources_path = Path("src") / "main" / "resources" / filename
            export_to_onnx(result['model'], len(feature_cols), str(versioned_path), alias_filename=str(resources_path))
            exported_path = str(versioned_path)

        score_rows.append({
            'model': name,
            'signals': result['total_signals'],
            'rows': result['total_rows'],
            'signal_rate': (result['total_signals'] / result['total_rows']) if result['total_rows'] else 0.0,
            'avg_precision': result['avg_precision'],
            'avg_threshold': result['avg_threshold'],
            'folds_used': result['folds_used'],
            'exported_to': exported_path,
        })

    print("\n>>> MODEL SCORECARD")
    print("Model | Signals/Rows | SignalRate | AvgPrecision | AvgThreshold | Folds | Export")
    for row in score_rows:
        print(
            f"{row['model']} | "
            f"{row['signals']}/{row['rows']} | "
            f"{row['signal_rate']:.2%} | "
            f"{row['avg_precision']:.2%} | "
            f"{row['avg_threshold']:.2f} | "
            f"{row['folds_used']} | "
            f"{row['exported_to']}"
        )

    print("\n==================================================")
    print(">>> PIPELINE COMPLETE.")
    print(">>> All 30-second models have been exported.")
    print(f">>> Versioned copy folder: {versioned_out_dir}")
    print(">>> Drop the .onnx files directly into your new Java branch.")
    print("==================================================")

if __name__ == "__main__":
    main()