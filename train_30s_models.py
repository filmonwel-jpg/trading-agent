import argparse
import hashlib
import json
import math
import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score, recall_score, accuracy_score
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import warnings
warnings.filterwarnings('ignore')

try:
    from onnxmltools import convert_lightgbm as convert_lightgbm_onnx
except Exception:
    convert_lightgbm_onnx = None

try:
    import onnx
except Exception:
    onnx = None

try:
    from lightgbm import LGBMClassifier
    LIGHTGBM_IMPORT_ERROR = ""
except Exception as exc:
    LGBMClassifier = None
    LIGHTGBM_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

try:
    from catboost import CatBoostClassifier
    CATBOOST_IMPORT_ERROR = ""
except Exception as exc:
    CatBoostClassifier = None
    CATBOOST_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

# --- CONFIGURATION: THE 4-MODEL PARAMETERS ---
# CHANGED: Now pointing to the new 30-second aggregated data
DEFAULT_CSV_FILE = Path(__file__).resolve().parent / "TSLA_30Sec_Historical_Bulk_fromTrainer.csv"
DEFAULT_SOURCE_5S_CLEAN_FILE = Path(__file__).resolve().parent / "TSLA_5Sec_Historical_Bulk_20260228_1558_clean.csv"
AUTO_BUILD_30S_IF_MISSING = os.getenv('AUTO_BUILD_30S_IF_MISSING', '1').strip().lower() not in ('0', 'false', 'no', 'off')

# Stronger targets for Entries (enforce positive R and handle costs better)
ENTRY_PROFIT_PCT = 0.0035    # +0.35%
ENTRY_RISK_PCT = 0.0025      # -0.25%
ENTRY_FILL_MODE = os.getenv('ENTRY_FILL_MODE', 'next_open').strip().lower()
ENTRY_SLIPPAGE_BPS = float(os.getenv('ENTRY_SLIPPAGE_BPS', '2.0'))
EXIT_SLIPPAGE_BPS = float(os.getenv('EXIT_SLIPPAGE_BPS', '2.0'))
MIN_NET_R_MULTIPLE = float(os.getenv('MIN_NET_R_MULTIPLE', '1.2'))
SUPPORTED_ENTRY_FILL_MODES = {'current_close', 'next_open', 'next_open_with_slippage'}
COST_AWARE_LABELS = os.getenv('COST_AWARE_LABELS', '1').strip().lower() not in ('0', 'false', 'no', 'off')
COST_AWARE_LABEL_SCHEMA_VERSION = 'setup_cost_aware_labels_v1'
COST_AWARE_MIN_NET_R_LABEL = float(os.getenv('COST_AWARE_MIN_NET_R_LABEL', '0.0'))
COST_AWARE_DEFAULT_SPREAD_BPS = float(os.getenv('COST_AWARE_DEFAULT_SPREAD_BPS', '0.0'))
COST_AWARE_FILL_PROBABILITY = float(os.getenv('COST_AWARE_FILL_PROBABILITY', '0.98'))
COST_AWARE_PARTIAL_FILL_PENALTY_R = float(os.getenv('COST_AWARE_PARTIAL_FILL_PENALTY_R', '0.02'))
COST_AWARE_MISSED_FILL_PENALTY_R = float(os.getenv('COST_AWARE_MISSED_FILL_PENALTY_R', '0.05'))

# Adjusted targets for Exits to act as material hazard/unwind detectors
EXIT_DROP_PCT = 0.0020       # 0.20%
EXIT_RISK_PCT = 0.0010       # 0.10%

# CHANGED: 20 bars * 30 seconds = 10 minutes lookahead for cleaner follow-through detection
FUTURE_WINDOW_BARS = 20      

# Walk-Forward Settings
N_SPLITS = 5                 # Number of sliding windows to test
DAY_GAP_BETWEEN_TRAIN_TEST = 0  # Optional day embargo between train and test folds.

# Keep Java compatibility by default (30 features: legacy 25 + 5 extended core features).
# Turn on to include the remaining 4 extended microstructure features.
USE_EXTENDED_FEATURES = False

# Optional training-only meta features emitted by build_30s_from_5s_csv.py producer baselines.
# Keep disabled by default so production Java shape remains unchanged unless explicitly enabled.
USE_META_PRODUCER_FEATURES = os.getenv('USE_META_PRODUCER_FEATURES', '0').strip().lower() not in ('0', 'false', 'no', 'off')
USE_NEWS_BAR_FEATURES = os.getenv('USE_NEWS_BAR_FEATURES', '1').strip().lower() not in ('0', 'false', 'no', 'off')
USE_REGIME_PROB_FEATURES = os.getenv('USE_REGIME_PROB_FEATURES', '1').strip().lower() not in ('0', 'false', 'no', 'off')
TRAIN_LEGACY_30S_EXIT_MODELS = os.getenv('TRAIN_LEGACY_30S_EXIT_MODELS', '1').strip().lower() not in ('0', 'false', 'no', 'off')
UPDATE_CANONICAL_MODEL_ALIASES = os.getenv('UPDATE_CANONICAL_MODEL_ALIASES', '1').strip().lower() not in ('0', 'false', 'no', 'off')
MODEL_EXPORTS_ROOT = Path(os.getenv('MODEL_EXPORTS_ROOT', 'model_exports')).expanduser()

MODEL_FAMILY = os.getenv('MODEL_FAMILY', 'random_forest').strip().lower()
REGIME_MODEL_FAMILY = os.getenv('REGIME_MODEL_FAMILY', MODEL_FAMILY).strip().lower()
REQUIRE_MODEL_FAMILY = os.getenv('REQUIRE_MODEL_FAMILY', '0').strip().lower() not in ('0', 'false', 'no', 'off')

SETUP_MANIFEST_SCHEMA_VERSION = "setup_30s_v1"

META_PRODUCER_FEATURE_COLS = [
    'tsm_ret_30s_p50',
    'tsm_ret_120s_p50',
    'tsm_ret_30s_p10',
    'tsm_ret_30s_p90',
    'tsm_up_prob_30s',
    'tsm_vol_forecast_120s',
    'tsm_uncertainty',
    'regime_trend_prob',
    'regime_chop_prob',
    'regime_volatile_prob',
    'regime_transition_prob',
    'news_event_earnings',
    'news_event_analyst',
    'news_event_legal',
    'news_event_earnings_beat_miss',
    'news_event_analyst_upgrade_downgrade',
    'news_event_legal_regulatory',
    'news_event_product_capex',
    'news_event_macro_spillover',
    'news_novelty_score',
    'news_relevance_score',
    'news_embedding_cluster',
    'seq_lstm_up_prob_30s',
    'seq_tcn_up_prob_30s',
    'seq_transformer_up_prob_30s',
    'seq_patchtst_up_prob_30s',
    'seq_model_consensus_up_prob_30s',
    'setup_breakout_prob',
    'setup_pullback_continuation_prob',
    'setup_reversal_prob',
    'setup_trend_exhaustion_prob',
    'setup_failed_breakout_prob',
]

REGIME_PROB_FEATURE_COLS = [
    'f_regime_prob_choppy',
    'f_regime_prob_trend',
    'f_regime_prob_volatile',
    'f_regime_prob_entropy',
]

NEWS_BAR_FEATURE_COLS = [
    'f_news_intensity_60s',
    'f_news_intensity_300s',
    'f_news_freshness',
    'f_news_provider_breadth',
    'f_news_confidence',
    'f_news_sentiment_level',
    'f_news_sentiment_shift',
    'f_news_sentiment_dispersion',
    'f_news_coverage',
    'f_news_relevance',
    'f_news_surprise',
    'f_news_directional_impulse',
    'f_news_event_earnings',
    'f_news_event_analyst',
    'f_news_event_legal',
    'f_news_event_product',
    'f_news_event_macro',
    'f_news_model_relevance',
    'f_news_model_impact',
    'f_news_model_novelty',
    'f_news_directional_conviction',
    'f_news_alpha_bias_60s',
    'f_news_alpha_bias_300s',
    'f_news_alpha_ret_60s_norm',
    'f_news_alpha_ret_300s_norm',
    'f_news_vol_shock',
    'f_news_event_strength',
]

REGIME_LABEL_TO_ID = {
    'choppy': 0,
    'trend': 1,
    'volatile': 2,
}

REGIME_ID_TO_LABEL = {v: k for k, v in REGIME_LABEL_TO_ID.items()}

MIN_REGIME_ROWS = 1200
MIN_REGIME_SIGNALS = 25
MIN_OPEN30_ROWS = 800
MIN_OPEN30_SIGNALS = 20

# --- CONFIGURATION: DERIVED PARAMETERS ---
# CHANGED: 20 bars * 30 seconds = 10 minutes lookahead
FUTURE_WINDOW_SECONDS = FUTURE_WINDOW_BARS * 30

# --- CONFIGURATION: FEATURE ENGINEERING PARAMETERS ---
# --- CONFIGURATION: MODEL TRAINING PARAMETERS ---

# Threshold optimization on probabilities
# Slightly stricter floors reduce fold collapse where thresholding predicts almost no positives.
MIN_RECALL = 0.10
MIN_PRED_POS_RATE = 0.04
MIN_TUNED_THRESHOLD = 0.60
THRESHOLD_GRID = np.arange(MIN_TUNED_THRESHOLD, 0.91, 0.02)
# Adaptive thresholding controls (relative to calibration label prevalence).
MIN_POS_FRACTION_OF_BASE = 0.20
TARGET_POS_FRACTION_OF_BASE = 0.35

# RandomForest settings tuned to avoid probability saturation at 0/1.
RF_N_ESTIMATORS = 220
RF_MAX_DEPTH = 8
RF_MIN_SAMPLES_SPLIT = 40
RF_MIN_SAMPLES_LEAF = 20
RF_MAX_FEATURES = 'sqrt'

# Volatility regime multipliers by hour bucket (TSLA 30s observations)
# open: 9-10 ET, midday: 11-14 ET, close: 15 ET
REGIME_MULTIPLIERS = {
    'open': {'entry_profit': 1.20, 'entry_risk': 1.10, 'exit_drop': 1.20, 'exit_risk': 1.10},
    'midday': {'entry_profit': 0.90, 'entry_risk': 0.90, 'exit_drop': 0.90, 'exit_risk': 0.90},
    'close': {'entry_profit': 1.00, 'entry_risk': 1.00, 'exit_drop': 1.00, 'exit_risk': 1.00},
}


def build_rf_classifier(random_state=42):
    return RandomForestClassifier(
        n_estimators=RF_N_ESTIMATORS,
        max_depth=RF_MAX_DEPTH,
        min_samples_split=RF_MIN_SAMPLES_SPLIT,
        min_samples_leaf=RF_MIN_SAMPLES_LEAF,
        max_features=RF_MAX_FEATURES,
        class_weight='balanced_subsample',
        random_state=random_state,
        n_jobs=-1,
    )


def _normalize_model_family(family):
    text = str(family or '').strip().lower()
    if text in ('rf', 'randomforest', 'random_forest'):
        return 'random_forest'
    if text in ('lgbm', 'lightgbm', 'light_gbm'):
        return 'lightgbm'
    if text in ('catboost', 'cb'):
        return 'catboost'
    return 'random_forest'


def _model_family_unavailable_message(model_family, context_name='MODEL_FAMILY'):
    family = _normalize_model_family(model_family)
    if family == 'lightgbm' and LGBMClassifier is None:
        detail = LIGHTGBM_IMPORT_ERROR or 'unknown'
    elif family == 'catboost' and CatBoostClassifier is None:
        detail = CATBOOST_IMPORT_ERROR or 'unknown'
    else:
        return ''
    return f"Requested {context_name}={family} is unavailable; import_error={detail}"


def require_model_family_available(model_family, context_name='MODEL_FAMILY'):
    message = _model_family_unavailable_message(model_family, context_name=context_name)
    if message and REQUIRE_MODEL_FAMILY:
        raise RuntimeError(message + "; refusing RandomForest fallback because REQUIRE_MODEL_FAMILY=1")
    return message


def build_classifier(model_family='random_forest', random_state=42, multi_class=False):
    family = _normalize_model_family(model_family)

    if family == 'lightgbm' and LGBMClassifier is not None:
        objective = 'multiclass' if multi_class else 'binary'
        kwargs = {
            'n_estimators': 350,
            'learning_rate': 0.04,
            'num_leaves': 31,
            'subsample': 0.9,
            'colsample_bytree': 0.8,
            'reg_alpha': 0.2,
            'reg_lambda': 0.6,
            'random_state': random_state,
            'class_weight': 'balanced',
            'verbose': -1,
        }
        if multi_class:
            kwargs['objective'] = objective
            kwargs['num_class'] = len(REGIME_LABEL_TO_ID)
        else:
            kwargs['objective'] = objective
        return LGBMClassifier(**kwargs)

    if family == 'catboost' and CatBoostClassifier is not None:
        kwargs = {
            'iterations': 450,
            'depth': 6,
            'learning_rate': 0.05,
            'loss_function': 'MultiClass' if multi_class else 'Logloss',
            'auto_class_weights': 'Balanced',
            'random_seed': random_state,
            'verbose': False,
            'allow_writing_files': False,
        }
        return CatBoostClassifier(**kwargs)

    message = require_model_family_available(family)
    if message:
        print(f"WARNING: {message}; falling back to RandomForest.")
    return build_rf_classifier(random_state=random_state)


def ensure_training_csv_available(csv_file, source_5s_clean_file):
    csv_file = Path(csv_file)
    source_5s_clean_file = Path(source_5s_clean_file) if source_5s_clean_file else DEFAULT_SOURCE_5S_CLEAN_FILE

    if csv_file.exists():
        return True

    if not AUTO_BUILD_30S_IF_MISSING:
        return False

    if not source_5s_clean_file.exists():
        return False

    try:
        from build_30s_from_5s_csv import build_30s_from_5s_csv
        print(f">>> 30s dataset missing. Auto-building from {source_5s_clean_file.name}...")
        build_30s_from_5s_csv(str(source_5s_clean_file), str(csv_file), add_meta_features=True)
    except Exception as exc:
        print(f"ERROR: Failed to auto-build 30s dataset from clean 5s source: {exc}")
        return False

    return csv_file.exists()


def parse_args():
    parser = argparse.ArgumentParser(
        description='Train/export 30-second trade and regime models from a chosen 30s CSV.'
    )
    parser.add_argument(
        '--input-csv',
        type=str,
        default=str(DEFAULT_CSV_FILE),
        help='Path to the 30-second training CSV (defaults to the repo TSLA training file).',
    )
    parser.add_argument(
        '--source-5s-csv',
        type=str,
        default=str(DEFAULT_SOURCE_5S_CLEAN_FILE),
        help='Optional clean 5s CSV to auto-build the 30s file when --input-csv is missing.',
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help=(
            'Directory for all outputs: ONNX files, setup_scorecard.csv, setup_manifest.json, '
            'calibration_manifest.json, calibration_reliability.csv, oof_setup_predictions.csv, '
            'threshold_grid.csv. Defaults to model_exports/<run_tag>/ when not specified.'
        ),
    )
    parser.add_argument(
        '--no-onnx',
        action='store_true',
        default=False,
        help='Skip ONNX export (useful for calibration/scorecard smoke runs).',
    )
    return parser.parse_args()


def filter_legacy_exit_model_specs(model_specs, context):
    if TRAIN_LEGACY_30S_EXIT_MODELS:
        return model_specs
    kept = [spec for spec in model_specs if spec[1] not in {'Label_Long_Exit', 'Label_Short_Exit'}]
    skipped = len(model_specs) - len(kept)
    if skipped:
        print(f">>> Skipping {skipped} legacy 30s exit model(s) for {context} (TRAIN_LEGACY_30S_EXIT_MODELS=0).")
    return kept


def _ensure_symbol_column(df):
    out = df.copy()
    if 'Symbol' not in out.columns:
        out['Symbol'] = 'SINGLE'
    out['Symbol'] = out['Symbol'].astype(str).str.strip().replace('', 'SINGLE').str.upper()
    return out


def _groupby_symbol(df):
    return df.groupby('Symbol', sort=False)


def _groupby_symbol_date(df):
    return df.groupby(['Symbol', 'Date'], sort=False)


def filter_raw_to_regular_session(raw_df):
    parsed = _ensure_symbol_column(raw_df)
    ts_str = parsed['Timestamp'].astype(str).str.strip()
    parsed_ts = pd.to_datetime(
        ts_str,
        format='%Y%m%d %H:%M:%S America/New_York',
        errors='coerce'
    )
    if parsed_ts.isna().any():
        fallback = pd.to_datetime(ts_str, errors='coerce')
        parsed_ts = parsed_ts.fillna(fallback)
    if parsed_ts.isna().any():
        bad_samples = ts_str[parsed_ts.isna()].head(3).tolist()
        raise ValueError(f"Unable to parse some Timestamp values. Examples: {bad_samples}")

    parsed['Timestamp'] = parsed_ts
    h = parsed['Timestamp'].dt.hour
    m = parsed['Timestamp'].dt.minute

    # Regular trading session only: 09:30 <= t < 16:00 ET.
    rth_mask = ((h > 9) | ((h == 9) & (m >= 30))) & (h < 16)
    filtered = parsed[rth_mask].copy()

    print(f">>> Regular-session filter applied: kept {len(filtered)}/{len(raw_df)} rows (09:30-15:59 ET).")
    return filtered


def _hour_bucket(hour):
    if hour in (9, 10):
        return 'open'
    if hour in (11, 12, 13, 14):
        return 'midday'
    return 'close'

def calculate_features(df):
    print(">>> Calculating Java-equivalent features (30-second adjusted)...")
    df = _ensure_symbol_column(df)

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

    # Keep row order strictly chronological within each symbol for all stateful transforms.
    df = df.sort_values(['Symbol', 'Timestamp']).reset_index(drop=True)

    df['Date'] = df['Timestamp'].dt.date
    symbol_group = _groupby_symbol(df)
    symbol_date_group = _groupby_symbol_date(df)

    # Time of Day (normalized 0-1 for intraday patterns)
    df['Hour'] = df['Timestamp'].dt.hour
    df['Minute'] = df['Timestamp'].dt.minute
    df['f_time_of_day'] = (df['Hour'] * 60 + df['Minute']) / (24 * 60)
    df['MinuteOfDay'] = df['Hour'] * 60 + df['Minute']
    
    # 1. VWAP
    df['TypicalPrice'] = (df['High'] + df['Low'] + df['Close']) / 3.0
    df['VolxTP'] = df['Volume'] * df['TypicalPrice']
    df['CumVol'] = symbol_date_group['Volume'].cumsum()
    df['CumPv'] = symbol_date_group['VolxTP'].cumsum()
    df['VWAP'] = df['CumPv'] / df['CumVol']
    df['VWAP'] = df['VWAP'].fillna(df['Close'])
    
    # 2. Daily High/Low
    df['DayHigh'] = symbol_date_group['High'].cummax()
    df['DayLow'] = symbol_date_group['Low'].cummin()

    # 3. Rolling Windows (On a 30s chart: 12-bar = 6 mins, 60-bar = 30 mins)
    df['SMA_12'] = symbol_group['Close'].transform(lambda s: s.rolling(window=12).mean())
    df['STD_12'] = symbol_group['Close'].transform(lambda s: s.rolling(window=12).std(ddof=1))
    df['BB_Lower'] = df['SMA_12'] - (2.5 * df['STD_12'])
    df['BB_Upper'] = df['SMA_12'] + (2.5 * df['STD_12'])
    df['SMA_60'] = symbol_group['Close'].transform(lambda s: s.rolling(window=60).mean())

    # 4. MACD
    df['EMA_12'] = symbol_group['Close'].transform(lambda s: s.ewm(span=12, adjust=False).mean())
    df['EMA_26'] = symbol_group['Close'].transform(lambda s: s.ewm(span=26, adjust=False).mean())
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['MACD_Signal'] = symbol_group['MACD'].transform(lambda s: s.ewm(span=9, adjust=False).mean())
    df['MACD_Diff'] = df['MACD'] - df['MACD_Signal']
    
    # 5. ATR (12-bar)
    df['PrevClose'] = symbol_group['Close'].shift(1)
    df['TR'] = np.maximum(df['High'] - df['Low'],
               np.maximum(abs(df['High'] - df['PrevClose']), 
                          abs(df['Low'] - df['PrevClose'])))
    df['ATR_12'] = symbol_group['TR'].transform(lambda s: s.ewm(alpha=1/12, adjust=False).mean())

    # --- Extended intraday context indicators ---
    # 1) Relative volume vs same minute-of-day baseline
    minute_median_vol = df.groupby(['Symbol', 'MinuteOfDay'], sort=False)['Volume'].transform('median')
    df['f_rel_volume_30s'] = df['Volume'] / (minute_median_vol + 1.0)

    # 2) Realized volatility regime (20 bars ~= 10 minutes)
    df['Ret_30s'] = symbol_group['Close'].pct_change()
    df['f_realized_vol_20'] = symbol_group['Ret_30s'].transform(lambda s: s.rolling(window=20).std(ddof=1))
    vol_mean = symbol_group['f_realized_vol_20'].transform(lambda s: s.rolling(window=100).mean())
    vol_std = symbol_group['f_realized_vol_20'].transform(lambda s: s.rolling(window=100).std(ddof=1))
    df['f_realized_vol_z'] = (df['f_realized_vol_20'] - vol_mean) / (vol_std + 1e-9)

    # 3) Opening range distances (first 10 bars = first 5 minutes)
    first_10_high = (
        symbol_date_group['High']
        .transform(lambda s: s.iloc[:10].max() if len(s) > 0 else np.nan)
    )
    first_10_low = (
        symbol_date_group['Low']
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
        spread_mean = symbol_group['f_spread_pct'].transform(lambda s: s.rolling(window=100).mean())
        spread_std = symbol_group['f_spread_pct'].transform(lambda s: s.rolling(window=100).std(ddof=1))
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
    delta = symbol_group['Close'].diff()
    gain = symbol_group['Close'].transform(lambda s: s.diff().where(s.diff() > 0, 0).ewm(alpha=1/14, adjust=False).mean())
    loss = symbol_group['Close'].transform(lambda s: (-s.diff().where(s.diff() < 0, 0)).ewm(alpha=1/14, adjust=False).mean())
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].fillna(50)

    # --- NEW: Price Action & Support/Resistance ---
    # 60-bar (30-minute) local resistance and support
    df['SwingHigh_60'] = symbol_group['High'].transform(lambda s: s.rolling(window=60).max())
    df['SwingLow_60'] = symbol_group['Low'].transform(lambda s: s.rolling(window=60).min())

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

    df['f_red_streak'] = symbol_date_group['is_red'].apply(_streak).reset_index(level=[0, 1], drop=True)
    df['f_green_streak'] = symbol_date_group['is_green'].apply(_streak).reset_index(level=[0, 1], drop=True)

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

    def _opt_numeric(col_name, default_value=0.0):
        if col_name not in df.columns:
            return pd.Series(default_value, index=df.index, dtype=float)
        return pd.to_numeric(df[col_name], errors='coerce').fillna(default_value).astype(float)

    news_count_60 = _opt_numeric('NewsCount60s', 0.0)
    news_count_300 = _opt_numeric('NewsCount300s', 0.0)
    news_provider_count = _opt_numeric('NewsUniqueProviders300s', 0.0)
    news_sentiment_latest = _opt_numeric('SentimentLatest', 0.0).clip(-1.0, 1.0)
    news_sentiment_mean = _opt_numeric('SentimentMean300s', 0.0).clip(-1.0, 1.0)
    news_sentiment_std = _opt_numeric('SentimentStd300s', 0.0).clip(0.0, 2.0)
    news_conf_mean = _opt_numeric('SentimentConfidenceMean300s', 0.0).clip(0.0, 1.0)
    news_conf_latest = _opt_numeric('SentimentConfidenceLatest', 0.0).clip(0.0, 1.0)
    news_lag_sec = _opt_numeric('NewsAsOfLagSec', 999999.0).clip(lower=0.0)
    news_coverage = _opt_numeric('NewsCoverage300s', 0.0).clip(0.0, 1.0)
    news_event_earnings = _opt_numeric('NewsEventEarningsBeatMiss300s', 0.0).clip(0.0, 1.0)
    news_event_analyst = _opt_numeric('NewsEventAnalystUpgradeDowngrade300s', 0.0).clip(0.0, 1.0)
    news_event_legal = _opt_numeric('NewsEventLegalRegulatory300s', 0.0).clip(0.0, 1.0)
    news_event_product = _opt_numeric('NewsEventProductCapex300s', 0.0).clip(0.0, 1.0)
    news_event_macro = _opt_numeric('NewsEventMacroSpillover300s', 0.0).clip(0.0, 1.0)
    news_relevance_latest = _opt_numeric('NewsRelevanceLatest', 0.0).clip(0.0, 1.0)
    news_relevance_mean = _opt_numeric('NewsRelevanceMean300s', 0.0).clip(0.0, 1.0)
    news_relevance_max = _opt_numeric('NewsRelevanceMax300s', 0.0).clip(0.0, 1.0)
    news_impact_latest = _opt_numeric('NewsImpactLatest', 0.0).clip(0.0, 1.0)
    news_impact_mean = _opt_numeric('NewsImpactMean300s', 0.0).clip(0.0, 1.0)
    news_impact_max = _opt_numeric('NewsImpactMax300s', 0.0).clip(0.0, 1.0)
    news_novelty_latest = _opt_numeric('NewsNoveltyLatest', 0.0).clip(0.0, 1.0)
    news_novelty_mean = _opt_numeric('NewsNoveltyMean300s', 0.0).clip(0.0, 1.0)
    news_novelty_max = _opt_numeric('NewsNoveltyMax300s', 0.0).clip(0.0, 1.0)
    news_directional_latest = _opt_numeric('NewsDirectionalImpulseLatest', 0.0).clip(-1.0, 1.0)
    news_directional_mean = _opt_numeric('NewsDirectionalImpulseMean300s', 0.0).clip(-1.0, 1.0)
    news_directional_decay = _opt_numeric('NewsDirectionalImpulseDecay300s', 0.0).clip(-1.0, 1.0)
    news_directional_abs_max = _opt_numeric('NewsAbsDirectionalImpulseMax300s', 0.0).clip(0.0, 1.0)
    news_alpha_up_60s_latest = _opt_numeric('NewsAlphaUpProb60sLatest', 0.0).clip(0.0, 1.0)
    news_alpha_down_60s_latest = _opt_numeric('NewsAlphaDownProb60sLatest', 0.0).clip(0.0, 1.0)
    news_alpha_up_300s_latest = _opt_numeric('NewsAlphaUpProb300sLatest', 0.0).clip(0.0, 1.0)
    news_alpha_down_300s_latest = _opt_numeric('NewsAlphaDownProb300sLatest', 0.0).clip(0.0, 1.0)
    news_alpha_up_60s_mean = _opt_numeric('NewsAlphaUpProb60sMean300s', 0.0).clip(0.0, 1.0)
    news_alpha_down_60s_mean = _opt_numeric('NewsAlphaDownProb60sMean300s', 0.0).clip(0.0, 1.0)
    news_alpha_up_300s_mean = _opt_numeric('NewsAlphaUpProb300sMean300s', 0.0).clip(0.0, 1.0)
    news_alpha_down_300s_mean = _opt_numeric('NewsAlphaDownProb300sMean300s', 0.0).clip(0.0, 1.0)
    news_alpha_ret_60s_latest = _opt_numeric('NewsAlphaExpectedRet60sBpsLatest', 0.0)
    news_alpha_ret_300s_latest = _opt_numeric('NewsAlphaExpectedRet300sBpsLatest', 0.0)
    news_alpha_ret_60s_mean = _opt_numeric('NewsAlphaExpectedRet60sBpsMean300s', 0.0)
    news_alpha_ret_300s_mean = _opt_numeric('NewsAlphaExpectedRet300sBpsMean300s', 0.0)
    news_alpha_ret_60s_decay = _opt_numeric('NewsAlphaExpectedRet60sBpsDecay300s', 0.0)
    news_alpha_ret_300s_decay = _opt_numeric('NewsAlphaExpectedRet300sBpsDecay300s', 0.0)
    news_vol_shock_latest = _opt_numeric('NewsVolatilityShockLatest', 0.0).clip(0.0, 1.0)
    news_vol_shock_mean = _opt_numeric('NewsVolatilityShockMean300s', 0.0).clip(0.0, 1.0)
    news_vol_shock_max = _opt_numeric('NewsVolatilityShockMax300s', 0.0).clip(0.0, 1.0)

    news_freshness = np.exp(-np.clip(news_lag_sec, 0.0, 3600.0) / 300.0)
    news_conf_mix = np.clip(0.5 * news_conf_mean + 0.5 * news_conf_latest, 0.0, 1.0)
    news_sentiment_shift = (news_sentiment_latest - news_sentiment_mean).clip(-2.0, 2.0)
    news_event_top = np.maximum.reduce([
        news_event_earnings.values,
        news_event_analyst.values,
        news_event_legal.values,
        news_event_product.values,
        news_event_macro.values,
    ])
    news_model_relevance = np.clip(
        0.45 * news_relevance_latest + 0.30 * news_relevance_mean + 0.25 * news_relevance_max,
        0.0,
        1.0,
    )
    news_model_impact = np.clip(
        0.45 * news_impact_latest + 0.30 * news_impact_mean + 0.25 * news_impact_max,
        0.0,
        1.0,
    )
    news_model_novelty = np.clip(
        0.45 * news_novelty_latest + 0.30 * news_novelty_mean + 0.25 * news_novelty_max,
        0.0,
        1.0,
    )
    news_directional_conviction = np.clip(
        0.60 * news_directional_decay + 0.25 * news_directional_latest + 0.15 * news_directional_mean,
        -1.0,
        1.0,
    )
    news_alpha_bias_60s = np.clip(
        0.55 * (news_alpha_up_60s_latest - news_alpha_down_60s_latest)
        + 0.45 * (news_alpha_up_60s_mean - news_alpha_down_60s_mean),
        -1.0,
        1.0,
    )
    news_alpha_bias_300s = np.clip(
        0.55 * (news_alpha_up_300s_latest - news_alpha_down_300s_latest)
        + 0.45 * (news_alpha_up_300s_mean - news_alpha_down_300s_mean),
        -1.0,
        1.0,
    )
    news_alpha_ret_60s_norm = np.tanh(
        (0.45 * news_alpha_ret_60s_latest + 0.35 * news_alpha_ret_60s_decay + 0.20 * news_alpha_ret_60s_mean) / 25.0
    )
    news_alpha_ret_300s_norm = np.tanh(
        (0.45 * news_alpha_ret_300s_latest + 0.35 * news_alpha_ret_300s_decay + 0.20 * news_alpha_ret_300s_mean) / 40.0
    )
    news_vol_shock = np.clip(
        0.45 * news_vol_shock_latest + 0.30 * news_vol_shock_mean + 0.25 * news_vol_shock_max,
        0.0,
        1.0,
    )
    news_event_strength = np.clip(
        (0.45 * news_model_relevance + 0.35 * news_model_impact + 0.20 * np.clip(news_event_top, 0.0, 1.0))
        * (0.60 + 0.40 * news_freshness),
        0.0,
        1.0,
    )
    heuristic_news_relevance = np.clip(
        0.25 * news_freshness
        + 0.20 * np.clip(news_provider_count / 4.0, 0.0, 1.0)
        + 0.20 * news_coverage
        + 0.20 * news_conf_mix
        + 0.15 * news_event_top,
        0.0,
        1.0,
    )

    df['f_news_intensity_60s'] = np.clip(news_count_60 / 3.0, 0.0, 1.0)
    df['f_news_intensity_300s'] = np.clip(news_count_300 / 8.0, 0.0, 1.0)
    df['f_news_freshness'] = news_freshness
    df['f_news_provider_breadth'] = np.clip(news_provider_count / 4.0, 0.0, 1.0)
    df['f_news_confidence'] = news_conf_mix
    df['f_news_sentiment_level'] = news_sentiment_latest
    df['f_news_sentiment_shift'] = news_sentiment_shift
    df['f_news_sentiment_dispersion'] = news_sentiment_std
    df['f_news_coverage'] = news_coverage
    df['f_news_relevance'] = np.clip(
        0.55 * heuristic_news_relevance + 0.45 * news_model_relevance,
        0.0,
        1.0,
    )
    df['f_news_surprise'] = np.clip(
        0.50 * (news_freshness * df['f_news_intensity_60s'] * np.abs(news_sentiment_shift))
        + 0.30 * news_model_novelty
        + 0.20 * (news_directional_abs_max * news_model_impact),
        0.0,
        1.0,
    )
    df['f_news_directional_impulse'] = np.clip(
        0.45 * (news_freshness * df['f_news_intensity_60s'] * news_sentiment_latest)
        + 0.35 * news_directional_conviction
        + 0.20 * news_alpha_bias_60s,
        -1.0,
        1.0,
    )
    df['f_news_event_earnings'] = news_event_earnings
    df['f_news_event_analyst'] = news_event_analyst
    df['f_news_event_legal'] = news_event_legal
    df['f_news_event_product'] = news_event_product
    df['f_news_event_macro'] = news_event_macro
    df['f_news_model_relevance'] = news_model_relevance
    df['f_news_model_impact'] = news_model_impact
    df['f_news_model_novelty'] = news_model_novelty
    df['f_news_directional_conviction'] = news_directional_conviction
    df['f_news_alpha_bias_60s'] = news_alpha_bias_60s
    df['f_news_alpha_bias_300s'] = news_alpha_bias_300s
    df['f_news_alpha_ret_60s_norm'] = news_alpha_ret_60s_norm
    df['f_news_alpha_ret_300s_norm'] = news_alpha_ret_300s_norm
    df['f_news_vol_shock'] = news_vol_shock
    df['f_news_event_strength'] = news_event_strength

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


def _validate_entry_fill_config():
    if ENTRY_FILL_MODE not in SUPPORTED_ENTRY_FILL_MODES:
        raise ValueError(
            f"Unsupported ENTRY_FILL_MODE={ENTRY_FILL_MODE!r}. "
            f"Supported modes: {sorted(SUPPORTED_ENTRY_FILL_MODES)}"
        )

    round_trip_cost_pct = (ENTRY_SLIPPAGE_BPS + EXIT_SLIPPAGE_BPS) / 10000.0
    net_reward = max(0.0, ENTRY_PROFIT_PCT - round_trip_cost_pct)
    net_risk = ENTRY_RISK_PCT + round_trip_cost_pct
    net_r_multiple = net_reward / net_risk if net_risk > 0.0 else 0.0
    print(
        ">>> Entry fill config: "
        f"mode={ENTRY_FILL_MODE} entry_slippage_bps={ENTRY_SLIPPAGE_BPS:.2f} "
        f"exit_slippage_bps={EXIT_SLIPPAGE_BPS:.2f} net_r_multiple={net_r_multiple:.2f}"
    )
    if net_r_multiple < MIN_NET_R_MULTIPLE:
        print(
            "WARNING: Net entry reward/risk after configured costs is below "
            f"MIN_NET_R_MULTIPLE={MIN_NET_R_MULTIPLE:.2f}: {net_r_multiple:.2f}"
        )


def _entry_fill_price(side, i, opens, closes):
    if ENTRY_FILL_MODE == 'current_close':
        fill = closes[i]
    else:
        fill = opens[i + 1] if np.isfinite(opens[i + 1]) else closes[i + 1]

    if ENTRY_FILL_MODE == 'next_open_with_slippage':
        slippage = ENTRY_SLIPPAGE_BPS / 10000.0
        if side == 'long':
            fill *= (1.0 + slippage)
        elif side == 'short':
            fill *= (1.0 - slippage)

    return float(fill)


def _finite_positive(value):
    try:
        v = float(value)
    except Exception:
        return False
    return np.isfinite(v) and v > 0.0


def _first_numeric_array(df, names):
    for name in names:
        if name in df.columns:
            return pd.to_numeric(df[name], errors='coerce').to_numpy(dtype=float)
    return None


def _quote_arrays(df):
    bid = _first_numeric_array(df, ['Bid', 'BidPrice', 'bid', 'BestBid', 'best_bid', 'BidLast'])
    ask = _first_numeric_array(df, ['Ask', 'AskPrice', 'ask', 'BestAsk', 'best_ask', 'AskLast'])
    return bid, ask


def _spread_pct_array(df, bids, asks):
    default_spread = max(0.0, COST_AWARE_DEFAULT_SPREAD_BPS / 10000.0)
    if bids is not None and asks is not None:
        mid = (bids + asks) / 2.0
        spread = np.where(
            np.isfinite(bids) & np.isfinite(asks) & (mid > 0.0),
            np.maximum(asks - bids, 0.0) / np.maximum(mid, 1e-9),
            np.nan,
        )
        if np.isfinite(spread).any():
            return np.nan_to_num(spread, nan=default_spread, posinf=default_spread, neginf=default_spread)
    if 'f_spread_pct' in df.columns:
        spread = pd.to_numeric(df['f_spread_pct'], errors='coerce').to_numpy(dtype=float)
        spread = np.where(np.isfinite(spread), np.maximum(spread, 0.0), default_spread)
        return spread
    return np.full(len(df), default_spread, dtype=float)


def _half_spread_pct(spread_pct, idx):
    if spread_pct is None or idx < 0 or idx >= len(spread_pct):
        return max(0.0, COST_AWARE_DEFAULT_SPREAD_BPS / 20000.0)
    value = float(spread_pct[idx])
    if not np.isfinite(value) or value < 0.0:
        value = max(0.0, COST_AWARE_DEFAULT_SPREAD_BPS / 10000.0)
    return value / 2.0


def _cost_aware_label_assumptions():
    return {
        'schema_version': COST_AWARE_LABEL_SCHEMA_VERSION,
        'enabled_for_entry_training': bool(COST_AWARE_LABELS),
        'label_type': 'expected_net_r_after_costs',
        'entry_fill_mode': ENTRY_FILL_MODE,
        'latency_assumption': 'entry at current close when ENTRY_FILL_MODE=current_close, otherwise next in-session bar',
        'entry_profit_pct_base': ENTRY_PROFIT_PCT,
        'entry_risk_pct_base': ENTRY_RISK_PCT,
        'adaptive_thresholds': 'hour_bucket_and_atr_scaled_entry_profit_pct_entry_risk_pct',
        'future_window_bars': FUTURE_WINDOW_BARS,
        'entry_spread_model': 'side-aware bid/ask when available, otherwise half f_spread_pct/default spread penalty',
        'exit_spread_model': 'target/stop/horizon price adjusted by half f_spread_pct/default spread penalty',
        'entry_slippage_bps': ENTRY_SLIPPAGE_BPS,
        'exit_slippage_bps': EXIT_SLIPPAGE_BPS,
        'default_spread_bps': COST_AWARE_DEFAULT_SPREAD_BPS,
        'fill_probability': COST_AWARE_FILL_PROBABILITY,
        'partial_fill_penalty_r': COST_AWARE_PARTIAL_FILL_PENALTY_R,
        'missed_fill_penalty_r': COST_AWARE_MISSED_FILL_PENALTY_R,
        'min_expected_net_r_positive_label': COST_AWARE_MIN_NET_R_LABEL,
        'ambiguous_tp_sl_policy': 'conservative_stop_when_tp_and_sl_hit_same_bar',
        'no_barrier_hit_policy': 'mark_to_horizon_close_after_exit_costs',
    }


def _validate_cost_aware_label_config():
    values = {
        'COST_AWARE_MIN_NET_R_LABEL': COST_AWARE_MIN_NET_R_LABEL,
        'COST_AWARE_DEFAULT_SPREAD_BPS': COST_AWARE_DEFAULT_SPREAD_BPS,
        'COST_AWARE_FILL_PROBABILITY': COST_AWARE_FILL_PROBABILITY,
        'COST_AWARE_PARTIAL_FILL_PENALTY_R': COST_AWARE_PARTIAL_FILL_PENALTY_R,
        'COST_AWARE_MISSED_FILL_PENALTY_R': COST_AWARE_MISSED_FILL_PENALTY_R,
        'ENTRY_SLIPPAGE_BPS': ENTRY_SLIPPAGE_BPS,
        'EXIT_SLIPPAGE_BPS': EXIT_SLIPPAGE_BPS,
    }
    bad = [name for name, value in values.items() if not np.isfinite(float(value))]
    if bad:
        raise ValueError(f"Missing/non-finite cost-aware label assumptions: {bad}")
    if COST_AWARE_FILL_PROBABILITY < 0.0 or COST_AWARE_FILL_PROBABILITY > 1.0:
        raise ValueError('COST_AWARE_FILL_PROBABILITY must be within [0, 1]')
    for name in ['COST_AWARE_DEFAULT_SPREAD_BPS', 'COST_AWARE_PARTIAL_FILL_PENALTY_R', 'COST_AWARE_MISSED_FILL_PENALTY_R', 'ENTRY_SLIPPAGE_BPS', 'EXIT_SLIPPAGE_BPS']:
        if values[name] < 0.0:
            raise ValueError(f'{name} must be non-negative')


def _cost_adjusted_expected_r(realized_net_r):
    if not np.isfinite(realized_net_r):
        return float('nan')
    return float(
        COST_AWARE_FILL_PROBABILITY * realized_net_r
        - COST_AWARE_PARTIAL_FILL_PENALTY_R
        - (1.0 - COST_AWARE_FILL_PROBABILITY) * COST_AWARE_MISSED_FILL_PENALTY_R
    )


def _cost_aware_entry_fill_price(side, i, entry_idx, opens, closes, bids, asks, spread_pct):
    slip = ENTRY_SLIPPAGE_BPS / 10000.0
    if side == 'long' and asks is not None and entry_idx < len(asks) and _finite_positive(asks[entry_idx]):
        return float(asks[entry_idx]) * (1.0 + slip)
    if side == 'short' and bids is not None and entry_idx < len(bids) and _finite_positive(bids[entry_idx]):
        return float(bids[entry_idx]) * (1.0 - slip)

    if ENTRY_FILL_MODE == 'current_close':
        base = closes[i]
    else:
        base = opens[entry_idx] if np.isfinite(opens[entry_idx]) else closes[entry_idx]
    if not _finite_positive(base):
        return float('nan')
    half_spread = _half_spread_pct(spread_pct, entry_idx)
    if side == 'long':
        return float(base) * (1.0 + half_spread + slip)
    return float(base) * max(0.0, 1.0 - half_spread - slip)


def _cost_aware_exit_fill_price(side, base_price, exit_idx, spread_pct):
    if not _finite_positive(base_price):
        return float('nan')
    half_spread = _half_spread_pct(spread_pct, exit_idx)
    slip = EXIT_SLIPPAGE_BPS / 10000.0
    if side == 'long':
        return float(base_price) * max(0.0, 1.0 - half_spread - slip)
    return float(base_price) * (1.0 + half_spread + slip)


def _cost_aware_expected_net_r_for_entry(side, i, opens, closes, highs, lows, dates, symbols, bids, asks, spread_pct, entry_profit_pct, entry_risk_pct):
    n = len(closes)
    if ENTRY_FILL_MODE == 'current_close':
        entry_idx = i
    else:
        entry_idx = i + 1
        if entry_idx >= n or dates[entry_idx] != dates[i] or symbols[entry_idx] != symbols[i]:
            return float('nan')

    entry_fill = _cost_aware_entry_fill_price(side, i, entry_idx, opens, closes, bids, asks, spread_pct)
    if not _finite_positive(entry_fill) or entry_risk_pct <= 0.0:
        return float('nan')

    risk_dollars = entry_fill * float(entry_risk_pct)
    if risk_dollars <= 0.0 or not np.isfinite(risk_dollars):
        return float('nan')

    if side == 'long':
        target = entry_fill * (1.0 + entry_profit_pct)
        stop = entry_fill * (1.0 - entry_risk_pct)
    else:
        target = entry_fill * (1.0 - entry_profit_pct)
        stop = entry_fill * (1.0 + entry_risk_pct)

    exit_idx = -1
    exit_base = float('nan')
    last_idx = -1
    for j in range(i + 1, min(n, i + FUTURE_WINDOW_BARS + 1)):
        if dates[j] != dates[i] or symbols[j] != symbols[i]:
            break
        last_idx = j
        h = highs[j]
        l = lows[j]
        if side == 'long':
            hit_target = h >= target
            hit_stop = l <= stop
        else:
            hit_target = l <= target
            hit_stop = h >= stop
        if hit_target and hit_stop:
            exit_idx = j
            exit_base = stop
            break
        if hit_target:
            exit_idx = j
            exit_base = target
            break
        if hit_stop:
            exit_idx = j
            exit_base = stop
            break

    if exit_idx < 0 and last_idx >= 0:
        exit_idx = last_idx
        exit_base = closes[last_idx]
    if exit_idx < 0 or not _finite_positive(exit_base):
        return float('nan')

    exit_fill = _cost_aware_exit_fill_price(side, exit_base, exit_idx, spread_pct)
    if not _finite_positive(exit_fill):
        return float('nan')
    if side == 'long':
        realized_net_r = (exit_fill - entry_fill) / risk_dollars
    else:
        realized_net_r = (entry_fill - exit_fill) / risk_dollars
    return _cost_adjusted_expected_r(realized_net_r)


def generate_labels(df):
    if TRAIN_LEGACY_30S_EXIT_MODELS:
        print(">>> Generating 4 event-ordered path-dependent labels...")
    else:
        print(">>> Generating entry labels only; legacy 30s exit labels/model training disabled.")
    _validate_entry_fill_config()
    _validate_cost_aware_label_config()

    df = df.copy()
    n = len(df)
    closes = df['Close'].values
    opens = df['Open'].values if 'Open' in df.columns else closes
    highs = df['High'].values
    lows = df['Low'].values
    dates = df['Date'].values
    symbols = df['Symbol'].values if 'Symbol' in df.columns else np.asarray(['SINGLE'] * n)
    hours = df['Hour'].values
    atr_norm = (df['ATR_12'] / df['Close']).replace([np.inf, -np.inf], np.nan).fillna(0.0010).values

    y_long_entry = np.zeros(n, dtype=np.int8)
    y_short_entry = np.zeros(n, dtype=np.int8)
    y_long_exit = np.zeros(n, dtype=np.int8)
    y_short_exit = np.zeros(n, dtype=np.int8)
    expected_long_entry_net_r = np.full(n, np.nan, dtype=float)
    expected_short_entry_net_r = np.full(n, np.nan, dtype=float)
    bids, asks = _quote_arrays(df)
    spread_pct = _spread_pct_array(df, bids, asks)

    usable = n - FUTURE_WINDOW_BARS
    for i in range(usable):
        ep, er, xd, xr = _adaptive_thresholds(hours[i], atr_norm[i])
        current_date = dates[i]
        current_symbol = symbols[i]

        c = closes[i]
        can_use_next_open = (i + 1 < n and dates[i + 1] == current_date and symbols[i + 1] == current_symbol)
        if ENTRY_FILL_MODE == 'current_close' or can_use_next_open:
            long_entry_fill = _entry_fill_price('long', i, opens, closes)
            short_entry_fill = _entry_fill_price('short', i, opens, closes)
        else:
            long_entry_fill = np.nan
            short_entry_fill = np.nan

        le_tp = long_entry_fill * (1.0 + ep)
        le_sl = long_entry_fill * (1.0 - er)

        se_tp = short_entry_fill * (1.0 - ep)
        se_sl = short_entry_fill * (1.0 + er)

        lx_tp = c * (1.0 - xd)
        lx_sl = c * (1.0 + xr)

        sx_tp = c * (1.0 + xd)
        sx_sl = c * (1.0 - xr)

        if np.isfinite(long_entry_fill):
            for j in range(i + 1, i + FUTURE_WINDOW_BARS + 1):
                # Never let a label look into the next trading day.
                if dates[j] != current_date or symbols[j] != current_symbol:
                    break
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

        expected_long_entry_net_r[i] = _cost_aware_expected_net_r_for_entry(
            'long', i, opens, closes, highs, lows, dates, symbols, bids, asks, spread_pct, ep, er
        )

        if np.isfinite(short_entry_fill):
            for j in range(i + 1, i + FUTURE_WINDOW_BARS + 1):
                if dates[j] != current_date or symbols[j] != current_symbol:
                    break
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

        expected_short_entry_net_r[i] = _cost_aware_expected_net_r_for_entry(
            'short', i, opens, closes, highs, lows, dates, symbols, bids, asks, spread_pct, ep, er
        )

        if TRAIN_LEGACY_30S_EXIT_MODELS:
            for j in range(i + 1, i + FUTURE_WINDOW_BARS + 1):
                if dates[j] != current_date or symbols[j] != current_symbol:
                    break
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
                if dates[j] != current_date or symbols[j] != current_symbol:
                    break
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

    cost_long_entry = (np.isfinite(expected_long_entry_net_r) & (expected_long_entry_net_r > COST_AWARE_MIN_NET_R_LABEL)).astype(np.int8)
    cost_short_entry = (np.isfinite(expected_short_entry_net_r) & (expected_short_entry_net_r > COST_AWARE_MIN_NET_R_LABEL)).astype(np.int8)
    df['Label_Long_Entry_TpBeforeSl'] = y_long_entry
    df['Label_Short_Entry_TpBeforeSl'] = y_short_entry
    df['Label_Long_Entry_ExpectedNetRAfterCosts'] = expected_long_entry_net_r
    df['Label_Short_Entry_ExpectedNetRAfterCosts'] = expected_short_entry_net_r
    df['Label_Long_Entry_CostAware'] = cost_long_entry
    df['Label_Short_Entry_CostAware'] = cost_short_entry
    df['Label_Long_Entry'] = cost_long_entry if COST_AWARE_LABELS else y_long_entry
    df['Label_Short_Entry'] = cost_short_entry if COST_AWARE_LABELS else y_short_entry
    df['Label_Long_Exit'] = y_long_exit
    df['Label_Short_Exit'] = y_short_exit

    df = df.iloc[:usable].copy()
    return df


def _summarize_expected_net_r(df, side):
    prefix = 'Long' if side == 'long' else 'Short'
    expected_col = f'Label_{prefix}_Entry_ExpectedNetRAfterCosts'
    cost_label_col = f'Label_{prefix}_Entry_CostAware'
    legacy_col = f'Label_{prefix}_Entry_TpBeforeSl'
    values = pd.to_numeric(df.get(expected_col, pd.Series(dtype=float)), errors='coerce')
    finite = values[np.isfinite(values)]
    labels = pd.to_numeric(df.get(cost_label_col, pd.Series(dtype=float)), errors='coerce').fillna(0).astype(int)
    legacy = pd.to_numeric(df.get(legacy_col, pd.Series(dtype=float)), errors='coerce').fillna(0).astype(int)
    if len(finite):
        quantiles = finite.quantile([0.10, 0.50, 0.90])
        min_value = float(finite.min())
        max_value = float(finite.max())
        mean_value = float(finite.mean())
        q10 = float(quantiles.loc[0.10])
        q50 = float(quantiles.loc[0.50])
        q90 = float(quantiles.loc[0.90])
    else:
        min_value = max_value = mean_value = q10 = q50 = q90 = None
    return {
        'side': side,
        'expected_net_r_column': expected_col,
        'cost_aware_label_column': cost_label_col,
        'legacy_label_column': legacy_col,
        'rows': int(len(df)),
        'finite_expected_net_r_rows': int(len(finite)),
        'positive_cost_aware_labels': int(labels.sum()),
        'positive_cost_aware_rate': float(labels.mean()) if len(labels) else 0.0,
        'positive_legacy_tp_before_sl_labels': int(legacy.sum()),
        'legacy_tp_before_sl_positive_rate': float(legacy.mean()) if len(legacy) else 0.0,
        'label_delta_count': int((labels != legacy).sum()) if len(labels) == len(legacy) else None,
        'expected_net_r_min': min_value,
        'expected_net_r_mean': mean_value,
        'expected_net_r_p10': q10,
        'expected_net_r_p50': q50,
        'expected_net_r_p90': q90,
        'expected_net_r_max': max_value,
    }


def write_cost_aware_label_artifacts(df, output_dir, input_csv, generated_at, commit_hash):
    label_cols = [
        'Symbol', 'Timestamp', 'Date',
        'Label_Long_Entry', 'Label_Short_Entry',
        'Label_Long_Entry_CostAware', 'Label_Short_Entry_CostAware',
        'Label_Long_Entry_TpBeforeSl', 'Label_Short_Entry_TpBeforeSl',
        'Label_Long_Entry_ExpectedNetRAfterCosts', 'Label_Short_Entry_ExpectedNetRAfterCosts',
    ]
    labels_path = output_dir / 'cost_aware_setup_labels.csv'
    cols = [col for col in label_cols if col in df.columns]
    df[cols].to_csv(labels_path, index=False)

    manifest = {
        'schema_version': COST_AWARE_LABEL_SCHEMA_VERSION,
        'generated_at_utc': generated_at,
        'code_commit': commit_hash,
        'input_csv': str(input_csv),
        'labels_csv': 'cost_aware_setup_labels.csv',
        'used_for_entry_training': bool(COST_AWARE_LABELS),
        'assumptions': _cost_aware_label_assumptions(),
        'label_columns': {
            'long_expected_net_r_after_costs': 'Label_Long_Entry_ExpectedNetRAfterCosts',
            'short_expected_net_r_after_costs': 'Label_Short_Entry_ExpectedNetRAfterCosts',
            'long_cost_aware_binary': 'Label_Long_Entry_CostAware',
            'short_cost_aware_binary': 'Label_Short_Entry_CostAware',
            'long_training_target': 'Label_Long_Entry',
            'short_training_target': 'Label_Short_Entry',
            'long_legacy_tp_before_sl': 'Label_Long_Entry_TpBeforeSl',
            'short_legacy_tp_before_sl': 'Label_Short_Entry_TpBeforeSl',
        },
        'summary': {
            'long': _summarize_expected_net_r(df, 'long'),
            'short': _summarize_expected_net_r(df, 'short'),
        },
        'errors': [],
        'warnings': [] if COST_AWARE_LABELS else [
            'COST_AWARE_LABELS is disabled; expected_net_r_after_costs labels were written but not used as entry training targets.'
        ],
    }
    manifest_path = output_dir / 'cost_aware_label_manifest.json'
    manifest_path.write_text(json.dumps(_json_safe(manifest), indent=2), encoding='utf-8')
    print(f">>> Wrote {labels_path}")
    print(f">>> Wrote {manifest_path}")
    return manifest


def current_git_commit():
    try:
        import subprocess
        return subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'], stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return 'unknown'


def _json_safe(obj):
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    return obj


def assign_market_regime(df):
    """
    Heuristic regime labels used to train the regime classifier:
    - volatile: high ATR / realized vol / spread stress
    - trend: directional extension with momentum (non-volatile)
    - choppy: everything else
    """
    labeled = df.copy()

    atr_q70 = labeled['f_atr_norm'].quantile(0.70)
    vol_q70 = labeled['f_realized_vol_20'].quantile(0.70)

    # Normalize trend intensity by ATR to avoid raw price-scale bias.
    trend_extension = (labeled['Close'] - labeled['SMA_60']).abs() / (labeled['ATR_12'] + 1e-9)
    momentum_strength = labeled['f_macd_diff'].abs() / (labeled['f_atr_norm'] + 1e-9)
    rsi_directional = (labeled['f_rsi'] > 58.0) | (labeled['f_rsi'] < 42.0)

    volatile_mask = (
        (labeled['f_atr_norm'] >= atr_q70)
        | (labeled['f_realized_vol_20'] >= vol_q70)
        | (labeled['f_realized_vol_z'] >= 1.0)
        | (labeled['f_spread_z'] >= 1.5)
    )

    trend_mask = (
        ~volatile_mask
        & ((trend_extension >= 1.20) | (momentum_strength >= 1.00))
        & rsi_directional
    )

    labeled['MarketRegime'] = 'choppy'
    labeled.loc[trend_mask, 'MarketRegime'] = 'trend'
    labeled.loc[volatile_mask, 'MarketRegime'] = 'volatile'
    labeled['RegimeLabel'] = labeled['MarketRegime'].map(REGIME_LABEL_TO_ID).astype(np.int8)

    regime_dist = labeled['MarketRegime'].value_counts(normalize=True).mul(100.0).round(2)
    print("\n>>> Regime distribution (% rows):")
    for regime_name in ['choppy', 'trend', 'volatile']:
        pct = float(regime_dist.get(regime_name, 0.0))
        print(f"{regime_name:>8}: {pct:6.2f}%")

    return labeled


def train_regime_classifier(X, y, dates, feature_count, out_dir, model_family='random_forest', no_onnx: bool = False):
    print("\n=========================================")
    print("--- Training Market Regime Classifier ---")
    print(f"Rows: {len(y)} | Classes: {sorted(np.unique(y).tolist())} | ModelFamily: {_normalize_model_family(model_family)}")

    fold_indices = build_day_walk_forward_splits(
        dates,
        n_splits=N_SPLITS,
        day_gap=DAY_GAP_BETWEEN_TRAIN_TEST,
    )
    if not fold_indices:
        print("WARNING: Not enough distinct days for day-based regime walk-forward validation.")
        print(">>> Falling back to training final regime classifier on 100% of rows only.")

    fold_acc = []

    fold = 1
    for train_index, test_index, train_days, test_days in fold_indices:
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]

        clf = build_classifier(model_family=model_family, random_state=42, multi_class=True)
        clf.fit(X_train, y_train)
        pred = clf.predict(X_test)
        acc = accuracy_score(y_test, pred)
        fold_acc.append(acc)
        print(
            f"Fold {fold} | TrainDays: {train_days} | TestDays: {test_days} "
            f"| Train: {len(X_train)} | Test: {len(X_test)} | Accuracy: {acc:.2%}"
        )
        fold += 1

    final_model = build_classifier(model_family=model_family, random_state=42, multi_class=True)
    final_model.fit(X, y)

    versioned_path = out_dir / "regime_classifier.onnx"
    exported_to: str
    if no_onnx:
        exported_to = "(skipped --no-onnx)"
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        export_to_onnx(final_model, feature_count, str(versioned_path), alias_filename=maybe_alias_path("regime_classifier.onnx"))
        exported_to = str(versioned_path)

    avg_acc = float(np.mean(fold_acc)) if fold_acc else 0.0
    print(f">>> Regime classifier average walk-forward accuracy: {avg_acc:.2%}")

    return {
        'avg_accuracy': avg_acc,
        'exported_to': exported_to,
    }


def build_regime_feature_subset(df, all_feature_cols):
    # Exclude direct proxies used in assign_market_regime to keep evaluation meaningful.
    excluded = {
        'f_atr_norm',
        'f_realized_vol_20',
        'f_realized_vol_z',
        'f_spread_z',
        'f_rsi',
        'f_macd_diff',
        'f_dist_sma',
    }
    subset = [col for col in all_feature_cols if col not in excluded]
    if len(subset) < 8:
        # Safety fallback: never train regime model on an excessively tiny schema.
        print("WARNING: Regime subset too small after exclusions, reverting to full feature set.")
        subset = list(all_feature_cols)
    return subset


def train_regime_specific_models(df, feature_cols, out_dir, no_onnx: bool = False):
    model_specs = [
        ("LONG ENTRY (Dip Buyer)", "Label_Long_Entry", "long_entry.onnx"),
        ("SHORT ENTRY (Rip Seller)", "Label_Short_Entry", "short_entry.onnx"),
        ("LONG EXIT (Top Detector)", "Label_Long_Exit", "long_exit.onnx"),
        ("SHORT EXIT (Bottom Detector)", "Label_Short_Exit", "short_exit.onnx"),
    ]
    model_specs = filter_legacy_exit_model_specs(model_specs, 'regime-specific training')

    summary_rows = []
    for regime_name in ['choppy', 'trend', 'volatile']:
        regime_df = df[df['MarketRegime'] == regime_name].copy()
        print("\n=========================================")
        print(f"--- Training {regime_name.upper()} Specialized Models ---")
        print(f"Rows in regime subset: {len(regime_df)}")

        if len(regime_df) < MIN_REGIME_ROWS:
            print(f"WARNING: Skipping {regime_name} models. Need >= {MIN_REGIME_ROWS} rows.")
            continue

        X_regime = regime_df[feature_cols].values.astype(np.float32)

        for model_name, label_col, base_filename in model_specs:
            positive_count = int(np.sum(regime_df[label_col].values, dtype=np.int64))
            if positive_count < MIN_REGIME_SIGNALS:
                print(
                    f"WARNING: Skipping {regime_name} {model_name}. "
                    f"Need >= {MIN_REGIME_SIGNALS} positives, found {positive_count}."
                )
                continue

            versioned_name = f"{regime_name}_{base_filename}"
            resources_name = f"{regime_name}_{base_filename}"

            result = perform_walk_forward_testing(
                X_regime,
                regime_df[label_col].values,
                regime_df['Date'].values,
                f"{regime_name.upper()} {model_name}",
                model_family=MODEL_FAMILY,
            )
            if result['model'] is None:
                continue

            versioned_path = out_dir / versioned_name
            if no_onnx:
                exported_to = "(skipped --no-onnx)"
            else:
                export_to_onnx(result['model'], len(feature_cols), str(versioned_path), alias_filename=maybe_alias_path(resources_name))
                exported_to = str(versioned_path)

            summary_rows.append({
                'regime': regime_name,
                'model': model_name,
                'signals': result['total_signals'],
                'rows': result['total_rows'],
                'avg_precision': result['avg_precision'],
                'avg_threshold': result['avg_threshold'],
                'exported_to': exported_to,
            })

    return summary_rows


def train_open30_models(df, feature_cols, out_dir, no_onnx: bool = False):
    model_specs = [
        ("LONG ENTRY (Dip Buyer)", "Label_Long_Entry", "open30_long_entry.onnx"),
        ("SHORT ENTRY (Rip Seller)", "Label_Short_Entry", "open30_short_entry.onnx"),
        ("LONG EXIT (Top Detector)", "Label_Long_Exit", "open30_long_exit.onnx"),
        ("SHORT EXIT (Bottom Detector)", "Label_Short_Exit", "open30_short_exit.onnx"),
    ]
    model_specs = filter_legacy_exit_model_specs(model_specs, 'opening-30m training')

    # 09:30:00 <= t < 10:00:00 ET
    open30_mask = (
        ((df['Hour'] == 9) & (df['Minute'] >= 30))
        | ((df['Hour'] == 10) & (df['Minute'] == 0) & (df['Timestamp'].dt.second == 0))
    )

    open_df = df[open30_mask].copy()
    print("\n=========================================")
    print("--- Training OPENING-30M Specialized Models ---")
    print(f"Rows in opening subset: {len(open_df)}")

    if len(open_df) < MIN_OPEN30_ROWS:
        print(f"WARNING: Skipping opening-30m models. Need >= {MIN_OPEN30_ROWS} rows.")
        return []

    X_open = open_df[feature_cols].values.astype(np.float32)
    summary_rows = []

    for model_name, label_col, filename in model_specs:
        positive_count = int(np.sum(open_df[label_col].values, dtype=np.int64))
        if positive_count < MIN_OPEN30_SIGNALS:
            print(
                f"WARNING: Skipping opening-30m {model_name}. "
                f"Need >= {MIN_OPEN30_SIGNALS} positives, found {positive_count}."
            )
            continue

        result = perform_walk_forward_testing(
            X_open,
            open_df[label_col].values,
            open_df['Date'].values,
            f"OPEN30 {model_name}",
            model_family=MODEL_FAMILY,
        )
        if result['model'] is None:
            continue

        versioned_path = out_dir / filename
        if no_onnx:
            exported_to = "(skipped --no-onnx)"
        else:
            export_to_onnx(result['model'], len(feature_cols), str(versioned_path), alias_filename=maybe_alias_path(filename))
            exported_to = str(versioned_path)

        summary_rows.append({
            'model': model_name,
            'signals': result['total_signals'],
            'rows': result['total_rows'],
            'avg_precision': result['avg_precision'],
            'avg_threshold': result['avg_threshold'],
            'exported_to': exported_to,
        })

    return summary_rows


def filter_after_opening_window(df):
    # Soft separation: train non-open models only on bars from 10:00 ET onward.
    rest_mask = (df['Hour'] >= 10)
    return df[rest_mask].copy()


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

    model_bytes = None
    onnx_model = None

    if isinstance(model, RandomForestClassifier):
        # Force probability tensor output (no ZipMap) to keep Java parsing stable and continuous.
        onnx_model = convert_sklearn(
            model,
            initial_types=initial_type,
            options={id(model): {'zipmap': False}},
            target_opset=12,
        )
        if getattr(onnx_model, 'ir_version', 0) > 9:
            onnx_model.ir_version = 9
        model_bytes = onnx_model.SerializeToString()
    elif LGBMClassifier is not None and isinstance(model, LGBMClassifier):
        if convert_lightgbm_onnx is None:
            raise RuntimeError('LightGBM ONNX export requires onnxmltools. Install onnxmltools to continue.')
        onnx_model = convert_lightgbm_onnx(model, initial_types=initial_type, target_opset=12)
        model_bytes = onnx_model.SerializeToString()
    elif CatBoostClassifier is not None and isinstance(model, CatBoostClassifier):
        out_path = Path(filename)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        model.save_model(str(out_path), format='onnx')
        model_bytes = out_path.read_bytes()
        if onnx is not None:
            onnx_model = onnx.load_from_string(model_bytes)
    else:
        # Best-effort fallback for other sklearn-compatible estimators.
        onnx_model = convert_sklearn(
            model,
            initial_types=initial_type,
            options={id(model): {'zipmap': False}},
            target_opset=12,
        )
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

    if onnx_model is not None:
        ir = getattr(onnx_model, 'ir_version', '?')
        opsets = ",".join(str(imp.version) for imp in onnx_model.opset_import)
        print(f">>> Exported Production Model: {out_path} (ir={ir}, opset={opsets})")
    else:
        print(f">>> Exported Production Model: {out_path}")
    if alias_filename:
        print(f">>> Updated Canonical Model: {alias_filename}")


def maybe_alias_path(filename):
    if not UPDATE_CANONICAL_MODEL_ALIASES:
        return None
    return str(Path("src") / "main" / "resources" / filename)


def optimize_threshold(y_true, probas):
    best_thr = MIN_TUNED_THRESHOLD
    best_prec = -1.0
    best_recall = 0.0
    best_pos_rate = 0.0
    best_score = -1e9

    base_pos_rate = float(np.mean(y_true)) if len(y_true) else 0.0
    adaptive_floor = max(
        MIN_PRED_POS_RATE,
        min(0.20, base_pos_rate * MIN_POS_FRACTION_OF_BASE)
    )
    target_pos_rate = min(0.25, max(0.08, base_pos_rate * TARGET_POS_FRACTION_OF_BASE))

    for thr in THRESHOLD_GRID:
        preds = (probas >= thr).astype(np.int8)
        pos_rate = preds.mean()
        if pos_rate <= 0:
            continue

        rec = recall_score(y_true, preds, zero_division=0)
        if rec < MIN_RECALL or pos_rate < adaptive_floor:
            continue

        prec = precision_score(y_true, preds, zero_division=0)
        # Balance precision with recall and avoid drifting to very sparse predictions.
        score = 0.65 * prec + 0.35 * rec - 0.15 * abs(pos_rate - target_pos_rate)
        if score > best_score or (score == best_score and prec > best_prec):
            best_score = float(score)
            best_prec = prec
            best_thr = float(thr)
            best_recall = float(rec)
            best_pos_rate = float(pos_rate)

    if best_prec < 0:
        # Fallback: keep production-safe minimum tuned threshold.
        return MIN_TUNED_THRESHOLD, 0.0, 0.0, 0.0
    return best_thr, best_prec, best_recall, best_pos_rate

def build_day_walk_forward_splits(dates, n_splits=5, day_gap=0):
    day_series = pd.Series(dates).reset_index(drop=True)
    unique_days = pd.Index(pd.unique(day_series))
    n_days = len(unique_days)

    if n_days < 2:
        return []

    test_days_per_fold = max(1, n_days // (n_splits + 1))
    splits = []

    for fold in range(1, n_splits + 1):
        test_start = fold * test_days_per_fold
        test_end = min(test_start + test_days_per_fold, n_days)
        train_end = max(0, test_start - int(day_gap))

        if test_start >= n_days or train_end <= 0:
            continue

        train_days = unique_days[:train_end]
        test_days = unique_days[test_start:test_end]
        if len(test_days) == 0:
            continue

        train_idx = day_series.index[day_series.isin(train_days)].to_numpy(dtype=np.int64)
        test_idx = day_series.index[day_series.isin(test_days)].to_numpy(dtype=np.int64)
        if len(train_idx) == 0 or len(test_idx) == 0:
            continue

        splits.append((train_idx, test_idx, len(train_days), len(test_days)))

    return splits


def perform_walk_forward_testing(X, y, dates, name, model_family='random_forest', collect_oof: bool = False):
    """Walk-forward evaluation and final production model training.

    When collect_oof=True also returns per-row OOF predictions (fold_id, threshold,
    margin_over_threshold, probability, y_true) and per-fold calibration metrics
    needed for setup_scorecard.csv, threshold_grid.csv, and oof_setup_predictions.csv.
    All new return keys are absent when collect_oof=False so existing call-sites
    are unaffected.
    """
    print(f"\n=========================================")
    print(f"--- Walk-Forward Testing: {name} ---")
    total_signals = int(np.sum(y, dtype=np.int64))
    print(f"Total signals found: {total_signals} / {len(y)} | ModelFamily: {_normalize_model_family(model_family)}")
    
    if int(np.sum(y, dtype=np.int64)) == 0:
        print(f"WARNING: No positive labels found. Adjust target percentages.")
        base = {
            'model': None,
            'total_signals': total_signals,
            'total_rows': len(y),
            'avg_precision': 0.0,
            'avg_threshold': MIN_TUNED_THRESHOLD,
            'folds_used': 0,
        }
        if collect_oof:
            base.update({'oof_rows': [], 'fold_grid': [], 'threshold_std': 0.0,
                         'threshold_max_dev': 0.0, 'brier_score': math.nan,
                         'ece': math.nan, 'calibration_rows': 0,
                         'calibration_reliability_rows': []})
        return base

    fold_indices = build_day_walk_forward_splits(
        dates,
        n_splits=N_SPLITS,
        day_gap=DAY_GAP_BETWEEN_TRAIN_TEST,
    )
    if not fold_indices:
        print("WARNING: Not enough distinct days for day-based walk-forward validation.")
        print(">>> Falling back to training final production model on 100% of rows only.")
    
    fold = 1
    precisions = []
    thresholds = []
    oof_rows: list[dict] = []      # populated only when collect_oof=True
    fold_grid: list[dict] = []     # populated only when collect_oof=True
    calibration_reliability_rows: list[dict] = []  # populated only when collect_oof=True

    for train_index, test_index, train_days, test_days in fold_indices:
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]
        
        if int(np.sum(y_train, dtype=np.int64)) == 0 or int(np.sum(y_test, dtype=np.int64)) == 0:
            print(f"Fold {fold}: Skipped (insufficient signal data in split)")
            fold += 1
            continue

        model = build_classifier(model_family=model_family, random_state=42, multi_class=False)
        model.fit(X_train, y_train)

        # Tune threshold from the tail of the train split (time-consistent; not calibration).
        calib_size = max(200, int(len(X_train) * 0.2))
        calib_size = min(calib_size, len(X_train) - 1)
        if calib_size > 0:
            X_cal = X_train[-calib_size:]
            y_cal = y_train[-calib_size:]
            cal_proba = predict_positive_proba(model, X_cal)
            thr, cal_prec, cal_rec, cal_pos = optimize_threshold(y_cal, cal_proba)
        else:
            thr, cal_prec, cal_rec, cal_pos = (MIN_TUNED_THRESHOLD, 0.0, 0.0, 0.0)

        test_proba = predict_positive_proba(model, X_test)
        y_pred = (test_proba >= thr).astype(np.int8)
        pred_pos_rate = float(y_pred.mean()) if len(y_pred) else 0.0
        test_pos_rate = float(y_test.mean()) if len(y_test) else 0.0
        pred_pos_count = int(np.sum(y_pred, dtype=np.int64))
        
        prec = precision_score(y_test, y_pred, zero_division=0)
        precisions.append(prec)
        thresholds.append(thr)

        if collect_oof:
            # Measure raw-probability calibration on the OOF test rows.
            cal = calibration_report(y_test, test_proba)
            fold_grid.append({
                'fold_id': fold,
                'train_days': int(train_days),
                'test_days': int(test_days),
                'train_rows': int(len(X_train)),
                'test_rows': int(len(X_test)),
                'threshold': float(thr),
                'test_precision': float(prec),
                'test_recall': float(recall_score(y_test, y_pred, zero_division=0)),
                'pred_pos_rate': float(pred_pos_rate),
                'brier_score': cal['brier_score'],
                'ece': cal['ece'],
                'calibration_rows': cal['rows'],
            })
            for bin_row in cal.get('bins', []):
                calibration_reliability_rows.append({'fold_id': int(fold), **bin_row})
            margin = float(thr)
            for pos_in_fold, global_idx in enumerate(test_index):
                prob = float(test_proba[pos_in_fold])
                oof_rows.append({
                    'input_row_idx': int(global_idx),
                    'fold_id': int(fold),
                    'y_true': int(y_test[pos_in_fold]),
                    'prob': prob,
                    'threshold': float(thr),
                    'margin_over_threshold': prob - margin,
                    'is_oof_prediction': True,
                })

        print(
            f"Fold {fold} | TrainDays: {train_days} | TestDays: {test_days} "
            f"| Train Size: {len(X_train)} | Test Size: {len(X_test)} "
            f"| Thr: {thr:.2f} | CalP/R/Pos: {cal_prec:.2%}/{cal_rec:.2%}/{cal_pos:.2%} "
            f"| Test Pos: {test_pos_rate:.2%} | Pred Pos: {pred_pos_rate:.2%} ({pred_pos_count}) "
            f"| Out-of-Sample Precision: {prec:.2%}"
        )
        fold += 1

    avg_precision = np.mean(precisions) if precisions else 0.0
    avg_threshold = np.mean(thresholds) if thresholds else MIN_TUNED_THRESHOLD
    if precisions:
        print(f">>> Average Walk-Forward Precision: {avg_precision:.2%}")
        print(f">>> Average Tuned Threshold: {avg_threshold:.2f}")
        if avg_precision < 0.40:
            print(f"!!! WARNING: This model shows poor out-of-sample prediction power.")
    
    print(f"\n>>> Training Final Production Model on 100% of data...")
    final_model = build_classifier(model_family=model_family, random_state=42, multi_class=False)
    final_model.fit(X, y)

    result = {
        'model': final_model,
        'total_signals': total_signals,
        'total_rows': len(y),
        'avg_precision': float(avg_precision),
        'avg_threshold': float(avg_threshold),
        'folds_used': len(precisions),
    }
    if collect_oof:
        thr_std = float(np.std(thresholds)) if len(thresholds) > 1 else 0.0
        thr_max_dev = float(max(abs(t - avg_threshold) for t in thresholds)) if thresholds else 0.0
        # Aggregate calibration across folds (weighted by test rows, ignoring nan).
        valid_folds = [r for r in fold_grid if math.isfinite(r['brier_score'])]
        if valid_folds:
            total_cal_rows = sum(r['calibration_rows'] for r in valid_folds)
            agg_brier = float(sum(r['brier_score'] * r['calibration_rows'] for r in valid_folds) / max(total_cal_rows, 1))
            agg_ece = float(sum(r['ece'] * r['calibration_rows'] for r in valid_folds) / max(total_cal_rows, 1))
        else:
            total_cal_rows = 0
            agg_brier = math.nan
            agg_ece = math.nan
        result.update({
            'oof_rows': oof_rows,
            'fold_grid': fold_grid,
            'calibration_reliability_rows': calibration_reliability_rows,
            'threshold_std': thr_std,
            'threshold_max_dev': thr_max_dev,
            'brier_score': agg_brier,
            'ece': agg_ece,
            'calibration_rows': total_cal_rows,
        })
    return result


def predict_positive_proba(model, X):
    raw = model.predict_proba(X)
    classes = getattr(model, 'classes_', None)
    if classes is None:
        return raw[:, 1] if raw.ndim == 2 and raw.shape[1] > 1 else np.zeros(len(X), dtype=np.float32)

    for idx, class_value in enumerate(classes):
        if int(class_value) == 1:
            return raw[:, idx]
    return np.zeros(len(X), dtype=np.float32)


def calibration_report(y_true, probas, *, bins: int = 10) -> dict:
    """Compute Brier score, ECE, and reliability bins from held-out probabilities.

    Matches the calibration_report() used in train_lifecycle_micro_models.py so
    that setup and lifecycle scorecards can be compared directly.
    """
    y = np.asarray(y_true, dtype=float)
    p = np.asarray(probas, dtype=float)
    mask = np.isfinite(y) & np.isfinite(p)
    y = y[mask]
    p = np.clip(p[mask], 0.0, 1.0)
    if len(y) == 0:
        return {"rows": 0, "brier_score": math.nan, "ece": math.nan, "bins": []}

    brier = float(np.mean((p - y) ** 2))
    edges = np.linspace(0.0, 1.0, int(bins) + 1)
    reliability_bins: list[dict] = []
    ece = 0.0
    for index in range(int(bins)):
        low = float(edges[index])
        high = float(edges[index + 1])
        if index == int(bins) - 1:
            bin_mask = (p >= low) & (p <= high)
        else:
            bin_mask = (p >= low) & (p < high)
        count = int(bin_mask.sum())
        if count:
            mean_pred = float(p[bin_mask].mean())
            observed = float(y[bin_mask].mean())
            abs_error = abs(mean_pred - observed)
            ece += (count / len(y)) * abs_error
        else:
            mean_pred = math.nan
            observed = math.nan
            abs_error = math.nan
        reliability_bins.append({
            "bin_index": index,
            "prob_min": low,
            "prob_max": high,
            "rows": count,
            "mean_predicted_probability": mean_pred,
            "observed_positive_rate": observed,
            "abs_calibration_error": abs_error,
        })
    return {"rows": int(len(y)), "brier_score": brier, "ece": float(ece), "bins": reliability_bins}


def feature_schema_hash(feature_columns: list[str]) -> str:
    """SHA-256 of newline-joined sorted feature column names.

    Matches the algorithm in train_lifecycle_micro_models.py and
    PingPongStrategy.java so all three use the same hash for schema validation.
    """
    return hashlib.sha256("\n".join(feature_columns).encode("utf-8")).hexdigest()

def ensure_optional_numeric_columns(df, columns, default_value=0.0):
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = default_value
        out[col] = pd.to_numeric(out[col], errors='coerce').fillna(default_value)
    return out


def add_regime_probability_features(df, regime_feature_cols, model_family='random_forest'):
    out = df.copy()
    X_regime = out[regime_feature_cols].values.astype(np.float32)
    y_regime = out['RegimeLabel'].values.astype(np.int64)
    dates = out['Date'].values

    proba_matrix = np.full((len(out), len(REGIME_LABEL_TO_ID)), np.nan, dtype=np.float32)
    fold_indices = build_day_walk_forward_splits(dates, n_splits=N_SPLITS, day_gap=DAY_GAP_BETWEEN_TRAIN_TEST)

    for fold_i, (train_idx, test_idx, _train_days, _test_days) in enumerate(fold_indices, start=1):
        train_labels = y_regime[train_idx]
        if len(np.unique(train_labels)) < 2:
            continue

        clf = build_classifier(model_family=model_family, random_state=200 + fold_i, multi_class=True)
        clf.fit(X_regime[train_idx], train_labels)
        raw = clf.predict_proba(X_regime[test_idx])

        aligned = np.zeros((len(test_idx), len(REGIME_LABEL_TO_ID)), dtype=np.float32)
        classes = getattr(clf, 'classes_', np.arange(len(REGIME_LABEL_TO_ID)))
        for local_idx, class_id in enumerate(classes):
            class_int = int(class_id)
            if 0 <= class_int < aligned.shape[1]:
                aligned[:, class_int] = raw[:, local_idx]
        proba_matrix[test_idx] = aligned

    missing_mask = np.isnan(proba_matrix).any(axis=1)
    if missing_mask.any():
        prevalence = np.bincount(y_regime, minlength=len(REGIME_LABEL_TO_ID)).astype(np.float32)
        prevalence = prevalence / max(prevalence.sum(), 1.0)
        proba_matrix[missing_mask] = prevalence

    out['f_regime_prob_choppy'] = proba_matrix[:, REGIME_LABEL_TO_ID['choppy']]
    out['f_regime_prob_trend'] = proba_matrix[:, REGIME_LABEL_TO_ID['trend']]
    out['f_regime_prob_volatile'] = proba_matrix[:, REGIME_LABEL_TO_ID['volatile']]

    entropy = -np.sum(proba_matrix * np.log(np.clip(proba_matrix, 1e-9, 1.0)), axis=1)
    out['f_regime_prob_entropy'] = entropy / np.log(float(len(REGIME_LABEL_TO_ID)))
    return out

def main():
    args = parse_args()
    csv_file = Path(args.input_csv).expanduser().resolve()
    source_5s_clean_file = Path(args.source_5s_csv).expanduser().resolve() if args.source_5s_csv else DEFAULT_SOURCE_5S_CLEAN_FILE

    run_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    versioned_out_dir = MODEL_EXPORTS_ROOT / run_tag

    # If --output-dir is given, all outputs (ONNX + artifacts) go there.
    # Otherwise fall back to the legacy versioned_out_dir.
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
    else:
        output_dir = versioned_out_dir

    no_onnx = bool(getattr(args, 'no_onnx', False))

    print(f">>> Training model family: {_normalize_model_family(MODEL_FAMILY)}")
    print(f">>> Regime model family: {_normalize_model_family(REGIME_MODEL_FAMILY)}")
    print(f">>> Train legacy 30s exit models: {TRAIN_LEGACY_30S_EXIT_MODELS}")

    require_model_family_available(MODEL_FAMILY, context_name='MODEL_FAMILY')
    require_model_family_available(REGIME_MODEL_FAMILY, context_name='REGIME_MODEL_FAMILY')

    print(f">>> Loading historical data from {csv_file}...")
    if not ensure_training_csv_available(csv_file, source_5s_clean_file):
        print(
            "ERROR: Training CSV missing and could not be prepared automatically. "
            f"Expected 30s file: {csv_file} | clean 5s fallback: {source_5s_clean_file}"
        )
        return

    try:
        raw_df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"ERROR: {csv_file} not found. Please run the Java Historical Bulk Scraper first.")
        return

    raw_df = filter_raw_to_regular_session(raw_df)
    if raw_df.empty:
        print("ERROR: No regular-session rows available after filtering (09:30-15:59 ET).")
        return

    df = calculate_features(raw_df)
    df = generate_labels(df)
    df = assign_market_regime(df)
    df_rest = filter_after_opening_window(df)

    if df_rest.empty:
        print("ERROR: No rows available for >=10:00 ET training after soft separation filter.")
        return

    print(f">>> Soft separation enabled: training non-open models on >=10:00 ET rows only ({len(df_rest)}/{len(df)} rows).")
    print_label_prevalence_by_hour(df)

    # BASE feature array to match Java default output.
    feature_cols = [
        'f_dist_vwap', 'f_bb_lower_dist', 'f_bb_upper_dist', 'f_macd_diff',
        'f_body_size', 'f_lower_wick', 'f_upper_wick', 'f_atr_norm',
        'f_dist_sma', 'f_dist_high', 'f_dist_low', 'f_rsi', 'f_gap_from_prev_close',
        'f_time_of_day', 'f_dist_swing_high', 'f_dist_swing_low', 'f_is_new_high', 
        'f_is_new_low', 'f_dist_whole_num', 'f_is_green', 'f_green_streak',
        'f_red_streak', 'f_put_call_ratio', 'f_vol_ask_ratio', 'f_vol_bid_ratio',
        # Promote first 5 "extended" features into the base schema.
        'f_rel_volume_30s', 'f_realized_vol_20', 'f_realized_vol_z',
        'f_dist_or_high_atr', 'f_dist_or_low_atr'
    ]

    extended_feature_cols = [
        'f_spread_pct', 'f_spread_z', 'f_l1_imbalance', 'f_signed_flow_30s'
    ]

    base_feature_count = len(feature_cols)

    if USE_EXTENDED_FEATURES:
        feature_cols = feature_cols + extended_feature_cols
        print(">>> Using extended feature set (34 features: base 30 + 4 microstructure tail).")
    else:
        print(f">>> Using base {base_feature_count}-feature set (Java-compatible).")

    if USE_NEWS_BAR_FEATURES:
        feature_cols = feature_cols + NEWS_BAR_FEATURE_COLS
        print(
            ">>> Live-news bar feature block enabled "
            f"(+{len(NEWS_BAR_FEATURE_COLS)} columns, requires bar-aligned news inputs for non-zero signal)."
        )

    if USE_META_PRODUCER_FEATURES:
        # Accept producer columns from enriched 30s CSV when present; inject safe numeric defaults otherwise.
        df_rest = ensure_optional_numeric_columns(df_rest, META_PRODUCER_FEATURE_COLS, default_value=0.0)
        feature_cols = feature_cols + META_PRODUCER_FEATURE_COLS
        print(
            ">>> Meta producer feature block enabled "
            f"(+{len(META_PRODUCER_FEATURE_COLS)} columns, training-only schema extension)."
        )

    regime_feature_cols = build_regime_feature_subset(df_rest, feature_cols)
    print(
        f">>> Regime classifier feature subset: {len(regime_feature_cols)} "
        f"(excluded direct regime-rule proxies)."
    )

    if USE_REGIME_PROB_FEATURES:
        full_regime_feature_cols = build_regime_feature_subset(df, feature_cols)
        df = add_regime_probability_features(df, full_regime_feature_cols, model_family=REGIME_MODEL_FAMILY)
        df_rest = add_regime_probability_features(df_rest, regime_feature_cols, model_family=REGIME_MODEL_FAMILY)
        feature_cols = feature_cols + REGIME_PROB_FEATURE_COLS
        print(
            ">>> Regime probability features enabled "
            f"(+{len(REGIME_PROB_FEATURE_COLS)} columns from walk-forward regime model outputs)."
        )

    print(f">>> Feature count used for training: {len(feature_cols)}")
    X = df_rest[feature_cols].values.astype(np.float32)
    X_regime = df_rest[regime_feature_cols].values.astype(np.float32)

    regime_report = train_regime_classifier(
        X_regime,
        df_rest['RegimeLabel'].values.astype(np.int64),
        df_rest['Date'].values,
        len(regime_feature_cols),
        output_dir,
        model_family=REGIME_MODEL_FAMILY,
        no_onnx=no_onnx,
    )

    models = [
        ("LONG ENTRY (Dip Buyer)", df_rest['Label_Long_Entry'].values, "long_entry.onnx"),
        ("SHORT ENTRY (Rip Seller)", df_rest['Label_Short_Entry'].values, "short_entry.onnx"),
        ("LONG EXIT (Top Detector)", df_rest['Label_Long_Exit'].values, "long_exit.onnx"),
        ("SHORT EXIT (Bottom Detector)", df_rest['Label_Short_Exit'].values, "short_exit.onnx")
    ]
    models = [
        (name, y_data, filename)
        for name, y_data, filename in models
        if TRAIN_LEGACY_30S_EXIT_MODELS or filename not in {'long_exit.onnx', 'short_exit.onnx'}
    ]
    if not TRAIN_LEGACY_30S_EXIT_MODELS:
        print(">>> Skipping legacy base 30s exit model training (long_exit.onnx, short_exit.onnx).")

    score_rows = []
    all_threshold_grid_rows: list[dict] = []
    all_calibration_reliability_rows: list[dict] = []
    long_entry_oof_rows: list[dict] = []
    short_entry_oof_rows: list[dict] = []

    # Only the two primary entry models need OOF predictions for downstream lifecycle/micro training.
    _entry_filenames = {'long_entry.onnx', 'short_entry.onnx'}

    for name, y_data, filename in models:
        is_entry_model = filename in _entry_filenames
        result = perform_walk_forward_testing(
            X, y_data, df_rest['Date'].values, name,
            model_family=MODEL_FAMILY,
            collect_oof=is_entry_model,
        )
        exported_path = "-"
        if result['model'] is not None and not no_onnx:
            versioned_path = output_dir / filename
            export_to_onnx(result['model'], len(feature_cols), str(versioned_path), alias_filename=maybe_alias_path(filename))
            exported_path = str(versioned_path)
        elif result['model'] is not None and no_onnx:
            exported_path = "(skipped --no-onnx)"

        thr_std = result.get('threshold_std', math.nan)
        thr_max_dev = result.get('threshold_max_dev', math.nan)
        brier = result.get('brier_score', math.nan)
        ece = result.get('ece', math.nan)
        cal_rows = result.get('calibration_rows', 0)

        score_rows.append({
            'model': name,
            'filename': filename,
            'signals': result['total_signals'],
            'rows': result['total_rows'],
            'signal_rate': (result['total_signals'] / result['total_rows']) if result['total_rows'] else 0.0,
            'avg_precision': result['avg_precision'],
            'avg_threshold': result['avg_threshold'],
            'threshold_std': thr_std,
            'threshold_max_dev': thr_max_dev,
            'brier_score': brier,
            'ece': ece,
            'calibration_rows': cal_rows,
            'folds_used': result['folds_used'],
            'exported_to': exported_path,
        })

        if is_entry_model and result.get('fold_grid'):
            side = 'long' if filename == 'long_entry.onnx' else 'short'
            for row in result['fold_grid']:
                all_threshold_grid_rows.append({'model': filename, 'side': side, **row})
            for row in result.get('calibration_reliability_rows', []):
                all_calibration_reliability_rows.append({'model': filename, 'side': side, **row})
        if filename == 'long_entry.onnx':
            long_entry_oof_rows = result.get('oof_rows', [])
        elif filename == 'short_entry.onnx':
            short_entry_oof_rows = result.get('oof_rows', [])

    print("\n>>> MODEL SCORECARD")
    print("Model | Signals/Rows | SignalRate | AvgPrecision | AvgThreshold | ThrStd | Brier | ECE | Folds | Export")
    for row in score_rows:
        brier_s = f"{row['brier_score']:.4f}" if math.isfinite(row['brier_score']) else "n/a"
        ece_s = f"{row['ece']:.4f}" if math.isfinite(row['ece']) else "n/a"
        thr_std_s = f"{row['threshold_std']:.4f}" if math.isfinite(row['threshold_std']) else "n/a"
        print(
            f"{row['model']} | "
            f"{row['signals']}/{row['rows']} | "
            f"{row['signal_rate']:.2%} | "
            f"{row['avg_precision']:.2%} | "
            f"{row['avg_threshold']:.2f} | "
            f"{thr_std_s} | "
            f"{brier_s} | "
            f"{ece_s} | "
            f"{row['folds_used']} | "
            f"{row['exported_to']}"
        )

    regime_specific_rows = train_regime_specific_models(df_rest, feature_cols, output_dir, no_onnx=no_onnx)
    if regime_specific_rows:
        print("\n>>> REGIME-SPECIFIC MODEL SCORECARD")
        print("Regime | Model | Signals/Rows | AvgPrecision | AvgThreshold | Export")
        for row in regime_specific_rows:
            print(
                f"{row['regime']} | {row['model']} | "
                f"{row['signals']}/{row['rows']} | "
                f"{row['avg_precision']:.2%} | "
                f"{row['avg_threshold']:.2f} | "
                f"{row['exported_to']}"
            )
    else:
        print("\n>>> REGIME-SPECIFIC MODEL SCORECARD")
        print("No trend/volatile specialized models were exported (insufficient rows/signals).")

    open30_rows = train_open30_models(df, feature_cols, output_dir, no_onnx=no_onnx)
    if open30_rows:
        print("\n>>> OPENING-30M MODEL SCORECARD")
        print("Model | Signals/Rows | AvgPrecision | AvgThreshold | Export")
        for row in open30_rows:
            print(
                f"{row['model']} | "
                f"{row['signals']}/{row['rows']} | "
                f"{row['avg_precision']:.2%} | "
                f"{row['avg_threshold']:.2f} | "
                f"{row['exported_to']}"
            )
    else:
        print("\n>>> OPENING-30M MODEL SCORECARD")
        print("No opening-30m specialized models were exported (insufficient rows/signals).")

    print("\n>>> REGIME CLASSIFIER")
    print(
        "MarketRegimeClassifier | "
        f"AvgAccuracy={regime_report['avg_accuracy']:.2%} | "
        f"Export={regime_report['exported_to']}"
    )

    # ------------------------------------------------------------------ #
    # Write machine-readable artifacts                                     #
    # ------------------------------------------------------------------ #
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    fschema_hash = feature_schema_hash(feature_cols)
    commit_hash = current_git_commit()
    cost_aware_label_manifest = write_cost_aware_label_artifacts(df, output_dir, str(csv_file), generated_at, commit_hash)

    # --- setup_scorecard.csv ---
    scorecard_path = output_dir / "setup_scorecard.csv"
    pd.DataFrame(score_rows).to_csv(scorecard_path, index=False)
    print(f"\n>>> Wrote {scorecard_path}")

    # --- threshold_grid.csv (always written; empty when no folds ran) ---
    tgrid_path = output_dir / "threshold_grid.csv"
    tgrid_df = pd.DataFrame(all_threshold_grid_rows) if all_threshold_grid_rows else pd.DataFrame(
        columns=["model", "side", "fold_id", "train_days", "test_days", "train_rows", "test_rows",
                 "threshold", "test_precision", "test_recall", "pred_pos_rate",
                 "brier_score", "ece", "calibration_rows"]
    )
    tgrid_df.to_csv(tgrid_path, index=False)
    print(f">>> Wrote {tgrid_path} ({len(tgrid_df)} fold rows)")

    # --- oof_setup_predictions.csv (wide format: one row per bar in df_rest) ---
    # Build lookup dicts keyed by input_row_idx so we can join both sides per bar.
    df_meta = df_rest.reset_index(drop=True)
    symbol_col = 'Symbol' if 'Symbol' in df_meta.columns else None
    ts_col = 'Timestamp' if 'Timestamp' in df_meta.columns else None
    date_col = 'Date' if 'Date' in df_meta.columns else None
    _long_oof_by_idx: dict[int, dict] = {r['input_row_idx']: r for r in long_entry_oof_rows}
    _short_oof_by_idx: dict[int, dict] = {r['input_row_idx']: r for r in short_entry_oof_rows}

    _wide_cols = [
        'Symbol', 'Timestamp', 'Date', 'Label_Long_Entry', 'Label_Short_Entry',
        'Label_Long_Entry_CostAware', 'Label_Short_Entry_CostAware',
        'Label_Long_Entry_TpBeforeSl', 'Label_Short_Entry_TpBeforeSl',
        'Label_Long_Entry_ExpectedNetRAfterCosts', 'Label_Short_Entry_ExpectedNetRAfterCosts',
        'f_long_setup_prob', 'long_setup_fold_id',
        'f_long_setup_threshold', 'f_long_setup_threshold_margin',
        'f_short_setup_prob', 'short_setup_fold_id',
        'f_short_setup_threshold', 'f_short_setup_threshold_margin',
        'is_oof_setup_prediction',
    ]
    _wide_rows: list[dict] = []
    for _idx in range(len(df_meta)):
        lr = _long_oof_by_idx.get(_idx)
        sr = _short_oof_by_idx.get(_idx)
        row_w: dict = {}
        if symbol_col:
            row_w['Symbol'] = df_meta.at[_idx, symbol_col]
        if ts_col:
            row_w['Timestamp'] = df_meta.at[_idx, ts_col]
        if date_col:
            row_w['Date'] = df_meta.at[_idx, date_col]
        row_w['Label_Long_Entry'] = (
            df_meta.at[_idx, 'Label_Long_Entry'] if 'Label_Long_Entry' in df_meta.columns else float('nan')
        )
        row_w['Label_Short_Entry'] = (
            df_meta.at[_idx, 'Label_Short_Entry'] if 'Label_Short_Entry' in df_meta.columns else float('nan')
        )
        for _label_col in [
            'Label_Long_Entry_CostAware', 'Label_Short_Entry_CostAware',
            'Label_Long_Entry_TpBeforeSl', 'Label_Short_Entry_TpBeforeSl',
            'Label_Long_Entry_ExpectedNetRAfterCosts', 'Label_Short_Entry_ExpectedNetRAfterCosts',
        ]:
            row_w[_label_col] = df_meta.at[_idx, _label_col] if _label_col in df_meta.columns else float('nan')
        # Long side: NaN / -1 when bar was never in an OOF test fold
        row_w['f_long_setup_prob'] = lr['prob'] if lr else float('nan')
        row_w['long_setup_fold_id'] = int(lr['fold_id']) if lr else -1
        row_w['f_long_setup_threshold'] = lr['threshold'] if lr else float('nan')
        row_w['f_long_setup_threshold_margin'] = lr['margin_over_threshold'] if lr else float('nan')
        # Short side
        row_w['f_short_setup_prob'] = sr['prob'] if sr else float('nan')
        row_w['short_setup_fold_id'] = int(sr['fold_id']) if sr else -1
        row_w['f_short_setup_threshold'] = sr['threshold'] if sr else float('nan')
        row_w['f_short_setup_threshold_margin'] = sr['margin_over_threshold'] if sr else float('nan')
        # Only mark as OOF prediction when BOTH long and short have a prediction
        row_w['is_oof_setup_prediction'] = 1 if (lr is not None and sr is not None) else 0
        _wide_rows.append(row_w)

    oof_path = output_dir / "oof_setup_predictions.csv"
    if _wide_rows:
        oof_df = pd.DataFrame(_wide_rows)
        # Enforce column order, adding any missing as NaN
        for _c in _wide_cols:
            if _c not in oof_df.columns:
                oof_df[_c] = float('nan')
        oof_df = oof_df[[c for c in _wide_cols if c in oof_df.columns]]
    else:
        oof_df = pd.DataFrame(columns=_wide_cols)
    oof_df.to_csv(oof_path, index=False)
    oof_long_count = int(oof_df['f_long_setup_prob'].notna().sum()) if _wide_rows else 0
    oof_short_count = int(oof_df['f_short_setup_prob'].notna().sum()) if _wide_rows else 0
    oof_paired_count = int(oof_df['is_oof_setup_prediction'].sum()) if _wide_rows else 0
    print(f">>> Wrote {oof_path} (total_rows={len(oof_df)} paired_oof_rows={oof_paired_count})")

    # --- calibration_manifest.json and calibration_reliability.csv ---
    entry_score_rows = [r for r in score_rows if r['filename'] in _entry_filenames]
    cal_model_entries: list[dict] = []
    for sr in entry_score_rows:
        cal_model_entries.append({
            'model': sr['filename'],
            'side': 'long' if 'long_entry' in sr['filename'] else 'short',
            'brier_score': sr['brier_score'] if math.isfinite(sr.get('brier_score', math.nan)) else None,
            'ece': sr['ece'] if math.isfinite(sr.get('ece', math.nan)) else None,
            'calibration_rows': sr.get('calibration_rows', 0),
            'folds_used': sr.get('folds_used', 0),
            'calibration_reliability_artifact': 'calibration_reliability.csv',
        })
        # Collect per-fold bin detail from threshold_grid for this model
        # (Reliability bins per fold are already aggregated into brier/ece above;
        # full per-bin rows would require storing them in fold_grid which is a
        # future enhancement. Mark bins as empty here until a post-hoc calibrator
        # fitting step generates them from the frozen holdout.)

    calibration_manifest = {
        'schema_version': SETUP_MANIFEST_SCHEMA_VERSION,
        'calibration_schema_version': 'setup_30s_calibration_v1',
        'method': 'raw_random_forest_probability_no_posthoc_calibrator',
        'split_convention': 'chronological_walk_forward_folds_train_tail_threshold_tune',
        'generated_at_utc': generated_at,
        'artifacts': {
            'scorecard': 'setup_scorecard.csv',
            'threshold_grid': 'threshold_grid.csv',
            'oof_predictions': 'oof_setup_predictions.csv',
            'reliability': 'calibration_reliability.csv',
            'cost_aware_labels': 'cost_aware_setup_labels.csv',
            'cost_aware_label_manifest': 'cost_aware_label_manifest.json',
        },
        'models': cal_model_entries,
        'errors': [],
        'warnings': [
            'Metrics describe raw RandomForest predict_proba outputs averaged across walk-forward '
            'folds; no isotonic/Platt post-hoc calibrator is fitted yet. Treat bundles as '
            'research-only until post-hoc calibration, frozen-holdout threshold stability, and '
            'paper/shadow drift checks pass.'
        ],
    }
    cal_manifest_path = output_dir / "calibration_manifest.json"
    cal_manifest_path.write_text(json.dumps(calibration_manifest, indent=2), encoding='utf-8')
    print(f">>> Wrote {cal_manifest_path}")

    # --- calibration_reliability.csv (per-fold reliability bins; always written) ---
    cal_rel_path = output_dir / "calibration_reliability.csv"
    rel_cols = ['model', 'side', 'fold_id', 'bin_index', 'prob_min', 'prob_max',
                'rows', 'mean_predicted_probability', 'observed_positive_rate',
                'abs_calibration_error']
    if all_calibration_reliability_rows:
        pd.DataFrame(all_calibration_reliability_rows)[rel_cols].to_csv(cal_rel_path, index=False)
    else:
        pd.DataFrame(columns=rel_cols).to_csv(cal_rel_path, index=False)
    print(f">>> Wrote {cal_rel_path}")

    # --- setup_manifest.json ---
    oof_coverage = oof_paired_count / max(len(df_rest), 1)

    entry_sr_long = next((r for r in score_rows if r['filename'] == 'long_entry.onnx'), {})
    entry_sr_short = next((r for r in score_rows if r['filename'] == 'short_entry.onnx'), {})

    setup_manifest = {
        'schema_version': SETUP_MANIFEST_SCHEMA_VERSION,
        'generated_at_utc': generated_at,
        'code_commit': commit_hash,
        'input_csv': str(csv_file),
        'feature_columns': feature_cols,
        'feature_count': len(feature_cols),
        'feature_schema_sha256': fschema_hash,
        'model_family': _normalize_model_family(MODEL_FAMILY),
        'label_info': {
            'type': 'binary_expected_net_r_after_costs' if COST_AWARE_LABELS else 'binary_tp_before_sl',
            'entry_fill_mode': ENTRY_FILL_MODE,
            'entry_profit_pct': ENTRY_PROFIT_PCT,
            'entry_risk_pct': ENTRY_RISK_PCT,
            'entry_slippage_bps': ENTRY_SLIPPAGE_BPS,
            'exit_slippage_bps': EXIT_SLIPPAGE_BPS,
            'future_window_bars': FUTURE_WINDOW_BARS,
            'cost_aware': bool(COST_AWARE_LABELS),
            'expected_net_r_after_costs_available': True,
            'cost_aware_label_schema_version': COST_AWARE_LABEL_SCHEMA_VERSION,
            'cost_aware_label_manifest': 'cost_aware_label_manifest.json',
            'cost_aware_labels_csv': 'cost_aware_setup_labels.csv',
            'long_training_target': 'Label_Long_Entry',
            'short_training_target': 'Label_Short_Entry',
            'long_expected_net_r_after_costs': 'Label_Long_Entry_ExpectedNetRAfterCosts',
            'short_expected_net_r_after_costs': 'Label_Short_Entry_ExpectedNetRAfterCosts',
            'long_legacy_tp_before_sl': 'Label_Long_Entry_TpBeforeSl',
            'short_legacy_tp_before_sl': 'Label_Short_Entry_TpBeforeSl',
            'assumptions': cost_aware_label_manifest.get('assumptions', {}),
            'summary': cost_aware_label_manifest.get('summary', {}),
            'note': 'Entry labels use expected_net_r_after_costs by default; legacy tp_before_sl labels are retained for audit.' if COST_AWARE_LABELS else (
                'Expected_net_r_after_costs labels were generated, but COST_AWARE_LABELS=0 kept legacy tp_before_sl as the training target.'
            ),
        },
        'training_rows': len(df_rest),
        'walk_forward': {
            'n_splits': N_SPLITS,
            'day_gap': DAY_GAP_BETWEEN_TRAIN_TEST,
        },
        'long_entry': {
            'avg_threshold': entry_sr_long.get('avg_threshold', math.nan),
            'threshold_std': entry_sr_long.get('threshold_std', math.nan),
            'threshold_max_dev': entry_sr_long.get('threshold_max_dev', math.nan),
            'avg_precision': entry_sr_long.get('avg_precision', math.nan),
            'brier_score': entry_sr_long.get('brier_score', math.nan),
            'ece': entry_sr_long.get('ece', math.nan),
            'calibration_rows': entry_sr_long.get('calibration_rows', 0),
            'folds_used': entry_sr_long.get('folds_used', 0),
            'oof_rows': oof_long_count,
        },
        'short_entry': {
            'avg_threshold': entry_sr_short.get('avg_threshold', math.nan),
            'threshold_std': entry_sr_short.get('threshold_std', math.nan),
            'threshold_max_dev': entry_sr_short.get('threshold_max_dev', math.nan),
            'avg_precision': entry_sr_short.get('avg_precision', math.nan),
            'brier_score': entry_sr_short.get('brier_score', math.nan),
            'ece': entry_sr_short.get('ece', math.nan),
            'calibration_rows': entry_sr_short.get('calibration_rows', 0),
            'folds_used': entry_sr_short.get('folds_used', 0),
            'oof_rows': oof_short_count,
        },
        'oof_predictions': {
            'long_rows': oof_long_count,
            'short_rows': oof_short_count,
            'paired_rows': oof_paired_count,
            'total_rows': len(df_rest),
            'oof_coverage_frac': oof_coverage,
        },
        'artifacts': {
            'scorecard': 'setup_scorecard.csv',
            'threshold_grid': 'threshold_grid.csv',
            'oof_predictions': 'oof_setup_predictions.csv',
            'calibration_manifest': 'calibration_manifest.json',
            'calibration_reliability': 'calibration_reliability.csv',
            'cost_aware_labels': 'cost_aware_setup_labels.csv',
            'cost_aware_label_manifest': 'cost_aware_label_manifest.json',
        },
        'errors': [],
        'warnings': calibration_manifest['warnings'],
    }

    manifest_path = output_dir / "setup_manifest.json"
    manifest_path.write_text(json.dumps(_json_safe(setup_manifest), indent=2), encoding='utf-8')
    print(f">>> Wrote {manifest_path}")

    print("\n==================================================")
    print(">>> PIPELINE COMPLETE.")
    if no_onnx:
        print(">>> ONNX export disabled (--no-onnx); scorecard/calibration/OOF artifacts were written only.")
    elif TRAIN_LEGACY_30S_EXIT_MODELS:
        print(">>> All 30-second models have been exported.")
    else:
        print(">>> Entry/regime 30-second models have been exported; legacy exit models were skipped.")
    print(f">>> Output directory: {output_dir}")
    if not no_onnx:
        print(">>> Drop the .onnx files directly into your new Java branch.")
    print("==================================================")

if __name__ == "__main__":
    main()
