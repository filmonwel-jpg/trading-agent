package com.calgary.fili.trader.bot.strategy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.calgary.fili.trader.bot.trader.IBKRTrader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-symbol trading strategy implemented as an actor.
 *
 * <p>The most important design choice in this class is that strategy state is mutated from exactly one thread:
 * the internal {@code eventProcessorThread}. External callers such as Databento readers, IBKR callbacks, REST
 * controllers, and execution acknowledgements do not directly touch strategy state. They enqueue typed events,
 * and the actor thread applies them in order.</p>
 *
 * <p>Why that matters:</p>
 * <ul>
 *   <li>Live trading receives callbacks from multiple sources concurrently.</li>
 *   <li>Position, quote, tape, AI bars, and order acknowledgements must be observed in a consistent order.</li>
 *   <li>A single-writer design avoids ad-hoc locking around dozens of live state fields.</li>
 * </ul>
 *
 * <p>Data flow inside this class:</p>
 * <ol>
 *   <li>1-second or legacy-named source bars arrive and are aggregated into 30-second buckets.</li>
 *   <li>At bucket close, technical features and regime features are computed.</li>
 *   <li>The appropriate ONNX models are selected based on opening-window vs regime routing.</li>
 *   <li>Entry/exit decisions are evaluated.</li>
 *   <li>Execution requests are emitted back to {@code IBKRTrader}, which performs broker/risk routing.</li>
 * </ol>
 *
 * <p>Execution intent inside this strategy is quote-aware. Entry sizing and exit references now prefer ask/bid
 * instead of last trade so the strategy's requested prices better match the side it is trying to execute.</p>
 */
public class PingPongStrategy implements TradingStrategy {

    private enum MarketRegime {
        CHOPPY,
        TREND,
        VOLATILE
    }

    private enum ThresholdAction {
        LONG_ENTRY,
        SHORT_ENTRY,
        LONG_EXIT,
        SHORT_EXIT
    }

    private record RegimeDecision(MarketRegime regime, Map<String, Float> probabilityFeatures) {}

    private record MicroBar(long epoch, double open, double high, double low, double close, long volume, double wap) {}

    public record SetupCandidateDiagnostic(String side, long epoch, String marketTime, double probability,
                                           double threshold, double margin, double rsi, double referencePrice,
                                           int quantity, String regime) {}

    public record AiDecisionDiagnostics(long aiEvaluations, long missingMarketTime, long preMarketBlocked,
                                        long missingPreviousClose, long varianceBlocked, long positionOpenSkipped,
                                        long flatEntryEvaluations, long entryGateOpen, long entryGateClosed,
                                        long allowNewEntriesBlocked, long maxTradesBlocked,
                                        long positionSyncBlocked, long hardStopCooldownBlocked,
                                        long hardStopBudgetBlocked, long buyQuantityBlocked,
                                        long sellQuantityBlocked, long longRsiGateBlocked,
                                        long shortRsiGateBlocked, long longModelUnavailable,
                                        long shortModelUnavailable, long longEntryModelEvaluations,
                                        long shortEntryModelEvaluations, long longEntryPasses,
                                        long shortEntryPasses, long longNearMisses, long shortNearMisses,
                                        double maxLongEntryProbability, double maxLongEntryThreshold,
                                        double maxLongEntryMargin, long maxLongEntryEpoch,
                                        String maxLongEntryTime, double maxShortEntryProbability,
                                        double maxShortEntryThreshold, double maxShortEntryMargin,
                                        long maxShortEntryEpoch, String maxShortEntryTime,
                                        List<SetupCandidateDiagnostic> closestSetupEvents) {}

    public interface LifecycleTelemetryListener {
        LifecycleTelemetryListener NOOP = new LifecycleTelemetryListener() {};

        default void onMicroEntryArmed(String symbol, String side, long armEpoch, double setupProbability, double setupThreshold) {}

        default void onMicroEntryArmCleared(String symbol, String side, long armEpoch, String reason) {}

        default void onMicroEntryConfirmed(String symbol, String side, long armEpoch, long confirmEpoch, double probability,
                                           double threshold, int quantity, double referencePrice) {}

        default void onMicroExitGuardEvaluated(String symbol, String side, long epoch, double probability, double threshold,
                                               boolean fired) {}

        default void onLifecycleExitEvaluated(String symbol, String side, long epoch, double probability, double threshold,
                                              boolean fired, double unrealizedR) {}

        default void onHardRiskExit(String symbol, String side, String reason) {}

        default void onEndOfDayExit(String symbol, String side, long epoch, double executionPrice) {}
    }

    public record AiThresholdConfig(
        double baseLongEntryThreshold,
        double baseShortEntryThreshold,
        double baseLongExitThreshold,
        double baseShortExitThreshold,
        double open30LongEntryThreshold,
        double open30ShortEntryThreshold,
        double open30LongExitThreshold,
        double open30ShortExitThreshold,
        double choppyLongEntryThreshold,
        double choppyShortEntryThreshold,
        double choppyLongExitThreshold,
        double choppyShortExitThreshold,
        double trendLongEntryThreshold,
        double trendShortEntryThreshold,
        double trendLongExitThreshold,
        double trendShortExitThreshold,
        double volatileLongEntryThreshold,
        double volatileShortEntryThreshold,
        double volatileLongExitThreshold,
        double volatileShortExitThreshold,
        double regimeClassifierThreshold
    ) {
        public static AiThresholdConfig defaults() {
            return new AiThresholdConfig(
                DEFAULT_LONG_ENTRY_THRESHOLD,
                DEFAULT_SHORT_ENTRY_THRESHOLD,
                DEFAULT_LONG_EXIT_THRESHOLD,
                DEFAULT_SHORT_EXIT_THRESHOLD,
                DEFAULT_LONG_ENTRY_THRESHOLD,
                DEFAULT_SHORT_ENTRY_THRESHOLD,
                DEFAULT_LONG_EXIT_THRESHOLD,
                DEFAULT_SHORT_EXIT_THRESHOLD,
                DEFAULT_LONG_ENTRY_THRESHOLD,
                DEFAULT_SHORT_ENTRY_THRESHOLD,
                DEFAULT_LONG_EXIT_THRESHOLD,
                DEFAULT_SHORT_EXIT_THRESHOLD,
                DEFAULT_LONG_ENTRY_THRESHOLD,
                DEFAULT_SHORT_ENTRY_THRESHOLD,
                DEFAULT_LONG_EXIT_THRESHOLD,
                DEFAULT_SHORT_EXIT_THRESHOLD,
                DEFAULT_LONG_ENTRY_THRESHOLD,
                DEFAULT_SHORT_ENTRY_THRESHOLD,
                DEFAULT_LONG_EXIT_THRESHOLD,
                DEFAULT_SHORT_EXIT_THRESHOLD,
                DEFAULT_REGIME_THRESHOLD
            );
        }

        public AiThresholdConfig normalized() {
            double baseLongEntry = normalize(baseLongEntryThreshold, DEFAULT_LONG_ENTRY_THRESHOLD);
            double baseShortEntry = normalize(baseShortEntryThreshold, DEFAULT_SHORT_ENTRY_THRESHOLD);
            double baseLongExit = normalize(baseLongExitThreshold, DEFAULT_LONG_EXIT_THRESHOLD);
            double baseShortExit = normalize(baseShortExitThreshold, DEFAULT_SHORT_EXIT_THRESHOLD);
            return new AiThresholdConfig(
                baseLongEntry,
                baseShortEntry,
                baseLongExit,
                baseShortExit,
                normalize(open30LongEntryThreshold, baseLongEntry),
                normalize(open30ShortEntryThreshold, baseShortEntry),
                normalize(open30LongExitThreshold, baseLongExit),
                normalize(open30ShortExitThreshold, baseShortExit),
                normalize(choppyLongEntryThreshold, baseLongEntry),
                normalize(choppyShortEntryThreshold, baseShortEntry),
                normalize(choppyLongExitThreshold, baseLongExit),
                normalize(choppyShortExitThreshold, baseShortExit),
                normalize(trendLongEntryThreshold, baseLongEntry),
                normalize(trendShortEntryThreshold, baseShortEntry),
                normalize(trendLongExitThreshold, baseLongExit),
                normalize(trendShortExitThreshold, baseShortExit),
                normalize(volatileLongEntryThreshold, baseLongEntry),
                normalize(volatileShortEntryThreshold, baseShortEntry),
                normalize(volatileLongExitThreshold, baseLongExit),
                normalize(volatileShortExitThreshold, baseShortExit),
                normalize(regimeClassifierThreshold, DEFAULT_REGIME_THRESHOLD)
            );
        }

        public double thresholdFor(boolean openingThirty, MarketRegime regime, ThresholdAction action) {
            if (openingThirty) {
                return switch (action) {
                    case LONG_ENTRY -> open30LongEntryThreshold;
                    case SHORT_ENTRY -> open30ShortEntryThreshold;
                    case LONG_EXIT -> open30LongExitThreshold;
                    case SHORT_EXIT -> open30ShortExitThreshold;
                };
            }
            return switch (regime) {
                case TREND -> switch (action) {
                    case LONG_ENTRY -> trendLongEntryThreshold;
                    case SHORT_ENTRY -> trendShortEntryThreshold;
                    case LONG_EXIT -> trendLongExitThreshold;
                    case SHORT_EXIT -> trendShortExitThreshold;
                };
                case VOLATILE -> switch (action) {
                    case LONG_ENTRY -> volatileLongEntryThreshold;
                    case SHORT_ENTRY -> volatileShortEntryThreshold;
                    case LONG_EXIT -> volatileLongExitThreshold;
                    case SHORT_EXIT -> volatileShortExitThreshold;
                };
                case CHOPPY -> switch (action) {
                    case LONG_ENTRY -> choppyLongEntryThreshold;
                    case SHORT_ENTRY -> choppyShortEntryThreshold;
                    case LONG_EXIT -> choppyLongExitThreshold;
                    case SHORT_EXIT -> choppyShortExitThreshold;
                };
            };
        }

        public String summary() {
            return "base{le=" + fmt(baseLongEntryThreshold) + " se=" + fmt(baseShortEntryThreshold) + " lx=" + fmt(baseLongExitThreshold) + " sx=" + fmt(baseShortExitThreshold)
                + " regime=" + fmt(regimeClassifierThreshold) + "}"
                + " open30{le=" + fmt(open30LongEntryThreshold) + " se=" + fmt(open30ShortEntryThreshold) + " lx=" + fmt(open30LongExitThreshold) + " sx=" + fmt(open30ShortExitThreshold) + "}"
                + " choppy{le=" + fmt(choppyLongEntryThreshold) + " se=" + fmt(choppyShortEntryThreshold) + " lx=" + fmt(choppyLongExitThreshold) + " sx=" + fmt(choppyShortExitThreshold) + "}"
                + " trend{le=" + fmt(trendLongEntryThreshold) + " se=" + fmt(trendShortEntryThreshold) + " lx=" + fmt(trendLongExitThreshold) + " sx=" + fmt(trendShortExitThreshold) + "}"
                + " volatile{le=" + fmt(volatileLongEntryThreshold) + " se=" + fmt(volatileShortEntryThreshold) + " lx=" + fmt(volatileLongExitThreshold) + " sx=" + fmt(volatileShortExitThreshold) + "}";
        }

        private static double normalize(double threshold, double fallback) {
            if (Double.isNaN(threshold) || Double.isInfinite(threshold)) {
                return fallback;
            }
            return Math.max(0.0, Math.min(1.0, threshold));
        }

        private static String fmt(double value) {
            return String.format(Locale.US, "%.4f", value);
        }
    }

    private static final class LazyAiPredictor {
        private final String modelName;
        private final String modelDir;
        private final String fallbackLog;
        private volatile AiPredictor delegate;
        private volatile boolean attemptedLoad = false;

        private LazyAiPredictor(String modelName, String modelDir, String fallbackLog) {
            this.modelName = modelName;
            this.modelDir = modelDir;
            this.fallbackLog = fallbackLog == null ? "" : fallbackLog;
        }

        static LazyAiPredictor eager(String modelName, String modelDir) throws Exception {
            LazyAiPredictor predictor = new LazyAiPredictor(modelName, modelDir, "");
            predictor.loadRequired();
            return predictor;
        }

        static LazyAiPredictor lazy(String modelName, String modelDir, String fallbackLog) {
            return new LazyAiPredictor(modelName, modelDir, fallbackLog);
        }

        private synchronized void loadRequired() throws Exception {
            if (delegate != null) {
                return;
            }
            attemptedLoad = true;
            delegate = new AiPredictor(modelName, modelDir);
        }

        private AiPredictor ensureLoaded() {
            AiPredictor current = delegate;
            if (current != null) {
                return current;
            }
            synchronized (this) {
                current = delegate;
                if (current != null) {
                    return current;
                }
                if (attemptedLoad) {
                    return null;
                }
                attemptedLoad = true;
                try {
                    delegate = new AiPredictor(modelName, modelDir);
                    return delegate;
                } catch (Exception e) {
                    log.info(">>> [FLOW][INFO][AI.INIT] {} model={} reason={}", fallbackLog, modelName, e.getMessage());
                    return null;
                }
            }
        }

        boolean isAvailable() {
            return ensureLoaded() != null;
        }

        double predictProbability(float[] features) {
            AiPredictor current = ensureLoaded();
            return current == null ? 0.0 : current.predictProbability(features);
        }

        AiPredictor.MultiClassPredictionOutcome predictMultiClassOutcome(float[] features, int fallbackLabel) {
            AiPredictor current = ensureLoaded();
            return current == null
                ? new AiPredictor.MultiClassPredictionOutcome(fallbackLabel, 0.0, Map.of())
                : current.predictMultiClassOutcome(features, fallbackLabel);
        }

        int expectedFeatureCountOr(int fallbackFeatureCount) {
            AiPredictor current = ensureLoaded();
            return current == null ? fallbackFeatureCount : current.getExpectedFeatureCount();
        }

        void close() {
            AiPredictor current = delegate;
            delegate = null;
            if (current != null) {
                current.close();
            }
        }
    }

    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final DateTimeFormatter MARKET_TS_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV");
    private static final Logger log = LoggerFactory.getLogger(PingPongStrategy.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final boolean USE_RSI_PRE_GATES = Boolean.parseBoolean(System.getProperty("strategy.useRsiPreGate", "false"));
    private static final double RSI_LONG_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiLongExitThreshold", "50.0"));
    private static final double RSI_SHORT_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiShortExitThreshold", "50.0"));
    private static final double RSI_LONG_ENTRY_OPEN_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiLongEntryOpenThreshold", "34.0"));
    private static final double RSI_LONG_ENTRY_REGULAR_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiLongEntryRegularThreshold", "40.0"));
    private static final double RSI_SHORT_ENTRY_OPEN_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiShortEntryOpenThreshold", "66.0"));
    private static final double RSI_SHORT_ENTRY_REGULAR_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiShortEntryRegularThreshold", "60.0"));
    private static final double DEFAULT_LONG_ENTRY_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.longEntryThreshold", "0.68"));
    private static final double DEFAULT_SHORT_ENTRY_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.shortEntryThreshold", "0.63"));
    private static final double DEFAULT_LONG_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.longExitThreshold", "0.58"));
    private static final double DEFAULT_SHORT_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.shortExitThreshold", "0.60"));
    private static final double DEFAULT_REGIME_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.regimeThreshold", "0.50"));
    private static final boolean UPGRADED_MODEL_ROUTE_REQUIRED = Boolean.parseBoolean(System.getProperty("strategy.model.upgradedRouteRequired", "false"));
    private static final boolean LEGACY_30S_EXIT_ENABLED = Boolean.parseBoolean(System.getProperty("strategy.exit.legacy30sEnabled", "false"));
    private static final boolean LIFECYCLE_EXIT_ENABLED = Boolean.parseBoolean(System.getProperty("strategy.exit.lifecycleEnabled", "false"));
    private static final boolean LIFECYCLE_DIAGNOSTIC_FALLBACK = Boolean.parseBoolean(System.getProperty("strategy.lifecycle.diagnosticFallback", "false"));
    private static final boolean MICRO_ENTRY_ENABLED = Boolean.parseBoolean(System.getProperty("strategy.micro.entryEnabled", "false"));
    private static final boolean MICRO_EXIT_GUARD_ENABLED = Boolean.parseBoolean(System.getProperty("strategy.micro.exitGuardEnabled", "false"));
    private static final boolean POSTHOC_CALIBRATION_ENABLED = Boolean.parseBoolean(System.getProperty("strategy.calibration.posthocEnabled", "true"));
    private static final String LIFECYCLE_MODEL_DIR = System.getProperty("strategy.lifecycle.modelDir", "").trim();
    private static final String MICRO_MODEL_DIR = System.getProperty("strategy.micro.modelDir", "").trim();
    private static final double LIFECYCLE_LONG_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.exit.lifecycle.longThreshold", "0.60"));
    private static final double LIFECYCLE_SHORT_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.exit.lifecycle.shortThreshold", "0.60"));
    private static final double MICRO_LONG_ENTRY_THRESHOLD = Double.parseDouble(System.getProperty("strategy.micro.longEntryThreshold", "0.58"));
    private static final double MICRO_SHORT_ENTRY_THRESHOLD = Double.parseDouble(System.getProperty("strategy.micro.shortEntryThreshold", "0.58"));
    private static final double MICRO_LONG_EXIT_GUARD_THRESHOLD = Double.parseDouble(System.getProperty("strategy.micro.longExitGuardThreshold", "0.70"));
    private static final double MICRO_SHORT_EXIT_GUARD_THRESHOLD = Double.parseDouble(System.getProperty("strategy.micro.shortExitGuardThreshold", "0.70"));
    private static final long MICRO_ARM_TTL_SECONDS = Long.parseLong(System.getProperty("strategy.micro.armTtlSeconds", "30"));
    private static final double LIFECYCLE_ENTRY_RISK_PCT = Double.parseDouble(System.getProperty("strategy.exit.lifecycle.entryRiskPct", "0.0025"));
    private static final double LIFECYCLE_ENTRY_PROFIT_PCT = Double.parseDouble(System.getProperty("strategy.exit.lifecycle.entryProfitPct", "0.0035"));
    private static final int LIFECYCLE_HORIZON_30S = Integer.parseInt(System.getProperty("strategy.exit.lifecycle.horizon30s", "20"));
    private static final long DEFAULT_POST_HARD_STOP_ENTRY_COOLDOWN_MS = 300_000L;
    private static final int DEFAULT_MAX_HARD_STOPS_PER_DAY = 3;
    private static final int OPEN30_MIN_BARS = Integer.parseInt(System.getProperty("strategy.ai.open30MinBars", "12"));
    private static final int REGULAR_MIN_BARS = Integer.parseInt(System.getProperty("strategy.ai.regularMinBars", "60"));
    private static final int AI_DIAGNOSTIC_TOP_SETUP_EVENTS = Integer.parseInt(System.getProperty("strategy.ai.diagnosticTopSetups", "5"));
    private static final double AI_DIAGNOSTIC_NEAR_MISS_MARGIN = Double.parseDouble(System.getProperty("strategy.ai.nearMissMargin", "0.05"));
    private static final long AI_BUCKET_SECONDS = 30L;
    private static final long MICRO_BUCKET_SECONDS = 5L;
    private static final List<String> LEGACY_FEATURE_COLUMNS = List.of(
        "f_dist_vwap", "f_bb_lower_dist", "f_bb_upper_dist", "f_macd_diff",
        "f_body_size", "f_lower_wick", "f_upper_wick", "f_atr_norm",
        "f_dist_sma", "f_dist_high", "f_dist_low", "f_rsi", "f_gap_from_prev_close",
        "f_time_of_day", "f_dist_swing_high", "f_dist_swing_low", "f_is_new_high",
        "f_is_new_low", "f_dist_whole_num", "f_is_green", "f_green_streak",
        "f_red_streak", "f_put_call_ratio", "f_vol_ask_ratio", "f_vol_bid_ratio"
    );
    private static final List<String> BASE_FEATURE_COLUMNS = List.of(
        "f_dist_vwap", "f_bb_lower_dist", "f_bb_upper_dist", "f_macd_diff",
        "f_body_size", "f_lower_wick", "f_upper_wick", "f_atr_norm",
        "f_dist_sma", "f_dist_high", "f_dist_low", "f_rsi", "f_gap_from_prev_close",
        "f_time_of_day", "f_dist_swing_high", "f_dist_swing_low", "f_is_new_high",
        "f_is_new_low", "f_dist_whole_num", "f_is_green", "f_green_streak",
        "f_red_streak", "f_put_call_ratio", "f_vol_ask_ratio", "f_vol_bid_ratio",
        "f_rel_volume_30s", "f_realized_vol_20", "f_realized_vol_z",
        "f_dist_or_high_atr", "f_dist_or_low_atr"
    );
    private static final List<String> EXTENDED_FEATURE_COLUMNS = List.of(
        "f_dist_vwap", "f_bb_lower_dist", "f_bb_upper_dist", "f_macd_diff",
        "f_body_size", "f_lower_wick", "f_upper_wick", "f_atr_norm",
        "f_dist_sma", "f_dist_high", "f_dist_low", "f_rsi", "f_gap_from_prev_close",
        "f_time_of_day", "f_dist_swing_high", "f_dist_swing_low", "f_is_new_high",
        "f_is_new_low", "f_dist_whole_num", "f_is_green", "f_green_streak",
        "f_red_streak", "f_put_call_ratio", "f_vol_ask_ratio", "f_vol_bid_ratio",
        "f_rel_volume_30s", "f_realized_vol_20", "f_realized_vol_z",
        "f_dist_or_high_atr", "f_dist_or_low_atr",
        "f_spread_pct", "f_spread_z", "f_l1_imbalance", "f_signed_flow_30s"
    );
    private static final List<String> NEWS_BAR_FEATURE_COLUMNS = List.of(
        "f_news_intensity_60s",
        "f_news_intensity_300s",
        "f_news_freshness",
        "f_news_provider_breadth",
        "f_news_confidence",
        "f_news_sentiment_level",
        "f_news_sentiment_shift",
        "f_news_sentiment_dispersion",
        "f_news_coverage",
        "f_news_relevance",
        "f_news_surprise",
        "f_news_directional_impulse",
        "f_news_event_earnings",
        "f_news_event_analyst",
        "f_news_event_legal",
        "f_news_event_product",
        "f_news_event_macro",
        "f_news_model_relevance",
        "f_news_model_impact",
        "f_news_model_novelty",
        "f_news_directional_conviction",
        "f_news_alpha_bias_60s",
        "f_news_alpha_bias_300s",
        "f_news_alpha_ret_60s_norm",
        "f_news_alpha_ret_300s_norm",
        "f_news_vol_shock",
        "f_news_event_strength"
    );
    private static final List<String> REGIME_PROBABILITY_FEATURE_COLUMNS = List.of(
        "f_regime_prob_choppy",
        "f_regime_prob_trend",
        "f_regime_prob_volatile",
        "f_regime_prob_entropy"
    );
    private static final List<String> META_PRODUCER_FEATURE_COLUMNS = EnhancedLiveFeatureProducer.META_PRODUCER_FEATURE_COLUMNS;
    private static final Set<String> REGIME_EXCLUDED_FEATURE_COLUMNS = Set.of(
        "f_atr_norm",
        "f_realized_vol_20",
        "f_realized_vol_z",
        "f_spread_z",
        "f_rsi",
        "f_macd_diff",
        "f_dist_sma"
    );
    private static final int LEGACY_LIVE_FEATURE_COUNT = LEGACY_FEATURE_COLUMNS.size();
    private static final int BASE_LIVE_FEATURE_COUNT = BASE_FEATURE_COLUMNS.size();
    private static final int EXTENDED_LIVE_FEATURE_COUNT = EXTENDED_FEATURE_COLUMNS.size();
    private static final List<String> BASE_PLUS_NEWS_FEATURE_COLUMNS = concatFeatureColumns(BASE_FEATURE_COLUMNS, NEWS_BAR_FEATURE_COLUMNS);
    private static final List<String> BASE_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS = concatFeatureColumns(BASE_PLUS_NEWS_FEATURE_COLUMNS, REGIME_PROBABILITY_FEATURE_COLUMNS);
    private static final List<String> EXTENDED_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS = concatFeatureColumns(EXTENDED_FEATURE_COLUMNS, NEWS_BAR_FEATURE_COLUMNS, REGIME_PROBABILITY_FEATURE_COLUMNS);
    private static final List<String> BASE_PLUS_NEWS_PLUS_META_PRODUCER_COLUMNS = concatFeatureColumns(BASE_PLUS_NEWS_FEATURE_COLUMNS, META_PRODUCER_FEATURE_COLUMNS);
    private static final List<String> ENHANCED_MAIN_FEATURE_COLUMNS = concatFeatureColumns(BASE_PLUS_NEWS_PLUS_META_PRODUCER_COLUMNS, REGIME_PROBABILITY_FEATURE_COLUMNS);
    private static final List<String> LEGACY_REGIME_FEATURE_COLUMNS = filterRegimeColumns(LEGACY_FEATURE_COLUMNS);
    private static final List<String> BASE_REGIME_FEATURE_COLUMNS = filterRegimeColumns(BASE_FEATURE_COLUMNS);
    private static final List<String> EXTENDED_REGIME_FEATURE_COLUMNS = filterRegimeColumns(EXTENDED_FEATURE_COLUMNS);
    private static final List<String> NEWS_AWARE_REGIME_FEATURE_COLUMNS = filterRegimeColumns(BASE_PLUS_NEWS_FEATURE_COLUMNS);
    private static final List<String> ENHANCED_REGIME_FEATURE_COLUMNS = filterRegimeColumns(BASE_PLUS_NEWS_PLUS_META_PRODUCER_COLUMNS);
    private static final List<String> COMMON_30S_TRAINING_FEATURE_COLUMNS = List.of(
        "f_30s_body_pct",
        "f_30s_is_close_hour",
        "f_30s_is_open_hour",
        "f_30s_lower_wick_pct",
        "f_30s_option_call_delta",
        "f_30s_option_delta_put_call_ratio",
        "f_30s_option_put_call_ratio",
        "f_30s_option_put_delta",
        "f_30s_option_volume_burst",
        "f_30s_range_pct",
        "f_30s_realized_vol_20",
        "f_30s_rel_volume_20",
        "f_30s_ret_1",
        "f_30s_ret_3",
        "f_30s_spread_bps",
        "f_30s_time_of_day",
        "f_30s_upper_wick_pct",
        "f_30s_vwap_dist",
        "f_regime_choppy",
        "f_regime_trend",
        "f_regime_volatile"
    );
    private static final List<String> COMMON_5S_TRAINING_FEATURE_COLUMNS = List.of(
        "f_5s_body_pct",
        "f_5s_is_close_hour",
        "f_5s_is_open_hour",
        "f_5s_lower_wick_pct",
        "f_5s_option_call_delta",
        "f_5s_option_delta_put_call_ratio",
        "f_5s_option_put_call_ratio",
        "f_5s_option_put_delta",
        "f_5s_option_volume_burst",
        "f_5s_range_pct",
        "f_5s_realized_vol_20",
        "f_5s_rel_volume_20",
        "f_5s_ret_1",
        "f_5s_ret_3",
        "f_5s_spread_bps",
        "f_5s_time_of_day",
        "f_5s_upper_wick_pct",
        "f_5s_vwap_dist"
    );
    private static final List<String> LIFECYCLE_FEATURE_COLUMNS = concatFeatureColumns(
        COMMON_30S_TRAINING_FEATURE_COLUMNS,
        List.of(
            "f_entry_score_proxy",
            "f_entry_prob",
            "f_entry_threshold",
            "f_entry_threshold_margin",
            "f_entry_side_long",
            "f_entry_side_short",
            "f_pos_side",
            "f_bars_since_entry",
            "f_unrealized_pnl_r",
            "f_mfe_r",
            "f_mae_r",
            "f_target_remaining_r",
            "f_stop_remaining_r"
        )
    );
    private static final List<String> MICRO_ENTRY_FEATURE_COLUMNS = concatFeatureColumns(
        COMMON_5S_TRAINING_FEATURE_COLUMNS,
        COMMON_30S_TRAINING_FEATURE_COLUMNS,
        List.of("f_setup_score_proxy", "f_setup_prob", "f_setup_threshold", "f_setup_threshold_margin", "f_seconds_since_arm")
    );
    private static final List<String> MICRO_EXIT_GUARD_FEATURE_COLUMNS = concatFeatureColumns(
        COMMON_5S_TRAINING_FEATURE_COLUMNS,
        COMMON_30S_TRAINING_FEATURE_COLUMNS,
        List.of(
            "f_entry_score_proxy",
            "f_entry_prob",
            "f_entry_threshold",
            "f_entry_threshold_margin",
            "f_entry_side_long",
            "f_entry_side_short",
            "f_pos_side",
            "f_bars_since_entry_5s",
            "f_unrealized_pnl_r",
            "f_mfe_r",
            "f_mae_r"
        )
    );

    public record StrategyState(double lastPrice, int tradeCount, boolean enabled, boolean isArmed, boolean isVolatile, double yesterdayClose) {}

    private static List<String> concatFeatureColumns(List<String>... groups) {
        List<String> merged = new ArrayList<>();
        if (groups == null) {
            return merged;
        }
        for (List<String> group : groups) {
            if (group != null && !group.isEmpty()) {
                merged.addAll(group);
            }
        }
        return List.copyOf(merged);
    }

    private static List<String> filterRegimeColumns(List<String> sourceColumns) {
        List<String> filtered = new ArrayList<>();
        for (String column : sourceColumns) {
            if (!REGIME_EXCLUDED_FEATURE_COLUMNS.contains(column)) {
                filtered.add(column);
            }
        }
        return List.copyOf(filtered);
    }

    private final IBKRTrader parent;
    private final String symbol;

    // Core Risk Parameters
    private final int tradeQuantity;
    private final int maxTrades;
    private final int rsiPeriod;
    private final double stopLossPercentage;
    private final double maxDailyDrawdown;
    private final long postHardStopEntryCooldownMs;
    private final int maxHardStopsPerDay;

    // The 4-Model AI Architecture
    private LazyAiPredictor longEntryAi;
    private LazyAiPredictor shortEntryAi;
    private LazyAiPredictor longExitAi;
    private LazyAiPredictor shortExitAi;
    private LazyAiPredictor regimeClassifierAi;
    private LazyAiPredictor choppyLongEntryAi;
    private LazyAiPredictor choppyShortEntryAi;
    private LazyAiPredictor choppyLongExitAi;
    private LazyAiPredictor choppyShortExitAi;
    private LazyAiPredictor trendLongEntryAi;
    private LazyAiPredictor trendShortEntryAi;
    private LazyAiPredictor trendLongExitAi;
    private LazyAiPredictor trendShortExitAi;
    private LazyAiPredictor volatileLongEntryAi;
    private LazyAiPredictor volatileShortEntryAi;
    private LazyAiPredictor volatileLongExitAi;
    private LazyAiPredictor volatileShortExitAi;
    private LazyAiPredictor open30LongEntryAi;
    private LazyAiPredictor open30ShortEntryAi;
    private LazyAiPredictor open30LongExitAi;
    private LazyAiPredictor open30ShortExitAi;
    private LazyAiPredictor longExitLifecycleAi;
    private LazyAiPredictor shortExitLifecycleAi;
    private LazyAiPredictor longMicroEntryAi;
    private LazyAiPredictor shortMicroEntryAi;
    private LazyAiPredictor longMicroExitGuardAi;
    private LazyAiPredictor shortMicroExitGuardAi;
    private final Map<String, ProbabilityCalibrator> upgradedRouteCalibrators = new ConcurrentHashMap<>();
    private final Map<String, Double> upgradedRouteThresholds = new ConcurrentHashMap<>();
    private volatile boolean upgradedModelRouteValid = true;
    private volatile MarketRegime lastDetectedRegime = MarketRegime.CHOPPY;

    // Actor Model Event Queue
    private final LinkedBlockingQueue<StrategyEvent> eventQueue = new LinkedBlockingQueue<>();
    private final Thread eventProcessorThread;
    private volatile boolean isRunning = true;

    // State Management (Single Writer: Actor Thread)
    private volatile double lastPrice = 0.0;
    private volatile int currentPosition = 0;
    private volatile int tradeCount = 0;
    private volatile boolean enabled = true;
    private volatile boolean positionSynced = false;
    private volatile boolean inFlightOrder = false;
    private volatile boolean allowNewEntries = true;
    private volatile boolean circuitBreakerTripped = false;
    private int hardStopExitCount = 0;
    private long lastHardStopExitTimeMs = 0L;
    private double avgEntryPrice = 0.0;
    private volatile double dailyNetPnL = 0.0;
    private volatile double totalNetPnL = 0.0;
    private volatile LocalDateTime currentMarketTime;
    private volatile LifecycleTelemetryListener lifecycleTelemetryListener = LifecycleTelemetryListener.NOOP;

    private long lastOrderSubmitTime = 0;
    private int pendingOrderId = -1;
    private boolean pendingOrderReconcileRequested = false;
    private long lastPendingOrderReconcileLogTime = 0;

    // 5-Second Bar Data
    private double barOpen, barHigh, barLow, barClose;
    private long barVolume;
    private double prevBarClose = 0.0;
    private int barsCount = 0;

    // Real-Time Institutional Indicators
    private double dayHigh = 0.0;
    private double dayLow = 0.0;
    private double cumPv = 0.0;
    private long cumVol = 0;
    private double vwap = 0.0;
    
    private final Deque<Double> bbWindow = new ArrayDeque<>();
    private final Deque<Double> smaWindow = new ArrayDeque<>();
    
    // NEW: Price Action Tracking Windows
    private final Deque<Double> highWindow = new ArrayDeque<>();
    private final Deque<Double> lowWindow = new ArrayDeque<>();
    
    private double avgGain = 0.0;
    private double avgLoss = 0.0;
    
    private double ema12 = 0.0;
    private double ema26 = 0.0;
    private double ema9Signal = 0.0;
    private double macdDiff = 0.0;
    
    private double atr12 = 0.0;  // Changed from atr60 to atr12
    private double yesterdayClose = 0.0;
    private long currentBarVolAsk = 0L;
    private long currentBarVolBid = 0L;
    private float currentPutCallRatio = 1.0f;
    private long latestPutVolume = 0L;
    private long latestCallVolume = 0L;
    private long prevPutVolume = 0L;
    private long prevCallVolume = 0L;
    private double latestBidPrice = 0.0;
    private double latestAskPrice = 0.0;
    private long latestBidSize = 0L;
    private long latestAskSize = 0L;
    private double latestShortableShares = 0.0;
    private boolean optionVolumeWarningLogged = false;
    private int greenStreak = 0;
    private int redStreak = 0;
    private volatile double longEntryProbabilityThreshold = DEFAULT_LONG_ENTRY_THRESHOLD;
    private volatile double shortEntryProbabilityThreshold = DEFAULT_SHORT_ENTRY_THRESHOLD;
    private volatile double longExitProbabilityThreshold = DEFAULT_LONG_EXIT_THRESHOLD;
    private volatile double shortExitProbabilityThreshold = DEFAULT_SHORT_EXIT_THRESHOLD;
    private volatile double regimeProbabilityThreshold = DEFAULT_REGIME_THRESHOLD;
    private volatile AiThresholdConfig aiThresholdConfig = AiThresholdConfig.defaults();

    // Backtest/live no-trade diagnostics. These are intentionally cumulative for the strategy lifetime.
    private long aiDecisionEvaluationCount = 0L;
    private long aiMissingMarketTimeCount = 0L;
    private long aiPreMarketBlockedCount = 0L;
    private long aiMissingPreviousCloseCount = 0L;
    private long aiVarianceBlockedCount = 0L;
    private long aiPositionOpenSkippedCount = 0L;
    private long aiFlatEntryEvaluationCount = 0L;
    private long aiEntryGateOpenCount = 0L;
    private long aiEntryGateClosedCount = 0L;
    private long aiAllowNewEntriesBlockedCount = 0L;
    private long aiMaxTradesBlockedCount = 0L;
    private long aiPositionSyncBlockedCount = 0L;
    private long aiHardStopCooldownBlockedCount = 0L;
    private long aiHardStopBudgetBlockedCount = 0L;
    private long aiBuyQuantityBlockedCount = 0L;
    private long aiSellQuantityBlockedCount = 0L;
    private long aiLongRsiGateBlockedCount = 0L;
    private long aiShortRsiGateBlockedCount = 0L;
    private long aiLongModelUnavailableCount = 0L;
    private long aiShortModelUnavailableCount = 0L;
    private long aiLongEntryModelEvaluationCount = 0L;
    private long aiShortEntryModelEvaluationCount = 0L;
    private long aiLongEntryPassCount = 0L;
    private long aiShortEntryPassCount = 0L;
    private long aiLongNearMissCount = 0L;
    private long aiShortNearMissCount = 0L;
    private double aiMaxLongEntryProbability = Double.NEGATIVE_INFINITY;
    private double aiMaxLongEntryThreshold = Double.NaN;
    private double aiMaxLongEntryMargin = Double.NaN;
    private long aiMaxLongEntryEpoch = 0L;
    private String aiMaxLongEntryTime = "";
    private double aiMaxShortEntryProbability = Double.NEGATIVE_INFINITY;
    private double aiMaxShortEntryThreshold = Double.NaN;
    private double aiMaxShortEntryMargin = Double.NaN;
    private long aiMaxShortEntryEpoch = 0L;
    private String aiMaxShortEntryTime = "";
    private final List<SetupCandidateDiagnostic> aiClosestSetupEvents = new ArrayList<>();

    // Extended features state (safe to keep even if model uses base 23 features).
    private final Map<Integer, Double> minuteVolumeBaseline = new HashMap<>();
    private final Deque<Double> returnWindow20 = new ArrayDeque<>();
    private final Deque<Double> realizedVolWindow100 = new ArrayDeque<>();
    private final Deque<Double> spreadWindow100 = new ArrayDeque<>();
    private final Deque<Double> volumeWindow = new ArrayDeque<>();
    private LocalDate featureSessionDate = null;
    private int openingRangeBarsCount = 0;
    private double openingRangeHigh = 0.0;
    private double openingRangeLow = 0.0;

    // Independent micro-cadence state. This mirrors the Python builder's combined 1s source frame
    // without reading from or mutating the 30-second AI aggregation bucket.
    private static final int SOURCE_BAR_WINDOW_SIZE = 180;
    private static final int MICRO_5S_WINDOW_SIZE = 120;
    private final Deque<MicroBar> sourceBarWindow = new ArrayDeque<>();
    private final Deque<MicroBar> micro5sWindow = new ArrayDeque<>();
    private final Deque<Double> micro5sReturnWindow20 = new ArrayDeque<>();
    private final Deque<Double> micro5sVolumeWindow20 = new ArrayDeque<>();
    private final Deque<Double> micro5sOptionFlowWindow20 = new ArrayDeque<>();
    private long micro5sBucketStartEpoch = -1L;
    private long lastAlignedMicro5sWaitLogTargetEpoch = -1L;
    private double micro5sOpen = 0.0;
    private double micro5sHigh = 0.0;
    private double micro5sLow = Double.MAX_VALUE;
    private double micro5sClose = 0.0;
    private long micro5sVolume = 0L;
    private double micro5sWapSum = 0.0;
    private long microPrevPutVolume = 0L;
    private long microPrevCallVolume = 0L;
    private long latestSourceBarEpoch = 0L;

    // --- 30-SECOND AGGREGATION BUCKET ---
    private long bucketStartEpoch = -1L;
    private long lastFinalizedBucketStartEpoch = -1L;
    private long lastAligned30sWaitLogTargetEpoch = -1L;
    private double bucketOpen = 0.0;
    private double bucketHigh = 0.0;
    private double bucketLow = Double.MAX_VALUE;
    private double bucketClose = 0.0;
    private long bucketVolume = 0L;
    private double bucketWapSum = 0.0;
    private final Deque<Double> training30sReturnWindow20 = new ArrayDeque<>();
    private final Deque<Double> training30sVolumeWindow20 = new ArrayDeque<>();
    private final Deque<Double> training30sOptionFlowWindow20 = new ArrayDeque<>();
    private Map<String, Float> lastTraining30sFeatureValues = new HashMap<>();
    private double lastTraining30sClose = 0.0;
    private long lastTraining30sEpoch = 0L;
    private long current30sAiDecisionEpoch = 0L;

    private boolean microLongEntryArmed = false;
    private boolean microShortEntryArmed = false;
    private long microArmEpoch = 0L;
    private double armedSetupProbability = 0.0;
    private double armedSetupThreshold = 0.0;
    private double armedSetupThresholdMargin = 0.0;
    private Map<String, Float> armed30sFeatureValues = new HashMap<>();

    private long positionEntryEpoch = 0L;
    private double positionEntryPrice = 0.0;
    private double pendingEntryProbability = 0.0;
    private double pendingEntryThreshold = 0.0;
    private double pendingEntryThresholdMargin = 0.0;
    private double positionEntryProbability = 0.0;
    private double positionEntryThreshold = 0.0;
    private double positionEntryThresholdMargin = 0.0;
    private int positionEntrySide = 0;
    private int barsSincePositionEntry30s = 0;
    private int barsSincePositionEntry5s = 0;
    private double positionMfeR = 0.0;
    private double positionMaeR = 0.0;


    // Latency Tracking
    private ConcurrentHashMap<Integer, Long> orderSentTimes = new ConcurrentHashMap<>();
    private long currentTickArrivalTime = 0;
    private static final int STOP_LATENCY_WINDOW_SIZE = 200;
    private final Deque<Double> stopAckLatencyMsWindow = new ArrayDeque<>();
    private volatile int lastStopQueueDepth = 0;
    private volatile double lastStopAckLatencyMs = 0.0;

    private void recordStopAckLatency(double latencyMs) {
        if (Double.isNaN(latencyMs) || Double.isInfinite(latencyMs) || latencyMs < 0.0) {
            return;
        }
        synchronized (stopAckLatencyMsWindow) {
            stopAckLatencyMsWindow.addLast(latencyMs);
            if (stopAckLatencyMsWindow.size() > STOP_LATENCY_WINDOW_SIZE) {
                stopAckLatencyMsWindow.removeFirst();
            }
        }
    }

    private String stopLatencySummary() {
        synchronized (stopAckLatencyMsWindow) {
            if (stopAckLatencyMsWindow.isEmpty()) {
                return "p50=n/a p95=n/a n=0";
            }

            double[] sorted = stopAckLatencyMsWindow.stream().mapToDouble(Double::doubleValue).toArray();
            Arrays.sort(sorted);
            double p50 = percentileFromSorted(sorted, 0.50);
            double p95 = percentileFromSorted(sorted, 0.95);
            return "p50=" + String.format("%.3f", p50)
                + " p95=" + String.format("%.3f", p95)
                + " n=" + sorted.length;
        }
    }

    private double percentileFromSorted(double[] sorted, double percentile) {
        if (sorted.length == 0) {
            return 0.0;
        }
        int idx = (int) Math.ceil(percentile * sorted.length) - 1;
        idx = Math.max(0, Math.min(idx, sorted.length - 1));
        return sorted[idx];
    }

    private double latencyPercentile(double percentile) {
        synchronized (stopAckLatencyMsWindow) {
            if (stopAckLatencyMsWindow.isEmpty()) {
                return 0.0;
            }
            double[] sorted = stopAckLatencyMsWindow.stream().mapToDouble(Double::doubleValue).toArray();
            Arrays.sort(sorted);
            return percentileFromSorted(sorted, percentile);
        }
    }

    public int getLastStopQueueDepth() {
        return lastStopQueueDepth;
    }

    public double getLastStopAckLatencyMs() {
        return lastStopAckLatencyMs;
    }

    public double getStopAckLatencyP50Ms() {
        return latencyPercentile(0.50);
    }

    public double getStopAckLatencyP95Ms() {
        return latencyPercentile(0.95);
    }

    public int getStopAckLatencySampleCount() {
        synchronized (stopAckLatencyMsWindow) {
            return stopAckLatencyMsWindow.size();
        }
    }

    public void hotloadWarmupData() {
        String dateSuffix = LocalDate.now(MARKET_ZONE).format(DateTimeFormatter.BASIC_ISO_DATE);
        Path warmupPath = Paths.get("..", this.symbol, this.symbol + "_5s_warmup_" + dateSuffix + ".csv").toAbsolutePath().normalize();

        java.io.File file = warmupPath.toFile();
        if (!file.exists()) {
            flowInfo("WARMUP", "No warmup file found at: " + warmupPath + ". Waiting for live bars.");
            return;
        }
        flowData("WARMUP", "source=" + warmupPath);
        try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(file))) {
            String line;
            java.util.List<String> lines = new java.util.ArrayList<>();
            br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
            int barsNeeded = OPEN30_MIN_BARS * 6;
            int startIdx = Math.max(0, lines.size() - barsNeeded);
            flowInfo("WARMUP", "Hot-loading " + (lines.size() - startIdx) + " warmup bars from Harvester...");

            java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV");

            for (int i = startIdx; i < lines.size(); i++) {
                String[] parts = lines.get(i).split(",");
                if (parts.length >= 9) {
                    String ts = parts[0] == null ? "" : parts[0].trim();
                    if (ts.endsWith("America/Edmonton")) {
                        ts = ts.replace("America/Edmonton", "America/New_York");
                    }
                    java.time.ZonedDateTime zdt = java.time.ZonedDateTime.parse(ts, formatter);
                    long epoch = zdt.toEpochSecond();

                    double o = Double.parseDouble(parts[1]);
                    double h = Double.parseDouble(parts[2]);
                    double l = Double.parseDouble(parts[3]);
                    double c = Double.parseDouble(parts[4]);
                    long v = (long) Double.parseDouble(parts[5]);
                    double wap = Double.parseDouble(parts[6]);
                    
                    on5SecondBar(epoch, o, h, l, c, v, wap);
                }
            }
            flowInfo("WARMUP", "Warmup complete. Strategy Armed.");
        } catch (Exception e) {
            log.error(">>> [FLOW][ERROR][WARMUP] Failed to hot-load: {}", e.getMessage(), e);
        }
    }

    public PingPongStrategy(IBKRTrader parent, String symbol, double gapPercentage, int tradeQuantity, int maxTrades,
                            boolean autoRegimeEnabled, int regimeWindowTicks, int rsiPeriod, double reversalPercentage,
                            double stopLossPercentage, double maxDailyDrawdown,
                            double minDirectionalMove, double trendStrengthThreshold) {
        this(parent, symbol, gapPercentage, tradeQuantity, maxTrades, autoRegimeEnabled, regimeWindowTicks, rsiPeriod,
            reversalPercentage, stopLossPercentage, maxDailyDrawdown, minDirectionalMove, trendStrengthThreshold, null);
    }

    public PingPongStrategy(IBKRTrader parent, String symbol, double gapPercentage, int tradeQuantity, int maxTrades,
                            boolean autoRegimeEnabled, int regimeWindowTicks, int rsiPeriod, double reversalPercentage,
                            double stopLossPercentage, double maxDailyDrawdown,
                            double minDirectionalMove, double trendStrengthThreshold, String modelDir) {
        this(parent, symbol, gapPercentage, tradeQuantity, maxTrades, autoRegimeEnabled, regimeWindowTicks, rsiPeriod,
            reversalPercentage, stopLossPercentage, maxDailyDrawdown, minDirectionalMove, trendStrengthThreshold, modelDir,
            DEFAULT_POST_HARD_STOP_ENTRY_COOLDOWN_MS, DEFAULT_MAX_HARD_STOPS_PER_DAY);
    }

    public PingPongStrategy(IBKRTrader parent, String symbol, double gapPercentage, int tradeQuantity, int maxTrades,
                            boolean autoRegimeEnabled, int regimeWindowTicks, int rsiPeriod, double reversalPercentage,
                            double stopLossPercentage, double maxDailyDrawdown,
                            double minDirectionalMove, double trendStrengthThreshold, String modelDir,
                            long postHardStopEntryCooldownMs, int maxHardStopsPerDay) {
        this.parent = parent;
        this.symbol = symbol;
        this.tradeQuantity = tradeQuantity;
        this.maxTrades = maxTrades;
        this.rsiPeriod = Math.max(2, rsiPeriod);
        this.stopLossPercentage = Math.max(0.0001, stopLossPercentage);
        this.maxDailyDrawdown = Math.max(1.0, maxDailyDrawdown);
        this.postHardStopEntryCooldownMs = Math.max(0L, postHardStopEntryCooldownMs);
        this.maxHardStopsPerDay = Math.max(1, maxHardStopsPerDay);

        // Load entry/regime models. Legacy generic 30s exits are not part of the upgraded exit route.
        try {
            this.longEntryAi = LazyAiPredictor.eager("long_entry.onnx", modelDir);
        } catch (Exception e) {
            flowError("AI.INIT", "Failed to load long_entry.onnx. Trading disabled. " + e.getMessage());
            this.enabled = false;
        }

        try {
            this.shortEntryAi = LazyAiPredictor.eager("short_entry.onnx", modelDir);
        } catch (Exception e) {
            flowError("AI.INIT", "Failed to load short_entry.onnx. Short entries disabled.");
            this.shortEntryAi = null;
        }

        boolean loadLegacy30sExitModels = LEGACY_30S_EXIT_ENABLED && !LIFECYCLE_EXIT_ENABLED;
        if (loadLegacy30sExitModels) {
            try {
                this.longExitAi = LazyAiPredictor.eager("long_exit.onnx", modelDir);
            } catch (Exception e) {
                flowError("AI.INIT", "Failed to load legacy long_exit.onnx. Longs will rely on hard stop-loss.");
                this.longExitAi = null;
            }

            try {
                this.shortExitAi = LazyAiPredictor.eager("short_exit.onnx", modelDir);
            } catch (Exception e) {
                flowError("AI.INIT", "Failed to load legacy short_exit.onnx. Shorts will rely on hard stop-loss.");
                this.shortExitAi = null;
            }
        } else {
            this.longExitAi = null;
            this.shortExitAi = null;
            flowInfo(
                "AI.INIT",
                "Legacy 30s exit models disabled symbol=" + symbol
                    + " lifecycleEnabled=" + LIFECYCLE_EXIT_ENABLED
                    + " legacy30sExitEnabled=" + LEGACY_30S_EXIT_ENABLED
            );
        }

        this.regimeClassifierAi = tryLoadOptionalModel("regime_classifier.onnx", modelDir, "Market regime classifier unavailable. Falling back to CHOPPY.");
        this.choppyLongEntryAi = tryLoadOptionalModel("choppy_long_entry.onnx", modelDir, "Choppy long-entry model unavailable. Using base/default model.");
        this.choppyShortEntryAi = tryLoadOptionalModel("choppy_short_entry.onnx", modelDir, "Choppy short-entry model unavailable. Using base/default model.");
        this.choppyLongExitAi = loadLegacy30sExitModels ? tryLoadOptionalModel("choppy_long_exit.onnx", modelDir, "Choppy legacy long-exit model unavailable. Using base model.") : null;
        this.choppyShortExitAi = loadLegacy30sExitModels ? tryLoadOptionalModel("choppy_short_exit.onnx", modelDir, "Choppy legacy short-exit model unavailable. Using base model.") : null;
        this.trendLongEntryAi = tryLoadOptionalModel("trend_long_entry.onnx", modelDir, "Trend long-entry model unavailable. Falling back to CHOPPY/default model.");
        this.trendShortEntryAi = tryLoadOptionalModel("trend_short_entry.onnx", modelDir, "Trend short-entry model unavailable. Falling back to CHOPPY/default model.");
        this.trendLongExitAi = loadLegacy30sExitModels ? tryLoadOptionalModel("trend_long_exit.onnx", modelDir, "Trend legacy long-exit model unavailable. Using base model.") : null;
        this.trendShortExitAi = loadLegacy30sExitModels ? tryLoadOptionalModel("trend_short_exit.onnx", modelDir, "Trend legacy short-exit model unavailable. Using base model.") : null;
        this.volatileLongEntryAi = tryLoadOptionalModel("volatile_long_entry.onnx", modelDir, "Volatile long-entry model unavailable. Falling back to CHOPPY/default model.");
        this.volatileShortEntryAi = tryLoadOptionalModel("volatile_short_entry.onnx", modelDir, "Volatile short-entry model unavailable. Falling back to CHOPPY/default model.");
        this.volatileLongExitAi = loadLegacy30sExitModels ? tryLoadOptionalModel("volatile_long_exit.onnx", modelDir, "Volatile legacy long-exit model unavailable. Using base model.") : null;
        this.volatileShortExitAi = loadLegacy30sExitModels ? tryLoadOptionalModel("volatile_short_exit.onnx", modelDir, "Volatile legacy short-exit model unavailable. Using base model.") : null;
        this.open30LongEntryAi = tryLoadOptionalModel("open30_long_entry.onnx", modelDir, "Open30 long-entry model unavailable. Using regime/base model.");
        this.open30ShortEntryAi = tryLoadOptionalModel("open30_short_entry.onnx", modelDir, "Open30 short-entry model unavailable. Using regime/base model.");
        this.open30LongExitAi = loadLegacy30sExitModels ? tryLoadOptionalModel("open30_long_exit.onnx", modelDir, "Open30 legacy long-exit model unavailable. Using regime/base model.") : null;
        this.open30ShortExitAi = loadLegacy30sExitModels ? tryLoadOptionalModel("open30_short_exit.onnx", modelDir, "Open30 legacy short-exit model unavailable. Using regime/base model.") : null;

        String lifecycleModelDir = LIFECYCLE_MODEL_DIR.isEmpty() ? modelDir : LIFECYCLE_MODEL_DIR;
        String microModelDir = MICRO_MODEL_DIR.isEmpty() ? lifecycleModelDir : MICRO_MODEL_DIR;
        this.longExitLifecycleAi = loadUpgradedRouteModel("long_exit_lifecycle.onnx", lifecycleModelDir, LIFECYCLE_EXIT_ENABLED, "lifecycle long exit");
        this.shortExitLifecycleAi = loadUpgradedRouteModel("short_exit_lifecycle.onnx", lifecycleModelDir, LIFECYCLE_EXIT_ENABLED, "lifecycle short exit");
        this.longMicroEntryAi = loadUpgradedRouteModel("long_micro_entry_5s.onnx", microModelDir, MICRO_ENTRY_ENABLED, "5s long micro entry");
        this.shortMicroEntryAi = loadUpgradedRouteModel("short_micro_entry_5s.onnx", microModelDir, MICRO_ENTRY_ENABLED, "5s short micro entry");
        this.longMicroExitGuardAi = loadUpgradedRouteModel("long_micro_exit_guard_5s.onnx", microModelDir, MICRO_EXIT_GUARD_ENABLED, "5s long micro exit guard");
        this.shortMicroExitGuardAi = loadUpgradedRouteModel("short_micro_exit_guard_5s.onnx", microModelDir, MICRO_EXIT_GUARD_ENABLED, "5s short micro exit guard");
        validateUpgradedRouteManifest(lifecycleModelDir, microModelDir);

        if (LIFECYCLE_EXIT_ENABLED && (this.longExitLifecycleAi == null || this.shortExitLifecycleAi == null)) {
            if (LIFECYCLE_DIAGNOSTIC_FALLBACK) {
                flowWarn("AI.ROUTE", "WARNING lifecycle exit enabled but lifecycle model is missing; explicit diagnostic hard-risk-only fallback active symbol=" + symbol + " resultsNotPromotable=true");
            } else {
                invalidateUpgradedRoute("missing lifecycle exit model while strategy.exit.lifecycleEnabled=true and strategy.lifecycle.diagnosticFallback=false");
            }
        }

        if (lifecycleMicroRouteActive() && !upgradedModelRouteValid) {
            this.allowNewEntries = false;
            flowError("AI.ROUTE", "Upgraded lifecycle/micro route invalid; new entries disabled for symbol=" + symbol);
        }

        if (UPGRADED_MODEL_ROUTE_REQUIRED && !upgradedModelRouteValid) {
            this.enabled = false;
            flowError("AI.ROUTE", "Upgraded model route invalid; trading disabled for symbol=" + symbol);
        }

        this.eventProcessorThread = new Thread(this::processEvents);
        this.eventProcessorThread.setName("Strategy-Actor-Thread-" + symbol);
        this.eventProcessorThread.start();

        flowData(
            "AI.CONFIG",
            "symbol=" + symbol
                + " thresholds longEntry=" + formatProb(longEntryProbabilityThreshold)
                + " shortEntry=" + formatProb(shortEntryProbabilityThreshold)
                + " longExit=" + formatProb(longExitProbabilityThreshold)
                + " shortExit=" + formatProb(shortExitProbabilityThreshold)
                + " regime=" + formatProb(regimeProbabilityThreshold)
        );
        
        hotloadWarmupData();
    }

    private LazyAiPredictor tryLoadOptionalModel(String modelName, String modelDir, String fallbackLog) {
        return LazyAiPredictor.lazy(modelName, modelDir, fallbackLog);
    }

    private LazyAiPredictor loadUpgradedRouteModel(String modelName, String modelDir, boolean activeMode, String routeName) {
        if (!activeMode && !UPGRADED_MODEL_ROUTE_REQUIRED) {
            return null;
        }
        try {
            LazyAiPredictor predictor = LazyAiPredictor.eager(modelName, modelDir);
            flowInfo("AI.ROUTE", "Loaded upgraded route model symbol=" + symbol + " route=" + routeName + " model=" + modelName);
            return predictor;
        } catch (Exception exception) {
            boolean lifecycleModel = routeName.startsWith("lifecycle ");
            boolean diagnosticLifecycleFallback = lifecycleModel && activeMode && LIFECYCLE_DIAGNOSTIC_FALLBACK && !UPGRADED_MODEL_ROUTE_REQUIRED;
            if (diagnosticLifecycleFallback) {
                flowWarn("AI.ROUTE", "Lifecycle model unavailable under explicit diagnostic fallback symbol=" + symbol + " route=" + routeName + " model=" + modelName + " reason=" + exception.getMessage());
            } else {
                upgradedModelRouteValid = false;
                flowError("AI.ROUTE", "Missing required upgraded route model symbol=" + symbol + " route=" + routeName + " model=" + modelName + " reason=" + exception.getMessage());
            }
            return null;
        }
    }

    private boolean lifecycleMicroRouteActive() {
        return UPGRADED_MODEL_ROUTE_REQUIRED || LIFECYCLE_EXIT_ENABLED || MICRO_ENTRY_ENABLED || MICRO_EXIT_GUARD_ENABLED;
    }

    private void validateUpgradedRouteManifest(String lifecycleModelDir, String microModelDir) {
        if (!lifecycleMicroRouteActive()) {
            return;
        }
        Map<String, List<String>> expectedSchemas = expectedUpgradedRouteSchemas();
        if (expectedSchemas.isEmpty()) {
            return;
        }
        if (sameModelDir(lifecycleModelDir, microModelDir)) {
            validateUpgradedRouteManifestRows(lifecycleModelDir, expectedSchemas);
            return;
        }
        Map<String, List<String>> lifecycleSchemas = new LinkedHashMap<>();
        Map<String, List<String>> microSchemas = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> expected : expectedSchemas.entrySet()) {
            if (expected.getKey().contains("Lifecycle")) {
                lifecycleSchemas.put(expected.getKey(), expected.getValue());
            } else {
                microSchemas.put(expected.getKey(), expected.getValue());
            }
        }
        validateUpgradedRouteManifestRows(lifecycleModelDir, lifecycleSchemas);
        if (upgradedModelRouteValid) {
            validateUpgradedRouteManifestRows(microModelDir, microSchemas);
        }
    }

    private boolean sameModelDir(String left, String right) {
        if (left == null || right == null) {
            return left == right;
        }
        return new File(left).getAbsoluteFile().equals(new File(right).getAbsoluteFile());
    }

    private void validateUpgradedRouteManifestRows(String modelDir, Map<String, List<String>> expectedSchemas) {
        if (expectedSchemas.isEmpty()) {
            return;
        }
        if (modelDir == null || modelDir.isBlank()) {
            invalidateUpgradedRoute("missing lifecycle/micro model directory for route manifest validation");
            return;
        }
        File manifest = new File(modelDir, "lifecycle_micro_route_manifest.json");
        if (!manifest.isFile()) {
            invalidateUpgradedRoute("missing lifecycle_micro_route_manifest.json under " + modelDir);
            return;
        }
        try {
            List<Map<String, Object>> rows = JSON_MAPPER.readValue(manifest, new TypeReference<List<Map<String, Object>>>() {});
            Map<String, Map<String, Object>> rowByModel = new HashMap<>();
            for (Map<String, Object> row : rows) {
                Object modelName = row.get("model");
                if (modelName instanceof String name && !name.isBlank()) {
                    rowByModel.put(name, row);
                }
            }
            boolean anyPosthocEnabled = rowByModel.values().stream().anyMatch(this::routePosthocEnabled);
            Map<String, ProbabilityCalibrator> selectedCalibrators = anyPosthocEnabled
                ? loadSelectedPosthocCalibrators(new File(modelDir, "posthoc_calibrators.json"))
                : Map.of();
            if (!upgradedModelRouteValid) {
                return;
            }
            for (Map.Entry<String, List<String>> expected : expectedSchemas.entrySet()) {
                String modelName = expected.getKey();
                Map<String, Object> row = rowByModel.get(modelName);
                validateManifestSchemaRow(row, modelName, expected.getValue());
                if (!upgradedModelRouteValid) {
                    return;
                }
                validateAndRecordRouteThreshold(row, modelName);
                if (!upgradedModelRouteValid) {
                    return;
                }
                validateAndRecordPosthocCalibrator(row, modelName, selectedCalibrators);
                if (!upgradedModelRouteValid) {
                    return;
                }
            }
            flowInfo("AI.ROUTE", "Validated lifecycle/micro route manifest schema symbol=" + symbol + " manifest=" + manifest.getAbsolutePath());
        } catch (Exception exception) {
            invalidateUpgradedRoute("failed to read/validate lifecycle_micro_route_manifest.json reason=" + exception.getMessage());
        }
    }

    private Map<String, ProbabilityCalibrator> loadSelectedPosthocCalibrators(File artifact) {
        if (!POSTHOC_CALIBRATION_ENABLED) {
            invalidateUpgradedRoute("route manifest requires post-hoc calibration but strategy.calibration.posthocEnabled=false");
            return Map.of();
        }
        if (artifact == null || !artifact.isFile()) {
            invalidateUpgradedRoute("route manifest requires post-hoc calibration but missing posthoc_calibrators.json under " + (artifact == null ? "<unknown>" : artifact.getParent()));
            return Map.of();
        }
        try {
            Map<String, Object> export = JSON_MAPPER.readValue(artifact, new TypeReference<Map<String, Object>>() {});
            Object rawModels = export.get("models");
            if (!(rawModels instanceof List<?> models)) {
                invalidateUpgradedRoute("posthoc_calibrators.json missing models list path=" + artifact.getAbsolutePath());
                return Map.of();
            }
            Map<String, ProbabilityCalibrator> selected = new HashMap<>();
            for (Object item : models) {
                if (!(item instanceof Map<?, ?> rawRow)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> row = (Map<String, Object>) rawRow;
                ProbabilityCalibrator calibrator = ProbabilityCalibrator.fromPosthocExportRow(row);
                if (!calibrator.selected()) {
                    continue;
                }
                ProbabilityCalibrator existing = selected.putIfAbsent(calibrator.modelName(), calibrator);
                if (existing != null) {
                    invalidateUpgradedRoute("duplicate selected posthoc calibrator model=" + calibrator.modelName() + " path=" + artifact.getAbsolutePath());
                    return Map.of();
                }
            }
            flowInfo("AI.CALIBRATION", "Loaded selected posthoc calibrators symbol=" + symbol + " count=" + selected.size() + " artifact=" + artifact.getAbsolutePath());
            return selected;
        } catch (Exception exception) {
            invalidateUpgradedRoute("failed to read/validate posthoc_calibrators.json reason=" + exception.getMessage());
            return Map.of();
        }
    }

    private void validateAndRecordRouteThreshold(Map<String, Object> row, String modelName) {
        double routeThreshold = finiteProbability(row == null ? null : row.get("threshold"), Double.NaN);
        if (!Double.isFinite(routeThreshold)) {
            invalidateUpgradedRoute("route manifest model=" + modelName + " missing finite probability threshold");
            return;
        }
        double runtimeThreshold = routeThreshold;
        Map<String, Object> posthoc = routePosthoc(row);
        if (posthoc != null && routePosthocEnabled(row)) {
            Map<String, Object> selectedMetrics = objectMap(posthoc.get("selected_metrics"));
            double posthocThreshold = finiteProbability(selectedMetrics == null ? null : selectedMetrics.get("threshold"), Double.NaN);
            if (Double.isFinite(posthocThreshold)) {
                runtimeThreshold = posthocThreshold;
                if (Math.abs(routeThreshold - posthocThreshold) > 1.0e-9) {
                    flowWarn("AI.CALIBRATION", "Route manifest raw threshold differs from selected posthoc threshold; using selected posthoc threshold symbol=" + symbol + " model=" + modelName + " routeThreshold=" + formatProb(routeThreshold) + " posthocThreshold=" + formatProb(posthocThreshold));
                }
            }
        }
        upgradedRouteThresholds.put(modelName, runtimeThreshold);
    }

    private void validateAndRecordPosthocCalibrator(Map<String, Object> row, String modelName, Map<String, ProbabilityCalibrator> selectedCalibrators) {
        if (!routePosthocEnabled(row)) {
            upgradedRouteCalibrators.remove(modelName);
            return;
        }
        Map<String, Object> posthoc = routePosthoc(row);
        String selectedMethod = optionalString(posthoc == null ? null : posthoc.get("selected_method"));
        if (selectedMethod.isBlank() || !ProbabilityCalibrator.isSupportedMethodName(selectedMethod)) {
            invalidateUpgradedRoute("route manifest model=" + modelName + " has unsupported posthoc selected_method=" + selectedMethod);
            return;
        }
        ProbabilityCalibrator calibrator = selectedCalibrators.get(modelName);
        if (calibrator == null) {
            invalidateUpgradedRoute("route manifest model=" + modelName + " requires selected posthoc calibrator but artifact has none");
            return;
        }
        String normalizedRouteMethod = ProbabilityCalibrator.normalizeMethodName(selectedMethod);
        if (!normalizedRouteMethod.equals(calibrator.methodName())) {
            invalidateUpgradedRoute("posthoc selected_method mismatch model=" + modelName + " route=" + normalizedRouteMethod + " artifact=" + calibrator.methodName());
            return;
        }
        String routeSchemaHash = optionalString(row == null ? null : row.get("feature_schema_sha256"));
        if (!routeSchemaHash.isBlank() && !calibrator.featureSchemaSha256().isBlank() && !routeSchemaHash.equalsIgnoreCase(calibrator.featureSchemaSha256())) {
            invalidateUpgradedRoute("posthoc feature_schema_sha256 mismatch model=" + modelName + " route=" + routeSchemaHash + " artifact=" + calibrator.featureSchemaSha256());
            return;
        }
        upgradedRouteCalibrators.put(modelName, calibrator);
        flowInfo(
            "AI.CALIBRATION",
            "Applied runtime posthoc calibrator symbol=" + symbol
                + " model=" + modelName
                + " method=" + calibrator.methodName()
                + " threshold=" + formatProb(upgradedRouteThreshold(modelName, Double.NaN))
                + " featureSchemaSha256=" + routeSchemaHash
                + " holdoutFingerprintSha256=" + calibrator.holdoutFingerprintSha256()
        );
    }

    private boolean routePosthocEnabled(Map<String, Object> row) {
        Map<String, Object> posthoc = routePosthoc(row);
        return posthoc != null && booleanValue(posthoc.get("enabled"));
    }

    private Map<String, Object> routePosthoc(Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        Map<String, Object> calibration = objectMap(row.get("calibration"));
        return calibration == null ? null : objectMap(calibration.get("posthoc"));
    }

    private Map<String, List<String>> expectedUpgradedRouteSchemas() {
        Map<String, List<String>> expected = new LinkedHashMap<>();
        if (UPGRADED_MODEL_ROUTE_REQUIRED || (LIFECYCLE_EXIT_ENABLED && !LIFECYCLE_DIAGNOSTIC_FALLBACK)) {
            expected.put("longExitLifecycleAi", LIFECYCLE_FEATURE_COLUMNS);
            expected.put("shortExitLifecycleAi", LIFECYCLE_FEATURE_COLUMNS);
        }
        if (UPGRADED_MODEL_ROUTE_REQUIRED || MICRO_ENTRY_ENABLED) {
            expected.put("longMicroEntryAi", MICRO_ENTRY_FEATURE_COLUMNS);
            expected.put("shortMicroEntryAi", MICRO_ENTRY_FEATURE_COLUMNS);
        }
        if (UPGRADED_MODEL_ROUTE_REQUIRED || MICRO_EXIT_GUARD_ENABLED) {
            expected.put("longMicroExitGuardAi", MICRO_EXIT_GUARD_FEATURE_COLUMNS);
            expected.put("shortMicroExitGuardAi", MICRO_EXIT_GUARD_FEATURE_COLUMNS);
        }
        return expected;
    }

    private void validateManifestSchemaRow(Map<String, Object> row, String modelName, List<String> expectedColumns) {
        if (row == null) {
            invalidateUpgradedRoute("route manifest missing model=" + modelName);
            return;
        }
        Object rawColumns = row.get("feature_columns");
        if (!(rawColumns instanceof List<?> rawColumnList)) {
            invalidateUpgradedRoute("route manifest model=" + modelName + " missing feature_columns list");
            return;
        }
        List<String> actualColumns = rawColumnList.stream().map(Object::toString).toList();
        if (!actualColumns.equals(expectedColumns)) {
            invalidateUpgradedRoute("route manifest feature_columns mismatch model=" + modelName + " " + firstSchemaDifference(actualColumns, expectedColumns));
            return;
        }
        Object rawCount = row.get("feature_count");
        int actualCount = rawCount instanceof Number number ? number.intValue() : actualColumns.size();
        if (actualCount != expectedColumns.size()) {
            invalidateUpgradedRoute("route manifest feature_count mismatch model=" + modelName + " actual=" + actualCount + " expected=" + expectedColumns.size());
            return;
        }
        Object rawHash = row.get("feature_schema_sha256");
        if (!(rawHash instanceof String actualHash) || actualHash.isBlank()) {
            invalidateUpgradedRoute("route manifest model=" + modelName + " missing feature_schema_sha256");
            return;
        }
        String expectedHash = featureSchemaHash(expectedColumns);
        if (!expectedHash.equalsIgnoreCase(actualHash.trim())) {
            invalidateUpgradedRoute("route manifest feature_schema_sha256 mismatch model=" + modelName + " actual=" + actualHash + " expected=" + expectedHash);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> objectMap(Object raw) {
        return raw instanceof Map<?, ?> map ? (Map<String, Object>) map : null;
    }

    private boolean booleanValue(Object raw) {
        if (raw instanceof Boolean bool) {
            return bool;
        }
        return raw != null && Boolean.parseBoolean(raw.toString());
    }

    private String optionalString(Object raw) {
        return raw == null ? "" : raw.toString().trim();
    }

    private double finiteProbability(Object raw, double fallback) {
        double value;
        if (raw instanceof Number number) {
            value = number.doubleValue();
        } else {
            try {
                value = raw == null ? Double.NaN : Double.parseDouble(raw.toString());
            } catch (NumberFormatException exception) {
                value = Double.NaN;
            }
        }
        if (!Double.isFinite(value) || value < 0.0 || value > 1.0) {
            return fallback;
        }
        return value;
    }

    private double upgradedRouteThreshold(String modelName, double fallback) {
        Double threshold = upgradedRouteThresholds.get(modelName);
        return threshold == null ? fallback : threshold;
    }

    private double predictUpgradedRouteProbability(String modelName, LazyAiPredictor predictor, float[] features) {
        double rawProbability = predictor == null ? 0.0 : predictor.predictProbability(features);
        ProbabilityCalibrator calibrator = upgradedRouteCalibrators.get(modelName);
        return calibrator == null ? ProbabilityCalibrator.clipProbability(rawProbability) : calibrator.apply(rawProbability);
    }

    private String firstSchemaDifference(List<String> actualColumns, List<String> expectedColumns) {
        int common = Math.min(actualColumns.size(), expectedColumns.size());
        for (int i = 0; i < common; i++) {
            String actual = actualColumns.get(i);
            String expected = expectedColumns.get(i);
            if (!actual.equals(expected)) {
                return "index=" + i + " actual=" + actual + " expected=" + expected;
            }
        }
        return "actualSize=" + actualColumns.size() + " expectedSize=" + expectedColumns.size();
    }

    private void invalidateUpgradedRoute(String reason) {
        upgradedModelRouteValid = false;
        allowNewEntries = false;
        flowError("AI.ROUTE", "FATAL upgraded lifecycle/micro route invalid; new entries disabled symbol=" + symbol + " reason=" + reason);
    }

    private static String featureSchemaHash(List<String> columns) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(String.join("\n", columns).getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                out.append(String.format(Locale.ROOT, "%02x", b));
            }
            return out.toString();
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to compute feature schema hash", exception);
        }
    }

    private LazyAiPredictor modelForRegime(MarketRegime regime, LazyAiPredictor baseModel, LazyAiPredictor choppyModel, LazyAiPredictor trendModel, LazyAiPredictor volatileModel) {
        LazyAiPredictor selectedModel = switch (regime) {
            case CHOPPY -> choppyModel;
            case TREND -> trendModel;
            case VOLATILE -> volatileModel;
        };

        LazyAiPredictor availableSelectedModel = availableModel(selectedModel);
        if (availableSelectedModel != null) {
            return availableSelectedModel;
        }

        if (regime != MarketRegime.CHOPPY) {
            LazyAiPredictor availableChoppyModel = availableModel(choppyModel);
            if (availableChoppyModel != null) {
                return availableChoppyModel;
            }
        }

        return baseModel;
    }

    private LazyAiPredictor availableModel(LazyAiPredictor predictor) {
        return predictor != null && predictor.isAvailable() ? predictor : null;
    }

    private RegimeDecision detectMarketRegime(float[] regimeFeatures) {
        if (regimeClassifierAi == null) {
            lastDetectedRegime = MarketRegime.CHOPPY;
            return new RegimeDecision(MarketRegime.CHOPPY, defaultRegimeProbabilityFeatures(1.0, 0.0, 0.0));
        }

        AiPredictor.MultiClassPredictionOutcome outcome = regimeClassifierAi.predictMultiClassOutcome(regimeFeatures, 0);
        int predicted = outcome.classLabel();
        double confidence = outcome.confidence();
        Map<Integer, Double> probabilities = outcome.classProbabilities() == null ? Map.of() : outcome.classProbabilities();
        double choppyProb = clampProbability(probabilities.getOrDefault(0, predicted == 0 ? 1.0 : 0.0), 0.0);
        double trendProb = clampProbability(probabilities.getOrDefault(1, predicted == 1 ? 1.0 : 0.0), 0.0);
        double volatileProb = clampProbability(probabilities.getOrDefault(2, predicted == 2 ? 1.0 : 0.0), 0.0);
        double probabilitySum = choppyProb + trendProb + volatileProb;
        if (probabilitySum > 0.0) {
            choppyProb /= probabilitySum;
            trendProb /= probabilitySum;
            volatileProb /= probabilitySum;
        }
        Map<String, Float> regimeProbabilityFeatures = defaultRegimeProbabilityFeatures(choppyProb, trendProb, volatileProb);

        if (confidence < regimeProbabilityThreshold) {
            flowCondition(
                "AI.REGIME",
                "CONFIDENCE_THRESHOLD_MET",
                false,
                "symbol=" + symbol
                    + " predictedLabel=" + predicted
                    + " confidence=" + formatProb(confidence)
                    + " threshold=" + formatProb(regimeProbabilityThreshold)
                    + " fallback=CHOPPY"
            );
            lastDetectedRegime = MarketRegime.CHOPPY;
            return new RegimeDecision(MarketRegime.CHOPPY, regimeProbabilityFeatures);
        }

        flowCondition(
            "AI.REGIME",
            "CONFIDENCE_THRESHOLD_MET",
            true,
            "symbol=" + symbol
                + " predictedLabel=" + predicted
                + " confidence=" + formatProb(confidence)
                + " threshold=" + formatProb(regimeProbabilityThreshold)
        );

        MarketRegime detected = switch (predicted) {
            case 1 -> MarketRegime.TREND;
            case 2 -> MarketRegime.VOLATILE;
            default -> MarketRegime.CHOPPY;
        };

        lastDetectedRegime = detected;
        flowData(
            "AI.REGIME",
            "symbol=" + symbol
                + " detectedRegime=" + detected
                + " label=" + predicted
                + " confidence=" + formatProb(confidence)
        );
        return new RegimeDecision(detected, regimeProbabilityFeatures);
    }

    private boolean isOpeningThirtyMinutes() {
        if (currentMarketTime == null) {
            return false;
        }
        int hour = currentMarketTime.getHour();
        int minute = currentMarketTime.getMinute();
        return hour == 9 && minute >= 30;
    }

    public void setAiThresholds(double longEntry, double shortEntry, double longExit, double shortExit) {
        setAiThresholds(longEntry, shortEntry, longExit, shortExit, DEFAULT_REGIME_THRESHOLD);
    }

    public void setAiThresholds(double longEntry, double shortEntry, double longExit, double shortExit, double regimeThreshold) {
        setAiThresholds(
            new AiThresholdConfig(
                longEntry,
                shortEntry,
                longExit,
                shortExit,
                longEntry,
                shortEntry,
                longExit,
                shortExit,
                longEntry,
                shortEntry,
                longExit,
                shortExit,
                longEntry,
                shortEntry,
                longExit,
                shortExit,
                longEntry,
                shortEntry,
                longExit,
                shortExit,
                regimeThreshold
            )
        );
    }

    public void setAiThresholds(AiThresholdConfig config) {
        AiThresholdConfig normalized = (config == null ? AiThresholdConfig.defaults() : config.normalized());
        this.aiThresholdConfig = normalized;
        this.longEntryProbabilityThreshold = normalized.baseLongEntryThreshold();
        this.shortEntryProbabilityThreshold = normalized.baseShortEntryThreshold();
        this.longExitProbabilityThreshold = normalized.baseLongExitThreshold();
        this.shortExitProbabilityThreshold = normalized.baseShortExitThreshold();
        this.regimeProbabilityThreshold = normalized.regimeClassifierThreshold();

        flowData("AI.CONFIG", "symbol=" + symbol + " thresholds " + normalized.summary());
    }

    public void setLifecycleTelemetryListener(LifecycleTelemetryListener listener) {
        this.lifecycleTelemetryListener = listener == null ? LifecycleTelemetryListener.NOOP : listener;
    }

    public AiDecisionDiagnostics getAiDecisionDiagnostics() {
        List<SetupCandidateDiagnostic> closestSetupSnapshot;
        synchronized (aiClosestSetupEvents) {
            closestSetupSnapshot = List.copyOf(aiClosestSetupEvents);
        }
        return new AiDecisionDiagnostics(
            aiDecisionEvaluationCount,
            aiMissingMarketTimeCount,
            aiPreMarketBlockedCount,
            aiMissingPreviousCloseCount,
            aiVarianceBlockedCount,
            aiPositionOpenSkippedCount,
            aiFlatEntryEvaluationCount,
            aiEntryGateOpenCount,
            aiEntryGateClosedCount,
            aiAllowNewEntriesBlockedCount,
            aiMaxTradesBlockedCount,
            aiPositionSyncBlockedCount,
            aiHardStopCooldownBlockedCount,
            aiHardStopBudgetBlockedCount,
            aiBuyQuantityBlockedCount,
            aiSellQuantityBlockedCount,
            aiLongRsiGateBlockedCount,
            aiShortRsiGateBlockedCount,
            aiLongModelUnavailableCount,
            aiShortModelUnavailableCount,
            aiLongEntryModelEvaluationCount,
            aiShortEntryModelEvaluationCount,
            aiLongEntryPassCount,
            aiShortEntryPassCount,
            aiLongNearMissCount,
            aiShortNearMissCount,
            aiLongEntryModelEvaluationCount == 0L ? Double.NaN : aiMaxLongEntryProbability,
            aiLongEntryModelEvaluationCount == 0L ? Double.NaN : aiMaxLongEntryThreshold,
            aiLongEntryModelEvaluationCount == 0L ? Double.NaN : aiMaxLongEntryMargin,
            aiLongEntryModelEvaluationCount == 0L ? 0L : aiMaxLongEntryEpoch,
            aiLongEntryModelEvaluationCount == 0L ? "" : aiMaxLongEntryTime,
            aiShortEntryModelEvaluationCount == 0L ? Double.NaN : aiMaxShortEntryProbability,
            aiShortEntryModelEvaluationCount == 0L ? Double.NaN : aiMaxShortEntryThreshold,
            aiShortEntryModelEvaluationCount == 0L ? Double.NaN : aiMaxShortEntryMargin,
            aiShortEntryModelEvaluationCount == 0L ? 0L : aiMaxShortEntryEpoch,
            aiShortEntryModelEvaluationCount == 0L ? "" : aiMaxShortEntryTime,
            closestSetupSnapshot
        );
    }

    private void emitLifecycleTelemetry(Runnable callback) {
        try {
            callback.run();
        } catch (Exception exception) {
            flowWarn("BACKTEST.TELEMETRY", "Lifecycle telemetry callback failed symbol=" + symbol + " reason=" + exception.getMessage());
        }
    }

    private double clampProbability(double threshold, double fallback) {
        if (Double.isNaN(threshold) || Double.isInfinite(threshold)) {
            return fallback;
        }
        return Math.max(0.0, Math.min(1.0, threshold));
    }

    private String formatProb(double value) {
        return String.format("%.4f", value);
    }

    private void processEvents() {
        // The actor loop is the serialization point for all strategy behavior.
        // If you need to add new live inputs, prefer adding a StrategyEvent subtype and handling it here
        // instead of mutating state from the producer thread.
        while (isRunning) {
            try {
                StrategyEvent event = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                if (event == null) {
                    continue;
                }

                if (event instanceof StrategyEvent.TickEvent e) {
                    handleTickForExitsOnly(e.price);
                } else if (event instanceof StrategyEvent.BarEvent e) {
                    handle5SecondBar(e.time, e.open, e.high, e.low, e.close, e.volume, e.wap);
                } else if (event instanceof StrategyEvent.TapeTradeEvent e) {
                    handleTapeTrade(e.tradePrice, e.tradeSize, e.bidPrice, e.askPrice);
                } else if (event instanceof StrategyEvent.OptionVolumeEvent e) {
                    handleOptionVolumeUpdate(e.putVolume, e.callVolume);
                } else if (event instanceof StrategyEvent.QuoteSnapshotEvent e) {
                    handleQuoteSnapshot(e.bidPrice, e.askPrice, e.bidSize, e.askSize, e.shortableShares);
                } else if (event instanceof StrategyEvent.OrderFlowSnapshotEvent e) {
                    handleOrderFlowSnapshot(e.atBidVolume, e.atAskVolume);
                } else if (event instanceof StrategyEvent.OrderSubmittedEvent e) {
                    handleOrderSubmitted(e.orderId, e.action, e.quantity);
                } else if (event instanceof StrategyEvent.OrderProgressEvent e) {
                    handleOrderProgress(e.orderId, e.action, e.filledDelta, e.remaining, e.avgFillPrice);
                } else if (event instanceof StrategyEvent.OrderClosedEvent e) {
                    handleOrderClosed(e.orderId, e.status);
                } else if (event instanceof StrategyEvent.PositionSyncEvent e) {
                    handlePositionSync(e.brokerPosition, e.avgCost);
                } else if (event instanceof StrategyEvent.ResetForNewDayEvent e) {
                    handleResetForNewDay(e);
                } else if (event instanceof StrategyEvent.RestoreStateEvent e) {
                    handleRestoreState(e.rPrice, e.rTrades, e.rEnabled, e.rArmed, e.restoredYesterdayClose, e.restoredHardStopExitCount, e.restoredLastHardStopExitTimeMs);
                } else if (event instanceof StrategyEvent.SetEnabledEvent e) {
                    handleSetEnabled(e.status);
                } else if (event instanceof StrategyEvent.SetPositionSyncedEvent e) {
                    handleSetPositionSynced(e.synced);
                } else if (event instanceof StrategyEvent.SetAllowNewEntriesEvent e) {
                    handleSetAllowNewEntries(e.allow);
                } else if (event instanceof StrategyEvent.SetCurrentMarketTimeEvent e) {
                    handleSetCurrentMarketTime(e.time);
                } else if (event instanceof StrategyEvent.SetYesterdayCloseEvent e) {
                    handleSetYesterdayClose(e.close);
                } else if (event instanceof StrategyEvent.StopEvent e) {
                    handleStopEvent(e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error(">>> [FLOW][ERROR][STRATEGY.LOOP] Critical error in strategy loop: {}", e.getMessage(), e);
            }
        }
    }

    private double priceForAction(String action, double fallbackPrice) {
        // Quote selection is intentionally side-aware:
        //  - SELL paths prefer bid because that is the executable side for immediate selling
        //  - BUY paths prefer ask because that is the executable side for immediate buying
        // The fallback exists so the strategy can still operate when quote snapshots temporarily lag bars.
        if ("SELL".equalsIgnoreCase(action) && latestBidPrice > 0.0) {
            return latestBidPrice;
        }
        if ("BUY".equalsIgnoreCase(action) && latestAskPrice > 0.0) {
            return latestAskPrice;
        }
        return fallbackPrice;
    }

    private int sharesForAmount(String action, double fallbackPrice) {
        double price = priceForAction(action, fallbackPrice);
        if (price <= 10.0) {
            flowCondition("STRATEGY.RISK", "PRICE_GT_10", false, "symbol=" + symbol + " price=" + price);
            return 0;
        }
        flowCondition("STRATEGY.RISK", "PRICE_GT_10", true, "symbol=" + symbol + " price=" + price);
        int shares = (int) Math.floor(tradeQuantity / price);
        int absoluteMaxShares = 500;
        if (shares > absoluteMaxShares) {
            flowCondition("STRATEGY.RISK", "QTY_WITHIN_ABSOLUTE_CAP", false, "symbol=" + symbol + " requested=" + shares + " cap=" + absoluteMaxShares);
            return absoluteMaxShares;
        }
        flowCondition("STRATEGY.RISK", "QTY_WITHIN_ABSOLUTE_CAP", true, "symbol=" + symbol + " qty=" + shares);
        return Math.max(1, shares);
    }

    @Override public String getSymbol() { return symbol; }
    @Override
    public void updatePosition(int newPos) {
        syncPosition(newPos, 0.0);
    }
    @Override
    public void setEnabled(boolean status) {
        eventQueue.offer(new StrategyEvent.SetEnabledEvent(status));
    }
    @Override public boolean isEnabled() { return enabled; }

    public void syncPosition(int newPosition, double avgCost) {
        eventQueue.offer(new StrategyEvent.PositionSyncEvent(newPosition, avgCost));
    }

    private void handlePositionSync(int newPosition, double avgCost) {
        this.currentPosition = newPosition;
        if (newPosition != 0 && avgCost > 0) {
            this.avgEntryPrice = avgCost; // Official IBKR entry price for stop-loss
            if (positionEntryPrice <= 0.0 || positionEntrySide != Integer.signum(newPosition)) {
                positionEntryPrice = avgCost;
                positionEntryProbability = pendingEntryProbability;
                positionEntryThreshold = pendingEntryThreshold;
                positionEntryThresholdMargin = pendingEntryThresholdMargin;
                pendingEntryProbability = 0.0;
                pendingEntryThreshold = 0.0;
                pendingEntryThresholdMargin = 0.0;
                positionEntrySide = Integer.signum(newPosition);
                positionEntryEpoch = currentMarketTime == null ? 0L : currentMarketTime.atZone(MARKET_ZONE).toEpochSecond();
                barsSincePositionEntry30s = 0;
                barsSincePositionEntry5s = 0;
                positionMfeR = 0.0;
                positionMaeR = 0.0;
            }
        } else if (newPosition == 0) {
            this.avgEntryPrice = 0.0;
            positionEntryPrice = 0.0;
            pendingEntryProbability = 0.0;
            pendingEntryThreshold = 0.0;
            pendingEntryThresholdMargin = 0.0;
            positionEntryProbability = 0.0;
            positionEntryThreshold = 0.0;
            positionEntryThresholdMargin = 0.0;
            positionEntrySide = 0;
            positionEntryEpoch = 0L;
            barsSincePositionEntry30s = 0;
            barsSincePositionEntry5s = 0;
            positionMfeR = 0.0;
            positionMaeR = 0.0;
            clearMicroEntryArms("position-sync-flat");
            if (this.inFlightOrder && this.pendingOrderReconcileRequested) {
                this.inFlightOrder = false;
                this.pendingOrderReconcileRequested = false;
                this.pendingOrderId = -1;
                this.lastPendingOrderReconcileLogTime = 0;
                flowData("STRATEGY.ORDER", "unlocked after flat position sync symbol=" + symbol);
            }
        }
        flowData("STRATEGY.SYNC", "symbol=" + symbol + " brokerPosition=" + currentPosition + " avgCost=" + avgCost);
    }

    private void handleSetEnabled(boolean status) {
        this.enabled = status;
        flowData("STRATEGY.STATE", "enabled=" + status + " symbol=" + symbol);
    }

    private void handleSetPositionSynced(boolean synced) {
        this.positionSynced = synced;
    }

    private void handleSetAllowNewEntries(boolean allow) {
        this.allowNewEntries = allow;
    }

    private void handleSetCurrentMarketTime(LocalDateTime time) {
        this.currentMarketTime = time;
    }

    private void handleSetYesterdayClose(double close) {
        this.yesterdayClose = Math.max(0.0, close);
        flowCondition("STRATEGY.STATE", "YESTERDAY_CLOSE_NON_NEGATIVE", this.yesterdayClose >= 0.0, "symbol=" + symbol + " value=" + this.yesterdayClose);
    }

    private void handleStopEvent(StrategyEvent.StopEvent stopEvent) {
        try {
            this.enabled = false;
            this.isRunning = false;
            flowInfo("STRATEGY.STOP", "Stop event processed symbol=" + symbol + " queueDepth=" + eventQueue.size());
        } finally {
            if (stopEvent.ackLatch != null) {
                stopEvent.ackLatch.countDown();
            }
        }
    }

    // =========================================================================
    // STREAM 1: THE REFLEXES (1-Second Ticks for Hard Stop Loss Only)
    // =========================================================================
    public void onTickForExitsOnly(double price) {
        eventQueue.offer(new StrategyEvent.TickEvent(price));
    }

    public void onTapeTrade(double tradePrice, long tradeSize, double bidPrice, double askPrice) {
        eventQueue.offer(new StrategyEvent.TapeTradeEvent(tradePrice, tradeSize, bidPrice, askPrice));
    }

    public void onOptionVolumeUpdate(long putVolume, long callVolume) {
        eventQueue.offer(new StrategyEvent.OptionVolumeEvent(putVolume, callVolume));
    }

    public void onQuoteSnapshot(double bidPrice, double askPrice, long bidSize, long askSize, double shortableShares) {
        eventQueue.offer(new StrategyEvent.QuoteSnapshotEvent(bidPrice, askPrice, bidSize, askSize, shortableShares));
    }

    public void onOrderFlowSnapshot(long atBidVolume, long atAskVolume) {
        eventQueue.offer(new StrategyEvent.OrderFlowSnapshotEvent(atBidVolume, atAskVolume));
    }

    private void handleOptionVolumeUpdate(long putVolume, long callVolume) {
        latestPutVolume = Math.max(0L, putVolume);
        latestCallVolume = Math.max(0L, callVolume);
    }

    private void handleQuoteSnapshot(double bidPrice, double askPrice, long bidSize, long askSize, double shortableShares) {
        latestBidPrice = Math.max(0.0, bidPrice);
        latestAskPrice = Math.max(0.0, askPrice);
        latestBidSize = Math.max(0L, bidSize);
        latestAskSize = Math.max(0L, askSize);
        latestShortableShares = Math.max(0.0, shortableShares);
    }

    private void handleOrderFlowSnapshot(long atBidVolume, long atAskVolume) {
        currentBarVolBid += Math.max(0L, atBidVolume);
        currentBarVolAsk += Math.max(0L, atAskVolume);
    }

    private void handleTapeTrade(double tradePrice, long tradeSize, double bidPrice, double askPrice) {
        if (tradeSize <= 0) {
            flowCondition("STRATEGY.TAPE", "TRADE_SIZE_POSITIVE", false, "symbol=" + symbol + " tradeSize=" + tradeSize);
            return;
        }
        flowCondition("STRATEGY.TAPE", "TRADE_SIZE_POSITIVE", true, "symbol=" + symbol + " tradeSize=" + tradeSize);

        if (askPrice > 0.0 && tradePrice >= askPrice) {
            currentBarVolAsk += tradeSize;
            flowData("STRATEGY.TAPE", "classified=ASK tradePrice=" + tradePrice + " ask=" + askPrice + " size=" + tradeSize);
            return;
        }

        if (bidPrice > 0.0 && tradePrice <= bidPrice) {
            currentBarVolBid += tradeSize;
            flowData("STRATEGY.TAPE", "classified=BID tradePrice=" + tradePrice + " bid=" + bidPrice + " size=" + tradeSize);
            return;
        }

        long splitAsk = tradeSize / 2;
        currentBarVolAsk += splitAsk;
        currentBarVolBid += (tradeSize - splitAsk);
        flowData("STRATEGY.TAPE", "classified=MID_SPLIT tradePrice=" + tradePrice + " size=" + tradeSize + " askPart=" + splitAsk + " bidPart=" + (tradeSize - splitAsk));
    }

    private void handleTickForExitsOnly(double price) {
        // Tick path = "reflexes".
        // We keep this narrow on purpose: it handles watchdog logic and hard stop-loss protection, not the full
        // AI decision engine. That separation keeps urgent exit handling available on fast quote/tick updates while
        // heavier feature generation remains tied to bar-close cadence.
        // WATCHDOG: 3-Second Active Kill Switch for Connection Drops & Hung Partial Fills.
        // Do not clear inFlightOrder on timeout alone. If broker/order-status callbacks lag, unlocking here can
        // resubmit the same exit against a stale broker position. Keep the lock until terminal order status or an
        // authoritative flat position sync arrives.
        if (this.inFlightOrder && (System.currentTimeMillis() - this.lastOrderSubmitTime > 3000)) {
            long now = System.currentTimeMillis();
            if (!this.pendingOrderReconcileRequested) {
                this.pendingOrderReconcileRequested = true;
                this.lastPendingOrderReconcileLogTime = now;
                flowError("WATCHDOG", "Order hung >3s. Requesting cancel/resync but keeping in-flight lock. symbol=" + symbol + " orderId=" + pendingOrderId + " position=" + currentPosition);
                if (this.parent != null && this.pendingOrderId != -1) {
                    this.parent.cancelStaleOrder(this.pendingOrderId);
                }
                if (this.parent != null) {
                    this.parent.requestPositions(); // Find out exactly what filled
                    this.parent.requestOpenOrdersSync(); // Reconcile gateway/broker open-order state
                }
            } else if (now - this.lastPendingOrderReconcileLogTime > 10_000) {
                this.lastPendingOrderReconcileLogTime = now;
                flowInfo("WATCHDOG", "Waiting for broker terminal status/reconcile before unlocking. symbol=" + symbol + " orderId=" + pendingOrderId + " position=" + currentPosition);
            }
        }
        this.lastPrice = price;
        evaluateHardRiskExit("tick", priceForAction("SELL", price), priceForAction("BUY", price), price);
    }

    private void evaluateHardRiskExit(String source, double longStopProbePrice, double shortStopProbePrice, double executionFallbackPrice) {
        boolean gateOpen = !circuitBreakerTripped && currentPosition != 0 && !inFlightOrder;
        flowCondition("STRATEGY.RISK", "HARD_EXIT_GATE", gateOpen, "symbol=" + symbol + " source=" + source + " circuitBreaker=" + circuitBreakerTripped + " position=" + currentPosition + " inFlight=" + inFlightOrder + " longProbe=" + longStopProbePrice + " shortProbe=" + shortStopProbePrice);
        if (!gateOpen) return;

        int position = currentPosition;
        double currentDailyPnL = dailyNetPnL;
        if (currentDailyPnL <= -maxDailyDrawdown) {
            circuitBreakerTripped = true;
            flowCondition("STRATEGY.RISK", "DAILY_DRAWDOWN_WITHIN_LIMIT", false, "symbol=" + symbol + " source=" + source + " dailyNetPnL=" + currentDailyPnL + " limit=" + (-maxDailyDrawdown));
            String action = (position > 0) ? "SELL" : "BUY";
            double executionPrice = priceForAction(action, executionFallbackPrice);
            String side = position > 0 ? "long" : "short";
            emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onHardRiskExit(symbol, side, "daily_drawdown"));
            this.inFlightOrder = true;
            flowWarn("STRATEGY.RISK", "Submitting hard-risk daily-drawdown exit symbol=" + symbol + " side=" + side + " action=" + action + " qty=" + Math.abs(position) + " execPrice=" + executionPrice + " dailyNetPnL=" + currentDailyPnL + " limit=" + (-maxDailyDrawdown));
            parent.placeTrade(symbol, action, executionPrice, Math.abs(position), "MKT");
            return;
        }
        flowCondition("STRATEGY.RISK", "DAILY_DRAWDOWN_WITHIN_LIMIT", true, "symbol=" + symbol + " source=" + source + " dailyNetPnL=" + currentDailyPnL + " limit=" + (-maxDailyDrawdown));

        double currentAvgEntry = avgEntryPrice;
        if (currentAvgEntry <= 0.0) {
            flowCondition("STRATEGY.STOP", "AVG_ENTRY_AVAILABLE", false, "symbol=" + symbol + " source=" + source + " avgEntry=" + currentAvgEntry);
            return;
        }

        double longStopThreshold = currentAvgEntry * (1.0 - stopLossPercentage);
        double shortStopThreshold = currentAvgEntry * (1.0 + stopLossPercentage);
        long secondsSinceEntry = secondsSincePositionEntry();
        if (position > 0 && longStopProbePrice > 0.0 && longStopProbePrice <= longStopThreshold) {
            double executionPrice = priceForAction("SELL", executionFallbackPrice);
            flowCondition("STRATEGY.STOP", "LONG_HARD_STOP_TRIGGER", true, "symbol=" + symbol + " source=" + source + " probePrice=" + longStopProbePrice + " threshold=" + longStopThreshold + " avgEntry=" + currentAvgEntry + " stopLossPct=" + stopLossPercentage + " secondsSinceEntry=" + secondsSinceEntry);
            recordHardStopExit("LONG", source);
            emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onHardRiskExit(symbol, "long", "hard_stop_" + source));
            this.inFlightOrder = true;
            flowWarn("STRATEGY.STOP", "Submitting long hard-stop exit symbol=" + symbol + " source=" + source + " action=SELL qty=" + Math.abs(position) + " execPrice=" + executionPrice + " probePrice=" + longStopProbePrice + " threshold=" + longStopThreshold + " avgEntry=" + currentAvgEntry + " secondsSinceEntry=" + secondsSinceEntry);
            parent.placeTrade(symbol, "SELL", executionPrice, Math.abs(position), "MKT");
            return;
        }
        if (position < 0 && shortStopProbePrice > 0.0 && shortStopProbePrice >= shortStopThreshold) {
            double executionPrice = priceForAction("BUY", executionFallbackPrice);
            flowCondition("STRATEGY.STOP", "SHORT_HARD_STOP_TRIGGER", true, "symbol=" + symbol + " source=" + source + " probePrice=" + shortStopProbePrice + " threshold=" + shortStopThreshold + " avgEntry=" + currentAvgEntry + " stopLossPct=" + stopLossPercentage + " secondsSinceEntry=" + secondsSinceEntry);
            recordHardStopExit("SHORT", source);
            emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onHardRiskExit(symbol, "short", "hard_stop_" + source));
            this.inFlightOrder = true;
            flowWarn("STRATEGY.STOP", "Submitting short hard-stop exit symbol=" + symbol + " source=" + source + " action=BUY qty=" + Math.abs(position) + " execPrice=" + executionPrice + " probePrice=" + shortStopProbePrice + " threshold=" + shortStopThreshold + " avgEntry=" + currentAvgEntry + " secondsSinceEntry=" + secondsSinceEntry);
            parent.placeTrade(symbol, "BUY", executionPrice, Math.abs(position), "MKT");
            return;
        }
        flowCondition("STRATEGY.STOP", "HARD_STOP_TRIGGERED", false, "symbol=" + symbol + " source=" + source + " longProbe=" + longStopProbePrice + " shortProbe=" + shortStopProbePrice + " avgEntry=" + currentAvgEntry + " position=" + position);
    }

    private long secondsSincePositionEntry() {
        if (positionEntryEpoch <= 0L || currentMarketTime == null) {
            return -1L;
        }
        long nowEpoch = currentMarketTime.atZone(MARKET_ZONE).toEpochSecond();
        return Math.max(0L, nowEpoch - positionEntryEpoch);
    }

    private void recordHardStopExit(String side, String source) {
        hardStopExitCount++;
        lastHardStopExitTimeMs = System.currentTimeMillis();
        if (hardStopExitCount >= maxHardStopsPerDay) {
            allowNewEntries = false;
        }
        flowData(
            "STRATEGY.STOP",
            "hardStopExitRecorded symbol=" + symbol
                + " side=" + side
                + " source=" + source
                + " hardStopExitCount=" + hardStopExitCount
                + " maxHardStopsPerDay=" + maxHardStopsPerDay
                + " postHardStopEntryCooldownMs=" + postHardStopEntryCooldownMs
                + " allowNewEntries=" + allowNewEntries
        );
    }

    // =========================================================================
    // STREAM 2: THE BRAIN (5-second feed aggregated into 30-second AI bars)
    // =========================================================================
    public void onSourceBar(long time, double open, double high, double low, double close, long volume, double wap) {
        eventQueue.offer(new StrategyEvent.BarEvent(time, open, high, low, close, volume, wap));
    }

    public void on5SecondBar(long time, double open, double high, double low, double close, long volume, double wap) {
        onSourceBar(time, open, high, low, close, volume, wap);
    }

    private void handle5SecondBar(long time, double open, double high, double low, double close, long volume, double wap) {
        // Method name is legacy, but the input stream may now be 1-second bars from Databento.
        // The only thing that matters here is the event timestamp; we rebucket incoming source bars into fixed
        // 30-second AI bars regardless of the source cadence.
        this.latestSourceBarEpoch = time;
        this.currentTickArrivalTime = System.currentTimeMillis();
        this.lastPrice = close;
        flowData("STRATEGY.BAR", "symbol=" + symbol + " epoch=" + time + " ohlc=" + open + "/" + high + "/" + low + "/" + close + " vol=" + volume + " wap=" + wap);

        updateIndependentMicroBarState(time, open, high, low, close, volume, wap);

        // Bar path = guaranteed safety backstop. Databento deployments may not always deliver a separate tick stream,
        // so hard exits must also inspect every source bar. For stops, use bar low/high as the adverse excursion probe;
        // order routing still uses close/quotes as the execution reference for the market order.
        evaluateHardRiskExit("bar", low, high, close);

        long nextBucketStart = aligned30SecondBucketStart(time);
        if (bucketStartEpoch < 0L && nextBucketStart <= lastFinalizedBucketStartEpoch) {
            flowCondition("STRATEGY.BAR", "LATE_FINALIZED_BUCKET_BAR_DROPPED", false,
                "symbol=" + symbol + " epoch=" + time + " bucketStart=" + nextBucketStart + " lastFinalizedBucketStart=" + lastFinalizedBucketStartEpoch);
            return;
        }
        if (bucketStartEpoch < 0L) {
            if (shouldWaitForAligned30SecondBucketStart(time)) {
                waitForAligned30SecondBucketStart(time, nextBucketStart);
                return;
            }
            startNew30SecondBucket(nextBucketStart, open, high, low, close, volume, wap);
            if (time >= bucketStartEpoch + 29L) {
                finalizeCurrent30SecondBucket();
            }
            return;
        }

        if (nextBucketStart < bucketStartEpoch) {
            flowCondition("STRATEGY.BAR", "OUT_OF_ORDER_BUCKET_BAR_DROPPED", false,
                "symbol=" + symbol + " epoch=" + time + " bucketStart=" + nextBucketStart + " activeBucketStart=" + bucketStartEpoch);
            return;
        }

        if (nextBucketStart != bucketStartEpoch) {
            finalizeCurrent30SecondBucket();
            if (shouldWaitForAligned30SecondBucketStart(time)) {
                waitForAligned30SecondBucketStart(time, nextBucketStart);
                return;
            }
            startNew30SecondBucket(nextBucketStart, open, high, low, close, volume, wap);
            if (time >= bucketStartEpoch + 29L) {
                finalizeCurrent30SecondBucket();
            }
            return;
        }

        accumulateIntoCurrent30SecondBucket(high, low, close, volume, wap);
        if (time >= bucketStartEpoch + 29L) {
            finalizeCurrent30SecondBucket();
        }
    }

    private long alignedBucketStart(long epochSec, long bucketSeconds) {
        return epochSec - Math.floorMod(epochSec, bucketSeconds);
    }

    private boolean isAlignedBucketStart(long epochSec, long bucketSeconds) {
        return Math.floorMod(epochSec, bucketSeconds) == 0L;
    }

    private long aligned30SecondBucketStart(long epochSec) {
        return alignedBucketStart(epochSec, AI_BUCKET_SECONDS);
    }

    private boolean isAligned30SecondBucketStart(long epochSec) {
        return isAlignedBucketStart(epochSec, AI_BUCKET_SECONDS);
    }

    private boolean shouldWaitForAligned30SecondBucketStart(long epochSec) {
        return !isAligned30SecondBucketStart(epochSec);
    }

    private void waitForAligned30SecondBucketStart(long epochSec, long currentBucketStart) {
        long nextAlignedStart = currentBucketStart + AI_BUCKET_SECONDS;
        if (lastAligned30sWaitLogTargetEpoch != nextAlignedStart) {
            flowCondition("STRATEGY.BAR", "WAIT_FOR_ALIGNED_30S_BUCKET_START", false,
                "symbol=" + symbol + " epoch=" + epochSec + " secondOffset=" + Math.floorMod(epochSec, AI_BUCKET_SECONDS) + " nextAlignedBucketStart=" + nextAlignedStart);
            lastAligned30sWaitLogTargetEpoch = nextAlignedStart;
        }
    }

    private long aligned5SecondMicroBucketStart(long epochSec) {
        return alignedBucketStart(epochSec, MICRO_BUCKET_SECONDS);
    }

    private boolean shouldWaitForAligned5SecondMicroBucketStart(long epochSec) {
        return !isAlignedBucketStart(epochSec, MICRO_BUCKET_SECONDS);
    }

    private void waitForAligned5SecondMicroBucketStart(long epochSec, long currentBucketStart) {
        long nextAlignedStart = currentBucketStart + MICRO_BUCKET_SECONDS;
        if (lastAlignedMicro5sWaitLogTargetEpoch != nextAlignedStart) {
            flowCondition("STRATEGY.MICRO", "WAIT_FOR_ALIGNED_5S_MICRO_BUCKET_START", false,
                "symbol=" + symbol + " epoch=" + epochSec + " secondOffset=" + Math.floorMod(epochSec, MICRO_BUCKET_SECONDS) + " nextAlignedMicroBucketStart=" + nextAlignedStart);
            lastAlignedMicro5sWaitLogTargetEpoch = nextAlignedStart;
        }
    }

    private void updateIndependentMicroBarState(long time, double open, double high, double low, double close, long volume, double wap) {
        MicroBar sourceBar = new MicroBar(time, open, high, low, close, Math.max(0L, volume), wap > 0.0 ? wap : close);
        sourceBarWindow.addLast(sourceBar);
        while (sourceBarWindow.size() > SOURCE_BAR_WINDOW_SIZE) {
            sourceBarWindow.removeFirst();
        }
        accumulateIndependent5SecondMicroBucket(sourceBar);
    }

    private void accumulateIndependent5SecondMicroBucket(MicroBar sourceBar) {
        long nextBucketStart = aligned5SecondMicroBucketStart(sourceBar.epoch());
        if (micro5sBucketStartEpoch < 0L) {
            if (shouldWaitForAligned5SecondMicroBucketStart(sourceBar.epoch())) {
                waitForAligned5SecondMicroBucketStart(sourceBar.epoch(), nextBucketStart);
                return;
            }
            startIndependent5SecondMicroBucket(nextBucketStart, sourceBar);
            return;
        }
        if (nextBucketStart < micro5sBucketStartEpoch) {
            flowCondition("STRATEGY.MICRO", "OUT_OF_ORDER_MICRO_BAR_DROPPED", false,
                "symbol=" + symbol + " epoch=" + sourceBar.epoch() + " microBucket=" + nextBucketStart + " activeMicroBucket=" + micro5sBucketStartEpoch);
            return;
        }
        if (nextBucketStart != micro5sBucketStartEpoch) {
            finalizeIndependent5SecondMicroBucket();
            if (shouldWaitForAligned5SecondMicroBucketStart(sourceBar.epoch())) {
                waitForAligned5SecondMicroBucketStart(sourceBar.epoch(), nextBucketStart);
                return;
            }
            startIndependent5SecondMicroBucket(nextBucketStart, sourceBar);
            return;
        }
        micro5sHigh = Math.max(micro5sHigh, sourceBar.high());
        micro5sLow = Math.min(micro5sLow, sourceBar.low());
        micro5sClose = sourceBar.close();
        micro5sVolume += Math.max(0L, sourceBar.volume());
        micro5sWapSum += sourceBar.wap() * Math.max(0L, sourceBar.volume());
    }

    private void startIndependent5SecondMicroBucket(long bucketStart, MicroBar sourceBar) {
        micro5sBucketStartEpoch = bucketStart;
        lastAlignedMicro5sWaitLogTargetEpoch = -1L;
        micro5sOpen = sourceBar.open();
        micro5sHigh = sourceBar.high();
        micro5sLow = sourceBar.low();
        micro5sClose = sourceBar.close();
        micro5sVolume = Math.max(0L, sourceBar.volume());
        micro5sWapSum = sourceBar.wap() * Math.max(0L, sourceBar.volume());
    }

    private void finalizeIndependent5SecondMicroBucket() {
        if (micro5sBucketStartEpoch < 0L) {
            return;
        }
        double finalWap = micro5sVolume > 0L ? micro5sWapSum / micro5sVolume : micro5sClose;
        micro5sWindow.addLast(new MicroBar(
            micro5sBucketStartEpoch,
            micro5sOpen,
            micro5sHigh,
            micro5sLow,
            micro5sClose,
            micro5sVolume,
            finalWap
        ));
        while (micro5sWindow.size() > MICRO_5S_WINDOW_SIZE) {
            micro5sWindow.removeFirst();
        }
        evaluateMicroRoutes(micro5sWindow.peekLast());
    }

    private void clearIndependentMicroBarState() {
        sourceBarWindow.clear();
        micro5sWindow.clear();
        micro5sReturnWindow20.clear();
        micro5sVolumeWindow20.clear();
        micro5sOptionFlowWindow20.clear();
        micro5sBucketStartEpoch = -1L;
        micro5sOpen = 0.0;
        micro5sHigh = 0.0;
        micro5sLow = Double.MAX_VALUE;
        micro5sClose = 0.0;
        micro5sVolume = 0L;
        micro5sWapSum = 0.0;
        lastAlignedMicro5sWaitLogTargetEpoch = -1L;
        microPrevPutVolume = latestPutVolume;
        microPrevCallVolume = latestCallVolume;
        latestSourceBarEpoch = 0L;
        current30sAiDecisionEpoch = 0L;
    }

    private void evaluateMicroRoutes(MicroBar microBar) {
        if (microBar == null || microBar.close() <= 0.0 || inFlightOrder || !enabled || circuitBreakerTripped) {
            return;
        }
        Map<String, Float> microFeatures = constructTraining5sFeatureValueMap(microBar);

        if (MICRO_EXIT_GUARD_ENABLED && currentPosition != 0) {
            evaluateMicroExitGuard(microBar, microFeatures);
        }

        if (!MICRO_ENTRY_ENABLED || currentPosition != 0 || !positionSynced || !allowNewEntries || tradeCount >= maxTrades) {
            return;
        }
        if (!microLongEntryArmed && !microShortEntryArmed) {
            return;
        }
        long secondsSinceArm = Math.max(0L, microBar.epoch() - microArmEpoch);
        if (secondsSinceArm > MICRO_ARM_TTL_SECONDS) {
            clearMicroEntryArms("expired secondsSinceArm=" + secondsSinceArm);
            return;
        }
        Map<String, Float> features = new LinkedHashMap<>(microFeatures);
        features.putAll(armed30sFeatureValues);
        features.put("f_setup_score_proxy", (float) armedSetupProbability);
        features.put("f_setup_prob", (float) armedSetupProbability);
        features.put("f_setup_threshold", (float) armedSetupThreshold);
        features.put("f_setup_threshold_margin", (float) armedSetupThresholdMargin);
        features.put("f_seconds_since_arm", (float) secondsSinceArm);

        if (microLongEntryArmed && longMicroEntryAi != null && longMicroEntryAi.isAvailable()) {
            double threshold = upgradedRouteThreshold("longMicroEntryAi", MICRO_LONG_ENTRY_THRESHOLD);
            double prob = predictUpgradedRouteProbability("longMicroEntryAi", longMicroEntryAi, buildFeatureVector(MICRO_ENTRY_FEATURE_COLUMNS, features));
            boolean pass = prob >= threshold;
            flowCondition("AI.MICRO.LONG.ENTRY", "MICRO_ENTRY_CONFIRMS", pass, "symbol=" + symbol + " prob=" + formatProb(prob) + " threshold=" + formatProb(threshold) + " secondsSinceArm=" + secondsSinceArm);
            if (pass) {
                int qty = sharesForAmount("BUY", microBar.close());
                if (qty > 0) {
                    pendingEntryProbability = armedSetupProbability;
                    pendingEntryThreshold = armedSetupThreshold;
                    pendingEntryThresholdMargin = armedSetupThresholdMargin;
                    long confirmedArmEpoch = microArmEpoch;
                    emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onMicroEntryConfirmed(symbol, "long", confirmedArmEpoch, microBar.epoch(), prob, threshold, qty, microBar.close()));
                    clearMicroEntryArms("long-confirmed");
                    this.inFlightOrder = true;
                    parent.placeTrade(symbol, "BUY", priceForAction("BUY", microBar.close()), qty, "FAST_LMT");
                    return;
                }
            }
        }

        if (microShortEntryArmed && shortMicroEntryAi != null && shortMicroEntryAi.isAvailable()) {
            double threshold = upgradedRouteThreshold("shortMicroEntryAi", MICRO_SHORT_ENTRY_THRESHOLD);
            double prob = predictUpgradedRouteProbability("shortMicroEntryAi", shortMicroEntryAi, buildFeatureVector(MICRO_ENTRY_FEATURE_COLUMNS, features));
            boolean pass = prob >= threshold;
            flowCondition("AI.MICRO.SHORT.ENTRY", "MICRO_ENTRY_CONFIRMS", pass, "symbol=" + symbol + " prob=" + formatProb(prob) + " threshold=" + formatProb(threshold) + " secondsSinceArm=" + secondsSinceArm);
            if (pass) {
                int qty = sharesForAmount("SELL", microBar.close());
                if (qty > 0) {
                    pendingEntryProbability = armedSetupProbability;
                    pendingEntryThreshold = armedSetupThreshold;
                    pendingEntryThresholdMargin = armedSetupThresholdMargin;
                    long confirmedArmEpoch = microArmEpoch;
                    emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onMicroEntryConfirmed(symbol, "short", confirmedArmEpoch, microBar.epoch(), prob, threshold, qty, microBar.close()));
                    clearMicroEntryArms("short-confirmed");
                    this.inFlightOrder = true;
                    parent.placeTrade(symbol, "SELL", priceForAction("SELL", microBar.close()), qty, "FAST_LMT");
                }
            }
        }
    }

    private void evaluateMicroExitGuard(MicroBar microBar, Map<String, Float> microFeatures) {
        if (positionEntryPrice <= 0.0) {
            return;
        }
        updatePositionPathStats(microBar.high(), microBar.low(), microBar.close(), false);
        Map<String, Float> features = new LinkedHashMap<>(microFeatures);
        features.putAll(lastTraining30sFeatureValues);
        features.putAll(positionFeatureValues(microBar.close(), false));

        if (currentPosition > 0 && longMicroExitGuardAi != null && longMicroExitGuardAi.isAvailable()) {
            double threshold = upgradedRouteThreshold("longMicroExitGuardAi", MICRO_LONG_EXIT_GUARD_THRESHOLD);
            double prob = predictUpgradedRouteProbability("longMicroExitGuardAi", longMicroExitGuardAi, buildFeatureVector(MICRO_EXIT_GUARD_FEATURE_COLUMNS, features));
            boolean shouldExit = prob >= threshold;
            flowCondition("AI.MICRO.LONG.EXIT", "MICRO_EXIT_GUARD_TRIGGERS", shouldExit, "symbol=" + symbol + " prob=" + formatProb(prob) + " threshold=" + formatProb(threshold));
            emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onMicroExitGuardEvaluated(symbol, "long", microBar.epoch(), prob, threshold, shouldExit));
            if (shouldExit) {
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "SELL", priceForAction("SELL", microBar.close()), Math.abs(currentPosition), "MKT");
            }
        } else if (currentPosition < 0 && shortMicroExitGuardAi != null && shortMicroExitGuardAi.isAvailable()) {
            double threshold = upgradedRouteThreshold("shortMicroExitGuardAi", MICRO_SHORT_EXIT_GUARD_THRESHOLD);
            double prob = predictUpgradedRouteProbability("shortMicroExitGuardAi", shortMicroExitGuardAi, buildFeatureVector(MICRO_EXIT_GUARD_FEATURE_COLUMNS, features));
            boolean shouldExit = prob >= threshold;
            flowCondition("AI.MICRO.SHORT.EXIT", "MICRO_EXIT_GUARD_TRIGGERS", shouldExit, "symbol=" + symbol + " prob=" + formatProb(prob) + " threshold=" + formatProb(threshold));
            emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onMicroExitGuardEvaluated(symbol, "short", microBar.epoch(), prob, threshold, shouldExit));
            if (shouldExit) {
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "BUY", priceForAction("BUY", microBar.close()), Math.abs(currentPosition), "MKT");
            }
        }
    }

    private Map<String, Float> constructTraining5sFeatureValueMap(MicroBar microBar) {
        double prevClose = previousMicroClose(microBar.close());
        double ret1 = prevClose > 0.0 ? (microBar.close() / prevClose) - 1.0 : 0.0;
        micro5sReturnWindow20.addLast(ret1);
        if (micro5sReturnWindow20.size() > 20) micro5sReturnWindow20.removeFirst();
        micro5sVolumeWindow20.addLast((double) microBar.volume());
        if (micro5sVolumeWindow20.size() > 20) micro5sVolumeWindow20.removeFirst();

        long putDelta = Math.max(0L, latestPutVolume - microPrevPutVolume);
        long callDelta = Math.max(0L, latestCallVolume - microPrevCallVolume);
        microPrevPutVolume = latestPutVolume;
        microPrevCallVolume = latestCallVolume;
        double optionFlow = putDelta + callDelta;
        micro5sOptionFlowWindow20.addLast(optionFlow);
        if (micro5sOptionFlowWindow20.size() > 20) micro5sOptionFlowWindow20.removeFirst();

        return commonTrainingFeatureValues("f_5s_", microBar.epoch(), microBar.open(), microBar.high(), microBar.low(), microBar.close(), microBar.volume(), microBar.wap(), ret1, pctReturnFromMicroWindow(3), stdDev(micro5sReturnWindow20), average(micro5sVolumeWindow20), putDelta, callDelta, average(micro5sOptionFlowWindow20), false);
    }

    private double previousMicroClose(double fallback) {
        if (micro5sWindow.size() < 2) {
            return fallback;
        }
        MicroBar last = micro5sWindow.removeLast();
        MicroBar prev = micro5sWindow.peekLast();
        micro5sWindow.addLast(last);
        return prev == null ? fallback : prev.close();
    }

    private double pctReturnFromMicroWindow(int barsBack) {
        if (micro5sWindow.size() <= barsBack) {
            return 0.0;
        }
        MicroBar[] bars = micro5sWindow.toArray(new MicroBar[0]);
        double previous = bars[bars.length - barsBack - 1].close();
        double current = bars[bars.length - 1].close();
        return previous > 0.0 ? (current / previous) - 1.0 : 0.0;
    }

    private void armMicroEntry(String side, Map<String, Float> contextFeatures, long armEpoch, double setupProb, double setupThreshold) {
        armed30sFeatureValues = new HashMap<>(contextFeatures);
        microArmEpoch = armEpoch;
        armedSetupProbability = setupProb;
        armedSetupThreshold = setupThreshold;
        armedSetupThresholdMargin = setupProb - setupThreshold;
        microLongEntryArmed = "long".equalsIgnoreCase(side);
        microShortEntryArmed = "short".equalsIgnoreCase(side);
        flowInfo("AI.MICRO.ENTRY", "Armed " + side + " micro-entry symbol=" + symbol + " epoch=" + armEpoch + " ttlSeconds=" + MICRO_ARM_TTL_SECONDS + " setupProb=" + formatProb(setupProb));
        emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onMicroEntryArmed(symbol, side == null ? "" : side.toLowerCase(Locale.US), armEpoch, setupProb, setupThreshold));
    }

    private long currentMicroArmEpoch() {
        if (current30sAiDecisionEpoch > 0L) {
            return current30sAiDecisionEpoch;
        }
        if (lastTraining30sEpoch > 0L) {
            return lastTraining30sEpoch + 30L;
        }
        if (currentMarketTime != null) {
            return currentMarketTime.atZone(MARKET_ZONE).toEpochSecond();
        }
        return 0L;
    }

    private void clearMicroEntryArms(String reason) {
        boolean wasLongArmed = microLongEntryArmed;
        boolean wasShortArmed = microShortEntryArmed;
        long clearedArmEpoch = microArmEpoch;
        if (wasLongArmed || wasShortArmed) {
            flowInfo("AI.MICRO.ENTRY", "Cleared micro-entry arms symbol=" + symbol + " reason=" + reason);
            String side = wasLongArmed ? "long" : (wasShortArmed ? "short" : "");
            emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onMicroEntryArmCleared(symbol, side, clearedArmEpoch, reason));
        }
        microLongEntryArmed = false;
        microShortEntryArmed = false;
        microArmEpoch = 0L;
        armedSetupProbability = 0.0;
        armedSetupThreshold = 0.0;
        armedSetupThresholdMargin = 0.0;
        armed30sFeatureValues = new HashMap<>();
    }

    private void startNew30SecondBucket(long bucketEpoch, double open, double high, double low, double close, long volume, double wap) {
        bucketStartEpoch = bucketEpoch;
        lastAligned30sWaitLogTargetEpoch = -1L;
        bucketOpen = open;
        bucketHigh = high;
        bucketLow = low;
        bucketClose = close;
        bucketVolume = Math.max(0L, volume);
        double effectiveWap = wap > 0.0 ? wap : close;
        bucketWapSum = effectiveWap * Math.max(0L, volume);
    }

    private void accumulateIntoCurrent30SecondBucket(double high, double low, double close, long volume, double wap) {
        bucketHigh = Math.max(bucketHigh, high);
        bucketLow = Math.min(bucketLow, low);
        bucketClose = close;
        bucketVolume += Math.max(0L, volume);
        double effectiveWap = wap > 0.0 ? wap : close;
        bucketWapSum += (effectiveWap * Math.max(0L, volume));
    }

    private void finalizeCurrent30SecondBucket() {
        if (bucketStartEpoch < 0L) {
            return;
        }
        long finalizedBucketStart = bucketStartEpoch;
        double finalWap = bucketVolume > 0 ? (bucketWapSum / bucketVolume) : bucketClose;
        if (!isAligned30SecondBucketStart(finalizedBucketStart)) {
            flowCondition("STRATEGY.BAR", "UNALIGNED_30S_AI_LIFECYCLE_BUCKET_DROPPED", false,
                "symbol=" + symbol + " bucketStart=" + finalizedBucketStart + " secondOffset=" + Math.floorMod(finalizedBucketStart, AI_BUCKET_SECONDS));
            clearCurrent30SecondBucket(false);
            return;
        }
        System.out.printf(
            ">>> [30s BUCKET] epoch=%d ohlc=%.2f/%.2f/%.2f/%.2f vol=%d vwap=%.4f%n",
            finalizedBucketStart, bucketOpen, bucketHigh, bucketLow, bucketClose, bucketVolume, finalWap
        );
        current30sAiDecisionEpoch = finalizedBucketStart + AI_BUCKET_SECONDS;
        process30SecondBar(finalizedBucketStart, bucketOpen, bucketHigh, bucketLow, bucketClose, bucketVolume, finalWap);
        lastFinalizedBucketStartEpoch = finalizedBucketStart;

        clearCurrent30SecondBucket(false);
    }

    private void clearCurrent30SecondBucket(boolean resetFinalizedState) {
        bucketStartEpoch = -1L;
        if (resetFinalizedState) {
            lastFinalizedBucketStartEpoch = -1L;
        }
        lastAligned30sWaitLogTargetEpoch = -1L;
        bucketOpen = 0.0;
        bucketHigh = 0.0;
        bucketLow = Double.MAX_VALUE;
        bucketClose = 0.0;
        bucketVolume = 0L;
        bucketWapSum = 0.0;
    }

    private void process30SecondBar(long time, double open, double high, double low, double close, long volume, double wap) {
        // This is the main "brain" pass.
        // By the time execution gets here, the 30-second bucket is final and we can safely compute features that
        // assume a closed bar, such as momentum, realized volatility, opening-range distance, and order-flow ratios.
        
        this.currentMarketTime = LocalDateTime.ofEpochSecond(time, 0, ZoneOffset.UTC)
                              .atZone(ZoneId.of("UTC"))
                              .withZoneSameInstant(ZoneId.of("America/New_York")) // LOCKED TO ET
                                              .toLocalDateTime();

        LocalDate barDate = this.currentMarketTime.toLocalDate();
        if (featureSessionDate == null || !featureSessionDate.equals(barDate)) {
            featureSessionDate = barDate;
            openingRangeBarsCount = 0;
            openingRangeHigh = 0.0;
            openingRangeLow = 0.0;
            returnWindow20.clear();
            realizedVolWindow100.clear();
            spreadWindow100.clear();
            clearIndependentMicroBarState();
            greenStreak = 0;
            redStreak = 0;
        }

        this.barOpen = open;
        this.barHigh = high;
        this.barLow = low;
        this.barClose = close;
        this.barVolume = volume;

        if (barClose >= barOpen) {
            greenStreak++;
            redStreak = 0;
        } else {
            redStreak++;
            greenStreak = 0;
        }

        if (openingRangeBarsCount < 10) {
            openingRangeHigh = openingRangeBarsCount == 0 ? barHigh : Math.max(openingRangeHigh, barHigh);
            openingRangeLow = openingRangeBarsCount == 0 ? barLow : Math.min(openingRangeLow, barLow);
            openingRangeBarsCount++;
        }

        int minuteOfDay = (currentMarketTime.getHour() * 60) + currentMarketTime.getMinute();
        double baselineVol = minuteVolumeBaseline.getOrDefault(minuteOfDay, (double) Math.max(1L, barVolume));
        minuteVolumeBaseline.put(minuteOfDay, (0.95 * baselineVol) + (0.05 * Math.max(1L, barVolume)));

        double spread = 0.0;
        if (latestBidPrice > 0.0 && latestAskPrice > 0.0 && latestAskPrice >= latestBidPrice) {
            spread = latestAskPrice - latestBidPrice;
        }
        spreadWindow100.addLast(spread);
        if (spreadWindow100.size() > 100) spreadWindow100.removeFirst();

        double typicalPrice = (barHigh + barLow + barClose) / 3.0;
        cumPv += (typicalPrice * barVolume);
        cumVol += barVolume;
        vwap = cumVol > 0 ? (cumPv / cumVol) : barClose;

        if (dayHigh == 0.0 || barHigh > dayHigh) dayHigh = barHigh;
        if (dayLow == 0.0 || barLow < dayLow) dayLow = barLow;

        bbWindow.addLast(barClose);
        if (bbWindow.size() > 12) bbWindow.removeFirst();

        smaWindow.addLast(barClose);
        if (smaWindow.size() > 60) smaWindow.removeFirst();

        volumeWindow.addLast((double) barVolume);
        if (volumeWindow.size() > 60) volumeWindow.removeFirst();

        // NEW: Track the 60-bar (5-minute) local highs and lows
        highWindow.addLast(barHigh);
        if (highWindow.size() > 60) highWindow.removeFirst();
        
        lowWindow.addLast(barLow);
        if (lowWindow.size() > 60) lowWindow.removeFirst();

        if (prevBarClose > 0) {
            double change = barClose - prevBarClose;
            double gain = Math.max(0, change);
            double loss = Math.max(0, -change);
            if (barsCount < rsiPeriod) {
                avgGain += gain / (double) rsiPeriod;
                avgLoss += loss / (double) rsiPeriod;
            } else {
                avgGain = (avgGain * (rsiPeriod - 1.0) + gain) / (double) rsiPeriod;
                avgLoss = (avgLoss * (rsiPeriod - 1.0) + loss) / (double) rsiPeriod;
            }
        }

        double a12 = 2.0 / 13.0, a26 = 2.0 / 27.0, a9 = 2.0 / 10.0;
        ema12 = (ema12 == 0) ? barClose : (barClose - ema12) * a12 + ema12;
        ema26 = (ema26 == 0) ? barClose : (barClose - ema26) * a26 + ema26;
        double macd = ema12 - ema26;
        ema9Signal = (ema9Signal == 0) ? macd : (macd - ema9Signal) * a9 + ema9Signal;
        macdDiff = macd - ema9Signal;

        if (prevBarClose > 0) {
            double tr = Math.max(barHigh - barLow, Math.max(Math.abs(barHigh - prevBarClose), Math.abs(barLow - prevBarClose)));
            atr12 = (atr12 == 0) ? tr : (atr12 * 11.0 + tr) / 12.0;

            double barReturn = (barClose - prevBarClose) / prevBarClose;
            returnWindow20.addLast(barReturn);
            if (returnWindow20.size() > 20) returnWindow20.removeFirst();

            if (returnWindow20.size() > 1) {
                double realizedVol = stdDev(returnWindow20);
                realizedVolWindow100.addLast(realizedVol);
                if (realizedVolWindow100.size() > 100) realizedVolWindow100.removeFirst();
            }
        }

        prevBarClose = barClose;
        barsCount++;

        long deltaPut = Math.max(0L, latestPutVolume - prevPutVolume);
        long deltaCall = Math.max(0L, latestCallVolume - prevCallVolume);
        prevPutVolume = latestPutVolume;
        prevCallVolume = latestCallVolume;
        currentPutCallRatio = deltaCall > 0L ? (float) deltaPut / (float) deltaCall : 1.0f;
        lastTraining30sFeatureValues = constructTraining30sFeatureValueMap(time, wap, deltaPut, deltaCall, activeRegimeForFeatures());
        lastTraining30sClose = barClose;
        lastTraining30sEpoch = time;
        if (currentPosition != 0 && positionEntryPrice > 0.0) {
            barsSincePositionEntry30s++;
            updatePositionPathStats(barHigh, barLow, barClose, true);
        }

        if (!optionVolumeWarningLogged) {
            int hour = currentMarketTime.getHour();
            int minute = currentMarketTime.getMinute();
            boolean postOpenWindow = hour > 9 || (hour == 9 && minute >= 35);
            if (postOpenWindow && latestPutVolume == 0L && latestCallVolume == 0L) {
                optionVolumeWarningLogged = true;
                flowCondition("STRATEGY.OPTIONS", "OPTION_VOLUME_STREAM_AVAILABLE", false, "symbol=" + symbol + " putVol=" + latestPutVolume + " callVol=" + latestCallVolume);
            }
        }

        boolean openingThirty = isOpeningThirtyMinutes();
        int requiredBars = openingThirty ? OPEN30_MIN_BARS : REGULAR_MIN_BARS;
        boolean barsReadyForProfile = barsCount >= requiredBars;

        flowCondition(
            "STRATEGY.WARMUP",
            "BARS_READY_FOR_PROFILE",
            barsReadyForProfile,
            "symbol=" + symbol + " barsCount=" + barsCount + " required=" + requiredBars + " profile=" + (openingThirty ? "OPEN30" : "REGULAR")
        );
        flowCondition("STRATEGY.WARMUP", "NO_INFLIGHT_ORDER", !inFlightOrder, "symbol=" + symbol + " inFlightOrder=" + inFlightOrder);
        flowCondition("STRATEGY.WARMUP", "STRATEGY_ENABLED", enabled, "symbol=" + symbol + " enabled=" + enabled);
        flowCondition("STRATEGY.WARMUP", "CIRCUIT_BREAKER_CLEAR", !circuitBreakerTripped, "symbol=" + symbol + " circuitBreakerTripped=" + circuitBreakerTripped);

        System.out.printf(
            ">>> [30s DIAGNOSTIC] Vol=%d CumVol=%d VWAP=%.2f MACD=%.6f ATR=%.6f%n",
            barVolume, cumVol, vwap, macdDiff, atr12
        );

        currentBarVolAsk = 0L;
        currentBarVolBid = 0L;

        if (barsReadyForProfile && !inFlightOrder && enabled && !circuitBreakerTripped) {
            flowAnalyze("STRATEGY->AI", "Dispatching AI evaluation symbol=" + symbol + " time=" + currentMarketTime + " close=" + barClose);
            askArtificialIntelligence();
        }
    }

    private double stdDev(Deque<Double> values) {
        if (values == null || values.size() < 2) return 0.0;
        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double var = values.stream().mapToDouble(v -> Math.pow(v - mean, 2)).sum() / (values.size() - 1);
        return Math.sqrt(Math.max(0.0, var));
    }

    private double average(Deque<Double> values) {
        return values == null || values.isEmpty() ? 0.0 : values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    private MarketRegime activeRegimeForFeatures() {
        return currentMarketTime != null && isOpeningThirtyMinutes() ? MarketRegime.CHOPPY : lastDetectedRegime;
    }

    private Map<String, Float> constructTraining30sFeatureValueMap(long epoch, double wap, long putDelta, long callDelta, MarketRegime regime) {
        double ret1 = lastTraining30sClose > 0.0 ? (barClose / lastTraining30sClose) - 1.0 : 0.0;
        training30sReturnWindow20.addLast(ret1);
        if (training30sReturnWindow20.size() > 20) training30sReturnWindow20.removeFirst();
        training30sVolumeWindow20.addLast((double) barVolume);
        if (training30sVolumeWindow20.size() > 20) training30sVolumeWindow20.removeFirst();
        double optionFlow = putDelta + callDelta;
        training30sOptionFlowWindow20.addLast(optionFlow);
        if (training30sOptionFlowWindow20.size() > 20) training30sOptionFlowWindow20.removeFirst();

        Map<String, Float> values = commonTrainingFeatureValues(
            "f_30s_",
            epoch,
            barOpen,
            barHigh,
            barLow,
            barClose,
            barVolume,
            wap,
            ret1,
            pctReturnFrom30sWindow(3),
            stdDev(training30sReturnWindow20),
            average(training30sVolumeWindow20),
            putDelta,
            callDelta,
            average(training30sOptionFlowWindow20),
            true
        );
        values.put("f_regime_choppy", regime == MarketRegime.CHOPPY ? 1.0f : 0.0f);
        values.put("f_regime_trend", regime == MarketRegime.TREND ? 1.0f : 0.0f);
        values.put("f_regime_volatile", regime == MarketRegime.VOLATILE ? 1.0f : 0.0f);
        return values;
    }

    private Map<String, Float> commonTrainingFeatureValues(String prefix, long epoch, double open, double high, double low, double close,
                                                           long volume, double wap, double ret1, double ret3, double realizedVol20,
                                                           double avgVolume20, long putDelta, long callDelta, double avgOptionFlow20,
                                                           boolean thirtySecondCadence) {
        Map<String, Float> values = new HashMap<>();
        double safeClose = close > 0.0 ? close : 1.0;
        double effectiveWap = wap > 0.0 ? wap : close;
        double bid = latestBidPrice;
        double ask = latestAskPrice;
        double mid = bid > 0.0 && ask > 0.0 ? (bid + ask) / 2.0 : close;
        double spreadBps = mid > 0.0 && ask >= bid && bid > 0.0 ? ((ask - bid) / mid) * 10_000.0 : 0.0;
        long putTotal = latestPutVolume;
        long callTotal = latestCallVolume;
        long optionFlow = Math.max(0L, putDelta) + Math.max(0L, callDelta);
        LocalDateTime ts = LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC).atZone(ZoneId.of("UTC")).withZoneSameInstant(MARKET_ZONE).toLocalDateTime();
        int minuteOfDay = ts.getHour() * 60 + ts.getMinute();
        double normalizedTod = Math.max(0.0, Math.min(1.0, (minuteOfDay - (9 * 60 + 30)) / (6.5 * 60.0)));

        values.put(prefix + "ret_1", (float) ret1);
        values.put(prefix + "ret_3", (float) ret3);
        values.put(prefix + "range_pct", (float) ((high - low) / safeClose));
        values.put(prefix + "body_pct", (float) ((close - open) / safeClose));
        values.put(prefix + "upper_wick_pct", (float) ((high - Math.max(open, close)) / safeClose));
        values.put(prefix + "lower_wick_pct", (float) ((Math.min(open, close) - low) / safeClose));
        values.put(prefix + "vwap_dist", (float) ((close - effectiveWap) / safeClose));
        values.put(prefix + "rel_volume_20", (float) (volume / (avgVolume20 + 1.0)));
        values.put(prefix + "realized_vol_20", (float) realizedVol20);
        values.put(prefix + "spread_bps", (float) spreadBps);
        values.put(prefix + "option_put_delta", (float) putDelta);
        values.put(prefix + "option_call_delta", (float) callDelta);
        values.put(prefix + "option_put_call_ratio", (float) (putTotal / (callTotal + 1.0)));
        values.put(prefix + "option_delta_put_call_ratio", (float) (putDelta / (callDelta + 1.0)));
        values.put(prefix + "option_volume_burst", (float) (optionFlow / (avgOptionFlow20 + 1.0)));
        values.put(prefix + "time_of_day", (float) normalizedTod);
        values.put(prefix + "is_open_hour", minuteOfDay < 10 * 60 + 30 ? 1.0f : 0.0f);
        values.put(prefix + "is_close_hour", minuteOfDay >= 15 * 60 ? 1.0f : 0.0f);
        return values;
    }

    private double pctReturnFrom30sWindow(int barsBack) {
        if (barsBack <= 1 || training30sReturnWindow20.isEmpty()) {
            return training30sReturnWindow20.stream().reduce(0.0, (acc, r) -> acc + r);
        }
        double compounded = 1.0;
        Double[] returns = training30sReturnWindow20.toArray(new Double[0]);
        int start = Math.max(0, returns.length - barsBack);
        for (int i = start; i < returns.length; i++) {
            compounded *= (1.0 + returns[i]);
        }
        return compounded - 1.0;
    }

    private boolean evaluateExitSignal(String flowTag, String exitAction, boolean rsiGate, double rsiThreshold, LazyAiPredictor predictor,
                                       float[] features, double threshold, double currentRsi, double referencePrice,
                                       MarketRegime activeRegime) {
        boolean modelReady = predictor != null && predictor.isAvailable();
        double executablePrice = priceForAction(exitAction, referencePrice);
        String positionState = currentPosition > 0 ? "LONG" : (currentPosition < 0 ? "SHORT" : "FLAT");
        flowCondition(flowTag, "RSI_PRE_GATE", rsiGate, "symbol=" + symbol + " enabled=" + USE_RSI_PRE_GATES + " rsi=" + currentRsi + " threshold=" + rsiThreshold);
        flowCondition(flowTag, "MODEL_AVAILABLE", modelReady, "symbol=" + symbol + " regime=" + activeRegime);
        flowData(
            flowTag,
            "EVAL_CONTEXT symbol=" + symbol
                + " position=" + currentPosition
                + " positionState=" + positionState
                + " action=" + exitAction
                + " regime=" + activeRegime
                + " close=" + referencePrice
                + " bid=" + latestBidPrice
                + " ask=" + latestAskPrice
                + " execPx=" + executablePrice
                + " expectedThreshold=" + formatProb(threshold)
                + " rsi=" + String.format("%.2f", currentRsi)
                + " rsiGate=" + rsiGate
                + " modelReady=" + modelReady
        );
        if (!rsiGate || !modelReady) {
            flowData(
                flowTag,
                "EVAL_RESULT symbol=" + symbol
                    + " action=" + exitAction
                    + " prob=NA"
                    + " expectedThreshold=" + formatProb(threshold)
                    + " close=" + referencePrice
                    + " execPx=" + executablePrice
                    + " decision=false"
                    + " skippedBy=" + (!rsiGate ? "RSI_GATE" : "MODEL_UNAVAILABLE")
            );
            return false;
        }

        double prob = predictor.predictProbability(features);
        boolean shouldExit = prob >= threshold;
        flowData(
            flowTag,
            "EVAL_RESULT symbol=" + symbol
                + " action=" + exitAction
                + " prob=" + formatProb(prob)
                + " expectedThreshold=" + formatProb(threshold)
                + " close=" + referencePrice
                + " bid=" + latestBidPrice
                + " ask=" + latestAskPrice
                + " execPx=" + executablePrice
                + " decision=" + shouldExit
        );
        flowCondition(
            flowTag,
            "AI_PREDICTS_EXIT",
            shouldExit,
            "symbol=" + symbol
                + " rsi=" + currentRsi
                + " close=" + referencePrice
                + " prob=" + formatProb(prob)
                + " threshold=" + formatProb(threshold)
        );
        return shouldExit;
    }

    private boolean evaluateLifecycleExitSignal(String side, LazyAiPredictor predictor, double threshold, double referencePrice) {
        boolean modelReady = predictor != null && predictor.isAvailable();
        boolean positionReady = positionEntryPrice > 0.0 && currentPosition != 0;
        flowCondition("AI.LIFECYCLE.EXIT", "MODEL_AVAILABLE", modelReady, "symbol=" + symbol + " side=" + side);
        flowCondition("AI.LIFECYCLE.EXIT", "POSITION_CONTEXT_AVAILABLE", positionReady, "symbol=" + symbol + " side=" + side + " entryPrice=" + positionEntryPrice + " position=" + currentPosition);
        if (!modelReady || !positionReady) {
            return false;
        }
        Map<String, Float> features = new LinkedHashMap<>(lastTraining30sFeatureValues);
        features.putAll(positionFeatureValues(referencePrice, true));
        String modelName = "long".equalsIgnoreCase(side) ? "longExitLifecycleAi" : "shortExitLifecycleAi";
        double prob = predictUpgradedRouteProbability(modelName, predictor, buildFeatureVector(LIFECYCLE_FEATURE_COLUMNS, features));
        boolean shouldExit = prob >= threshold;
        flowCondition(
            "AI.LIFECYCLE.EXIT",
            "LIFECYCLE_EXIT_TRIGGERS",
            shouldExit,
            "symbol=" + symbol + " side=" + side + " prob=" + formatProb(prob) + " threshold=" + formatProb(threshold) + " unrealizedR=" + features.getOrDefault("f_unrealized_pnl_r", 0.0f)
        );
        float unrealizedR = features.getOrDefault("f_unrealized_pnl_r", 0.0f);
        long epoch = currentMarketTime == null ? 0L : currentMarketTime.atZone(MARKET_ZONE).toEpochSecond();
        emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onLifecycleExitEvaluated(symbol, side, epoch, prob, threshold, shouldExit, unrealizedR));
        return shouldExit;
    }

    private Map<String, Float> positionFeatureValues(double referencePrice, boolean lifecycleCadence) {
        Map<String, Float> values = new HashMap<>();
        double unrealizedR = unrealizedR(referencePrice);
        values.put("f_entry_score_proxy", (float) positionEntryProbability);
        values.put("f_entry_prob", (float) positionEntryProbability);
        values.put("f_entry_threshold", (float) positionEntryThreshold);
        values.put("f_entry_threshold_margin", (float) positionEntryThresholdMargin);
        values.put("f_entry_side_long", positionEntrySide > 0 ? 1.0f : 0.0f);
        values.put("f_entry_side_short", positionEntrySide < 0 ? 1.0f : 0.0f);
        values.put("f_pos_side", positionEntrySide > 0 ? 1.0f : -1.0f);
        values.put("f_bars_since_entry", (float) (barsSincePositionEntry30s / Math.max(1.0, LIFECYCLE_HORIZON_30S)));
        values.put("f_bars_since_entry_5s", (float) barsSincePositionEntry5s);
        values.put("f_unrealized_pnl_r", (float) unrealizedR);
        values.put("f_mfe_r", (float) positionMfeR);
        values.put("f_mae_r", (float) positionMaeR);
        values.put("f_target_remaining_r", (float) ((LIFECYCLE_ENTRY_PROFIT_PCT / LIFECYCLE_ENTRY_RISK_PCT) - unrealizedR));
        values.put("f_stop_remaining_r", (float) (unrealizedR + 1.0));
        return values;
    }

    private void updatePositionPathStats(double high, double low, double close, boolean thirtySecondCadence) {
        if (positionEntryPrice <= 0.0 || positionEntrySide == 0) {
            return;
        }
        if (!thirtySecondCadence) {
            barsSincePositionEntry5s++;
        }
        double favR;
        double advR;
        if (positionEntrySide > 0) {
            favR = ((Math.max(high, close) - positionEntryPrice) / positionEntryPrice) / LIFECYCLE_ENTRY_RISK_PCT;
            advR = ((Math.min(low, close) - positionEntryPrice) / positionEntryPrice) / LIFECYCLE_ENTRY_RISK_PCT;
        } else {
            favR = ((positionEntryPrice - Math.min(low, close)) / positionEntryPrice) / LIFECYCLE_ENTRY_RISK_PCT;
            advR = ((positionEntryPrice - Math.max(high, close)) / positionEntryPrice) / LIFECYCLE_ENTRY_RISK_PCT;
        }
        positionMfeR = Math.max(positionMfeR, favR);
        positionMaeR = Math.min(positionMaeR, advR);
    }

    private double unrealizedR(double referencePrice) {
        if (positionEntryPrice <= 0.0 || referencePrice <= 0.0 || positionEntrySide == 0) {
            return 0.0;
        }
        double pnlPct = positionEntrySide > 0
            ? (referencePrice - positionEntryPrice) / positionEntryPrice
            : (positionEntryPrice - referencePrice) / positionEntryPrice;
        return pnlPct / LIFECYCLE_ENTRY_RISK_PCT;
    }

    private void askArtificialIntelligence() {
        // AI routing is structured around position-aware execution:
        //  1) long position  -> evaluate long-exit models and only SELL if there is inventory to close
        //  2) short position -> evaluate short-exit models and only BUY if there is inventory to cover
        //  3) flat           -> still evaluate both exit models for research/telemetry, then evaluate entries
        //
        // This keeps exit-model telemetry flowing even while flat without allowing flat-state exit signals to submit
        // contradictory closing orders.
        aiDecisionEvaluationCount++;
        boolean timeReady = currentMarketTime != null;
        flowCondition("AI.GATE", "CURRENT_MARKET_TIME_PRESENT", timeReady, "symbol=" + symbol + " currentMarketTime=" + currentMarketTime);
        if (!timeReady) {
            aiMissingMarketTimeCount++;
            return;
        }

        double currentRsi = calculateRsi();
        int currentHour = currentMarketTime.getHour();
        
        flowAnalyze("AI.INPUT", "symbol=" + symbol + " rsi=" + String.format("%.2f", currentRsi) + " position=" + currentPosition + " tradeCount=" + tradeCount + " maxTrades=" + maxTrades + " allowNewEntries=" + allowNewEntries);

        boolean sessionAllowed = !(currentHour == 9 && currentMarketTime.getMinute() < 30);
        flowCondition("AI.GATE", "SESSION_AFTER_0930", sessionAllowed, "symbol=" + symbol + " time=" + currentMarketTime);
        if (!sessionAllowed) {
            aiPreMarketBlockedCount++;
            return;
        }

        if (yesterdayClose > 0) {
            double variance = Math.abs(barClose - yesterdayClose) / yesterdayClose;
            boolean varianceAllowed = variance <= 0.05;
            flowCondition("AI.GATE", "YESTERDAY_CLOSE_AVAILABLE", true, "symbol=" + symbol + " yesterdayClose=" + yesterdayClose);
            flowCondition("AI.GATE", "PRICE_VARIANCE_LE_5PCT", varianceAllowed, "symbol=" + symbol + " variance=" + variance + " close=" + barClose + " yesterdayClose=" + yesterdayClose);
            if (!varianceAllowed) {
                aiVarianceBlockedCount++;
                return;
            }
        } else {
            aiMissingPreviousCloseCount++;
            flowCondition("AI.GATE", "YESTERDAY_CLOSE_AVAILABLE", false, "symbol=" + symbol + " yesterdayClose=" + yesterdayClose);
        }

        boolean openingThirty = isOpeningThirtyMinutes();
        Map<String, Float> liveFeatureValues = constructFeatureValueMap(currentRsi);
        float[] loggedBaseFeatures = buildFeatureVector(BASE_FEATURE_COLUMNS, liveFeatureValues);
        flowData("AI.INPUT", "symbol=" + symbol + " features=" + Arrays.toString(loggedBaseFeatures));

        int regimeExpectedFeatureCount = expectedFeatureCountForModel(regimeClassifierAi, BASE_PLUS_NEWS_FEATURE_COLUMNS.size());
        float[] regimeFeatures = buildRegimeClassifierFeaturesForExpectedCount(regimeExpectedFeatureCount, liveFeatureValues);
        RegimeDecision regimeDecision = openingThirty
            ? new RegimeDecision(MarketRegime.CHOPPY, defaultRegimeProbabilityFeatures(1.0, 0.0, 0.0))
            : detectMarketRegime(regimeFeatures);
        liveFeatureValues.putAll(regimeDecision.probabilityFeatures());
        MarketRegime activeRegime = regimeDecision.regime();
        boolean lifecycleExitRouteAvailable = !LIFECYCLE_EXIT_ENABLED || (longExitLifecycleAi != null && shortExitLifecycleAi != null);
        if (!lifecycleExitRouteAvailable) {
            activeRegime = MarketRegime.CHOPPY;
            liveFeatureValues.putAll(defaultRegimeProbabilityFeatures(1.0, 0.0, 0.0));
            flowError("AI.ROUTE", "Lifecycle exit route unavailable; forcing CHOPPY/default routing symbol=" + symbol);
        }
        LazyAiPredictor activeLongEntryAi;
        LazyAiPredictor activeShortEntryAi;
        LazyAiPredictor activeLongExitAi = null;
        LazyAiPredictor activeShortExitAi = null;

        if (openingThirty) {
            activeLongEntryAi = open30LongEntryAi != null ? open30LongEntryAi : longEntryAi;
            activeShortEntryAi = open30ShortEntryAi != null ? open30ShortEntryAi : shortEntryAi;
            if (LEGACY_30S_EXIT_ENABLED && !LIFECYCLE_EXIT_ENABLED) {
                activeLongExitAi = open30LongExitAi != null ? open30LongExitAi : longExitAi;
                activeShortExitAi = open30ShortExitAi != null ? open30ShortExitAi : shortExitAi;
            }
            flowData("AI.ROUTER", "symbol=" + symbol + " profile=OPEN30");
        } else {
            activeLongEntryAi = modelForRegime(activeRegime, longEntryAi, choppyLongEntryAi, trendLongEntryAi, volatileLongEntryAi);
            activeShortEntryAi = modelForRegime(activeRegime, shortEntryAi, choppyShortEntryAi, trendShortEntryAi, volatileShortEntryAi);
            if (LEGACY_30S_EXIT_ENABLED && !LIFECYCLE_EXIT_ENABLED) {
                activeLongExitAi = modelForRegime(activeRegime, longExitAi, choppyLongExitAi, trendLongExitAi, volatileLongExitAi);
                activeShortExitAi = modelForRegime(activeRegime, shortExitAi, choppyShortExitAi, trendShortExitAi, volatileShortExitAi);
            }
            flowData("AI.ROUTER", "symbol=" + symbol + " profile=REGIME activeRegime=" + activeRegime);
        }

        double activeLongEntryThreshold = aiThresholdConfig.thresholdFor(openingThirty, activeRegime, ThresholdAction.LONG_ENTRY);
        double activeShortEntryThreshold = aiThresholdConfig.thresholdFor(openingThirty, activeRegime, ThresholdAction.SHORT_ENTRY);
        double activeLongExitThreshold = aiThresholdConfig.thresholdFor(openingThirty, activeRegime, ThresholdAction.LONG_EXIT);
        double activeShortExitThreshold = aiThresholdConfig.thresholdFor(openingThirty, activeRegime, ThresholdAction.SHORT_EXIT);
        flowData(
            "AI.ROUTER",
            "symbol=" + symbol
                + " thresholdProfile=" + (openingThirty ? "OPEN30" : activeRegime)
                + " longEntry=" + formatProb(activeLongEntryThreshold)
                + " shortEntry=" + formatProb(activeShortEntryThreshold)
                + " longExit=" + formatProb(activeLongExitThreshold)
                + " shortExit=" + formatProb(activeShortExitThreshold)
        );

        int longEntryFeatureCount = expectedFeatureCountForModel(activeLongEntryAi, BASE_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS.size());
        int shortEntryFeatureCount = expectedFeatureCountForModel(activeShortEntryAi, BASE_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS.size());
        float[] longEntryFeatures = buildFeatureVectorForExpectedCount(longEntryFeatureCount, liveFeatureValues);
        float[] shortEntryFeatures = buildFeatureVectorForExpectedCount(shortEntryFeatureCount, liveFeatureValues);
        float[] longExitFeatures = new float[0];
        float[] shortExitFeatures = new float[0];
        if (LEGACY_30S_EXIT_ENABLED && !LIFECYCLE_EXIT_ENABLED) {
            int longExitFeatureCount = expectedFeatureCountForModel(activeLongExitAi, BASE_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS.size());
            int shortExitFeatureCount = expectedFeatureCountForModel(activeShortExitAi, BASE_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS.size());
            longExitFeatures = buildFeatureVectorForExpectedCount(longExitFeatureCount, liveFeatureValues);
            shortExitFeatures = buildFeatureVectorForExpectedCount(shortExitFeatureCount, liveFeatureValues);
        }
        flowData(
            "AI.ROUTER",
            "symbol=" + symbol
                + " featureCounts longEntry=" + longEntryFeatures.length
                + " shortEntry=" + shortEntryFeatures.length
                + " longExit=" + longExitFeatures.length
                + " shortExit=" + shortExitFeatures.length
                + " regime=" + regimeFeatures.length
        );

        boolean shouldExitLong = false;
        if (currentPosition > 0 && LIFECYCLE_EXIT_ENABLED) {
            shouldExitLong = evaluateLifecycleExitSignal("long", longExitLifecycleAi, upgradedRouteThreshold("longExitLifecycleAi", LIFECYCLE_LONG_EXIT_THRESHOLD), barClose);
        } else if (currentPosition >= 0 && LEGACY_30S_EXIT_ENABLED && !LIFECYCLE_EXIT_ENABLED) {
            boolean rsiGate = !USE_RSI_PRE_GATES || currentRsi > RSI_LONG_EXIT_THRESHOLD;
            shouldExitLong = evaluateExitSignal(
                "AI.LONG.EXIT",
                "SELL",
                rsiGate,
                RSI_LONG_EXIT_THRESHOLD,
                activeLongExitAi,
                longExitFeatures,
                activeLongExitThreshold,
                currentRsi,
                barClose,
                activeRegime
            );
        }

        boolean shouldExitShort = false;
        if (currentPosition < 0 && LIFECYCLE_EXIT_ENABLED) {
            shouldExitShort = evaluateLifecycleExitSignal("short", shortExitLifecycleAi, upgradedRouteThreshold("shortExitLifecycleAi", LIFECYCLE_SHORT_EXIT_THRESHOLD), barClose);
        } else if (currentPosition <= 0 && LEGACY_30S_EXIT_ENABLED && !LIFECYCLE_EXIT_ENABLED) {
            boolean rsiGate = !USE_RSI_PRE_GATES || currentRsi < RSI_SHORT_EXIT_THRESHOLD;
            shouldExitShort = evaluateExitSignal(
                "AI.SHORT.EXIT",
                "BUY",
                rsiGate,
                RSI_SHORT_EXIT_THRESHOLD,
                activeShortExitAi,
                shortExitFeatures,
                activeShortExitThreshold,
                currentRsi,
                barClose,
                activeRegime
            );
        }

        // ==========================================
        // SCENARIO 1: WE ARE ALREADY LONG
        // ==========================================
        if (currentPosition > 0) {
            aiPositionOpenSkippedCount++;
            if (shouldExitLong) {
                flowInfo("AI.LONG.EXIT", "Top detector signaled exit. Taking LONG profits symbol=" + symbol);
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "SELL", priceForAction("SELL", barClose), Math.abs(currentPosition), "MKT");
            }
            return; 
        }

        // ==========================================
        // SCENARIO 2: WE ARE ALREADY SHORT
        // ==========================================
        if (currentPosition < 0) {
            aiPositionOpenSkippedCount++;
            if (shouldExitShort) {
                flowInfo("AI.SHORT.EXIT", "Bottom detector signaled cover. Covering SHORT symbol=" + symbol);
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "BUY", priceForAction("BUY", barClose), Math.abs(currentPosition), "MKT");
            }
            return; 
        }

        if (shouldExitLong) {
            flowCondition("AI.LONG.EXIT", "POSITION_GATE", false, "symbol=" + symbol + " position=" + currentPosition + " note=flat_signal_logged_only");
        }
        if (shouldExitShort) {
            flowCondition("AI.SHORT.EXIT", "POSITION_GATE", false, "symbol=" + symbol + " position=" + currentPosition + " note=flat_signal_logged_only");
        }

        // ==========================================
        // SCENARIO 3: WE ARE FLAT (LOOKING FOR ENTRIES)
        // ==========================================
        aiFlatEntryEvaluationCount++;
        flowCondition("AI.ENTRY", "POSITION_SYNCED", positionSynced, "symbol=" + symbol + " positionSynced=" + positionSynced);
        long nowMs = System.currentTimeMillis();
        long hardStopCooldownRemainingMs = Math.max(0L, postHardStopEntryCooldownMs - (nowMs - lastHardStopExitTimeMs));
        boolean hardStopCooldownElapsed = lastHardStopExitTimeMs <= 0L || hardStopCooldownRemainingMs == 0L;
        boolean hardStopBudgetAvailable = hardStopExitCount < maxHardStopsPerDay;
        boolean entryGateOpen = allowNewEntries && tradeCount < maxTrades && positionSynced && hardStopCooldownElapsed && hardStopBudgetAvailable;
        flowCondition(
            "AI.ENTRY",
            "ENTRY_GATE_OPEN",
            entryGateOpen,
            "symbol=" + symbol
                + " allowNewEntries=" + allowNewEntries
                + " tradeCount=" + tradeCount
                + " maxTrades=" + maxTrades
                + " positionSynced=" + positionSynced
                + " hardStopExitCount=" + hardStopExitCount
                + " maxHardStopsPerDay=" + maxHardStopsPerDay
                + " hardStopCooldownElapsed=" + hardStopCooldownElapsed
                + " hardStopCooldownRemainingMs=" + hardStopCooldownRemainingMs
        );
        if (!entryGateOpen) {
            recordClosedEntryGate(allowNewEntries, tradeCount, maxTrades, positionSynced, hardStopCooldownElapsed, hardStopBudgetAvailable);
        }
        if (entryGateOpen) {
            aiEntryGateOpenCount++;
            double buyReferencePrice = priceForAction("BUY", barClose);
            double sellReferencePrice = priceForAction("SELL", barClose);
            int buyQty = sharesForAmount("BUY", barClose);
            int sellQty = sharesForAmount("SELL", barClose);
            flowCondition("AI.ENTRY", "BUY_QTY_POSITIVE", buyQty > 0, "symbol=" + symbol + " qty=" + buyQty + " askOrFallback=" + buyReferencePrice);
            flowCondition("AI.ENTRY", "SELL_QTY_POSITIVE", sellQty > 0, "symbol=" + symbol + " qty=" + sellQty + " bidOrFallback=" + sellReferencePrice);
            if (buyQty <= 0) {
                aiBuyQuantityBlockedCount++;
            }
            if (sellQty <= 0) {
                aiSellQuantityBlockedCount++;
            }

            // --- DIP BUYING (LONG ENTRY) ---
            double longThreshold = (currentHour == 9) ? RSI_LONG_ENTRY_OPEN_THRESHOLD : RSI_LONG_ENTRY_REGULAR_THRESHOLD;
            boolean longRsiGate = !USE_RSI_PRE_GATES || currentRsi < longThreshold;
            boolean longModelReady = activeLongEntryAi != null && activeLongEntryAi.isAvailable();
            flowCondition("AI.LONG.ENTRY", "RSI_PRE_GATE", longRsiGate, "symbol=" + symbol + " enabled=" + USE_RSI_PRE_GATES + " rsi=" + currentRsi + " threshold=" + longThreshold);
            flowCondition("AI.LONG.ENTRY", "MODEL_AVAILABLE", longModelReady, "symbol=" + symbol + " regime=" + activeRegime);
            if (!longRsiGate) {
                aiLongRsiGateBlockedCount++;
            }
            if (!longModelReady) {
                aiLongModelUnavailableCount++;
            }
            boolean shouldEnterLong = false;
            double longEntryProb = 0.0;
            if (longRsiGate && longModelReady) {
                aiLongEntryModelEvaluationCount++;
                longEntryProb = activeLongEntryAi.predictProbability(longEntryFeatures);
                shouldEnterLong = longEntryProb >= activeLongEntryThreshold;
                if (shouldEnterLong) {
                    aiLongEntryPassCount++;
                }
                recordSetupCandidate("long", longEntryProb, activeLongEntryThreshold, currentRsi, buyReferencePrice, buyQty, activeRegime);
                flowCondition(
                    "AI.LONG.ENTRY",
                    "AI_PREDICTS_ENTRY",
                    shouldEnterLong,
                    "symbol=" + symbol
                        + " rsi=" + currentRsi
                            + " askOrFallback=" + buyReferencePrice
                            + " qty=" + buyQty
                        + " prob=" + formatProb(longEntryProb)
                        + " threshold=" + formatProb(activeLongEntryThreshold)
                );
            }
            if (shouldEnterLong && buyQty > 0) {
                if (MICRO_ENTRY_ENABLED) {
                    armMicroEntry("long", lastTraining30sFeatureValues, currentMicroArmEpoch(), longEntryProb, activeLongEntryThreshold);
                    return;
                }
                flowInfo("AI.LONG.ENTRY", "Dip buyer firing order symbol=" + symbol + " rsi=" + String.format("%.2f", currentRsi));
                pendingEntryProbability = longEntryProb;
                pendingEntryThreshold = activeLongEntryThreshold;
                pendingEntryThresholdMargin = longEntryProb - activeLongEntryThreshold;
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "BUY", buyReferencePrice, buyQty, "FAST_LMT");
                return;
            }

            // --- RIP SELLING (SHORT ENTRY) ---
            double shortThreshold = (currentHour == 9) ? RSI_SHORT_ENTRY_OPEN_THRESHOLD : RSI_SHORT_ENTRY_REGULAR_THRESHOLD;
            boolean shortRsiGate = !USE_RSI_PRE_GATES || currentRsi > shortThreshold;
            boolean shortModelReady = activeShortEntryAi != null && activeShortEntryAi.isAvailable();
            flowCondition("AI.SHORT.ENTRY", "RSI_PRE_GATE", shortRsiGate, "symbol=" + symbol + " enabled=" + USE_RSI_PRE_GATES + " rsi=" + currentRsi + " threshold=" + shortThreshold);
            flowCondition("AI.SHORT.ENTRY", "MODEL_AVAILABLE", shortModelReady, "symbol=" + symbol + " regime=" + activeRegime);
            if (!shortRsiGate) {
                aiShortRsiGateBlockedCount++;
            }
            if (!shortModelReady) {
                aiShortModelUnavailableCount++;
            }
            boolean shouldEnterShort = false;
            double shortEntryProb = 0.0;
            if (shortRsiGate && shortModelReady) {
                aiShortEntryModelEvaluationCount++;
                shortEntryProb = activeShortEntryAi.predictProbability(shortEntryFeatures);
                shouldEnterShort = shortEntryProb >= activeShortEntryThreshold;
                if (shouldEnterShort) {
                    aiShortEntryPassCount++;
                }
                recordSetupCandidate("short", shortEntryProb, activeShortEntryThreshold, currentRsi, sellReferencePrice, sellQty, activeRegime);
                flowCondition(
                    "AI.SHORT.ENTRY",
                    "AI_PREDICTS_ENTRY",
                    shouldEnterShort,
                    "symbol=" + symbol
                        + " rsi=" + currentRsi
                            + " bidOrFallback=" + sellReferencePrice
                            + " qty=" + sellQty
                        + " prob=" + formatProb(shortEntryProb)
                        + " threshold=" + formatProb(activeShortEntryThreshold)
                );
            }
            if (shouldEnterShort && sellQty > 0) {
                if (MICRO_ENTRY_ENABLED) {
                    armMicroEntry("short", lastTraining30sFeatureValues, currentMicroArmEpoch(), shortEntryProb, activeShortEntryThreshold);
                    return;
                }
                flowInfo("AI.SHORT.ENTRY", "Rip seller firing order symbol=" + symbol + " rsi=" + String.format("%.2f", currentRsi));
                pendingEntryProbability = shortEntryProb;
                pendingEntryThreshold = activeShortEntryThreshold;
                pendingEntryThresholdMargin = shortEntryProb - activeShortEntryThreshold;
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "SELL", sellReferencePrice, sellQty, "FAST_LMT");
            }
        }
    }

    private void recordClosedEntryGate(boolean allowEntries, int tradesSoFar, int maxAllowedTrades,
                                       boolean synced, boolean hardStopCooldownElapsed,
                                       boolean hardStopBudgetAvailable) {
        aiEntryGateClosedCount++;
        if (!allowEntries) {
            aiAllowNewEntriesBlockedCount++;
        }
        if (tradesSoFar >= maxAllowedTrades) {
            aiMaxTradesBlockedCount++;
        }
        if (!synced) {
            aiPositionSyncBlockedCount++;
        }
        if (!hardStopCooldownElapsed) {
            aiHardStopCooldownBlockedCount++;
        }
        if (!hardStopBudgetAvailable) {
            aiHardStopBudgetBlockedCount++;
        }
    }

    private void recordSetupCandidate(String side, double probability, double threshold, double currentRsi,
                                      double referencePrice, int quantity, MarketRegime activeRegime) {
        if (!Double.isFinite(probability) || !Double.isFinite(threshold)) {
            return;
        }
        double margin = probability - threshold;
        long epoch = currentMicroArmEpoch();
        String marketTime = currentMarketTime == null ? "" : currentMarketTime.toString();
        String normalizedSide = side == null ? "" : side.toLowerCase(Locale.US);
        String regime = activeRegime == null ? "" : activeRegime.name();

        if ("long".equals(normalizedSide)) {
            if (probability > aiMaxLongEntryProbability) {
                aiMaxLongEntryProbability = probability;
                aiMaxLongEntryThreshold = threshold;
                aiMaxLongEntryMargin = margin;
                aiMaxLongEntryEpoch = epoch;
                aiMaxLongEntryTime = marketTime;
            }
            if (margin < 0.0 && margin >= -Math.abs(AI_DIAGNOSTIC_NEAR_MISS_MARGIN)) {
                aiLongNearMissCount++;
            }
        } else if ("short".equals(normalizedSide)) {
            if (probability > aiMaxShortEntryProbability) {
                aiMaxShortEntryProbability = probability;
                aiMaxShortEntryThreshold = threshold;
                aiMaxShortEntryMargin = margin;
                aiMaxShortEntryEpoch = epoch;
                aiMaxShortEntryTime = marketTime;
            }
            if (margin < 0.0 && margin >= -Math.abs(AI_DIAGNOSTIC_NEAR_MISS_MARGIN)) {
                aiShortNearMissCount++;
            }
        }

        synchronized (aiClosestSetupEvents) {
            aiClosestSetupEvents.add(new SetupCandidateDiagnostic(
                normalizedSide,
                epoch,
                marketTime,
                probability,
                threshold,
                margin,
                currentRsi,
                referencePrice,
                quantity,
                regime
            ));
            aiClosestSetupEvents.sort((left, right) -> Double.compare(right.margin(), left.margin()));
            int maxEvents = Math.max(0, AI_DIAGNOSTIC_TOP_SETUP_EVENTS);
            while (aiClosestSetupEvents.size() > maxEvents) {
                aiClosestSetupEvents.remove(aiClosestSetupEvents.size() - 1);
            }
        }
    }

    private double calculateRsi() {
        double currentRsi = 50.0;
        if (avgLoss > 0) {
            currentRsi = 100.0 - (100.0 / (1.0 + (avgGain / avgLoss)));
        } else if (avgGain > 0) {
            currentRsi = 100.0;
        }
        return currentRsi;
    }

    private float[] buildRegimeClassifierFeaturesForExpectedCount(int expectedFeatureCount, Map<String, Float> liveFeatureValues) {
        List<String> regimeColumns = regimeFeatureColumnsForExpectedCount(expectedFeatureCount);
        float[] regimeFeatures = buildFeatureVector(regimeColumns, liveFeatureValues);
        flowData(
            "AI.REGIME",
            "symbol=" + symbol
                + " regimeFeatureCount=" + regimeFeatures.length
                + " baseFeatureCount=" + liveFeatureColumnsForExpectedCount(expectedFeatureCount).size()
                + " expectedFeatureCount=" + expectedFeatureCount
        );
        return regimeFeatures;
    }

    private float[] constructModelFeatures(double currentRsi) {
        return buildFeatureVectorForExpectedCount(BASE_LIVE_FEATURE_COUNT, constructFeatureValueMap(currentRsi));
    }

    private float[] buildFeatureVectorForExpectedCount(int expectedFeatureCount, Map<String, Float> liveFeatureValues) {
        return buildFeatureVector(liveFeatureColumnsForExpectedCount(expectedFeatureCount), liveFeatureValues);
    }

    private List<String> liveFeatureColumnsForExpectedCount(int expectedFeatureCount) {
        if (expectedFeatureCount <= LEGACY_LIVE_FEATURE_COUNT) {
            return LEGACY_FEATURE_COLUMNS;
        }
        if (expectedFeatureCount <= BASE_LIVE_FEATURE_COUNT) {
            return BASE_FEATURE_COLUMNS;
        }
        if (expectedFeatureCount <= EXTENDED_LIVE_FEATURE_COUNT) {
            return EXTENDED_FEATURE_COLUMNS;
        }
        if (expectedFeatureCount <= BASE_PLUS_NEWS_FEATURE_COLUMNS.size()) {
            return BASE_PLUS_NEWS_FEATURE_COLUMNS;
        }
        if (expectedFeatureCount <= BASE_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS.size()) {
            return BASE_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS;
        }
        if (expectedFeatureCount <= EXTENDED_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS.size()) {
            return EXTENDED_PLUS_NEWS_PLUS_REGIME_PROBABILITY_COLUMNS;
        }
        if (expectedFeatureCount <= BASE_PLUS_NEWS_PLUS_META_PRODUCER_COLUMNS.size()) {
            return BASE_PLUS_NEWS_PLUS_META_PRODUCER_COLUMNS;
        }
        return ENHANCED_MAIN_FEATURE_COLUMNS;
    }

    private List<String> regimeFeatureColumnsForExpectedCount(int expectedFeatureCount) {
        if (expectedFeatureCount <= LEGACY_LIVE_FEATURE_COUNT) {
            return LEGACY_REGIME_FEATURE_COLUMNS;
        }
        if (expectedFeatureCount <= BASE_LIVE_FEATURE_COUNT) {
            return BASE_REGIME_FEATURE_COLUMNS;
        }
        if (expectedFeatureCount <= EXTENDED_LIVE_FEATURE_COUNT) {
            return EXTENDED_REGIME_FEATURE_COLUMNS;
        }
        if (expectedFeatureCount < ENHANCED_REGIME_FEATURE_COLUMNS.size()) {
            return NEWS_AWARE_REGIME_FEATURE_COLUMNS;
        }
        return ENHANCED_REGIME_FEATURE_COLUMNS;
    }

    private int expectedFeatureCountForModel(LazyAiPredictor predictor, int fallbackFeatureCount) {
        if (predictor == null) {
            return fallbackFeatureCount;
        }
        int expected = predictor.expectedFeatureCountOr(fallbackFeatureCount);
        if (expected <= 0) {
            return fallbackFeatureCount;
        }
        return expected;
    }

    private float[] buildFeatureVector(List<String> featureColumns, Map<String, Float> liveFeatureValues) {
        if (featureColumns == null || featureColumns.isEmpty()) {
            return new float[0];
        }
        float[] features = new float[featureColumns.size()];
        for (int i = 0; i < featureColumns.size(); i++) {
            features[i] = liveFeatureValues.getOrDefault(featureColumns.get(i), 0.0f);
        }
        return features;
    }

    private Map<String, Float> constructFeatureValueMap(double currentRsi) {
        float f_dist_vwap = (float) ((barClose - vwap) / vwap);

        double bbMean = bbWindow.stream().mapToDouble(d -> d).average().orElse(barClose);
        double sumSq = bbWindow.stream().mapToDouble(d -> Math.pow(d - bbMean, 2)).sum();
        double variance = bbWindow.size() > 1 ? sumSq / (bbWindow.size() - 1) : 0.0;
        double stdDev = Math.sqrt(variance);
        double lowerBb = bbMean - (2.5 * stdDev);
        double upperBb = bbMean + (2.5 * stdDev);
        
        float f_bb_lower_dist = (float) ((barClose - lowerBb) / barClose);
        float f_bb_upper_dist = (float) ((upperBb - barClose) / barClose);
        
        float f_macd_diff = (float) macdDiff;
        
        float f_body_size = (float) (Math.abs(barClose - barOpen) / barClose);
        float f_lower_wick = (float) ((Math.min(barOpen, barClose) - barLow) / barClose);
        float f_upper_wick = (float) ((barHigh - Math.max(barOpen, barClose)) / barClose);
        
        float f_atr_norm = (float) (atr12 / barClose);
        
        double sma = smaWindow.stream().mapToDouble(d -> d).average().orElse(barClose);
        float f_dist_sma = (float) ((barClose - sma) / sma);
        
        float f_dist_high = (float) ((dayHigh - barClose) / barClose);
        float f_dist_low = (float) ((barClose - dayLow) / barClose);
        
        float f_rsi = (float) currentRsi;
        float f_gap_from_prev_close = yesterdayClose > 0.0
            ? (float) ((barClose - yesterdayClose) / yesterdayClose)
            : 0.0f;

        // TIME OF DAY
        int hour = currentMarketTime.getHour();
        int minute = currentMarketTime.getMinute();
        float f_time_of_day = (hour * 60 + minute) / (24.0f * 60.0f);

        // PRICE ACTION
        double swingHigh = highWindow.stream().mapToDouble(d -> d).max().orElse(barHigh);
        double swingLow = lowWindow.stream().mapToDouble(d -> d).min().orElse(barLow);

        float f_dist_swing_high = (float) ((swingHigh - barClose) / barClose);
        float f_dist_swing_low = (float) ((barClose - swingLow) / barClose);
        float f_is_new_high = (barClose >= dayHigh) ? 1.0f : 0.0f;
        float f_is_new_low = (barClose <= dayLow) ? 1.0f : 0.0f;
        
        // WHOLE NUMBER & CANDLE DIRECTION
        double nearestWhole = Math.round(barClose);
        float f_dist_whole_num = (float) Math.abs(barClose - nearestWhole);
        float f_is_green = (barClose >= barOpen) ? 1.0f : -1.0f;
        float f_green_streak = (float) greenStreak;
        float f_red_streak = (float) redStreak;
        
        float f_put_call_ratio = currentPutCallRatio;
        float f_vol_ask_ratio = 0.33f;
        float f_vol_bid_ratio = 0.33f;
        // FIX: Only calculate ratios if we ACTUALLY received live tape data.
        // In historical backtesting, this stays false, leaving the ratios at 0.33 to match Python perfectly.
        if ((currentBarVolAsk > 0 || currentBarVolBid > 0) && barVolume > 0) {
            f_vol_ask_ratio = (float) currentBarVolAsk / (float) barVolume;
            f_vol_bid_ratio = (float) currentBarVolBid / (float) barVolume;
        }

        // --- Extended features ---
        int minuteOfDay = (currentMarketTime.getHour() * 60) + currentMarketTime.getMinute();
        double baselineVol = minuteVolumeBaseline.getOrDefault(minuteOfDay, (double) Math.max(1L, barVolume));
        float f_rel_volume_30s = (float) (barVolume / (baselineVol + 1.0));

        double realizedVol20 = returnWindow20.size() > 1 ? stdDev(returnWindow20) : 0.0;
        float f_realized_vol_20 = (float) realizedVol20;
        double volMean = realizedVolWindow100.stream().mapToDouble(Double::doubleValue).average().orElse(realizedVol20);
        double volStd = stdDev(realizedVolWindow100);
        float f_realized_vol_z = volStd > 0.0 ? (float) ((realizedVol20 - volMean) / volStd) : 0.0f;

        float f_dist_or_high_atr = (openingRangeHigh > 0.0 && atr12 > 0.0)
            ? (float) ((openingRangeHigh - barClose) / atr12)
            : 0.0f;
        float f_dist_or_low_atr = (openingRangeLow > 0.0 && atr12 > 0.0)
            ? (float) ((barClose - openingRangeLow) / atr12)
            : 0.0f;

        double spread = (latestBidPrice > 0.0 && latestAskPrice > 0.0 && latestAskPrice >= latestBidPrice)
            ? latestAskPrice - latestBidPrice : 0.0;
        double mid = (latestBidPrice > 0.0 && latestAskPrice > 0.0)
            ? (latestBidPrice + latestAskPrice) / 2.0 : barClose;
        float f_spread_pct = (mid > 0.0) ? (float) (spread / mid) : 0.0f;
        double spreadMean = spreadWindow100.stream().mapToDouble(Double::doubleValue).average().orElse(spread);
        double spreadStd = stdDev(spreadWindow100);
        float f_spread_z = spreadStd > 0.0 ? (float) ((spread - spreadMean) / spreadStd) : 0.0f;

        float f_l1_imbalance = (latestBidSize + latestAskSize) > 0
            ? (float) ((latestBidSize - latestAskSize) / (double) (latestBidSize + latestAskSize))
            : 0.0f;

        float f_signed_flow_30s = (currentBarVolAsk + currentBarVolBid) > 0
            ? (float) ((currentBarVolAsk - currentBarVolBid) / (double) (currentBarVolAsk + currentBarVolBid))
            : 0.0f;

        Map<String, Float> featureValues = new LinkedHashMap<>();
        featureValues.put("f_dist_vwap", f_dist_vwap);
        featureValues.put("f_bb_lower_dist", f_bb_lower_dist);
        featureValues.put("f_bb_upper_dist", f_bb_upper_dist);
        featureValues.put("f_macd_diff", f_macd_diff);
        featureValues.put("f_body_size", f_body_size);
        featureValues.put("f_lower_wick", f_lower_wick);
        featureValues.put("f_upper_wick", f_upper_wick);
        featureValues.put("f_atr_norm", f_atr_norm);
        featureValues.put("f_dist_sma", f_dist_sma);
        featureValues.put("f_dist_high", f_dist_high);
        featureValues.put("f_dist_low", f_dist_low);
        featureValues.put("f_rsi", f_rsi);
        featureValues.put("f_gap_from_prev_close", f_gap_from_prev_close);
        featureValues.put("f_time_of_day", f_time_of_day);
        featureValues.put("f_dist_swing_high", f_dist_swing_high);
        featureValues.put("f_dist_swing_low", f_dist_swing_low);
        featureValues.put("f_is_new_high", f_is_new_high);
        featureValues.put("f_is_new_low", f_is_new_low);
        featureValues.put("f_dist_whole_num", f_dist_whole_num);
        featureValues.put("f_is_green", f_is_green);
        featureValues.put("f_green_streak", f_green_streak);
        featureValues.put("f_red_streak", f_red_streak);
        featureValues.put("f_put_call_ratio", f_put_call_ratio);
        featureValues.put("f_vol_ask_ratio", f_vol_ask_ratio);
        featureValues.put("f_vol_bid_ratio", f_vol_bid_ratio);
        featureValues.put("f_rel_volume_30s", f_rel_volume_30s);
        featureValues.put("f_realized_vol_20", f_realized_vol_20);
        featureValues.put("f_realized_vol_z", f_realized_vol_z);
        featureValues.put("f_dist_or_high_atr", f_dist_or_high_atr);
        featureValues.put("f_dist_or_low_atr", f_dist_or_low_atr);
        featureValues.put("f_spread_pct", f_spread_pct);
        featureValues.put("f_spread_z", f_spread_z);
        featureValues.put("f_l1_imbalance", f_l1_imbalance);
        featureValues.put("f_signed_flow_30s", f_signed_flow_30s);
        for (String newsColumn : NEWS_BAR_FEATURE_COLUMNS) {
            featureValues.put(newsColumn, 0.0f);
        }
        featureValues.putAll(
            EnhancedLiveFeatureProducer.produce(
                new EnhancedLiveFeatureProducer.Snapshot(
                    barOpen,
                    barHigh,
                    barLow,
                    barClose,
                    barVolume,
                    new ArrayList<>(smaWindow),
                    new ArrayList<>(highWindow),
                    new ArrayList<>(lowWindow),
                    new ArrayList<>(volumeWindow)
                )
            )
        );
        for (String regimeProbabilityColumn : REGIME_PROBABILITY_FEATURE_COLUMNS) {
            featureValues.put(regimeProbabilityColumn, 0.0f);
        }
        return featureValues;
    }

    private Map<String, Float> defaultRegimeProbabilityFeatures(double choppyProb, double trendProb, double volatileProb) {
        double entropy = 0.0;
        double[] probs = new double[] {choppyProb, trendProb, volatileProb};
        for (double prob : probs) {
            double safeProb = Math.max(1.0e-9, prob);
            entropy += -(prob * Math.log(safeProb));
        }
        double normalizedEntropy = entropy / Math.log(3.0);

        Map<String, Float> features = new HashMap<>();
        features.put("f_regime_prob_choppy", (float) choppyProb);
        features.put("f_regime_prob_trend", (float) trendProb);
        features.put("f_regime_prob_volatile", (float) volatileProb);
        features.put("f_regime_prob_entropy", (float) normalizedEntropy);
        return features;
    }

    @Override
    public void setPositionSynced(boolean synced) {
        eventQueue.offer(new StrategyEvent.SetPositionSyncedEvent(synced));
    }
    @Override public boolean isPositionSynced() { return positionSynced; }
    @Override public boolean hasInFlightOrder() { return inFlightOrder; }

    @Override
    public void onOrderSubmitted(int orderId, String action, int quantity) {
        eventQueue.offer(new StrategyEvent.OrderSubmittedEvent(orderId, action, quantity));
    }

    private void handleOrderSubmitted(int orderId, String action, int quantity) {
        this.inFlightOrder = true;
        this.pendingOrderId = orderId;
        this.lastOrderSubmitTime = System.currentTimeMillis(); 
        this.pendingOrderReconcileRequested = false;
        this.lastPendingOrderReconcileLogTime = 0;
        flowData("STRATEGY.ORDER", "submitted orderId=" + orderId + " action=" + action + " qty=" + quantity + " symbol=" + symbol);
    }

    @Override
    public void onOrderProgress(int orderId, String action, int filledDelta, int remaining, double avgFillPrice) {
        eventQueue.offer(new StrategyEvent.OrderProgressEvent(orderId, action, filledDelta, remaining, avgFillPrice));
    }

    private void handleOrderProgress(int orderId, String action, int filledDelta, int remaining, double avgFillPrice) {
        flowCondition("STRATEGY.ORDER", "FILLED_DELTA_POSITIVE", filledDelta > 0, "orderId=" + orderId + " filledDelta=" + filledDelta + " remaining=" + remaining + " avgFillPrice=" + avgFillPrice);
        if (filledDelta <= 0) return;

        double slippagePerShare = 0.03; // Conservative modeling

        int prevPosition = currentPosition;
        int prevAbsPos = Math.abs(prevPosition);
        int newPos = ("BUY".equalsIgnoreCase(action))
            ? currentPosition + filledDelta
            : currentPosition - filledDelta;
        currentPosition = newPos;
        int currAbsPos = Math.abs(newPos);

        if (currAbsPos > prevAbsPos) {
            double penalty = "BUY".equalsIgnoreCase(action) ? slippagePerShare : -slippagePerShare;
            avgEntryPrice = avgFillPrice + penalty;
            if (prevAbsPos == 0 || Integer.signum(prevPosition) != Integer.signum(newPos)) {
                positionEntryPrice = avgEntryPrice;
                positionEntryProbability = pendingEntryProbability;
                positionEntryThreshold = pendingEntryThreshold;
                positionEntryThresholdMargin = pendingEntryThresholdMargin;
                pendingEntryProbability = 0.0;
                pendingEntryThreshold = 0.0;
                pendingEntryThresholdMargin = 0.0;
                positionEntrySide = Integer.signum(newPos);
                positionEntryEpoch = currentMarketTime == null ? 0L : currentMarketTime.atZone(MARKET_ZONE).toEpochSecond();
                barsSincePositionEntry30s = 0;
                barsSincePositionEntry5s = 0;
                positionMfeR = 0.0;
                positionMaeR = 0.0;
                clearMicroEntryArms("position-opened");
            } else {
                positionEntryPrice = avgEntryPrice;
            }
        } else if (currAbsPos < prevAbsPos) {
            double exitPenalty = "SELL".equalsIgnoreCase(action) ? -slippagePerShare : slippagePerShare;
            double adjustedExitPrice = avgFillPrice + exitPenalty;

            // Use prior position sign so long/short realized PnL signs remain correct.
            double tradePnL = prevPosition > 0
                ? (adjustedExitPrice - avgEntryPrice) * filledDelta
                : (avgEntryPrice - adjustedExitPrice) * filledDelta;

            dailyNetPnL += tradePnL;
            totalNetPnL += tradePnL;
            logTradeToCsv(action, filledDelta, avgEntryPrice, adjustedExitPrice, tradePnL);
        }

        if (newPos == 0) {
            avgEntryPrice = 0.0;
            positionEntryPrice = 0.0;
            pendingEntryProbability = 0.0;
            pendingEntryThreshold = 0.0;
            pendingEntryThresholdMargin = 0.0;
            positionEntryProbability = 0.0;
            positionEntryThreshold = 0.0;
            positionEntryThresholdMargin = 0.0;
            positionEntrySide = 0;
            positionEntryEpoch = 0L;
            barsSincePositionEntry30s = 0;
            barsSincePositionEntry5s = 0;
            positionMfeR = 0.0;
            positionMaeR = 0.0;
        }

        double pnlSnapshot = totalNetPnL;

        if (remaining == 0) tradeCount++;
        flowAnalyze("STRATEGY.ORDER", "fill orderId=" + orderId + " action=" + action + " newPos=" + newPos + " netPnL=" + String.format("%.2f", pnlSnapshot) + " dailyPnL=" + String.format("%.2f", dailyNetPnL));
    }

    public void resetForNewDay() {
        eventQueue.offer(new StrategyEvent.ResetForNewDayEvent());
    }

    public boolean resetForNewDayAndWait(long timeoutMillis) {
        CountDownLatch resetAck = new CountDownLatch(1);
        eventQueue.offer(new StrategyEvent.ResetForNewDayEvent(resetAck));
        try {
            return resetAck.await(Math.max(1L, timeoutMillis), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void handleResetForNewDay(StrategyEvent.ResetForNewDayEvent resetEvent) {
        try {
            dailyNetPnL = 0.0;
            circuitBreakerTripped = false;
            tradeCount = 0;
            allowNewEntries = true;
            hardStopExitCount = 0;
            lastHardStopExitTimeMs = 0L;
            dayHigh = 0.0;
            dayLow = 0.0;
            cumPv = 0.0;
            cumVol = 0;
            greenStreak = 0;
            redStreak = 0;

            // FIX: Do NOT clear bbWindow, smaWindow, highWindow, lowWindow, avgGain, ema12, etc.
            // Python trains on continuous data across days. Java must maintain indicator memory across the night gap!

            currentBarVolAsk = 0L;
            currentBarVolBid = 0L;
            currentPutCallRatio = 1.0f;
            latestPutVolume = 0L;
            latestCallVolume = 0L;
            prevPutVolume = 0L;
            prevCallVolume = 0L;
            optionVolumeWarningLogged = false;
            clearCurrent30SecondBucket(true);
            clearIndependentMicroBarState();
            training30sReturnWindow20.clear();
            training30sVolumeWindow20.clear();
            training30sOptionFlowWindow20.clear();
            lastTraining30sFeatureValues = new HashMap<>();
            lastTraining30sClose = 0.0;
            lastTraining30sEpoch = 0L;
            clearMicroEntryArms("new-day-reset");

            // We still reset barsCount to ensure the Strategy rewarms for the configured opening-profile bar count after the morning bell.
            barsCount = 0;
            flowInfo("STRATEGY.RESET", "Daily limits reset. Indicator memory successfully carried over to new day symbol=" + symbol);
        } finally {
            if (resetEvent != null && resetEvent.ackLatch != null) {
                resetEvent.ackLatch.countDown();
            }
        }
    }



    public void forceEndOfDayFlatten(double currentPrice) {
        int position = currentPosition;
        String action = (position > 0) ? "SELL" : "BUY";
        double executionPrice = priceForAction(action, currentPrice);
        boolean flattenGate = position != 0 && !inFlightOrder && executionPrice > 0.0;
        flowCondition("STRATEGY.EOD", "FLATTEN_GATE", flattenGate, "symbol=" + symbol + " position=" + position + " inFlight=" + inFlightOrder + " executionPrice=" + executionPrice);
        if (!flattenGate) return;
        flowInfo("STRATEGY.EOD", "Closing position size=" + Math.abs(position) + " symbol=" + symbol + " price=" + executionPrice);
        String side = position > 0 ? "long" : "short";
        long epoch = currentMarketTime == null ? 0L : currentMarketTime.atZone(MARKET_ZONE).toEpochSecond();
        emitLifecycleTelemetry(() -> lifecycleTelemetryListener.onEndOfDayExit(symbol, side, epoch, executionPrice));
        this.inFlightOrder = true;
        parent.placeTrade(symbol, action, executionPrice, Math.abs(position), "MKT");
    }

    private void logTradeToCsv(String exitAction, int qty, double entryPrice, double exitPrice, double tradePnL) {
        LocalDateTime timestamp = currentMarketTime != null ? currentMarketTime : LocalDateTime.now(ZoneId.of("America/New_York"));
        String formattedTimestamp = timestamp.atZone(MARKET_ZONE).format(MARKET_TS_FORMAT);

        File datedLogFile = new File(parent.getTradeLogFile());
        appendTradeLog(datedLogFile, formattedTimestamp, exitAction, qty, entryPrice, exitPrice, tradePnL);
    }

    private void appendTradeLog(File logFile, String formattedTimestamp, String exitAction, int qty,
                                double entryPrice, double exitPrice, double tradePnL) {
        if (parent.isTradeLogFileEnabled()) {
            appendTradeCsv(logFile, formattedTimestamp, exitAction, qty, entryPrice, exitPrice, tradePnL);
        }
        parent.persistTradeLog(formattedTimestamp, symbol, exitAction, qty, entryPrice, exitPrice, tradePnL, totalNetPnL, logFile.getPath());
    }

    private void appendTradeCsv(File logFile, String formattedTimestamp, String exitAction, int qty,
                                double entryPrice, double exitPrice, double tradePnL) {
        boolean needsHeader = !logFile.exists() || logFile.length() == 0;
        try (FileWriter fileWriter = new FileWriter(logFile, true);
             PrintWriter printWriter = new PrintWriter(fileWriter)) {
            if (needsHeader) {
                printWriter.println("Timestamp,Symbol,Action,Quantity,EntryPrice,ExitPrice,TradePnL,CumulativePnL");
            }
            printWriter.printf("%s,%s,%s,%d,%.4f,%.4f,%.4f,%.4f%n",
                formattedTimestamp, symbol, exitAction, qty, entryPrice, exitPrice, tradePnL, totalNetPnL);
        } catch (IOException exception) {
            flowError("STRATEGY.PNL", "Trade CSV write failed file=" + logFile.getName() + " symbol=" + symbol + " reason=" + exception.getMessage());
        }
    }

    @Override
    public void onOrderClosed(int orderId, String status) {
        eventQueue.offer(new StrategyEvent.OrderClosedEvent(orderId, status));
    }

    private void handleOrderClosed(int orderId, String status) {
        if (orderId == this.pendingOrderId || this.pendingOrderId == -1) {
            this.inFlightOrder = false;
            this.pendingOrderReconcileRequested = false;
            this.pendingOrderId = -1;
            this.lastPendingOrderReconcileLogTime = 0;
        }
        flowData("STRATEGY.ORDER", "closed orderId=" + orderId + " status=" + status + " symbol=" + symbol);
    }

    @Override
    public void stop() {
        int queueDepthAtStop = eventQueue.size();
        long stopStartNanos = System.nanoTime();
        lastStopQueueDepth = queueDepthAtStop;
        CountDownLatch stopAck = new CountDownLatch(1);
        eventQueue.offer(new StrategyEvent.StopEvent(stopAck));

        try {
            boolean acknowledged = stopAck.await(1500, TimeUnit.MILLISECONDS);
            double ackLatencyMs = (System.nanoTime() - stopStartNanos) / 1_000_000.0;
            lastStopAckLatencyMs = ackLatencyMs;
            recordStopAckLatency(ackLatencyMs);
            String latencySummary = stopLatencySummary();
            if (!acknowledged) {
                flowCondition("STRATEGY.STOP", "ACTOR_ACK_RECEIVED", false, "symbol=" + symbol + " queueDepthAtStop=" + queueDepthAtStop + " ackLatencyMs=" + String.format("%.3f", ackLatencyMs) + " " + latencySummary);
            } else {
                flowCondition("STRATEGY.STOP", "ACTOR_ACK_RECEIVED", true, "symbol=" + symbol + " queueDepthAtStop=" + queueDepthAtStop + " ackLatencyMs=" + String.format("%.3f", ackLatencyMs) + " " + latencySummary);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (eventProcessorThread != null) {
            try {
                eventProcessorThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (eventProcessorThread.isAlive()) {
                eventProcessorThread.interrupt();
                try {
                    eventProcessorThread.join(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        if (longEntryAi != null) longEntryAi.close();
        if (shortEntryAi != null) shortEntryAi.close();
        if (longExitAi != null) longExitAi.close();
        if (shortExitAi != null) shortExitAi.close();
        if (regimeClassifierAi != null) regimeClassifierAi.close();
        if (choppyLongEntryAi != null) choppyLongEntryAi.close();
        if (choppyShortEntryAi != null) choppyShortEntryAi.close();
        if (choppyLongExitAi != null) choppyLongExitAi.close();
        if (choppyShortExitAi != null) choppyShortExitAi.close();
        if (trendLongEntryAi != null) trendLongEntryAi.close();
        if (trendShortEntryAi != null) trendShortEntryAi.close();
        if (trendLongExitAi != null) trendLongExitAi.close();
        if (trendShortExitAi != null) trendShortExitAi.close();
        if (volatileLongEntryAi != null) volatileLongEntryAi.close();
        if (volatileShortEntryAi != null) volatileShortEntryAi.close();
        if (volatileLongExitAi != null) volatileLongExitAi.close();
        if (volatileShortExitAi != null) volatileShortExitAi.close();
        if (open30LongEntryAi != null) open30LongEntryAi.close();
        if (open30ShortEntryAi != null) open30ShortEntryAi.close();
        if (open30LongExitAi != null) open30LongExitAi.close();
        if (open30ShortExitAi != null) open30ShortExitAi.close();
        if (longExitLifecycleAi != null) longExitLifecycleAi.close();
        if (shortExitLifecycleAi != null) shortExitLifecycleAi.close();
        if (longMicroEntryAi != null) longMicroEntryAi.close();
        if (shortMicroEntryAi != null) shortMicroEntryAi.close();
        if (longMicroExitGuardAi != null) longMicroExitGuardAi.close();
        if (shortMicroExitGuardAi != null) shortMicroExitGuardAi.close();
        flowInfo("STRATEGY.STOP", "Strategy stopped symbol=" + symbol);
    }

    public void setAllowNewEntries(boolean allow) {
        eventQueue.offer(new StrategyEvent.SetAllowNewEntriesEvent(allow));
    }
    public void setCurrentMarketTime(LocalDateTime time) {
        eventQueue.offer(new StrategyEvent.SetCurrentMarketTimeEvent(time));
    }
    public int getCurrentPosition() { return currentPosition; }
    public double getLastPrice() { return lastPrice; }
    public int getTradeCount() { return tradeCount; }
    public int getHardStopExitCount() { return hardStopExitCount; }
    public long getLastHardStopExitTimeMs() { return lastHardStopExitTimeMs; }
    public double getTotalNetPnL() { return totalNetPnL; }
    
    // Legacy interface preserved for TradingStrategy Interface
    @Override public void onTick(double price, long volume) {}

    public void restoreState(double rPrice, int rTrades, boolean rEnabled) { restoreState(rPrice, rTrades, rEnabled, false); }
    public StrategyState snapshotState() {
        return new StrategyState(lastPrice, tradeCount, enabled, isArmed(), isVolatile(), yesterdayClose);
    }

    public boolean isDirectionalRegime() { return false; }
    public boolean isRegimeAllowsTrading() { return enabled && !circuitBreakerTripped && tradeCount < maxTrades; }
    public boolean isArmed() { return allowNewEntries && currentPosition == 0; }
    public boolean isCircuitBreakerTripped() { return circuitBreakerTripped; }
    public boolean isVolatile() { return lastDetectedRegime == MarketRegime.VOLATILE; }
    public void setGapPercentage(double gapPercentage) {}
    public void setReversalPercentage(double reversalPercentage) {}
    public void setMaxVolatilityPercent(double maxVolatilityPercent) {}

    public void restoreState(double rPrice, int rTrades, boolean rEnabled, boolean rArmed) {
        restoreState(rPrice, rTrades, rEnabled, rArmed, this.yesterdayClose);
    }

    public void restoreState(double rPrice, int rTrades, boolean rEnabled, boolean rArmed, double restoredYesterdayClose) {
        eventQueue.offer(new StrategyEvent.RestoreStateEvent(rPrice, rTrades, rEnabled, rArmed, restoredYesterdayClose));
    }

    public void restoreState(double rPrice, int rTrades, boolean rEnabled, boolean rArmed, double restoredYesterdayClose,
                             int restoredHardStopExitCount, long restoredLastHardStopExitTimeMs) {
        eventQueue.offer(new StrategyEvent.RestoreStateEvent(rPrice, rTrades, rEnabled, rArmed, restoredYesterdayClose,
            restoredHardStopExitCount, restoredLastHardStopExitTimeMs));
    }

    private void handleRestoreState(double rPrice, int rTrades, boolean rEnabled, boolean rArmed, double restoredYesterdayClose) {
        handleRestoreState(rPrice, rTrades, rEnabled, rArmed, restoredYesterdayClose, 0, 0L);
    }

    private void handleRestoreState(double rPrice, int rTrades, boolean rEnabled, boolean rArmed, double restoredYesterdayClose,
                                    int restoredHardStopExitCount, long restoredLastHardStopExitTimeMs) {
        this.lastPrice = rPrice;
        this.tradeCount = Math.max(0, rTrades);
        this.enabled = rEnabled;
        this.allowNewEntries = rArmed;
        this.yesterdayClose = Math.max(0.0, restoredYesterdayClose);
        this.hardStopExitCount = Math.max(0, restoredHardStopExitCount);
        this.lastHardStopExitTimeMs = Math.max(0L, restoredLastHardStopExitTimeMs);
        if (this.hardStopExitCount >= maxHardStopsPerDay) {
            this.allowNewEntries = false;
        }
        flowData("STRATEGY.STATE", "restored symbol=" + symbol + " lastPrice=" + rPrice + " tradeCount=" + this.tradeCount + " enabled=" + rEnabled + " armed=" + this.allowNewEntries + " yesterdayClose=" + this.yesterdayClose + " hardStopExitCount=" + this.hardStopExitCount + " lastHardStopExitTimeMs=" + this.lastHardStopExitTimeMs);
    }

    public void setYesterdayClose(double yesterdayClose) {
        eventQueue.offer(new StrategyEvent.SetYesterdayCloseEvent(yesterdayClose));
    }

    public double getYesterdayClose() {
        return yesterdayClose;
    }

    public int getQueueSize() {
        return eventQueue.size();
    }

    private void flowInfo(String stage, String message) {
        log.info(">>> [FLOW][INFO][{}] {}", stage, message);
    }

    private void flowWarn(String stage, String message) {
        log.warn(">>> [FLOW][WARN][{}] {}", stage, message);
    }

    private boolean isTickerLevelStage(String stage) {
        return "STRATEGY.TAPE".equals(stage) || "STRATEGY.TICK".equals(stage);
    }

    private void flowData(String stage, String message) {
        if (isTickerLevelStage(stage)) {
            log.debug(">>> [FLOW][DATA][{}] {}", stage, message);
            return;
        }
        log.info(">>> [FLOW][DATA][{}] {}", stage, message);
    }

    private void flowAnalyze(String stage, String message) {
        log.info(">>> [FLOW][ANALYZE][{}] {}", stage, message);
    }

    private void flowError(String stage, String message) {
        log.error(">>> [FLOW][ERROR][{}] {}", stage, message);
    }

    private void flowCondition(String stage, String conditionName, boolean passed, String details) {
        String verdict = passed ? "PASS" : "FAIL";
        if (isTickerLevelStage(stage)) {
            log.debug(">>> [FLOW][COND][{}] {}={} | {}", stage, conditionName, verdict, details);
            return;
        }
        log.info(">>> [FLOW][COND][{}] {}={} | {}", stage, conditionName, verdict, details);
    }
}
