package com.calgary.fili.trader.bot.strategy;

import com.calgary.fili.trader.bot.trader.IBKRTrader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PingPongStrategy implements TradingStrategy {

    private enum MarketRegime {
        CHOPPY,
        TREND,
        VOLATILE
    }

    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final DateTimeFormatter MARKET_TS_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV");
    private static final Logger log = LoggerFactory.getLogger(PingPongStrategy.class);
    private static final boolean USE_RSI_PRE_GATES = Boolean.parseBoolean(System.getProperty("strategy.useRsiPreGate", "false"));
    private static final double RSI_LONG_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiLongExitThreshold", "50.0"));
    private static final double RSI_SHORT_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiShortExitThreshold", "50.0"));
    private static final double RSI_LONG_ENTRY_OPEN_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiLongEntryOpenThreshold", "34.0"));
    private static final double RSI_LONG_ENTRY_REGULAR_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiLongEntryRegularThreshold", "40.0"));
    private static final double RSI_SHORT_ENTRY_OPEN_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiShortEntryOpenThreshold", "66.0"));
    private static final double RSI_SHORT_ENTRY_REGULAR_THRESHOLD = Double.parseDouble(System.getProperty("strategy.rsiShortEntryRegularThreshold", "60.0"));
    private static final double DEFAULT_LONG_ENTRY_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.longEntryThreshold", "0.68"));
    private static final double DEFAULT_SHORT_ENTRY_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.shortEntryThreshold", "0.63"));
    private static final double DEFAULT_LONG_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.longExitThreshold", "0.61"));
    private static final double DEFAULT_SHORT_EXIT_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.shortExitThreshold", "0.63"));
    private static final double DEFAULT_REGIME_THRESHOLD = Double.parseDouble(System.getProperty("strategy.ai.regimeThreshold", "0.50"));
    private static final int OPEN30_MIN_BARS = Integer.parseInt(System.getProperty("strategy.ai.open30MinBars", "12"));
    private static final int REGULAR_MIN_BARS = Integer.parseInt(System.getProperty("strategy.ai.regularMinBars", "60"));

    public record StrategyState(double lastPrice, int tradeCount, boolean enabled, boolean isArmed, boolean isVolatile, double yesterdayClose) {}

    private final IBKRTrader parent;
    private final String symbol;

    // Core Risk Parameters
    private final int tradeQuantity;
    private final int maxTrades;
    private final int rsiPeriod;
    private final double stopLossPercentage;
    private final double maxDailyDrawdown;

    // The 4-Model AI Architecture
    private AiPredictor longEntryAi;
    private AiPredictor shortEntryAi;
    private AiPredictor longExitAi;
    private AiPredictor shortExitAi;
    private AiPredictor regimeClassifierAi;
    private AiPredictor choppyLongEntryAi;
    private AiPredictor choppyShortEntryAi;
    private AiPredictor choppyLongExitAi;
    private AiPredictor choppyShortExitAi;
    private AiPredictor trendLongEntryAi;
    private AiPredictor trendShortEntryAi;
    private AiPredictor trendLongExitAi;
    private AiPredictor trendShortExitAi;
    private AiPredictor volatileLongEntryAi;
    private AiPredictor volatileShortEntryAi;
    private AiPredictor volatileLongExitAi;
    private AiPredictor volatileShortExitAi;
    private AiPredictor open30LongEntryAi;
    private AiPredictor open30ShortEntryAi;
    private AiPredictor open30LongExitAi;
    private AiPredictor open30ShortExitAi;
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
    private double avgEntryPrice = 0.0;
    private volatile double dailyNetPnL = 0.0;
    private volatile double totalNetPnL = 0.0;
    private volatile LocalDateTime currentMarketTime;

    private long lastOrderSubmitTime = 0;
    private int pendingOrderId = -1;

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

    // Extended features state (safe to keep even if model uses base 23 features).
    private final Map<Integer, Double> minuteVolumeBaseline = new HashMap<>();
    private final Deque<Double> returnWindow20 = new ArrayDeque<>();
    private final Deque<Double> realizedVolWindow100 = new ArrayDeque<>();
    private final Deque<Double> spreadWindow100 = new ArrayDeque<>();
    private LocalDate featureSessionDate = null;
    private int openingRangeBarsCount = 0;
    private double openingRangeHigh = 0.0;
    private double openingRangeLow = 0.0;

    // --- 30-SECOND AGGREGATION BUCKET ---
    private static final int BARS_PER_30S_BUCKET = 6;
    private int bucketCount = 0;
    private long bucketEpoch = 0L;
    private double bucketOpen = 0.0;
    private double bucketHigh = 0.0;
    private double bucketLow = Double.MAX_VALUE;
    private double bucketClose = 0.0;
    private long bucketVolume = 0L;
    private double bucketWapSum = 0.0;


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
            flowInfo("WARMUP", "No warmup file found at: " + warmupPath + ". Waiting 5 minutes for live bars.");
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
            int barsNeeded = 60;
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
        this.parent = parent;
        this.symbol = symbol;
        this.tradeQuantity = tradeQuantity;
        this.maxTrades = maxTrades;
        this.rsiPeriod = Math.max(2, rsiPeriod);
        this.stopLossPercentage = Math.max(0.0001, stopLossPercentage);
        this.maxDailyDrawdown = Math.max(1.0, maxDailyDrawdown);

        // Load the 4 Distinct ONNX Models
        try {
            this.longEntryAi = new AiPredictor("long_entry.onnx");
        } catch (Exception e) {
            flowError("AI.INIT", "Failed to load long_entry.onnx. Trading disabled. " + e.getMessage());
            this.enabled = false;
        }

        try {
            this.shortEntryAi = new AiPredictor("short_entry.onnx");
        } catch (Exception e) {
            flowError("AI.INIT", "Failed to load short_entry.onnx. Short entries disabled.");
            this.shortEntryAi = null;
        }

        try {
            this.longExitAi = new AiPredictor("long_exit.onnx");
        } catch (Exception e) {
            flowError("AI.INIT", "Failed to load long_exit.onnx. Longs will rely on hard stop-loss.");
            this.longExitAi = null;
        }

        try {
            this.shortExitAi = new AiPredictor("short_exit.onnx");
        } catch (Exception e) {
            flowError("AI.INIT", "Failed to load short_exit.onnx. Shorts will rely on hard stop-loss.");
            this.shortExitAi = null;
        }

        this.regimeClassifierAi = tryLoadOptionalModel("regime_classifier.onnx", "Market regime classifier unavailable. Falling back to CHOPPY.");
        this.choppyLongEntryAi = tryLoadOptionalModel("choppy_long_entry.onnx", "Choppy long-entry model unavailable. Using base model.");
        this.choppyShortEntryAi = tryLoadOptionalModel("choppy_short_entry.onnx", "Choppy short-entry model unavailable. Using base model.");
        this.choppyLongExitAi = tryLoadOptionalModel("choppy_long_exit.onnx", "Choppy long-exit model unavailable. Using base model.");
        this.choppyShortExitAi = tryLoadOptionalModel("choppy_short_exit.onnx", "Choppy short-exit model unavailable. Using base model.");
        this.trendLongEntryAi = tryLoadOptionalModel("trend_long_entry.onnx", "Trend long-entry model unavailable. Using base model.");
        this.trendShortEntryAi = tryLoadOptionalModel("trend_short_entry.onnx", "Trend short-entry model unavailable. Using base model.");
        this.trendLongExitAi = tryLoadOptionalModel("trend_long_exit.onnx", "Trend long-exit model unavailable. Using base model.");
        this.trendShortExitAi = tryLoadOptionalModel("trend_short_exit.onnx", "Trend short-exit model unavailable. Using base model.");
        this.volatileLongEntryAi = tryLoadOptionalModel("volatile_long_entry.onnx", "Volatile long-entry model unavailable. Using base model.");
        this.volatileShortEntryAi = tryLoadOptionalModel("volatile_short_entry.onnx", "Volatile short-entry model unavailable. Using base model.");
        this.volatileLongExitAi = tryLoadOptionalModel("volatile_long_exit.onnx", "Volatile long-exit model unavailable. Using base model.");
        this.volatileShortExitAi = tryLoadOptionalModel("volatile_short_exit.onnx", "Volatile short-exit model unavailable. Using base model.");
        this.open30LongEntryAi = tryLoadOptionalModel("open30_long_entry.onnx", "Open30 long-entry model unavailable. Using regime/base model.");
        this.open30ShortEntryAi = tryLoadOptionalModel("open30_short_entry.onnx", "Open30 short-entry model unavailable. Using regime/base model.");
        this.open30LongExitAi = tryLoadOptionalModel("open30_long_exit.onnx", "Open30 long-exit model unavailable. Using regime/base model.");
        this.open30ShortExitAi = tryLoadOptionalModel("open30_short_exit.onnx", "Open30 short-exit model unavailable. Using regime/base model.");

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

    private AiPredictor tryLoadOptionalModel(String modelName, String fallbackLog) {
        try {
            return new AiPredictor(modelName);
        } catch (Exception e) {
            flowInfo("AI.INIT", fallbackLog + " model=" + modelName + " reason=" + e.getMessage());
            return null;
        }
    }

    private AiPredictor modelForRegime(MarketRegime regime, AiPredictor baseModel, AiPredictor choppyModel, AiPredictor trendModel, AiPredictor volatileModel) {
        if (regime == MarketRegime.CHOPPY && choppyModel != null) {
            return choppyModel;
        }
        if (regime == MarketRegime.TREND && trendModel != null) {
            return trendModel;
        }
        if (regime == MarketRegime.VOLATILE && volatileModel != null) {
            return volatileModel;
        }
        return baseModel;
    }

    private MarketRegime detectMarketRegime(float[] regimeFeatures) {
        if (regimeClassifierAi == null) {
            return MarketRegime.CHOPPY;
        }

        AiPredictor.ClassPredictionOutcome outcome = regimeClassifierAi.predictClassWithConfidence(regimeFeatures, 0);
        int predicted = outcome.classLabel();
        double confidence = outcome.confidence();

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
            return MarketRegime.CHOPPY;
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
        return detected;
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
        this.longEntryProbabilityThreshold = clampProbability(longEntry, DEFAULT_LONG_ENTRY_THRESHOLD);
        this.shortEntryProbabilityThreshold = clampProbability(shortEntry, DEFAULT_SHORT_ENTRY_THRESHOLD);
        this.longExitProbabilityThreshold = clampProbability(longExit, DEFAULT_LONG_EXIT_THRESHOLD);
        this.shortExitProbabilityThreshold = clampProbability(shortExit, DEFAULT_SHORT_EXIT_THRESHOLD);
        this.regimeProbabilityThreshold = clampProbability(regimeThreshold, DEFAULT_REGIME_THRESHOLD);

        flowData(
            "AI.CONFIG",
            "symbol=" + symbol
                + " thresholds longEntry=" + formatProb(longEntryProbabilityThreshold)
                + " shortEntry=" + formatProb(shortEntryProbabilityThreshold)
                + " longExit=" + formatProb(longExitProbabilityThreshold)
                + " shortExit=" + formatProb(shortExitProbabilityThreshold)
                + " regime=" + formatProb(regimeProbabilityThreshold)
        );
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
                } else if (event instanceof StrategyEvent.OrderSubmittedEvent e) {
                    handleOrderSubmitted(e.orderId, e.action, e.quantity);
                } else if (event instanceof StrategyEvent.OrderProgressEvent e) {
                    handleOrderProgress(e.orderId, e.action, e.filledDelta, e.remaining, e.avgFillPrice);
                } else if (event instanceof StrategyEvent.OrderClosedEvent e) {
                    handleOrderClosed(e.orderId, e.status);
                } else if (event instanceof StrategyEvent.PositionSyncEvent e) {
                    handlePositionSync(e.brokerPosition, e.avgCost);
                } else if (event instanceof StrategyEvent.ResetForNewDayEvent) {
                    handleResetForNewDay();
                } else if (event instanceof StrategyEvent.RestoreStateEvent e) {
                    handleRestoreState(e.rPrice, e.rTrades, e.rEnabled, e.rArmed, e.restoredYesterdayClose);
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

    private int sharesForAmount(double price) {
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
        } else if (newPosition == 0) {
            this.avgEntryPrice = 0.0;
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
        // WATCHDOG: 3-Second Active Kill Switch for Connection Drops & Hung Partial Fills
        if (this.inFlightOrder && (System.currentTimeMillis() - this.lastOrderSubmitTime > 3000)) {
            flowError("WATCHDOG", "Order hung >3s. Forcing cancel and resync.");
            if (this.parent != null && this.pendingOrderId != -1) {
                this.parent.cancelStaleOrder(this.pendingOrderId);
            }
            this.inFlightOrder = false; // Force drop the lock
            if (this.parent != null) {
                this.parent.requestPositions(); // Find out exactly what filled
            }
        }
        this.lastPrice = price;
        boolean tickGateOpen = !circuitBreakerTripped && currentPosition != 0 && !inFlightOrder;
        flowCondition("STRATEGY.TICK", "EXIT_TICK_GATE", tickGateOpen, "symbol=" + symbol + " circuitBreaker=" + circuitBreakerTripped + " position=" + currentPosition + " inFlight=" + inFlightOrder + " price=" + price);
        if (!tickGateOpen) return;

        double currentAvgEntry = avgEntryPrice;
        double currentDailyPnL = dailyNetPnL;

        int position = currentPosition;

        if (currentDailyPnL <= -maxDailyDrawdown) {
            circuitBreakerTripped = true;
            flowCondition("STRATEGY.RISK", "DAILY_DRAWDOWN_WITHIN_LIMIT", false, "symbol=" + symbol + " dailyNetPnL=" + currentDailyPnL + " limit=" + (-maxDailyDrawdown));
            String action = (position > 0) ? "SELL" : "BUY";
            this.inFlightOrder = true;
            parent.placeTrade(symbol, action, price, Math.abs(position), "MKT");
            return;
        }
        flowCondition("STRATEGY.RISK", "DAILY_DRAWDOWN_WITHIN_LIMIT", true, "symbol=" + symbol + " dailyNetPnL=" + currentDailyPnL + " limit=" + (-maxDailyDrawdown));

        // HARD STOP LOSS (Catastrophic Protection)
        if (currentAvgEntry > 0.0) {
            if (position > 0 && price <= currentAvgEntry * (1.0 - stopLossPercentage)) {
                flowCondition("STRATEGY.STOP", "LONG_HARD_STOP_TRIGGER", true, "symbol=" + symbol + " price=" + price + " threshold=" + (currentAvgEntry * (1.0 - stopLossPercentage)));
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "SELL", price, Math.abs(position), "MKT");
            } else if (position < 0 && price >= currentAvgEntry * (1.0 + stopLossPercentage)) {
                flowCondition("STRATEGY.STOP", "SHORT_HARD_STOP_TRIGGER", true, "symbol=" + symbol + " price=" + price + " threshold=" + (currentAvgEntry * (1.0 + stopLossPercentage)));
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "BUY", price, Math.abs(position), "MKT");
            } else {
                flowCondition("STRATEGY.STOP", "HARD_STOP_TRIGGERED", false, "symbol=" + symbol + " price=" + price + " avgEntry=" + currentAvgEntry + " position=" + position);
            }
        } else {
            flowCondition("STRATEGY.STOP", "AVG_ENTRY_AVAILABLE", false, "symbol=" + symbol + " avgEntry=" + currentAvgEntry);
        }
    }

    // =========================================================================
    // STREAM 2: THE BRAIN (5-second feed aggregated into 30-second AI bars)
    // =========================================================================
    public void on5SecondBar(long time, double open, double high, double low, double close, long volume, double wap) {
        eventQueue.offer(new StrategyEvent.BarEvent(time, open, high, low, close, volume, wap));
    }

    private void handle5SecondBar(long time, double open, double high, double low, double close, long volume, double wap) {
        this.currentTickArrivalTime = System.currentTimeMillis();
        this.lastPrice = close;
        flowData("STRATEGY.BAR", "symbol=" + symbol + " epoch=" + time + " ohlc=" + open + "/" + high + "/" + low + "/" + close + " vol=" + volume + " wap=" + wap);

        if (bucketCount == 0) {
            bucketEpoch = time;
            bucketOpen = open;
            bucketHigh = high;
            bucketLow = low;
            bucketClose = close;
            bucketVolume = volume;
            bucketWapSum = wap * volume;
        } else {
            bucketHigh = Math.max(bucketHigh, high);
            bucketLow = Math.min(bucketLow, low);
            bucketClose = close;
            bucketVolume += volume;
            bucketWapSum += (wap * volume);
        }

        bucketCount++;

        if (bucketCount == BARS_PER_30S_BUCKET) {
            double finalWap = bucketVolume > 0 ? (bucketWapSum / bucketVolume) : bucketClose;
            System.out.printf(
                ">>> [30s BUCKET] epoch=%d ohlc=%.2f/%.2f/%.2f/%.2f vol=%d vwap=%.4f%n",
                bucketEpoch, bucketOpen, bucketHigh, bucketLow, bucketClose, bucketVolume, finalWap
            );
            process30SecondBar(bucketEpoch, bucketOpen, bucketHigh, bucketLow, bucketClose, bucketVolume, finalWap);

            bucketCount = 0;
            bucketEpoch = 0L;
            bucketOpen = 0.0;
            bucketHigh = 0.0;
            bucketLow = Double.MAX_VALUE;
            bucketClose = 0.0;
            bucketVolume = 0L;
            bucketWapSum = 0.0;
        }
    }

    private void process30SecondBar(long time, double open, double high, double low, double close, long volume, double wap) {
        
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
        boolean barsReadyForProfile = barsCount > requiredBars;

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

    private void askArtificialIntelligence() {
        boolean timeReady = currentMarketTime != null;
        flowCondition("AI.GATE", "CURRENT_MARKET_TIME_PRESENT", timeReady, "symbol=" + symbol + " currentMarketTime=" + currentMarketTime);
        if (!timeReady) return;

        double currentRsi = calculateRsi();
        int currentHour = currentMarketTime.getHour();
        
        flowAnalyze("AI.INPUT", "symbol=" + symbol + " rsi=" + String.format("%.2f", currentRsi) + " position=" + currentPosition + " tradeCount=" + tradeCount + " maxTrades=" + maxTrades + " allowNewEntries=" + allowNewEntries);

        boolean sessionAllowed = !(currentHour == 9 && currentMarketTime.getMinute() < 30);
        flowCondition("AI.GATE", "SESSION_AFTER_0930", sessionAllowed, "symbol=" + symbol + " time=" + currentMarketTime);
        if (!sessionAllowed) return;

        if (yesterdayClose > 0) {
            double variance = Math.abs(barClose - yesterdayClose) / yesterdayClose;
            boolean varianceAllowed = variance <= 0.05;
            flowCondition("AI.GATE", "PRICE_VARIANCE_LE_5PCT", varianceAllowed, "symbol=" + symbol + " variance=" + variance + " close=" + barClose + " yesterdayClose=" + yesterdayClose);
            if (!varianceAllowed) return;
        } else {
            flowCondition("AI.GATE", "YESTERDAY_CLOSE_AVAILABLE", false, "symbol=" + symbol + " yesterdayClose=" + yesterdayClose);
        }

        float[] features = constructModelFeatures(currentRsi);
        float[] regimeFeatures = constructRegimeClassifierFeatures(features);
        flowData("AI.INPUT", "symbol=" + symbol + " features=" + Arrays.toString(features));

        boolean openingThirty = isOpeningThirtyMinutes();
        MarketRegime activeRegime = openingThirty ? MarketRegime.CHOPPY : detectMarketRegime(regimeFeatures);
        AiPredictor activeLongEntryAi;
        AiPredictor activeShortEntryAi;
        AiPredictor activeLongExitAi;
        AiPredictor activeShortExitAi;

        if (openingThirty) {
            activeLongEntryAi = open30LongEntryAi != null ? open30LongEntryAi : longEntryAi;
            activeShortEntryAi = open30ShortEntryAi != null ? open30ShortEntryAi : shortEntryAi;
            activeLongExitAi = open30LongExitAi != null ? open30LongExitAi : longExitAi;
            activeShortExitAi = open30ShortExitAi != null ? open30ShortExitAi : shortExitAi;
            flowData("AI.ROUTER", "symbol=" + symbol + " profile=OPEN30");
        } else {
            activeLongEntryAi = modelForRegime(activeRegime, longEntryAi, choppyLongEntryAi, trendLongEntryAi, volatileLongEntryAi);
            activeShortEntryAi = modelForRegime(activeRegime, shortEntryAi, choppyShortEntryAi, trendShortEntryAi, volatileShortEntryAi);
            activeLongExitAi = modelForRegime(activeRegime, longExitAi, choppyLongExitAi, trendLongExitAi, volatileLongExitAi);
            activeShortExitAi = modelForRegime(activeRegime, shortExitAi, choppyShortExitAi, trendShortExitAi, volatileShortExitAi);
            flowData("AI.ROUTER", "symbol=" + symbol + " profile=REGIME activeRegime=" + activeRegime);
        }

        // ==========================================
        // SCENARIO 1: WE ARE ALREADY LONG
        // ==========================================
        if (currentPosition > 0) {
            boolean rsiGate = !USE_RSI_PRE_GATES || currentRsi > RSI_LONG_EXIT_THRESHOLD;
            boolean modelReady = activeLongExitAi != null;
            flowCondition("AI.LONG.EXIT", "RSI_PRE_GATE", rsiGate, "symbol=" + symbol + " enabled=" + USE_RSI_PRE_GATES + " rsi=" + currentRsi + " threshold=" + RSI_LONG_EXIT_THRESHOLD);
            flowCondition("AI.LONG.EXIT", "MODEL_AVAILABLE", modelReady, "symbol=" + symbol + " regime=" + activeRegime);
            boolean shouldExitLong = false;
            if (rsiGate && modelReady) {
                double prob = activeLongExitAi.predictProbability(features);
                shouldExitLong = prob >= longExitProbabilityThreshold;
                flowCondition(
                    "AI.LONG.EXIT",
                    "AI_PREDICTS_EXIT",
                    shouldExitLong,
                    "symbol=" + symbol
                        + " rsi=" + currentRsi
                        + " close=" + barClose
                        + " prob=" + formatProb(prob)
                        + " threshold=" + formatProb(longExitProbabilityThreshold)
                );
            }
            if (shouldExitLong) {
                flowInfo("AI.LONG.EXIT", "Top detector signaled exit. Taking LONG profits symbol=" + symbol);
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "SELL", barClose, Math.abs(currentPosition), "MKT");
            }
            return; 
        }

        // ==========================================
        // SCENARIO 2: WE ARE ALREADY SHORT
        // ==========================================
        if (currentPosition < 0) {
            boolean rsiGate = !USE_RSI_PRE_GATES || currentRsi < RSI_SHORT_EXIT_THRESHOLD;
            boolean modelReady = activeShortExitAi != null;
            flowCondition("AI.SHORT.EXIT", "RSI_PRE_GATE", rsiGate, "symbol=" + symbol + " enabled=" + USE_RSI_PRE_GATES + " rsi=" + currentRsi + " threshold=" + RSI_SHORT_EXIT_THRESHOLD);
            flowCondition("AI.SHORT.EXIT", "MODEL_AVAILABLE", modelReady, "symbol=" + symbol + " regime=" + activeRegime);
            boolean shouldExitShort = false;
            if (rsiGate && modelReady) {
                double prob = activeShortExitAi.predictProbability(features);
                shouldExitShort = prob >= shortExitProbabilityThreshold;
                flowCondition(
                    "AI.SHORT.EXIT",
                    "AI_PREDICTS_EXIT",
                    shouldExitShort,
                    "symbol=" + symbol
                        + " rsi=" + currentRsi
                        + " close=" + barClose
                        + " prob=" + formatProb(prob)
                        + " threshold=" + formatProb(shortExitProbabilityThreshold)
                );
            }
            if (shouldExitShort) {
                flowInfo("AI.SHORT.EXIT", "Bottom detector signaled cover. Covering SHORT symbol=" + symbol);
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "BUY", barClose, Math.abs(currentPosition), "MKT");
            }
            return; 
        }

        // ==========================================
        // SCENARIO 3: WE ARE FLAT (LOOKING FOR ENTRIES)
        // ==========================================
        if (allowNewEntries && tradeCount < maxTrades) {
            int qty = sharesForAmount(barClose);
            flowCondition("AI.ENTRY", "QTY_POSITIVE", qty > 0, "symbol=" + symbol + " qty=" + qty + " close=" + barClose);
            if (qty <= 0) return;

            // --- DIP BUYING (LONG ENTRY) ---
            double longThreshold = (currentHour == 9) ? RSI_LONG_ENTRY_OPEN_THRESHOLD : RSI_LONG_ENTRY_REGULAR_THRESHOLD;
            boolean longRsiGate = !USE_RSI_PRE_GATES || currentRsi < longThreshold;
            boolean longModelReady = activeLongEntryAi != null;
            flowCondition("AI.LONG.ENTRY", "RSI_PRE_GATE", longRsiGate, "symbol=" + symbol + " enabled=" + USE_RSI_PRE_GATES + " rsi=" + currentRsi + " threshold=" + longThreshold);
            flowCondition("AI.LONG.ENTRY", "MODEL_AVAILABLE", longModelReady, "symbol=" + symbol + " regime=" + activeRegime);
            boolean shouldEnterLong = false;
            if (longRsiGate && longModelReady) {
                double prob = activeLongEntryAi.predictProbability(features);
                shouldEnterLong = prob >= longEntryProbabilityThreshold;
                flowCondition(
                    "AI.LONG.ENTRY",
                    "AI_PREDICTS_ENTRY",
                    shouldEnterLong,
                    "symbol=" + symbol
                        + " rsi=" + currentRsi
                        + " close=" + barClose
                        + " qty=" + qty
                        + " prob=" + formatProb(prob)
                        + " threshold=" + formatProb(longEntryProbabilityThreshold)
                );
            }
            if (shouldEnterLong) {
                flowInfo("AI.LONG.ENTRY", "Dip buyer firing order symbol=" + symbol + " rsi=" + String.format("%.2f", currentRsi));
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "BUY", barClose, qty, "FAST_LMT");
                return;
            }

            // --- RIP SELLING (SHORT ENTRY) ---
            double shortThreshold = (currentHour == 9) ? RSI_SHORT_ENTRY_OPEN_THRESHOLD : RSI_SHORT_ENTRY_REGULAR_THRESHOLD;
            boolean shortRsiGate = !USE_RSI_PRE_GATES || currentRsi > shortThreshold;
            boolean shortModelReady = activeShortEntryAi != null;
            flowCondition("AI.SHORT.ENTRY", "RSI_PRE_GATE", shortRsiGate, "symbol=" + symbol + " enabled=" + USE_RSI_PRE_GATES + " rsi=" + currentRsi + " threshold=" + shortThreshold);
            flowCondition("AI.SHORT.ENTRY", "MODEL_AVAILABLE", shortModelReady, "symbol=" + symbol + " regime=" + activeRegime);
            boolean shouldEnterShort = false;
            if (shortRsiGate && shortModelReady) {
                double prob = activeShortEntryAi.predictProbability(features);
                shouldEnterShort = prob >= shortEntryProbabilityThreshold;
                flowCondition(
                    "AI.SHORT.ENTRY",
                    "AI_PREDICTS_ENTRY",
                    shouldEnterShort,
                    "symbol=" + symbol
                        + " rsi=" + currentRsi
                        + " close=" + barClose
                        + " qty=" + qty
                        + " prob=" + formatProb(prob)
                        + " threshold=" + formatProb(shortEntryProbabilityThreshold)
                );
            }
            if (shouldEnterShort) {
                flowInfo("AI.SHORT.ENTRY", "Rip seller firing order symbol=" + symbol + " rsi=" + String.format("%.2f", currentRsi));
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "SELL", barClose, qty, "FAST_LMT");
            }
        } else {
            flowCondition("AI.ENTRY", "ENTRY_GATE_OPEN", false, "symbol=" + symbol + " allowNewEntries=" + allowNewEntries + " tradeCount=" + tradeCount + " maxTrades=" + maxTrades);
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

    private float[] constructRegimeClassifierFeatures(float[] baseFeatures) {
        if (baseFeatures == null || baseFeatures.length == 0) {
            return new float[0];
        }

        // Must match train_30s_models.py build_regime_feature_subset() exclusions.
        // Excluded in trainer: f_macd_diff, f_atr_norm, f_dist_sma, f_rsi,
        // f_realized_vol_20, f_realized_vol_z (f_spread_z is not in base-30 schema).
        if (baseFeatures.length < 30) {
            flowCondition(
                "AI.REGIME",
                "BASE_FEATURE_VECTOR_LEN_GE_30",
                false,
                "symbol=" + symbol + " actualLen=" + baseFeatures.length + " expectedLen=30"
            );
            return baseFeatures;
        }

        int[] includeIdx = new int[] {
            0, 1, 2,
            4, 5, 6,
            9, 10,
            12, 13,
            14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
            28, 29
        };

        float[] regimeFeatures = new float[includeIdx.length];
        for (int i = 0; i < includeIdx.length; i++) {
            regimeFeatures[i] = baseFeatures[includeIdx[i]];
        }

        flowData("AI.REGIME", "symbol=" + symbol + " regimeFeatureCount=" + regimeFeatures.length + " baseFeatureCount=" + baseFeatures.length);
        return regimeFeatures;
    }

    private float[] constructModelFeatures(double currentRsi) {
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

        // Base schema: 30 features (legacy 25 + first 5 extended core features).
        // Remaining microstructure tail features stay computed for optional 34-feature models.
        return new float[] {
            f_dist_vwap, f_bb_lower_dist, f_bb_upper_dist, f_macd_diff,
            f_body_size, f_lower_wick, f_upper_wick, f_atr_norm,
            f_dist_sma, f_dist_high, f_dist_low, f_rsi, f_gap_from_prev_close,
            f_time_of_day, 
            f_dist_swing_high, f_dist_swing_low, f_is_new_high, f_is_new_low,
            f_dist_whole_num, f_is_green, f_green_streak, f_red_streak, f_put_call_ratio,
            f_vol_ask_ratio, f_vol_bid_ratio,
            f_rel_volume_30s, f_realized_vol_20, f_realized_vol_z,
            f_dist_or_high_atr, f_dist_or_low_atr
        };
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
        }

        double pnlSnapshot = totalNetPnL;

        if (remaining == 0) tradeCount++;
        flowAnalyze("STRATEGY.ORDER", "fill orderId=" + orderId + " action=" + action + " newPos=" + newPos + " netPnL=" + String.format("%.2f", pnlSnapshot) + " dailyPnL=" + String.format("%.2f", dailyNetPnL));
    }

    public void resetForNewDay() {
        eventQueue.offer(new StrategyEvent.ResetForNewDayEvent());
    }

    private void handleResetForNewDay() {
        dailyNetPnL = 0.0;
        circuitBreakerTripped = false;
        tradeCount = 0;
        allowNewEntries = true;
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

        // We still reset barsCount to ensure the Strategy waits 5 minutes after the morning bell
        barsCount = 0;
        flowInfo("STRATEGY.RESET", "Daily limits reset. Indicator memory successfully carried over to new day symbol=" + symbol);
    }



    public void forceEndOfDayFlatten(double currentPrice) {
        int position = currentPosition;
        boolean flattenGate = position != 0 && !inFlightOrder && currentPrice > 0.0;
        flowCondition("STRATEGY.EOD", "FLATTEN_GATE", flattenGate, "symbol=" + symbol + " position=" + position + " inFlight=" + inFlightOrder + " currentPrice=" + currentPrice);
        if (!flattenGate) return;
        flowInfo("STRATEGY.EOD", "Closing position size=" + Math.abs(position) + " symbol=" + symbol + " price=" + currentPrice);
        String action = (position > 0) ? "SELL" : "BUY";
        this.inFlightOrder = true;
        parent.placeTrade(symbol, action, currentPrice, Math.abs(position), "MKT");
    }

    private void logTradeToCsv(String exitAction, int qty, double entryPrice, double exitPrice, double tradePnL) {
        LocalDateTime timestamp = currentMarketTime != null ? currentMarketTime : LocalDateTime.now(ZoneId.of("America/New_York"));
        String formattedTimestamp = timestamp.atZone(MARKET_ZONE).format(MARKET_TS_FORMAT);

        File datedLogFile = new File(parent.getTradeLogFile());
        appendTradeCsv(datedLogFile, formattedTimestamp, exitAction, qty, entryPrice, exitPrice, tradePnL);
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
        this.inFlightOrder = false;
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

    private void handleRestoreState(double rPrice, int rTrades, boolean rEnabled, boolean rArmed, double restoredYesterdayClose) {
        this.lastPrice = rPrice;
        this.tradeCount = Math.max(0, rTrades);
        this.enabled = rEnabled;
        this.allowNewEntries = rArmed;
        this.yesterdayClose = Math.max(0.0, restoredYesterdayClose);
        flowData("STRATEGY.STATE", "restored symbol=" + symbol + " lastPrice=" + rPrice + " tradeCount=" + this.tradeCount + " enabled=" + rEnabled + " armed=" + rArmed + " yesterdayClose=" + this.yesterdayClose);
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