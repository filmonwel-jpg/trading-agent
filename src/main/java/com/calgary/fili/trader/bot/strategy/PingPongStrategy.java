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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PingPongStrategy implements TradingStrategy {

    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final DateTimeFormatter MARKET_TS_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV");
    private static final Logger log = LoggerFactory.getLogger(PingPongStrategy.class);

    public record StrategyState(double lastPrice, int tradeCount, boolean enabled, boolean isArmed, boolean isVolatile, double yesterdayClose) {}

    private final IBKRTrader parent;
    private final String symbol;

    // Core Risk Parameters
    private final int tradeQuantity;
    private final int maxTrades;
    private final double stopLossPercentage;
    private final double maxDailyDrawdown;

    // The 4-Model AI Architecture
    private AiPredictor longEntryAi;
    private AiPredictor shortEntryAi;
    private AiPredictor longExitAi;
    private AiPredictor shortExitAi;

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
    private boolean optionVolumeWarningLogged = false;


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

        this.eventProcessorThread = new Thread(this::processEvents);
        this.eventProcessorThread.setName("Strategy-Actor-Thread-" + symbol);
        this.eventProcessorThread.start();
        
        hotloadWarmupData();
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

    private void handleOptionVolumeUpdate(long putVolume, long callVolume) {
        latestPutVolume = Math.max(0L, putVolume);
        latestCallVolume = Math.max(0L, callVolume);
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
    // STREAM 2: THE BRAIN (5-Second Bars for AI Evaluation)
    // =========================================================================
    public void on5SecondBar(long time, double open, double high, double low, double close, long volume, double wap) {
        eventQueue.offer(new StrategyEvent.BarEvent(time, open, high, low, close, volume, wap));
    }

    private void handle5SecondBar(long time, double open, double high, double low, double close, long volume, double wap) {
        this.currentTickArrivalTime = System.currentTimeMillis();
        this.lastPrice = close;
        flowData("STRATEGY.BAR", "symbol=" + symbol + " epoch=" + time + " ohlc=" + open + "/" + high + "/" + low + "/" + close + " vol=" + volume + " wap=" + wap);
        
        this.currentMarketTime = LocalDateTime.ofEpochSecond(time, 0, ZoneOffset.UTC)
                              .atZone(ZoneId.of("UTC"))
                              .withZoneSameInstant(ZoneId.of("America/New_York")) // LOCKED TO ET
                                              .toLocalDateTime();

        this.barOpen = open;
        this.barHigh = high;
        this.barLow = low;
        this.barClose = close;
        this.barVolume = volume;

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
            if (barsCount < 14) {
                avgGain += gain / 14.0;
                avgLoss += loss / 14.0;
            } else {
                avgGain = (avgGain * 13.0 + gain) / 14.0;
                avgLoss = (avgLoss * 13.0 + loss) / 14.0;
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

        flowCondition("STRATEGY.WARMUP", "BARS_GT_60", barsCount > 60, "symbol=" + symbol + " barsCount=" + barsCount);
        flowCondition("STRATEGY.WARMUP", "NO_INFLIGHT_ORDER", !inFlightOrder, "symbol=" + symbol + " inFlightOrder=" + inFlightOrder);
        flowCondition("STRATEGY.WARMUP", "STRATEGY_ENABLED", enabled, "symbol=" + symbol + " enabled=" + enabled);
        flowCondition("STRATEGY.WARMUP", "CIRCUIT_BREAKER_CLEAR", !circuitBreakerTripped, "symbol=" + symbol + " circuitBreakerTripped=" + circuitBreakerTripped);

        System.out.printf(
            ">>> [5s DIAGNOSTIC] Vol=%d CumVol=%d VWAP=%.2f MACD=%.6f ATR=%.6f%n",
            barVolume, cumVol, vwap, macdDiff, atr12
        );

        currentBarVolAsk = 0L;
        currentBarVolBid = 0L;

        // Changed warmup from 300 to 60 to match the longest window (smaWindow / highWindow)
        if (barsCount > 60 && !inFlightOrder && enabled && !circuitBreakerTripped) {
            flowAnalyze("STRATEGY->AI", "Dispatching AI evaluation symbol=" + symbol + " time=" + currentMarketTime + " close=" + barClose);
            askArtificialIntelligence();
        }
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

        float[] features = construct23Features(currentRsi);
        flowData("AI.INPUT", "symbol=" + symbol + " features=" + Arrays.toString(features));

        // ==========================================
        // SCENARIO 1: WE ARE ALREADY LONG
        // ==========================================
        if (currentPosition > 0) {
            // We use a loose RSI filter for exits. If RSI is above 40 and AI spots a top, we exit.
            boolean rsiGate = currentRsi > 55.0;
            boolean modelReady = longExitAi != null;
            flowCondition("AI.LONG.EXIT", "RSI_GT_40", rsiGate, "symbol=" + symbol + " rsi=" + currentRsi);
            flowCondition("AI.LONG.EXIT", "MODEL_AVAILABLE", modelReady, "symbol=" + symbol + " model=longExitAi");
            boolean shouldExitLong = false;
            if (rsiGate && modelReady) {
                shouldExitLong = longExitAi.predict(features);
                flowCondition("AI.LONG.EXIT", "AI_PREDICTS_EXIT", shouldExitLong, "symbol=" + symbol + " rsi=" + currentRsi + " close=" + barClose);
            }
            if (shouldExitLong) {
                flowInfo("AI.LONG.EXIT", "Top detector signaled exit. Taking LONG profits symbol=" + symbol);
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "SELL", barClose, Math.abs(currentPosition), "FAST_LMT");
            }
            return; 
        }

        // ==========================================
        // SCENARIO 2: WE ARE ALREADY SHORT
        // ==========================================
        if (currentPosition < 0) {
            // Loose RSI filter. If RSI is below 60 and AI spots a bounce, we cover.
            boolean rsiGate = currentRsi < 45.0;
            boolean modelReady = shortExitAi != null;
            flowCondition("AI.SHORT.EXIT", "RSI_LT_60", rsiGate, "symbol=" + symbol + " rsi=" + currentRsi);
            flowCondition("AI.SHORT.EXIT", "MODEL_AVAILABLE", modelReady, "symbol=" + symbol + " model=shortExitAi");
            boolean shouldExitShort = false;
            if (rsiGate && modelReady) {
                shouldExitShort = shortExitAi.predict(features);
                flowCondition("AI.SHORT.EXIT", "AI_PREDICTS_EXIT", shouldExitShort, "symbol=" + symbol + " rsi=" + currentRsi + " close=" + barClose);
            }
            if (shouldExitShort) {
                flowInfo("AI.SHORT.EXIT", "Bottom detector signaled cover. Covering SHORT symbol=" + symbol);
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "BUY", barClose, Math.abs(currentPosition), "FAST_LMT");
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
            double longThreshold = (currentHour == 9) ? 25.0 : 30.0;
            boolean longRsiGate = currentRsi < longThreshold;
            boolean longModelReady = longEntryAi != null;
            flowCondition("AI.LONG.ENTRY", "RSI_BELOW_THRESHOLD", longRsiGate, "symbol=" + symbol + " rsi=" + currentRsi + " threshold=" + longThreshold);
            flowCondition("AI.LONG.ENTRY", "MODEL_AVAILABLE", longModelReady, "symbol=" + symbol + " model=longEntryAi");
            boolean shouldEnterLong = false;
            if (longRsiGate && longModelReady) {
                shouldEnterLong = longEntryAi.predict(features);
                flowCondition("AI.LONG.ENTRY", "AI_PREDICTS_ENTRY", shouldEnterLong, "symbol=" + symbol + " rsi=" + currentRsi + " close=" + barClose + " qty=" + qty);
            }
            if (shouldEnterLong) {
                flowInfo("AI.LONG.ENTRY", "Dip buyer firing order symbol=" + symbol + " rsi=" + String.format("%.2f", currentRsi));
                this.inFlightOrder = true;
                parent.placeTrade(symbol, "BUY", barClose, qty, "FAST_LMT");
                return;
            }

            // --- RIP SELLING (SHORT ENTRY) ---
            double shortThreshold = (currentHour == 9) ? 75.0 : 70.0;
            boolean shortRsiGate = currentRsi > shortThreshold;
            boolean shortModelReady = shortEntryAi != null;
            flowCondition("AI.SHORT.ENTRY", "RSI_ABOVE_THRESHOLD", shortRsiGate, "symbol=" + symbol + " rsi=" + currentRsi + " threshold=" + shortThreshold);
            flowCondition("AI.SHORT.ENTRY", "MODEL_AVAILABLE", shortModelReady, "symbol=" + symbol + " model=shortEntryAi");
            boolean shouldEnterShort = false;
            if (shortRsiGate && shortModelReady) {
                shouldEnterShort = shortEntryAi.predict(features);
                flowCondition("AI.SHORT.ENTRY", "AI_PREDICTS_ENTRY", shouldEnterShort, "symbol=" + symbol + " rsi=" + currentRsi + " close=" + barClose + " qty=" + qty);
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

    private float[] construct23Features(double currentRsi) {
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
        
        float f_put_call_ratio = currentPutCallRatio;
        float f_vol_ask_ratio = 0.33f;
        float f_vol_bid_ratio = 0.33f;
        // FIX: Only calculate ratios if we ACTUALLY received live tape data.
        // In historical backtesting, this stays false, leaving the ratios at 0.33 to match Python perfectly.
        if ((currentBarVolAsk > 0 || currentBarVolBid > 0) && barVolume > 0) {
            f_vol_ask_ratio = (float) currentBarVolAsk / (float) barVolume;
            f_vol_bid_ratio = (float) currentBarVolBid / (float) barVolume;
        }

        // Return the exact 23-feature array matching Python
        return new float[] {
            f_dist_vwap, f_bb_lower_dist, f_bb_upper_dist, f_macd_diff,
            f_body_size, f_lower_wick, f_upper_wick, f_atr_norm,
            f_dist_sma, f_dist_high, f_dist_low, f_rsi, f_gap_from_prev_close,
            f_time_of_day, 
            f_dist_swing_high, f_dist_swing_low, f_is_new_high, f_is_new_low,
            f_dist_whole_num, f_is_green, f_put_call_ratio,
            f_vol_ask_ratio, f_vol_bid_ratio
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

        int prevAbsPos = Math.abs(currentPosition);
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

            double direction = "SELL".equalsIgnoreCase(action) ? 1.0 : -1.0;
            double tradePnL = (adjustedExitPrice - avgEntryPrice) * filledDelta * direction;

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
    public boolean isVolatile() { return false; }
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

    private void flowData(String stage, String message) {
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
        log.info(">>> [FLOW][COND][{}] {}={} | {}", stage, conditionName, verdict, details);
    }
}