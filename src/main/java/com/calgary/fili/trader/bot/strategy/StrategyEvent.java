package com.calgary.fili.trader.bot.strategy;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class StrategyEvent {

    public static class TickEvent extends StrategyEvent {
        public final double price;

        public TickEvent(double price) {
            this.price = price;
        }
    }

    public static class BarEvent extends StrategyEvent {
        public final long time;
        public final double open;
        public final double high;
        public final double low;
        public final double close;
        public final long volume;
        public final double wap;
        public final boolean sourceQualityPresent;
        public final long tradeCount;
        public final long quoteCount;
        public final long atBidVol;
        public final long atAskVol;
        public final double tradeSecondsPresent;
        public final double quoteUpdateSecondsPresent;
        public final double quoteStateSecondsValid;
        public final double syntheticSeconds;
        public final double tradeCoverage;
        public final double quoteUpdateCoverage;
        public final double quoteStateCoverage;
        public final double syntheticCoverage;
        public final double quoteAgeMsMean;
        public final double quoteAgeMsMax;
        public final double validSpreadCoverage;
        public final double lockedCrossedSeconds;
        public final double qualityScore;

        public BarEvent(long time, double open, double high, double low, double close, long volume, double wap) {
            this(time, open, high, low, close, volume, wap, false, 0L, 0L,
                0L, 0L,
                Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
                Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        }

        public BarEvent(long time, double open, double high, double low, double close, long volume, double wap,
                        boolean sourceQualityPresent, long tradeCount, long quoteCount,
                        long atBidVol, long atAskVol,
                        double tradeSecondsPresent, double quoteUpdateSecondsPresent,
                        double quoteStateSecondsValid, double syntheticSeconds,
                        double tradeCoverage, double quoteUpdateCoverage, double quoteStateCoverage,
                        double syntheticCoverage, double quoteAgeMsMean, double quoteAgeMsMax,
                        double validSpreadCoverage, double lockedCrossedSeconds, double qualityScore) {
            this.time = time;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.wap = wap;
            this.sourceQualityPresent = sourceQualityPresent;
            this.tradeCount = tradeCount;
            this.quoteCount = quoteCount;
            this.atBidVol = atBidVol;
            this.atAskVol = atAskVol;
            this.tradeSecondsPresent = tradeSecondsPresent;
            this.quoteUpdateSecondsPresent = quoteUpdateSecondsPresent;
            this.quoteStateSecondsValid = quoteStateSecondsValid;
            this.syntheticSeconds = syntheticSeconds;
            this.tradeCoverage = tradeCoverage;
            this.quoteUpdateCoverage = quoteUpdateCoverage;
            this.quoteStateCoverage = quoteStateCoverage;
            this.syntheticCoverage = syntheticCoverage;
            this.quoteAgeMsMean = quoteAgeMsMean;
            this.quoteAgeMsMax = quoteAgeMsMax;
            this.validSpreadCoverage = validSpreadCoverage;
            this.lockedCrossedSeconds = lockedCrossedSeconds;
            this.qualityScore = qualityScore;
        }
    }

    public static class TapeTradeEvent extends StrategyEvent {
        public final double tradePrice;
        public final long tradeSize;
        public final double bidPrice;
        public final double askPrice;

        public TapeTradeEvent(double tradePrice, long tradeSize, double bidPrice, double askPrice) {
            this.tradePrice = tradePrice;
            this.tradeSize = tradeSize;
            this.bidPrice = bidPrice;
            this.askPrice = askPrice;
        }
    }

    public static class OptionVolumeEvent extends StrategyEvent {
        public final long putVolume;
        public final long callVolume;

        public OptionVolumeEvent(long putVolume, long callVolume) {
            this.putVolume = putVolume;
            this.callVolume = callVolume;
        }
    }

    public static class QuoteSnapshotEvent extends StrategyEvent {
        public final double bidPrice;
        public final double askPrice;
        public final long bidSize;
        public final long askSize;
        public final double shortableShares;

        public QuoteSnapshotEvent(double bidPrice, double askPrice, long bidSize, long askSize, double shortableShares) {
            this.bidPrice = bidPrice;
            this.askPrice = askPrice;
            this.bidSize = bidSize;
            this.askSize = askSize;
            this.shortableShares = shortableShares;
        }
    }

    public static class OrderFlowSnapshotEvent extends StrategyEvent {
        public final long atBidVolume;
        public final long atAskVolume;

        public OrderFlowSnapshotEvent(long atBidVolume, long atAskVolume) {
            this.atBidVolume = atBidVolume;
            this.atAskVolume = atAskVolume;
        }
    }

    public static class EnrichedFeatureSnapshotEvent extends StrategyEvent {
        public final long epoch;
        public final Map<String, Float> features;
        public final String schemaVersion;
        public final String source;

        public EnrichedFeatureSnapshotEvent(long epoch, Map<String, Float> features, String schemaVersion, String source) {
            this.epoch = epoch;
            this.features = features == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(features));
            this.schemaVersion = schemaVersion == null ? "" : schemaVersion.trim();
            this.source = source == null ? "" : source.trim();
        }
    }

    public static class OrderSubmittedEvent extends StrategyEvent {
        public final int orderId;
        public final String action;
        public final int quantity;

        public OrderSubmittedEvent(int orderId, String action, int quantity) {
            this.orderId = orderId;
            this.action = action;
            this.quantity = quantity;
        }
    }

    public static class OrderProgressEvent extends StrategyEvent {
        public final int orderId;
        public final String action;
        public final int filledDelta;
        public final int remaining;
        public final double avgFillPrice;

        public OrderProgressEvent(int orderId, String action, int filledDelta, int remaining, double avgFillPrice) {
            this.orderId = orderId;
            this.action = action;
            this.filledDelta = filledDelta;
            this.remaining = remaining;
            this.avgFillPrice = avgFillPrice;
        }
    }

    public static class OrderClosedEvent extends StrategyEvent {
        public final int orderId;
        public final String status;

        public OrderClosedEvent(int orderId, String status) {
            this.orderId = orderId;
            this.status = status;
        }
    }

    public static class PositionSyncEvent extends StrategyEvent {
        public final int brokerPosition;
        public final double avgCost;

        public PositionSyncEvent(int brokerPosition, double avgCost) {
            this.brokerPosition = brokerPosition;
            this.avgCost = avgCost;
        }
    }

    public static class ResetForNewDayEvent extends StrategyEvent {
        public final CountDownLatch ackLatch;

        public ResetForNewDayEvent() {
            this(null);
        }

        public ResetForNewDayEvent(CountDownLatch ackLatch) {
            this.ackLatch = ackLatch;
        }
    }

    public static class RestoreStateEvent extends StrategyEvent {
        public final double rPrice;
        public final int rTrades;
        public final boolean rEnabled;
        public final boolean rArmed;
        public final double restoredYesterdayClose;
        public final int restoredHardStopExitCount;
        public final long restoredLastHardStopExitTimeMs;

        public RestoreStateEvent(double rPrice, int rTrades, boolean rEnabled, boolean rArmed, double restoredYesterdayClose) {
            this(rPrice, rTrades, rEnabled, rArmed, restoredYesterdayClose, 0, 0L);
        }

        public RestoreStateEvent(double rPrice, int rTrades, boolean rEnabled, boolean rArmed, double restoredYesterdayClose,
                                 int restoredHardStopExitCount, long restoredLastHardStopExitTimeMs) {
            this.rPrice = rPrice;
            this.rTrades = rTrades;
            this.rEnabled = rEnabled;
            this.rArmed = rArmed;
            this.restoredYesterdayClose = restoredYesterdayClose;
            this.restoredHardStopExitCount = restoredHardStopExitCount;
            this.restoredLastHardStopExitTimeMs = restoredLastHardStopExitTimeMs;
        }
    }

    public static class SetEnabledEvent extends StrategyEvent {
        public final boolean status;

        public SetEnabledEvent(boolean status) {
            this.status = status;
        }
    }

    public static class SetPositionSyncedEvent extends StrategyEvent {
        public final boolean synced;

        public SetPositionSyncedEvent(boolean synced) {
            this.synced = synced;
        }
    }

    public static class SetAllowNewEntriesEvent extends StrategyEvent {
        public final boolean allow;

        public SetAllowNewEntriesEvent(boolean allow) {
            this.allow = allow;
        }
    }

    public static class SetDataQualityAllowsNewEntriesEvent extends StrategyEvent {
        public final boolean allow;

        public SetDataQualityAllowsNewEntriesEvent(boolean allow) {
            this.allow = allow;
        }
    }

    public static class SetCurrentMarketTimeEvent extends StrategyEvent {
        public final LocalDateTime time;

        public SetCurrentMarketTimeEvent(LocalDateTime time) {
            this.time = time;
        }
    }

    public static class SetYesterdayCloseEvent extends StrategyEvent {
        public final double close;

        public SetYesterdayCloseEvent(double close) {
            this.close = close;
        }
    }

    public static class StopEvent extends StrategyEvent {
        public final CountDownLatch ackLatch;

        public StopEvent() {
            this(null);
        }

        public StopEvent(CountDownLatch ackLatch) {
            this.ackLatch = ackLatch;
        }
    }
}
