package com.calgary.fili.trader.bot.strategy;

import java.time.LocalDateTime;
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

        public BarEvent(long time, double open, double high, double low, double close, long volume, double wap) {
            this.time = time;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.wap = wap;
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
        public ResetForNewDayEvent() {
        }
    }

    public static class RestoreStateEvent extends StrategyEvent {
        public final double rPrice;
        public final int rTrades;
        public final boolean rEnabled;
        public final boolean rArmed;
        public final double restoredYesterdayClose;

        public RestoreStateEvent(double rPrice, int rTrades, boolean rEnabled, boolean rArmed, double restoredYesterdayClose) {
            this.rPrice = rPrice;
            this.rTrades = rTrades;
            this.rEnabled = rEnabled;
            this.rArmed = rArmed;
            this.restoredYesterdayClose = restoredYesterdayClose;
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
