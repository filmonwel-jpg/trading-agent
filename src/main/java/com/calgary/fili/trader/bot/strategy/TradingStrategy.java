package com.calgary.fili.trader.bot.strategy;

import java.util.Map;

public interface TradingStrategy {
    String getSymbol();
    void setEnabled(boolean status);
    boolean isEnabled();
    void updatePosition(int newPos);
    void onTick(double price, long volume);
    void setPositionSynced(boolean synced);
    boolean isPositionSynced();
    boolean hasInFlightOrder();
    void onOrderSubmitted(int orderId, String action, int quantity);
    void onOrderProgress(int orderId, String action, int filledDelta, int remaining, double avgFillPrice);
    void onOrderClosed(int orderId, String status);
    default void onEnrichedFeatureSnapshot(long epoch, Map<String, Float> features, String schemaVersion, String source) {}
    void stop();
}
