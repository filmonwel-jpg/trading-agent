package com.calgary.fili.trader.bot.strategy;

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
    void stop();
}
