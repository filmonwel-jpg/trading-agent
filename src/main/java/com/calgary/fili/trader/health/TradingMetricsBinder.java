package com.calgary.fili.trader.health;

import com.calgary.fili.trader.bot.trader.IBKRTrader;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;

@Component
public class TradingMetricsBinder implements MeterBinder {

    private final IBKRTrader trader;

    public TradingMetricsBinder(IBKRTrader trader) {
        this.trader = trader;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("trading.ibkr.connected", trader, t -> t.isConnected() ? 1 : 0)
            .description("IBKR connection status (1=connected, 0=disconnected)")
            .register(registry);

        Gauge.builder("trading.orders.open", trader, IBKRTrader::getOpenOrdersCount)
            .description("Open orders currently tracked by the trader")
            .register(registry);

        Gauge.builder("trading.orders.daily_submitted", trader, IBKRTrader::getDailySubmittedOrders)
            .description("Number of orders submitted in the current day")
            .register(registry);

        Gauge.builder("trading.strategy.enabled", trader, t -> t.isStrategyEnabled() ? 1 : 0)
            .description("Strategy enabled status (1=enabled, 0=disabled)")
            .register(registry);
    }
}
