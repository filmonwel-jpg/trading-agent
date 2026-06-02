package com.calgary.fili.trader.health;

import com.calgary.fili.trader.bot.trader.IBKRTrader;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Component
public class TradingMetricsBinder implements MeterBinder {

    private final IBKRTrader trader;

    public TradingMetricsBinder(IBKRTrader trader) {
        this.trader = trader;
    }

    @Override
    public void bindTo(@NonNull MeterRegistry registry) {
        Tags symbolTag = Tags.of("symbol", trader.getTrackedSymbol());

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

        Gauge.builder("trading.databento.enabled", trader, IBKRTrader::getDatabentoEnabledMetric)
            .description("Databento market-data provider enabled for this trader (1=yes, 0=no)")
            .tags(symbolTag)
            .register(registry);

        Gauge.builder("trading.databento.gateway.running", trader, IBKRTrader::getDatabentoGatewayRunningMetric)
            .description("Databento sidecar process running state (1=running, 0=down)")
            .tags(symbolTag)
            .register(registry);

        Gauge.builder("trading.databento.quote.fresh", trader, IBKRTrader::getDatabentoQuoteFreshMetric)
            .description("Databento quote freshness for the active symbol (1=fresh, 0=stale)")
            .tags(symbolTag)
            .register(registry);

        Gauge.builder("trading.databento.quote.age.ms", trader, IBKRTrader::getDatabentoQuoteAgeMsMetric)
            .description("Age in milliseconds of the last Databento quote for the active symbol")
            .tags(symbolTag)
            .register(registry);

        Gauge.builder("trading.databento.bar.age.ms", trader, IBKRTrader::getDatabentoBarAgeMsMetric)
            .description("Age in milliseconds of the last Databento equity bar for the active symbol")
            .tags(symbolTag)
            .register(registry);

        Gauge.builder("trading.databento.option.age.ms", trader, IBKRTrader::getDatabentoOptionAgeMsMetric)
            .description("Age in milliseconds of the last Databento option bar for the active symbol")
            .tags(symbolTag)
            .register(registry);

        Gauge.builder("trading.databento.restarts.total", trader, IBKRTrader::getDatabentoRestartCountMetric)
            .description("Total number of Databento sidecar restart requests for the active symbol")
            .tags(symbolTag)
            .register(registry);
    }
}
