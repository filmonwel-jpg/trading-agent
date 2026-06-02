package com.calgary.fili.trader.health;

import com.calgary.fili.trader.bot.trader.DatabentoFeedHealth;
import com.calgary.fili.trader.bot.trader.IBKRTrader;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class DatabentoHealthIndicator implements HealthIndicator {

    private final IBKRTrader trader;

    public DatabentoHealthIndicator(IBKRTrader trader) {
        this.trader = trader;
    }

    @Override
    public Health health() {
        if (!trader.isDatabentoMarketDataEnabled()) {
            return Health.up()
                .withDetail("provider", "ibkr")
                .withDetail("enabled", false)
                .build();
        }

        DatabentoFeedHealth.Snapshot snapshot = trader.currentDatabentoSnapshot();
        DatabentoFeedHealth.SymbolSnapshot primary = snapshot.primarySymbolHealth();
        boolean startupGrace = snapshot.gatewayRunning() && snapshot.withinStartupGrace();
        Health.Builder builder = (snapshot.healthy() || startupGrace) ? Health.up() : Health.down();
        builder = builder
            .withDetail("provider", "databento")
            .withDetail("symbol", snapshot.primarySymbol())
            .withDetail("gatewayRunning", snapshot.gatewayRunning())
            .withDetail("healthy", snapshot.healthy())
            .withDetail("restartRecommended", snapshot.restartRecommended())
            .withDetail("restartCount", snapshot.restartCount())
            .withDetail("lastRestartReason", snapshot.lastRestartReason())
            .withDetail("marketDataExpectedNow", snapshot.marketDataExpectedNow())
            .withDetail("startupReplayActive", snapshot.startupReplayActive())
            .withDetail("startupReplayPhase", snapshot.startupReplayPhase())
            .withDetail("startupReplayAgeMs", snapshot.startupReplayAgeMs())
            .withDetail("startupReplayCompletedAgeMs", snapshot.startupReplayCompletedAgeMs())
            .withDetail("withinStartupGrace", snapshot.withinStartupGrace())
            .withDetail("lastAnyEventAgeMs", snapshot.lastAnyEventAgeMs())
            .withDetail("lastStatusAgeMs", snapshot.lastStatusAgeMs())
            .withDetail("lastStatusMessage", snapshot.lastStatusMessage())
            .withDetail("quoteAgeMs", primary == null ? -1L : primary.quoteAgeMs())
            .withDetail("barAgeMs", primary == null ? -1L : primary.barAgeMs())
            .withDetail("optionAgeMs", primary == null ? -1L : primary.optionAgeMs())
            .withDetail("marketDataAgeMs", primary == null ? -1L : primary.marketDataAgeMs())
            .withDetail("quoteFresh", primary != null && primary.quoteFresh());
        if (snapshot.lastGatewayExitCode() != Integer.MIN_VALUE) {
            builder = builder.withDetail("lastGatewayExitCode", snapshot.lastGatewayExitCode());
        }
        return builder.build();
    }
}
