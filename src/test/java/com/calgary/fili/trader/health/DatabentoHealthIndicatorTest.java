package com.calgary.fili.trader.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.calgary.fili.trader.bot.trader.DatabentoFeedHealth;
import com.calgary.fili.trader.bot.trader.IBKRTrader;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

class DatabentoHealthIndicatorTest {

    @Test
    void reportsUpDuringStartupGraceAndOmitsUnsetExitCodeDetail() {
        IBKRTrader trader = mock(IBKRTrader.class);
        when(trader.isDatabentoMarketDataEnabled()).thenReturn(true);
        when(trader.currentDatabentoSnapshot()).thenReturn(snapshot(true, true, false, Integer.MIN_VALUE));

        Health health = new DatabentoHealthIndicator(trader).health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals(Boolean.FALSE, health.getDetails().get("startupReplayActive"));
        assertEquals(Boolean.TRUE, health.getDetails().get("withinStartupGrace"));
        assertFalse(health.getDetails().containsKey("lastGatewayExitCode"));
    }

    @Test
    void reportsDownWhenFeedIsUnhealthyOutsideStartupGrace() {
        IBKRTrader trader = mock(IBKRTrader.class);
        when(trader.isDatabentoMarketDataEnabled()).thenReturn(true);
        when(trader.currentDatabentoSnapshot()).thenReturn(snapshot(true, false, false, 3));

        Health health = new DatabentoHealthIndicator(trader).health();

        assertEquals(Status.DOWN, health.getStatus());
        assertEquals(3, health.getDetails().get("lastGatewayExitCode"));
        assertEquals(Boolean.FALSE, health.getDetails().get("quoteFresh"));
    }

    @Test
    void reportsUpWhenFeedIsHealthyEvenIfQuoteIsNotFresh() {
        IBKRTrader trader = mock(IBKRTrader.class);
        when(trader.isDatabentoMarketDataEnabled()).thenReturn(true);
        when(trader.currentDatabentoSnapshot()).thenReturn(snapshot(true, false, true, Integer.MIN_VALUE, 5_750L, false));

        Health health = new DatabentoHealthIndicator(trader).health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals(5_750L, health.getDetails().get("quoteAgeMs"));
        assertEquals(Boolean.FALSE, health.getDetails().get("quoteFresh"));
        assertFalse(health.getDetails().containsKey("lastGatewayExitCode"));
    }

    private DatabentoFeedHealth.Snapshot snapshot(
        boolean gatewayRunning,
        boolean withinStartupGrace,
        boolean healthy,
        int lastGatewayExitCode
    ) {
        return snapshot(gatewayRunning, withinStartupGrace, healthy, lastGatewayExitCode, -1L, false);
    }

    private DatabentoFeedHealth.Snapshot snapshot(
        boolean gatewayRunning,
        boolean withinStartupGrace,
        boolean healthy,
        int lastGatewayExitCode,
        long quoteAgeMs,
        boolean quoteFresh
    ) {
        DatabentoFeedHealth.SymbolSnapshot primary = new DatabentoFeedHealth.SymbolSnapshot(
            "TSLA",
            0L,
            quoteAgeMs,
            0L,
            quoteAgeMs,
            0L,
            -1L,
            0L,
            quoteAgeMs,
            0.0,
            0.0,
            0L,
            0L,
            quoteFresh,
            0L,
            0L,
            0L,
            0L,
            Double.NaN,
            "",
            ""
        );
        return new DatabentoFeedHealth.Snapshot(
            gatewayRunning,
            1_000L,
            900L,
            lastGatewayExitCode,
            2L,
            950L,
            "process-exit-" + lastGatewayExitCode,
            1_000L,
            100L,
            1_000L,
            100L,
            "startup-delay-seconds=40.0",
            true,
            false,
            0L,
            -1L,
            0L,
            -1L,
            "idle",
            withinStartupGrace,
            healthy,
            false,
            "TSLA",
            primary,
            Map.of("TSLA", primary)
        );
    }
}


