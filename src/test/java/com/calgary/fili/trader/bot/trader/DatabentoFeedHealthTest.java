package com.calgary.fili.trader.bot.trader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class DatabentoFeedHealthTest {

    @Test
    void marksQuoteFreshWhenRecentAskIsAvailable() {
        DatabentoFeedHealth health = new DatabentoFeedHealth();
        long nowMs = 1_000_000L;
        health.markGatewayStarted(List.of("SPY"), nowMs);

        DatabentoEvent event = new DatabentoEvent();
        event.event = "equity_bar";
        event.symbol = "SPY";
        event.bid = 499.95;
        event.ask = 500.05;
        event.bidSize = 100;
        event.askSize = 120;
        health.recordEquityBar(event, nowMs + 250L);

        DatabentoFeedHealth.Snapshot snapshot = health.snapshot("SPY", nowMs + 2_000L, 5_000L, 45_000L, true);
        assertTrue(snapshot.gatewayRunning());
        assertTrue(snapshot.healthy());
        assertTrue(snapshot.primarySymbolHealth().quoteFresh());
        assertEquals(1_750L, snapshot.primarySymbolHealth().quoteAgeMs());
        assertEquals(500.05, snapshot.primarySymbolHealth().lastAsk(), 0.0001);
    }

    @Test
    void recommendsRestartWhenFeedIsSilentPastThreshold() {
        DatabentoFeedHealth health = new DatabentoFeedHealth();
        long startMs = 10_000L;
        health.markGatewayStarted(List.of("NVDA"), startMs);

        DatabentoFeedHealth.Snapshot snapshot = health.snapshot("NVDA", startMs + 60_000L, 5_000L, 45_000L, true);
        assertTrue(snapshot.gatewayRunning());
        assertFalse(snapshot.withinStartupGrace());
        assertFalse(snapshot.healthy());
        assertTrue(snapshot.restartRecommended());
        assertEquals(-1L, snapshot.primarySymbolHealth().quoteAgeMs());
    }

    @Test
    void staysHealthyWhenQuoteIsStaleButMarketDataIsStillWithinSilenceBudget() {
        DatabentoFeedHealth health = new DatabentoFeedHealth();
        long nowMs = 2_000_000L;
        health.markGatewayStarted(List.of("ABBV"), nowMs);

        DatabentoEvent event = new DatabentoEvent();
        event.event = "equity_bar";
        event.symbol = "ABBV";
        event.bid = 204.49;
        event.ask = 208.28;
        event.bidSize = 100;
        event.askSize = 1;
        health.recordEquityBar(event, nowMs + 250L);

        DatabentoFeedHealth.Snapshot snapshot = health.snapshot("ABBV", nowMs + 6_000L, 5_000L, 120_000L, true);
        assertTrue(snapshot.gatewayRunning());
        assertTrue(snapshot.healthy());
        assertFalse(snapshot.restartRecommended());
        assertFalse(snapshot.primarySymbolHealth().quoteFresh());
        assertEquals(5_750L, snapshot.primarySymbolHealth().quoteAgeMs());
        assertEquals(5_750L, snapshot.primarySymbolHealth().marketDataAgeMs());
    }

    @Test
    void suppressesRestartWhileStartupHistoryReplayIsStillInProgress() {
        DatabentoFeedHealth health = new DatabentoFeedHealth();
        long startMs = 25_000L;
        health.markGatewayStarted(List.of("SPY"), startMs);
        health.recordStatus("startup-history-begin dataset=EQUS.MINI schema=ohlcv-1s", startMs + 1_000L);

        DatabentoFeedHealth.Snapshot snapshot = health.snapshot("SPY", startMs + 180_000L, 5_000L, 45_000L, true);

        assertTrue(snapshot.gatewayRunning());
        assertTrue(snapshot.startupReplayActive());
        assertEquals("startup-history", snapshot.startupReplayPhase());
        assertTrue(snapshot.withinStartupGrace());
        assertTrue(snapshot.healthy());
        assertFalse(snapshot.restartRecommended());
    }

    @Test
    void grantsShortLiveHandoffGraceAfterStartupHistoryCompletes() {
        DatabentoFeedHealth health = new DatabentoFeedHealth();
        long startMs = 50_000L;
        health.markGatewayStarted(List.of("QQQ"), startMs);
        health.recordStatus("startup-history-begin dataset=EQUS.MINI schema=ohlcv-1s", startMs + 1_000L);
        health.recordStatus("startup-history-complete emittedBars=42", startMs + 10_000L);

        DatabentoFeedHealth.Snapshot handoffSnapshot = health.snapshot("QQQ", startMs + 20_000L, 5_000L, 45_000L, true);
        assertFalse(handoffSnapshot.startupReplayActive());
        assertEquals("live-handoff", handoffSnapshot.startupReplayPhase());
        assertTrue(handoffSnapshot.withinStartupGrace());
        assertTrue(handoffSnapshot.healthy());
        assertFalse(handoffSnapshot.restartRecommended());

        DatabentoFeedHealth.Snapshot expiredSnapshot = health.snapshot("QQQ", startMs + 30_001L, 5_000L, 45_000L, true);
        assertFalse(expiredSnapshot.withinStartupGrace());
        assertFalse(expiredSnapshot.healthy());
        assertTrue(expiredSnapshot.restartRecommended());
    }

    @Test
    void tracksRestartRequestsAndGatewayExit() {
        DatabentoFeedHealth health = new DatabentoFeedHealth();
        long startMs = 100L;
        health.markGatewayStarted(List.of("AMD"), startMs);
        health.markRestartRequested("process-exit-137", startMs + 500L);
        health.markGatewayExited(137, startMs + 700L);

        DatabentoFeedHealth.Snapshot snapshot = health.snapshot("AMD", startMs + 1_000L, 5_000L, 45_000L, false);
        assertFalse(snapshot.gatewayRunning());
        assertEquals(1L, snapshot.restartCount());
        assertEquals("process-exit-137", snapshot.lastRestartReason());
        assertEquals(137, snapshot.lastGatewayExitCode());
        assertTrue(snapshot.restartRecommended());
    }

    @Test
    void tracksSanityContractQualityCounters() {
        DatabentoFeedHealth health = new DatabentoFeedHealth();
        long nowMs = 500_000L;
        health.markGatewayStarted(List.of("TSLA"), nowMs);

        DatabentoEvent event = new DatabentoEvent();
        event.event = "equity_bar";
        event.symbol = "TSLA";
        event.bid = 100.0;
        event.ask = 100.1;
        event.eventSchemaVersion = "databento_ndjson_v2";
        event.dataQualityFlags = "no_quote";
        event.qualityScore = 0.25;

        health.recordEquityBar(event, nowMs + 1_000L);
        health.recordRejectedEquityBar(event, nowMs + 1_000L);

        DatabentoFeedHealth.Snapshot snapshot = health.snapshot("TSLA", nowMs + 2_000L, 5_000L, 45_000L, true);
        assertEquals(1L, snapshot.primarySymbolHealth().equityBarCount());
        assertEquals(0L, snapshot.primarySymbolHealth().missingSanityContractCount());
        assertEquals(1L, snapshot.primarySymbolHealth().lowQualityBarCount());
        assertEquals(1L, snapshot.primarySymbolHealth().entryRejectedBarCount());
        assertEquals(0.25, snapshot.primarySymbolHealth().lastQualityScore(), 0.0001);
        assertEquals("no_quote", snapshot.primarySymbolHealth().lastDataQualityFlags());
    }
}
