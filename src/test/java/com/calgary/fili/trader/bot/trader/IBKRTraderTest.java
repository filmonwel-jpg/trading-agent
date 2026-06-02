package com.calgary.fili.trader.bot.trader;

import com.calgary.fili.trader.bot.strategy.PingPongStrategy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;

class IBKRTraderTest {

    @Test
    void resolveExecutionReferencePriceUsesBidForSellLimitOrders() {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        ReflectionTestUtils.setField(trader, "currentBidPrice", 101.25);
        ReflectionTestUtils.setField(trader, "currentAskPrice", 101.75);
        ReflectionTestUtils.setField(trader, "currentLastPrice", 101.50);

        Double sellReference = ReflectionTestUtils.invokeMethod(trader, "resolveExecutionReferencePrice", "SELL", 0.0, "LMT");
        Double buyReference = ReflectionTestUtils.invokeMethod(trader, "resolveExecutionReferencePrice", "BUY", 0.0, "LMT");

        assertEquals(101.25, sellReference);
        assertEquals(101.75, buyReference);
    }

    @Test
    void resolvePreferredOrderTypeUsesEntryExitIntent() {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());

        assertEquals("FAST_LMT", ReflectionTestUtils.invokeMethod(trader, "resolvePreferredOrderType", "BUY", false));
        assertEquals("FAST_LMT", ReflectionTestUtils.invokeMethod(trader, "resolvePreferredOrderType", "SELL", false));
        assertEquals("MKT", ReflectionTestUtils.invokeMethod(trader, "resolvePreferredOrderType", "SELL", true));
        assertEquals("MKT", ReflectionTestUtils.invokeMethod(trader, "resolvePreferredOrderType", "BUY", true));
        Boolean sharedFeedFallback = (Boolean) ReflectionTestUtils.getField(trader, "databentoSharedFeedFallbackToPrivateSidecar");
        assertFalse(Boolean.TRUE.equals(sharedFeedFallback));
    }

    @Test
    void effectiveSharedRelayStartupHistorySecondsUsesSharedFeedOverride() {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        ReflectionTestUtils.setField(trader, "databentoStartupHistorySeconds", 360.0);
        ReflectionTestUtils.setField(trader, "databentoSharedFeedStartupHistorySeconds", 0.0);

        Double disabled = ReflectionTestUtils.invokeMethod(trader, "effectiveSharedRelayStartupHistorySeconds");
        assertEquals(0.0, disabled == null ? -1.0 : disabled.doubleValue());

        ReflectionTestUtils.setField(trader, "databentoSharedFeedStartupHistorySeconds", 12.5);
        Double enabled = ReflectionTestUtils.invokeMethod(trader, "effectiveSharedRelayStartupHistorySeconds");
        assertEquals(12.5, enabled == null ? -1.0 : enabled.doubleValue());
    }

    @Test
    void buildAiThresholdConfigRaisesEntryThresholdsByEqualPercentageOnly() {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        ReflectionTestUtils.setField(trader, "aiLongEntryThreshold", 0.68);
        ReflectionTestUtils.setField(trader, "aiShortEntryThreshold", 0.63);
        ReflectionTestUtils.setField(trader, "aiLongExitThreshold", 0.58);
        ReflectionTestUtils.setField(trader, "aiShortExitThreshold", 0.60);
        ReflectionTestUtils.setField(trader, "aiRegimeThreshold", 0.50);
        ReflectionTestUtils.setField(trader, "aiEntryThresholdRaisePercent", 10.0);
        ReflectionTestUtils.setField(trader, "aiOpen30LongEntryThresholdRaw", "0.50");
        ReflectionTestUtils.setField(trader, "aiOpen30ShortEntryThresholdRaw", "0.90");
        ReflectionTestUtils.setField(trader, "aiChoppyLongEntryThresholdRaw", "0.40");
        ReflectionTestUtils.setField(trader, "aiTrendShortEntryThresholdRaw", "0.70");

        PingPongStrategy.AiThresholdConfig config = ReflectionTestUtils.invokeMethod(trader, "buildAiThresholdConfig");
        assertNotNull(config);

        assertEquals(0.748, config.baseLongEntryThreshold(), 1.0e-9);
        assertEquals(0.693, config.baseShortEntryThreshold(), 1.0e-9);
        assertEquals(0.55, config.open30LongEntryThreshold(), 1.0e-9);
        assertEquals(0.99, config.open30ShortEntryThreshold(), 1.0e-9);
        assertEquals(0.44, config.choppyLongEntryThreshold(), 1.0e-9);
        assertEquals(0.77, config.trendShortEntryThreshold(), 1.0e-9);
        assertEquals(0.58, config.baseLongExitThreshold(), 1.0e-9);
        assertEquals(0.60, config.open30ShortExitThreshold(), 1.0e-9);
        assertEquals(0.50, config.regimeClassifierThreshold(), 1.0e-9);
    }

    @Test
    void marketClockMonitorFlattensAtCloseWithoutWaitingForBar() {
        TestableIBKRTrader trader = new TestableIBKRTrader();
        PingPongStrategy strategy = mock(PingPongStrategy.class);
        trader.setShopStrategy(strategy);
        when(strategy.isEnabled()).thenReturn(true);

        ReflectionTestUtils.invokeMethod(
            trader,
            "monitorMarketScheduleClock",
            ZonedDateTime.of(LocalDate.of(2026, 4, 10), LocalTime.of(15, 59, 50), ZoneId.of("America/New_York"))
        );

        assertTrue(trader.flattenInvoked);
        assertEquals(List.of("cancel", "sync:eod-pre-flatten", "flatten", "sync:eod-post-flatten-confirm"), trader.actions);
        verify(strategy).setEnabled(false);
    }

    @Test
    void sharedGatewayOrderStatusForwardsFilledDeltasToStrategy() throws Exception {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        PingPongStrategy strategy = mock(PingPongStrategy.class);
        SharedIbkrGatewayClient gatewayClient = mock(SharedIbkrGatewayClient.class);
        trader.setShopStrategy(strategy);
        ReflectionTestUtils.setField(trader, "symbol", "AAPL");
        ReflectionTestUtils.setField(trader, "currentLastPrice", 101.0);
        ReflectionTestUtils.setField(trader, "currentAskPrice", 101.0);
        ReflectionTestUtils.setField(trader, "maxOrderNotional", 25_000.0);
        ReflectionTestUtils.setField(trader, "maxDailyOrders", 40);
        ReflectionTestUtils.setField(trader, "sharedIbkrGatewayClient", gatewayClient);
        ReflectionTestUtils.setField(trader, "ibkrSharedGatewaySkipDirectConnection", true);
        when(strategy.getCurrentPosition()).thenReturn(0);
        when(gatewayClient.isConnected()).thenReturn(true);
        SharedIbkrGatewayMessage ack = new SharedIbkrGatewayMessage();
        ack.ok = true;
        ack.payload = Map.of("gatewayOrderId", 123, "status", "submitted", "symbol", "AAPL", "action", "BUY", "quantity", 5);
        when(gatewayClient.submitOrder(anyString(), anyString(), anyInt(), anyString(), anyDouble(), any(), anyString(), any())).thenReturn(ack);
        SharedIbkrGatewayMessage syncAck = new SharedIbkrGatewayMessage();
        syncAck.ok = true;
        syncAck.detail = "position-sync-requested";
        syncAck.payload = Map.of("reqId", 456);
        when(gatewayClient.requestPositionSync("post-order-position-validation-AAPL-123-shared-gateway-submit")).thenReturn(syncAck);

        trader.placeTrade("AAPL", "BUY", 101.0, 5, "FAST_LMT");

        SharedIbkrGatewayMessage partial = new SharedIbkrGatewayMessage();
        partial.type = "event";
        partial.eventType = "order_status";
        partial.symbol = "AAPL";
        partial.gatewayOrderId = 123;
        partial.payload = Map.of("status", "Submitted", "filled", 2, "remaining", 3, "avg_fill_price", 101.1);
        ReflectionTestUtils.invokeMethod(trader, "handleSharedIbkrGatewayEvent", partial);

        SharedIbkrGatewayMessage filled = new SharedIbkrGatewayMessage();
        filled.type = "event";
        filled.eventType = "order_status";
        filled.symbol = "AAPL";
        filled.gatewayOrderId = 123;
        filled.payload = Map.of("status", "Filled", "filled", 5, "remaining", 0, "avg_fill_price", 101.2);
        ReflectionTestUtils.invokeMethod(trader, "handleSharedIbkrGatewayEvent", filled);

        verify(strategy).onOrderSubmitted(123, "BUY", 5);
        verify(strategy).onOrderProgress(123, "BUY", 2, 3, 101.1);
        verify(strategy).onOrderProgress(123, "BUY", 3, 0, 101.2);
        verify(strategy).onOrderClosed(123, "Filled");
        verify(gatewayClient).requestPositionSync("post-order-position-validation-AAPL-123-shared-gateway-submit");
    }

    @Test
    void pendingSubmitAndApiPendingAreNotTerminalWhenInitialGatewayRemainingIsZero() {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());

        Boolean pendingSubmitTerminal = ReflectionTestUtils.invokeMethod(trader, "isSharedGatewayEffectivelyTerminal", "PendingSubmit", 0, false);
        Boolean apiPendingTerminal = ReflectionTestUtils.invokeMethod(trader, "isSharedGatewayEffectivelyTerminal", "ApiPending", 0, false);
        Boolean pendingCancelTerminal = ReflectionTestUtils.invokeMethod(trader, "isSharedGatewayEffectivelyTerminal", "PendingCancel", 0, false);
        Boolean explicitTerminal = ReflectionTestUtils.invokeMethod(trader, "isSharedGatewayEffectivelyTerminal", "PendingSubmit", 0, true);

        assertFalse(Boolean.TRUE.equals(pendingSubmitTerminal));
        assertFalse(Boolean.TRUE.equals(apiPendingTerminal));
        assertTrue(Boolean.TRUE.equals(pendingCancelTerminal));
        assertTrue(Boolean.TRUE.equals(explicitTerminal));
    }

    @Test
    void sharedGatewayPendingSubmitAckKeepsOrderOpenAndRequestsPositionValidation() throws Exception {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        PingPongStrategy strategy = mock(PingPongStrategy.class);
        SharedIbkrGatewayClient gatewayClient = mock(SharedIbkrGatewayClient.class);
        trader.setShopStrategy(strategy);
        ReflectionTestUtils.setField(trader, "symbol", "AAPL");
        ReflectionTestUtils.setField(trader, "currentLastPrice", 101.0);
        ReflectionTestUtils.setField(trader, "currentAskPrice", 101.0);
        ReflectionTestUtils.setField(trader, "maxOrderNotional", 25_000.0);
        ReflectionTestUtils.setField(trader, "maxDailyOrders", 40);
        ReflectionTestUtils.setField(trader, "sharedIbkrGatewayClient", gatewayClient);
        ReflectionTestUtils.setField(trader, "ibkrSharedGatewaySkipDirectConnection", true);
        when(strategy.getCurrentPosition()).thenReturn(0);
        when(gatewayClient.isConnected()).thenReturn(true);
        SharedIbkrGatewayMessage submitAck = new SharedIbkrGatewayMessage();
        submitAck.ok = true;
        submitAck.payload = Map.of(
            "gatewayOrderId", 321,
            "status", "PendingSubmit",
            "remaining", 0,
            "filled", 0,
            "symbol", "AAPL",
            "action", "BUY",
            "quantity", 5
        );
        when(gatewayClient.submitOrder(anyString(), anyString(), anyInt(), anyString(), anyDouble(), any(), anyString(), any())).thenReturn(submitAck);
        SharedIbkrGatewayMessage syncAck = new SharedIbkrGatewayMessage();
        syncAck.ok = true;
        syncAck.detail = "position-sync-requested";
        syncAck.payload = Map.of("reqId", 654);
        when(gatewayClient.requestPositionSync("post-order-position-validation-AAPL-321-shared-gateway-submit")).thenReturn(syncAck);

        trader.placeTrade("AAPL", "BUY", 101.0, 5, "FAST_LMT");

        verify(strategy).onOrderSubmitted(321, "BUY", 5);
        verify(strategy, never()).onOrderClosed(anyInt(), anyString());
        verify(gatewayClient).requestPositionSync("post-order-position-validation-AAPL-321-shared-gateway-submit");
    }

    @Test
    void resetOptionVolumeAccumulatorsClearsReconnectInflation() {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        PingPongStrategy strategy = mock(PingPongStrategy.class);
        trader.setShopStrategy(strategy);
        ReflectionTestUtils.setField(trader, "symbol", "AAPL");
        ReflectionTestUtils.setField(trader, "latestPutVolume", 123L);
        ReflectionTestUtils.setField(trader, "latestCallVolume", 456L);

        ReflectionTestUtils.invokeMethod(trader, "resetOptionVolumeAccumulators", "unit-test");

        Number latestPutVolume = (Number) ReflectionTestUtils.getField(trader, "latestPutVolume");
        Number latestCallVolume = (Number) ReflectionTestUtils.getField(trader, "latestCallVolume");
        assertEquals(0L, latestPutVolume == null ? -1L : latestPutVolume.longValue());
        assertEquals(0L, latestCallVolume == null ? -1L : latestCallVolume.longValue());
        verify(strategy).onOptionVolumeUpdate(0L, 0L);
    }

    @Test
    void loadStrategyStateResetsStaleTradeCountAndPersistsTodayDate(@TempDir Path tempDir) throws IOException {
        Path statePath = tempDir.resolve("trader-state-AAPL.properties");
        Files.writeString(statePath, """
            strategy.lastPrice=123.45
            strategy.tradeCount=104
            strategy.yesterdayClose=7.89
            strategy.stateDate=2026-05-22
            strategy.hardStopExitCount=2
            strategy.lastHardStopExitTimeMs=999
            """);
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        PingPongStrategy strategy = mock(PingPongStrategy.class);
        trader.setShopStrategy(strategy);
        ReflectionTestUtils.setField(trader, "symbol", "AAPL");
        ReflectionTestUtils.setField(trader, "stateFile", statePath.toString());

        ReflectionTestUtils.invokeMethod(trader, "loadStrategyState");

        verify(strategy).restoreState(123.45, 0, true, false, 7.89, 0, 0L);
        String persisted = Files.readString(statePath);
        assertTrue(persisted.contains("strategy.tradeCount=0"));
        assertTrue(persisted.contains("strategy.stateDate=" + LocalDate.now(ZoneId.of("America/New_York"))));
        assertTrue(persisted.contains("strategy.hardStopExitCount=0"));
        assertTrue(persisted.contains("strategy.lastHardStopExitTimeMs=0"));
    }

    @Test
    void loadStrategyStatePreservesSameDayTradeCount(@TempDir Path tempDir) throws IOException {
        Path statePath = tempDir.resolve("trader-state-NVDA.properties");
        String today = LocalDate.now(ZoneId.of("America/New_York")).toString();
        Files.writeString(statePath, """
            strategy.lastPrice=456.78
            strategy.tradeCount=6
            strategy.yesterdayClose=444.44
            strategy.stateDate=%s
            strategy.hardStopExitCount=1
            strategy.lastHardStopExitTimeMs=123
            """.formatted(today));
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        PingPongStrategy strategy = mock(PingPongStrategy.class);
        trader.setShopStrategy(strategy);
        ReflectionTestUtils.setField(trader, "symbol", "NVDA");
        ReflectionTestUtils.setField(trader, "stateFile", statePath.toString());

        ReflectionTestUtils.invokeMethod(trader, "loadStrategyState");

        verify(strategy).restoreState(456.78, 6, true, false, 444.44, 1, 123L);
        assertTrue(Files.readString(statePath).contains("strategy.tradeCount=6"));
    }

    @Test
    void requestPositionsSoftRefreshPreservesBarForwardingWhenAlreadySynced() throws IOException {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        PingPongStrategy strategy = mock(PingPongStrategy.class);
        SharedIbkrGatewayClient gatewayClient = mock(SharedIbkrGatewayClient.class);
        trader.setShopStrategy(strategy);
        ReflectionTestUtils.setField(trader, "positionSyncComplete", true);
        ReflectionTestUtils.setField(trader, "sharedIbkrGatewayClient", gatewayClient);
        ReflectionTestUtils.setField(trader, "ibkrSharedGatewaySkipDirectConnection", true);
        when(gatewayClient.isConnected()).thenReturn(true);
        when(gatewayClient.requestPositionSync("strategy-refresh")).thenThrow(new IOException("ack-timeout"));

        trader.requestPositions();

        assertTrue(Boolean.TRUE.equals(ReflectionTestUtils.getField(trader, "positionSyncComplete")));
        verify(strategy).setPositionSynced(false);
        verify(gatewayClient).requestPositionSync("strategy-refresh");

        @SuppressWarnings("unchecked")
        Map<String, Object> status = trader.controlStatus();
        assertEquals("strategy-refresh", status.get("positionSyncReason"));
        assertEquals("PRESERVE_BAR_FORWARDING", status.get("positionSyncMode"));
        assertEquals("shared-gateway-request", status.get("positionSyncLastFailureStage"));
        assertEquals("ack-timeout", status.get("positionSyncLastFailureReason"));
        assertEquals("shared-gateway", status.get("positionSyncTransport"));
        assertEquals("failed:shared-gateway-request:ack-timeout", status.get("positionSyncState"));
    }

    @Test
    void cancelStaleOrderUsesSharedGatewayWhenConnected() throws IOException {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        SharedIbkrGatewayClient gatewayClient = mock(SharedIbkrGatewayClient.class);
        ReflectionTestUtils.setField(trader, "symbol", "PLTR");
        ReflectionTestUtils.setField(trader, "sharedIbkrGatewayClient", gatewayClient);
        ReflectionTestUtils.setField(trader, "ibkrSharedGatewaySkipDirectConnection", true);
        when(gatewayClient.isConnected()).thenReturn(true);
        SharedIbkrGatewayMessage ack = new SharedIbkrGatewayMessage();
        ack.ok = true;
        ack.detail = "cancel-requested";
        when(gatewayClient.cancelSymbolOrders("PLTR", "strategy-watchdog-stale-order-21003134")).thenReturn(ack);

        trader.cancelStaleOrder(21003134);

        verify(gatewayClient).cancelSymbolOrders("PLTR", "strategy-watchdog-stale-order-21003134");
    }

    @Test
    void sharedGatewayPositionSyncCompletionUpdatesDiagnosticStatus() {
        IBKRTrader trader = new IBKRTrader(new SimpleMeterRegistry());
        PingPongStrategy strategy = mock(PingPongStrategy.class);
        trader.setShopStrategy(strategy);
        ReflectionTestUtils.setField(trader, "symbol", "AAPL");
        ReflectionTestUtils.setField(trader, "positionSyncComplete", false);
        ReflectionTestUtils.setField(trader, "lastPositionSyncAttemptId", 7L);
        ReflectionTestUtils.setField(trader, "lastPositionSyncStartedAtMs", 12345L);
        ReflectionTestUtils.setField(trader, "lastPositionSyncReason", "unit-test");
        ReflectionTestUtils.setField(trader, "lastPositionSyncMode", "BLOCK_BAR_FORWARDING");
        ReflectionTestUtils.setField(trader, "lastPositionSyncTransport", "shared-gateway");

        SharedIbkrGatewayMessage message = new SharedIbkrGatewayMessage();
        message.type = "event";
        message.eventType = "position_sync_completed";
        message.symbol = "AAPL";
        ReflectionTestUtils.invokeMethod(trader, "handleSharedIbkrGatewayEvent", message);

        @SuppressWarnings("unchecked")
        Map<String, Object> status = trader.controlStatus();
        assertEquals(Boolean.TRUE, status.get("positionSyncComplete"));
        assertEquals("shared-gateway-position-sync-completed", status.get("positionSyncTransport"));
        assertTrue(((Number) status.get("positionSyncCompletedAtMs")).longValue() > 0L);
        assertEquals("synced:shared-gateway-position-sync-completed", status.get("positionSyncState"));
    }

    private static final class TestableIBKRTrader extends IBKRTrader {
        private boolean flattenInvoked = false;
        private final List<String> actions = new ArrayList<>();

        private TestableIBKRTrader() {
            super(new SimpleMeterRegistry());
        }

        @Override
        public int cancelOpenOrders() {
            actions.add("cancel");
            return 0;
        }

        @Override
        public String flattenPosition() {
            actions.add("flatten");
            flattenInvoked = true;
            return "flatten-requested";
        }

        @Override
        protected void requestPositionsForEndOfDay(String reason) {
            actions.add("sync:" + reason);
        }
    }
}

