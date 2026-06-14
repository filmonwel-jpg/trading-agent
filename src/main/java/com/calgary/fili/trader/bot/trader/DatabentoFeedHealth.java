package com.calgary.fili.trader.bot.trader;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DatabentoFeedHealth {

    private static final String STARTUP_DELAY_PREFIX = "startup-delay-seconds=";
    private static final String STARTUP_DELAY_COMPLETE = "startup-delay-complete";
    private static final String STARTUP_DELAY_INTERRUPTED = "startup-delay-interrupted";
    private static final String STARTUP_HISTORY_PREFIX = "startup-history-";
    private static final String STARTUP_HISTORY_COMPLETE = "startup-history-complete";
    private static final String STARTUP_HISTORY_SKIP = "startup-history-skip";

    private final Map<String, SymbolState> symbolStates = new ConcurrentHashMap<>();
    private final AtomicBoolean gatewayRunning = new AtomicBoolean(false);
    private final AtomicBoolean startupReplayActive = new AtomicBoolean(false);
    private final AtomicLong gatewayStartedAtMs = new AtomicLong(0L);
    private final AtomicLong startupReplayStartedAtMs = new AtomicLong(0L);
    private final AtomicLong startupReplayCompletedAtMs = new AtomicLong(0L);
    private final AtomicLong lastGatewayExitAtMs = new AtomicLong(0L);
    private final AtomicInteger lastGatewayExitCode = new AtomicInteger(Integer.MIN_VALUE);
    private final AtomicLong restartCount = new AtomicLong(0L);
    private final AtomicLong lastRestartAtMs = new AtomicLong(0L);
    private final AtomicLong lastAnyEventAtMs = new AtomicLong(0L);
    private final AtomicLong lastStatusAtMs = new AtomicLong(0L);

    private volatile String lastRestartReason = "";
    private volatile String lastStatusMessage = "";
    private volatile String startupReplayPhase = "idle";

    public record SymbolSnapshot(
        String symbol,
        long lastQuoteAtMs,
        long quoteAgeMs,
        long lastBarAtMs,
        long barAgeMs,
        long lastOptionAtMs,
        long optionAgeMs,
        long lastMarketDataAtMs,
        long marketDataAgeMs,
        double lastBid,
        double lastAsk,
        long lastBidSize,
        long lastAskSize,
        boolean quoteFresh,
        long equityBarCount,
        long missingSanityContractCount,
        long lowQualityBarCount,
        long entryRejectedBarCount,
        double lastQualityScore,
        String lastDataQualityFlags,
        String lastEventSchemaVersion
    ) {}

    public record Snapshot(
        boolean gatewayRunning,
        long gatewayStartedAtMs,
        long lastGatewayExitAtMs,
        int lastGatewayExitCode,
        long restartCount,
        long lastRestartAtMs,
        String lastRestartReason,
        long lastAnyEventAtMs,
        long lastAnyEventAgeMs,
        long lastStatusAtMs,
        long lastStatusAgeMs,
        String lastStatusMessage,
        boolean marketDataExpectedNow,
        boolean startupReplayActive,
        long startupReplayStartedAtMs,
        long startupReplayAgeMs,
        long startupReplayCompletedAtMs,
        long startupReplayCompletedAgeMs,
        String startupReplayPhase,
        boolean withinStartupGrace,
        boolean healthy,
        boolean restartRecommended,
        String primarySymbol,
        SymbolSnapshot primarySymbolHealth,
        Map<String, SymbolSnapshot> symbols
    ) {}

    public void registerSymbols(Collection<String> symbols) {
        if (symbols == null) {
            return;
        }
        for (String symbol : symbols) {
            symbolState(symbol);
        }
    }

    public void registerSymbol(String symbol) {
        symbolState(symbol);
    }

    public void markGatewayStarted(Collection<String> symbols, long nowMs) {
        registerSymbols(symbols);
        gatewayRunning.set(true);
        gatewayStartedAtMs.set(nowMs);
        startupReplayActive.set(false);
        startupReplayStartedAtMs.set(0L);
        startupReplayCompletedAtMs.set(0L);
        startupReplayPhase = "idle";
        lastGatewayExitCode.set(Integer.MIN_VALUE);
    }

    public void markGatewayExited(int exitCode, long nowMs) {
        gatewayRunning.set(false);
        startupReplayActive.set(false);
        startupReplayPhase = "gateway-exited";
        lastGatewayExitAtMs.set(nowMs);
        lastGatewayExitCode.set(exitCode);
    }

    public void markRestartRequested(String reason, long nowMs) {
        restartCount.incrementAndGet();
        lastRestartAtMs.set(nowMs);
        lastRestartReason = reason == null ? "" : reason;
    }

    public void recordStatus(String message, long nowMs) {
        lastAnyEventAtMs.set(nowMs);
        lastStatusAtMs.set(nowMs);
        lastStatusMessage = message == null ? "" : message;
        updateStartupReplayState(lastStatusMessage, nowMs);
    }

    public void recordEquityBar(DatabentoEvent event, long nowMs) {
        if (event == null) {
            return;
        }
        lastAnyEventAtMs.set(nowMs);
        SymbolState state = symbolState(event.symbol);
        state.equityBarCount.incrementAndGet();
        state.lastBarAtMs.set(nowMs);
        state.lastQualityScore = event.effectiveQualityScore();
        state.lastDataQualityFlags = event.dataQualityFlags == null ? "" : event.dataQualityFlags;
        state.lastEventSchemaVersion = event.eventSchemaVersion == null ? "" : event.eventSchemaVersion;
        if (!event.hasSanityContract()) {
            state.missingSanityContractCount.incrementAndGet();
        }
        if (event.hasEntryBlockingQualityFlag() || event.effectiveQualityScore() < 0.50) {
            state.lowQualityBarCount.incrementAndGet();
        }
        if (event.bid > 0.0 || event.ask > 0.0) {
            state.lastQuoteAtMs.set(nowMs);
        }
        if (event.bid > 0.0) {
            state.lastBid = event.bid;
        }
        if (event.ask > 0.0) {
            state.lastAsk = event.ask;
        }
        if (event.bidSize > 0L) {
            state.lastBidSize = event.bidSize;
        }
        if (event.askSize > 0L) {
            state.lastAskSize = event.askSize;
        }
    }

    public void recordRejectedEquityBar(DatabentoEvent event, long nowMs) {
        if (event == null) {
            return;
        }
        SymbolState state = symbolState(event.symbol);
        state.entryRejectedBarCount.incrementAndGet();
        state.lastRejectedAtMs.set(nowMs);
    }

    public void recordOptionBar(DatabentoEvent event, long nowMs) {
        if (event == null) {
            return;
        }
        lastAnyEventAtMs.set(nowMs);
        SymbolState state = symbolState(event.underlying);
        state.lastOptionAtMs.set(nowMs);
    }

    public Snapshot snapshot(String primarySymbol,
                             long nowMs,
                             long quoteStaleThresholdMs,
                             long feedSilenceThresholdMs,
                             boolean marketDataExpectedNow) {
        long quoteThreshold = Math.max(1L, quoteStaleThresholdMs);
        long feedThreshold = Math.max(1L, feedSilenceThresholdMs);
        String normalizedPrimary = normalize(primarySymbol);

        Map<String, SymbolSnapshot> snapshots = new TreeMap<>();
        for (Map.Entry<String, SymbolState> entry : symbolStates.entrySet()) {
            SymbolSnapshot snapshot = entry.getValue().snapshot(entry.getKey(), nowMs, quoteThreshold);
            snapshots.put(entry.getKey(), snapshot);
        }

        SymbolSnapshot primary = snapshots.get(normalizedPrimary);
        if (primary == null) {
            primary = SymbolState.emptySnapshot(normalizedPrimary, nowMs, quoteThreshold);
            snapshots.put(normalizedPrimary, primary);
        }

        long startedAt = gatewayStartedAtMs.get();
        boolean running = gatewayRunning.get();
        boolean replayActive = running && startupReplayActive.get();
        long replayStartedAt = startupReplayStartedAtMs.get();
        long replayCompletedAt = startupReplayCompletedAtMs.get();
        long replayCompletedAgeMs = age(nowMs, replayCompletedAt);
        boolean withinStartupGrace = running && (
            (replayCompletedAt <= 0L && startedAt > 0L && (nowMs - startedAt) <= feedThreshold)
                || replayActive
                || (replayCompletedAgeMs >= 0L && replayCompletedAgeMs <= replayHandoffGraceMs(feedThreshold))
        );
        boolean staleBySilence = marketDataExpectedNow
            && running
            && !withinStartupGrace
            && (primary.marketDataAgeMs() < 0L || primary.marketDataAgeMs() > feedThreshold);
        // Feed liveness should reflect whether market data for the symbol is still arriving within the configured
        // silence budget. Quote freshness remains available separately for diagnostics and order-entry safety gates.
        boolean healthy = running && (!marketDataExpectedNow || !staleBySilence);
        boolean restartRecommended = !running || staleBySilence;

        return new Snapshot(
            running,
            startedAt,
            lastGatewayExitAtMs.get(),
            lastGatewayExitCode.get(),
            restartCount.get(),
            lastRestartAtMs.get(),
            lastRestartReason,
            lastAnyEventAtMs.get(),
            age(nowMs, lastAnyEventAtMs.get()),
            lastStatusAtMs.get(),
            age(nowMs, lastStatusAtMs.get()),
            lastStatusMessage,
            marketDataExpectedNow,
            replayActive,
            replayStartedAt,
            age(nowMs, replayStartedAt),
            replayCompletedAt,
            replayCompletedAgeMs,
            startupReplayPhase,
            withinStartupGrace,
            healthy,
            restartRecommended,
            normalizedPrimary,
            primary,
            Map.copyOf(snapshots)
        );
    }

    public Map<String, Object> snapshotAsMap(String primarySymbol,
                                             long nowMs,
                                             long quoteStaleThresholdMs,
                                             long feedSilenceThresholdMs,
                                             boolean marketDataExpectedNow) {
        Snapshot snapshot = snapshot(primarySymbol, nowMs, quoteStaleThresholdMs, feedSilenceThresholdMs, marketDataExpectedNow);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("gatewayRunning", snapshot.gatewayRunning());
        payload.put("gatewayStartedAtMs", snapshot.gatewayStartedAtMs());
        payload.put("lastGatewayExitAtMs", snapshot.lastGatewayExitAtMs());
        payload.put("lastGatewayExitCode", snapshot.lastGatewayExitCode() == Integer.MIN_VALUE ? null : snapshot.lastGatewayExitCode());
        payload.put("restartCount", snapshot.restartCount());
        payload.put("lastRestartAtMs", snapshot.lastRestartAtMs());
        payload.put("lastRestartReason", snapshot.lastRestartReason());
        payload.put("lastAnyEventAtMs", snapshot.lastAnyEventAtMs());
        payload.put("lastAnyEventAgeMs", snapshot.lastAnyEventAgeMs());
        payload.put("lastStatusAtMs", snapshot.lastStatusAtMs());
        payload.put("lastStatusAgeMs", snapshot.lastStatusAgeMs());
        payload.put("lastStatusMessage", snapshot.lastStatusMessage());
        payload.put("marketDataExpectedNow", snapshot.marketDataExpectedNow());
        payload.put("startupReplayActive", snapshot.startupReplayActive());
        payload.put("startupReplayStartedAtMs", snapshot.startupReplayStartedAtMs());
        payload.put("startupReplayAgeMs", snapshot.startupReplayAgeMs());
        payload.put("startupReplayCompletedAtMs", snapshot.startupReplayCompletedAtMs());
        payload.put("startupReplayCompletedAgeMs", snapshot.startupReplayCompletedAgeMs());
        payload.put("startupReplayPhase", snapshot.startupReplayPhase());
        payload.put("withinStartupGrace", snapshot.withinStartupGrace());
        payload.put("healthy", snapshot.healthy());
        payload.put("restartRecommended", snapshot.restartRecommended());
        payload.put("primarySymbol", snapshot.primarySymbol());
        payload.put("primary", symbolSnapshotAsMap(snapshot.primarySymbolHealth()));

        Map<String, Object> symbols = new LinkedHashMap<>();
        snapshot.symbols().forEach((symbol, symbolSnapshot) -> symbols.put(symbol, symbolSnapshotAsMap(symbolSnapshot)));
        payload.put("symbols", symbols);
        return payload;
    }

    private Map<String, Object> symbolSnapshotAsMap(SymbolSnapshot snapshot) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("symbol", snapshot.symbol());
        payload.put("lastQuoteAtMs", snapshot.lastQuoteAtMs());
        payload.put("quoteAgeMs", snapshot.quoteAgeMs());
        payload.put("lastBarAtMs", snapshot.lastBarAtMs());
        payload.put("barAgeMs", snapshot.barAgeMs());
        payload.put("lastOptionAtMs", snapshot.lastOptionAtMs());
        payload.put("optionAgeMs", snapshot.optionAgeMs());
        payload.put("lastMarketDataAtMs", snapshot.lastMarketDataAtMs());
        payload.put("marketDataAgeMs", snapshot.marketDataAgeMs());
        payload.put("lastBid", snapshot.lastBid());
        payload.put("lastAsk", snapshot.lastAsk());
        payload.put("lastBidSize", snapshot.lastBidSize());
        payload.put("lastAskSize", snapshot.lastAskSize());
        payload.put("quoteFresh", snapshot.quoteFresh());
        payload.put("equityBarCount", snapshot.equityBarCount());
        payload.put("missingSanityContractCount", snapshot.missingSanityContractCount());
        payload.put("lowQualityBarCount", snapshot.lowQualityBarCount());
        payload.put("entryRejectedBarCount", snapshot.entryRejectedBarCount());
        payload.put("lastQualityScore", snapshot.lastQualityScore());
        payload.put("lastDataQualityFlags", snapshot.lastDataQualityFlags());
        payload.put("lastEventSchemaVersion", snapshot.lastEventSchemaVersion());
        return payload;
    }

    private SymbolState symbolState(String symbol) {
        String normalized = normalize(symbol);
        return symbolStates.computeIfAbsent(normalized, ignored -> new SymbolState());
    }

    private static String normalize(String symbol) {
        return symbol == null ? "" : symbol.trim().toUpperCase(Locale.US);
    }

    private static long age(long nowMs, long lastMs) {
        return lastMs > 0L ? Math.max(0L, nowMs - lastMs) : -1L;
    }

    private void updateStartupReplayState(String message, long nowMs) {
        String normalized = message == null ? "" : message.trim().toLowerCase(Locale.US);
        if (normalized.isBlank()) {
            return;
        }
        if (normalized.startsWith(STARTUP_DELAY_PREFIX) || isActiveStartupHistoryMessage(normalized)) {
            startupReplayActive.set(true);
            if (startupReplayStartedAtMs.get() <= 0L) {
                startupReplayStartedAtMs.set(nowMs);
            }
            startupReplayCompletedAtMs.set(0L);
            startupReplayPhase = normalized.startsWith(STARTUP_DELAY_PREFIX) ? "startup-delay" : "startup-history";
            return;
        }
        if (normalized.startsWith(STARTUP_DELAY_COMPLETE) || normalized.startsWith(STARTUP_DELAY_INTERRUPTED)) {
            startupReplayActive.set(false);
            if (startupReplayStartedAtMs.get() <= 0L) {
                startupReplayStartedAtMs.set(nowMs);
            }
            startupReplayCompletedAtMs.set(nowMs);
            startupReplayPhase = "startup-delay-complete";
            return;
        }
        if (normalized.startsWith(STARTUP_HISTORY_COMPLETE) || normalized.startsWith(STARTUP_HISTORY_SKIP)) {
            startupReplayActive.set(false);
            if (startupReplayStartedAtMs.get() <= 0L) {
                startupReplayStartedAtMs.set(nowMs);
            }
            startupReplayCompletedAtMs.set(nowMs);
            startupReplayPhase = "live-handoff";
        }
    }

    private boolean isActiveStartupHistoryMessage(String normalizedMessage) {
        return normalizedMessage.startsWith(STARTUP_HISTORY_PREFIX)
            && !normalizedMessage.startsWith(STARTUP_HISTORY_COMPLETE)
            && !normalizedMessage.startsWith(STARTUP_HISTORY_SKIP);
    }

    private static long replayHandoffGraceMs(long feedThresholdMs) {
        long boundedThreshold = Math.max(1L, feedThresholdMs);
        return Math.max(15_000L, Math.min(60_000L, boundedThreshold / 4L));
    }

    private static final class SymbolState {
        private final AtomicLong lastQuoteAtMs = new AtomicLong(0L);
        private final AtomicLong lastBarAtMs = new AtomicLong(0L);
        private final AtomicLong lastOptionAtMs = new AtomicLong(0L);
        private final AtomicLong equityBarCount = new AtomicLong(0L);
        private final AtomicLong missingSanityContractCount = new AtomicLong(0L);
        private final AtomicLong lowQualityBarCount = new AtomicLong(0L);
        private final AtomicLong entryRejectedBarCount = new AtomicLong(0L);
        private final AtomicLong lastRejectedAtMs = new AtomicLong(0L);
        private volatile double lastBid = 0.0;
        private volatile double lastAsk = 0.0;
        private volatile long lastBidSize = 0L;
        private volatile long lastAskSize = 0L;
        private volatile double lastQualityScore = Double.NaN;
        private volatile String lastDataQualityFlags = "";
        private volatile String lastEventSchemaVersion = "";

        private SymbolSnapshot snapshot(String symbol, long nowMs, long quoteStaleThresholdMs) {
            long quoteAt = lastQuoteAtMs.get();
            long barAt = lastBarAtMs.get();
            long optionAt = lastOptionAtMs.get();
            long marketDataAt = Math.max(quoteAt, Math.max(barAt, optionAt));
            long quoteAge = age(nowMs, quoteAt);
            return new SymbolSnapshot(
                symbol,
                quoteAt,
                quoteAge,
                barAt,
                age(nowMs, barAt),
                optionAt,
                age(nowMs, optionAt),
                marketDataAt,
                age(nowMs, marketDataAt),
                lastBid,
                lastAsk,
                lastBidSize,
                lastAskSize,
                quoteAge >= 0L && quoteAge <= quoteStaleThresholdMs && lastAsk > 0.0,
                equityBarCount.get(),
                missingSanityContractCount.get(),
                lowQualityBarCount.get(),
                entryRejectedBarCount.get(),
                lastQualityScore,
                lastDataQualityFlags,
                lastEventSchemaVersion
            );
        }

        private static SymbolSnapshot emptySnapshot(String symbol, long nowMs, long quoteStaleThresholdMs) {
            return new SymbolState().snapshot(symbol, nowMs, quoteStaleThresholdMs);
        }
    }
}
