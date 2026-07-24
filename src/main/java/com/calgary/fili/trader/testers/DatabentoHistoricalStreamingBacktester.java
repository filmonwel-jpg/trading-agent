package com.calgary.fili.trader.testers;

import com.calgary.fili.trader.bot.strategy.PingPongStrategy;
import com.calgary.fili.trader.bot.trader.DatabentoEvent;
import com.calgary.fili.trader.bot.trader.DatabentoLiveGateway;
import com.calgary.fili.trader.bot.trader.IBKRTrader;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Databento historical-stream backtester.
 *
 * <p>This is intentionally separate from {@link HistoricalBacktester}. It does not read combined CSVs. It launches
 * {@code scripts/databento_historical_streamer.py --source api}, consumes the same normalized NDJSON event shape used
 * by the live Databento path, and forwards events into {@link PingPongStrategy} with a simulated broker.</p>
 */
public class DatabentoHistoricalStreamingBacktester extends IBKRTrader {
    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final LocalTime ENTRY_CUTOFF = LocalTime.of(15, 50, 0);
    private static final LocalTime EOD_FLATTEN_TIME = LocalTime.of(15, 59, 50);
    private static final double SIMULATED_SLIPPAGE_PER_SHARE = 0.03;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final AtomicInteger simulatedOrderId = new AtomicInteger(1);
    private final AtomicInteger streamExitCode = new AtomicInteger(Integer.MIN_VALUE);
    private final CountDownLatch streamDone = new CountDownLatch(1);
    private final BacktestLifecycleStats lifecycleStats = new BacktestLifecycleStats();
    private final StreamSanityStats streamSanityStats = new StreamSanityStats();
    private PingPongStrategy strategy;
    private DatabentoLiveGateway gateway;
    private String backtestSymbol = "TSLA";
    private String backtestRunTimestamp;
    private String tradeLogFileName;
    private String orderHistoryFileName;
    private String tradeLifecycleSummaryFileName;
    private String streamSanityReportFileName;
    private long processedEquityBars = 0L;
    private long processedOptionBars = 0L;
    private long skippedEvents = 0L;
    private long latestPutVolume = 0L;
    private long latestCallVolume = 0L;
    private double latestBid = 0.0;
    private double latestAsk = 0.0;
    private long latestBidSize = 0L;
    private long latestAskSize = 0L;
    private LocalDate currentSessionDate;
    private LocalDateTime latestMarketTime;
    private double previousSessionClose = 0.0;
    private double lastClose = 0.0;
    private boolean eodFlattenRequested = false;
    private int simulatedBrokerPosition = 0;

    public DatabentoHistoricalStreamingBacktester() {
        super(new SimpleMeterRegistry(), null);
    }

    public static void main(String[] args) {
        DatabentoHistoricalStreamingBacktester backtester = new DatabentoHistoricalStreamingBacktester();
        String symbol = System.getProperty("backtest.symbol", "TSLA");
        if (args != null && args.length > 0 && args[0] != null && !args[0].isBlank()) {
            symbol = args[0].trim();
        }
        int exitCode = backtester.run(symbol);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public int run(String symbol) {
        backtestSymbol = normalizeSymbol(symbol);
        initializeStrategy();
        List<String> command = buildHistoricalStreamCommand(backtestSymbol);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Starting Databento historical streaming backtest symbol=" + backtestSymbol);
        System.out.println(">>> [FLOW][INFO][BACKTEST] broker=IBKR_SIMULATION fills=synchronous no_external_ibkr_connection=true");
        System.out.println(">>> [FLOW][DATA][BACKTEST] command=" + String.join(" ", command));
        System.out.println(">>> [FLOW][DATA][BACKTEST] tradeLog=" + getTradeLogFile());
        System.out.println(">>> [FLOW][DATA][BACKTEST] orderHistory=" + getOrderHistoryFile());
        initializeOrderHistoryFile();

        gateway = new DatabentoLiveGateway(
            command,
            Path.of("").toAbsolutePath().normalize(),
            Map.of(),
            this::handleHistoricalDatabentoEvent,
            line -> System.err.println(">>> [FLOW][ERROR][DATABENTO.STDERR] " + line),
            (code, wasRunning) -> {
                streamExitCode.set(code == null ? Integer.MIN_VALUE : code);
                System.out.println(">>> [FLOW][INFO][DATABENTO] historical stream exited code=" + code + " wasRunning=" + wasRunning);
                streamDone.countDown();
            }
        );

        try {
            gateway.start();
            long timeoutSeconds = Long.parseLong(System.getProperty("backtest.databento.timeoutSeconds", "0"));
            boolean completed = timeoutSeconds > 0
                ? streamDone.await(timeoutSeconds, TimeUnit.SECONDS)
                : awaitUnbounded();
            if (!completed) {
                System.err.println(">>> [FLOW][ERROR][BACKTEST] Databento historical stream timed out timeoutSeconds=" + timeoutSeconds);
                return 2;
            }
            int exitCode = streamExitCode.get();
            if (exitCode != Integer.MIN_VALUE && exitCode != 0) {
                System.err.println(">>> [FLOW][ERROR][BACKTEST] Databento historical stream failed exitCode=" + exitCode);
                return 3;
            }
            drainQueue();
            if (strategy != null && strategy.getCurrentPosition() != 0 && lastClose > 0.0) {
                strategy.forceEndOfDayFlatten(lastClose);
                drainQueue();
            }
            printSummary();
            return 0;
        } catch (Exception exception) {
            System.err.println(">>> [FLOW][ERROR][BACKTEST] historical streaming backtest failed reason=" + exception.getMessage());
            exception.printStackTrace(System.err);
            return 1;
        } finally {
            if (gateway != null) {
                gateway.stop();
            }
            if (strategy != null) {
                strategy.stop();
            }
        }
    }

    private boolean awaitUnbounded() throws InterruptedException {
        streamDone.await();
        return true;
    }

    private void initializeStrategy() {
        String modelDir = System.getProperty("trading.model.dir", "").trim();
        strategy = new PingPongStrategy(
            this,
            backtestSymbol,
            Double.parseDouble(System.getProperty("backtest.strategy.gapPercentage", "0.0025")),
            Integer.parseInt(System.getProperty("backtest.strategy.tradeAmount", "500000")),
            Integer.parseInt(System.getProperty("backtest.strategy.maxTrades", "2000")),
            true,
            Integer.parseInt(System.getProperty("backtest.strategy.regimeWindowTicks", "300")),
            Integer.parseInt(System.getProperty("backtest.strategy.rsiPeriod", "14")),
            Double.parseDouble(System.getProperty("backtest.strategy.reversalPercentage", "0.001")),
            Double.parseDouble(System.getProperty("backtest.strategy.stopLossPercentage", "0.0040")),
            Double.parseDouble(System.getProperty("backtest.strategy.maxDailyDrawdown", "5000.0")),
            Double.parseDouble(System.getProperty("backtest.strategy.minDirectionalMove", "1.20")),
            Double.parseDouble(System.getProperty("backtest.strategy.trendStrengthThreshold", "0.70")),
            modelDir.isBlank() ? null : modelDir
        );
        strategy.setAiThresholds(buildAiThresholdConfig());
        strategy.setLifecycleTelemetryListener(lifecycleStats);
        strategy.setMaxVolatilityPercent(10.0);
        strategy.setPositionSynced(true);
        double configuredPreviousClose = parsePositiveDouble(System.getProperty("backtest.previousClose", "0"), 0.0);
        if (configuredPreviousClose > 0.0) {
            previousSessionClose = configuredPreviousClose;
            strategy.setYesterdayClose(configuredPreviousClose);
            System.out.println(">>> [FLOW][CONDITION][BACKTEST.CONTEXT] PREVIOUS_CLOSE_OVERRIDE=PASS | symbol=" + backtestSymbol + " previousClose=" + configuredPreviousClose);
        }
    }

    private List<String> buildHistoricalStreamCommand(String symbol) {
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("backtest.databento.python", System.getProperty("trading.databento.python-bin", "python3")));
        command.add(System.getProperty("backtest.databento.streamer", "scripts/databento_historical_streamer.py"));
        command.add("--source");
        command.add(System.getProperty("backtest.databento.source", "api"));
        command.add("--symbols");
        command.add(symbol);
        addOptionalArg(command, "--start", System.getProperty("backtest.databento.start", ""));
        addOptionalArg(command, "--end", System.getProperty("backtest.databento.end", ""));
        addOptionalArg(command, "--equity-dataset", System.getProperty("backtest.databento.equityDataset", "EQUS.MINI"));
        addOptionalArg(command, "--equity-schema", System.getProperty("backtest.databento.equitySchema", "tbbo"));
        addOptionalArg(command, "--equity-stype-in", System.getProperty("backtest.databento.equityStypeIn", "raw_symbol"));
        addOptionalArg(command, "--options-dataset", System.getProperty("backtest.databento.optionsDataset", "OPRA.PILLAR"));
        addOptionalArg(command, "--options-schema", System.getProperty("backtest.databento.optionsSchema", "ohlcv-1s"));
        addOptionalArg(command, "--options-stype-in", System.getProperty("backtest.databento.optionsStypeIn", "parent"));
        addRepeatedOptionalArg(command, "--input-file", System.getProperty("backtest.databento.inputFile", System.getProperty("backtest.databento.recordedEventsFile", "")));
        if (Boolean.parseBoolean(System.getProperty("backtest.databento.dryRun", "false"))) {
            command.add("--dry-run");
        }
        return command;
    }

    private static void addOptionalArg(List<String> command, String flag, String value) {
        if (value != null && !value.isBlank()) {
            command.add(flag);
            command.add(value.trim());
        }
    }

    private static void addRepeatedOptionalArg(List<String> command, String flag, String rawValues) {
        if (rawValues == null || rawValues.isBlank()) {
            return;
        }
        for (String rawValue : rawValues.split(",")) {
            addOptionalArg(command, flag, rawValue);
        }
    }

    private void handleHistoricalDatabentoEvent(DatabentoEvent event) {
        try {
            if (event == null) {
                skippedEvents++;
                return;
            }
            if (event.isStatus()) {
                streamSanityStats.recordStatus(event);
                System.out.println(">>> [FLOW][INFO][DATABENTO] " + (event.message == null ? "status" : event.message));
                return;
            }
            if (event.isPreviousClose()) {
                handlePreviousClose(event);
                return;
            }
            if (event.isOptionBar()) {
                handleOptionBar(event);
                return;
            }
            if (event.isEquityBar()) {
                handleEquityBar(event);
                return;
            }
            skippedEvents++;
        } catch (Exception exception) {
            skippedEvents++;
            System.err.println(">>> [FLOW][ERROR][BACKTEST.EVENT] skipped event reason=" + exception.getMessage());
        }
    }

    private void handlePreviousClose(DatabentoEvent event) {
        String symbol = normalizeSymbol(event.symbol);
        double close = event.previousClose > 0.0 ? event.previousClose : event.close;
        if (!backtestSymbol.equals(symbol) || close <= 0.0 || strategy == null) {
            skippedEvents++;
            return;
        }
        previousSessionClose = close;
        strategy.setYesterdayClose(close);
        System.out.println(">>> [FLOW][CONDITION][BACKTEST.CONTEXT] PREVIOUS_CLOSE_AVAILABLE=PASS | symbol=" + backtestSymbol + " previousClose=" + close + " sessionDate=" + nullToEmpty(event.sessionDate));
        drainQueue();
    }

    private void handleOptionBar(DatabentoEvent event) {
        String underlying = normalizeSymbol(event.underlying == null || event.underlying.isBlank() ? event.symbol : event.underlying);
        if (!backtestSymbol.equals(underlying) || event.volume <= 0L) {
            return;
        }
        streamSanityStats.recordOption(event);
        if ("P".equalsIgnoreCase(event.right)) {
            latestPutVolume += event.volume;
        } else if ("C".equalsIgnoreCase(event.right)) {
            latestCallVolume += event.volume;
        } else {
            return;
        }
        processedOptionBars++;
        if (strategy != null) {
            strategy.onOptionVolumeUpdate(latestPutVolume, latestCallVolume);
        }
    }

    private void handleEquityBar(DatabentoEvent event) {
        if (!backtestSymbol.equals(normalizeSymbol(event.symbol)) || event.close <= 0.0) {
            return;
        }
        streamSanityStats.recordEquity(event, sanityMinQualityScore(), expectedEventSchemaVersion());
        ZonedDateTime barTs = Instant.ofEpochSecond(event.barEpochSec).atZone(ZoneOffset.UTC).withZoneSameInstant(MARKET_ZONE);
        applySessionState(barTs);
        if (event.blocksNewEntries(sanityMinQualityScore(), expectedEventSchemaVersion())) {
            strategy.setAllowNewEntries(false);
        }
        latestBid = event.bid > 0.0 ? event.bid : latestBid;
        latestAsk = event.ask > 0.0 ? event.ask : latestAsk;
        latestBidSize = event.bidSize > 0L ? event.bidSize : latestBidSize;
        latestAskSize = event.askSize > 0L ? event.askSize : latestAskSize;
        lastClose = event.close;
        latestMarketTime = barTs.toLocalDateTime();
        processedEquityBars++;
        lifecycleStats.onMarketBar(event.high, event.low, event.close);

        strategy.setCurrentMarketTime(latestMarketTime);
        strategy.onQuoteSnapshot(latestBid, latestAsk, latestBidSize, latestAskSize, 0.0);
        if (event.atBidVol > 0L || event.atAskVol > 0L) {
            strategy.onOrderFlowSnapshot(event.atBidVol, event.atAskVol);
        }
        if (event.hasEnrichedNumericFields()) {
            strategy.onEnrichedFeatureSnapshot(
                event.effectiveFeatureSnapshotEpochSec(),
                event.getEnrichedNumericFields(),
                event.featureSnapshotSchemaVersion,
                event.featureSnapshotSource
            );
        }
        strategy.onTickForExitsOnly(event.close);
        strategy.onSourceBar(
            event.barEpochSec, event.open, event.high, event.low, event.close, event.volume,
            event.wap > 0.0 ? event.wap : event.close,
            event.tradeCount, event.quoteCount, event.atBidVol, event.atAskVol,
            event.tradeSecondsPresent, event.quoteUpdateSecondsPresent, event.quoteStateSecondsValid, event.syntheticSeconds,
            event.tradeCoverage, event.quoteUpdateCoverage, event.quoteStateCoverage, event.syntheticCoverage,
            event.quoteAgeMsMean, event.quoteAgeMsMax, event.validSpreadCoverage, event.lockedCrossedSeconds, event.qualityScore
        );
        if (!eodFlattenRequested && !barTs.toLocalTime().isBefore(EOD_FLATTEN_TIME)) {
            eodFlattenRequested = true;
            strategy.forceEndOfDayFlatten(event.close);
        }
        drainQueue();
    }

    private void applySessionState(ZonedDateTime barTs) {
        LocalDate sessionDate = barTs.toLocalDate();
        if (currentSessionDate == null) {
            currentSessionDate = sessionDate;
            if (previousSessionClose > 0.0) {
                strategy.setYesterdayClose(previousSessionClose);
            }
        } else if (!currentSessionDate.equals(sessionDate)) {
            previousSessionClose = lastClose > 0.0 ? lastClose : previousSessionClose;
            currentSessionDate = sessionDate;
            latestPutVolume = 0L;
            latestCallVolume = 0L;
            eodFlattenRequested = false;
            strategy.resetForNewDay();
            strategy.setYesterdayClose(previousSessionClose);
            strategy.onOptionVolumeUpdate(0L, 0L);
        }
        LocalTime time = barTs.toLocalTime();
        strategy.setAllowNewEntries(!time.isBefore(LocalTime.of(9, 30)) && time.isBefore(ENTRY_CUTOFF));
    }

    @Override
    public void placeTrade(String symbol, String action, double currentPrice, int quantity, String orderType) {
        int finalQty = Math.min(Math.max(0, quantity), getMaxShareCap());
        if (finalQty <= 0 || strategy == null) {
            return;
        }
        int orderId = simulatedOrderId.getAndIncrement();
        int positionBefore = simulatedBrokerPosition;
        int signedQuantity = "BUY".equalsIgnoreCase(action) ? finalQty : -finalQty;
        simulatedBrokerPosition += signedQuantity;
        String transition = positionTransition(positionBefore, simulatedBrokerPosition);
        System.out.println(">>> [FLOW][DATA][BACKTEST.ORDER] simulated orderId=" + orderId + " action=" + action + " qty=" + finalQty + " symbol=" + symbol + " price=" + currentPrice + " type=" + orderType);
        appendOrderHistory(orderId, "SUBMITTED", symbol, action, finalQty, orderType, currentPrice, 0.0, "Submitted", positionBefore, positionBefore, "accepted");
        appendOrderHistory(orderId, "FILLED", symbol, action, finalQty, orderType, currentPrice, currentPrice, "Filled", positionBefore, simulatedBrokerPosition, transition);
        lifecycleStats.onSimulatedFill(action, finalQty, currentPrice, positionBefore, simulatedBrokerPosition, latestMarketTime, lifecycleEntryRiskPct());
        strategy.onOrderSubmitted(orderId, action, finalQty);
        strategy.onOrderProgress(orderId, action, finalQty, 0, currentPrice);
        strategy.onOrderClosed(orderId, "Filled");
    }

    @Override
    protected int getMaxShareCap() {
        return Integer.parseInt(System.getProperty("backtest.strategy.maxShareCap", "2000"));
    }

    @Override
    public void cancelStaleOrder(int orderIdToCancel) {
        appendOrderHistory(orderIdToCancel, "CANCELLED", backtestSymbol, "", 0, "", 0.0, 0.0, "Cancelled", simulatedBrokerPosition, simulatedBrokerPosition, "cancelled");
        if (strategy != null) {
            strategy.onOrderClosed(orderIdToCancel, "Cancelled");
        }
    }

    @Override
    public void requestPositions() {
        if (strategy != null) {
            strategy.setPositionSynced(true);
        }
    }

    @Override
    public void requestOpenOrdersSync() {
        // No-op: the simulated broker fills synchronously.
    }

    @Override
    public boolean isTradeLogFileEnabled() {
        return true;
    }

    @Override
    public boolean isTradeLogDatabaseEnabled() {
        return false;
    }

    @Override
    public void persistTradeLog(String formattedTimestamp, String tradeSymbol, String exitAction, int quantity,
                                double entryPrice, double exitPrice, double tradePnL, double cumulativePnL, String logFile) {
        // Backtests write CSV trade logs only.
    }

    @Override
    public String getTradeLogFile() {
        if (tradeLogFileName == null) {
            tradeLogFileName = System.getProperty("backtest.tradeLogFile", "runtime/backtests/databento-stream-" + backtestSymbol.toLowerCase(Locale.US) + "-" + backtestRunTimestamp() + ".csv");
        }
        return tradeLogFileName;
    }

    public String getOrderHistoryFile() {
        if (orderHistoryFileName == null) {
            orderHistoryFileName = System.getProperty("backtest.orderHistoryFile", "runtime/backtests/databento-stream-" + backtestSymbol.toLowerCase(Locale.US) + "-" + backtestRunTimestamp() + "-orders.csv");
        }
        return orderHistoryFileName;
    }

    public String getTradeLifecycleSummaryFile() {
        if (tradeLifecycleSummaryFileName == null) {
            tradeLifecycleSummaryFileName = System.getProperty("backtest.tradeLifecycleSummaryFile", "runtime/backtests/databento-stream-" + backtestSymbol.toLowerCase(Locale.US) + "-" + backtestRunTimestamp() + "-trade-lifecycle-summary.csv");
        }
        return tradeLifecycleSummaryFileName;
    }

    public String getStreamSanityReportFile() {
        if (streamSanityReportFileName == null) {
            streamSanityReportFileName = System.getProperty("backtest.streamSanityReportFile", "runtime/backtests/databento-stream-" + backtestSymbol.toLowerCase(Locale.US) + "-" + backtestRunTimestamp() + "-sanity.json");
        }
        return streamSanityReportFileName;
    }

    private String backtestRunTimestamp() {
        if (backtestRunTimestamp == null) {
            backtestRunTimestamp = LocalDateTime.now(MARKET_ZONE).format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        }
        return backtestRunTimestamp;
    }

    private void appendOrderHistory(int orderId, String eventType, String symbol, String action, int quantity, String orderType,
                                    double requestedPrice, double fillPrice, String status, int positionBefore,
                                    int positionAfter, String note) {
        Path path = Path.of(getOrderHistoryFile());
        try {
            boolean needsHeader = ensureOutputParentAndHeaderState(path);
            try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                if (needsHeader) {
                    writer.write(orderHistoryHeader());
                    writer.newLine();
                }
                writer.write(String.join(",",
                    csv(Instant.now().toString()),
                    csv(latestMarketTime == null ? "" : latestMarketTime.atZone(MARKET_ZONE).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)),
                    Integer.toString(orderId),
                    csv(eventType),
                    csv(symbol),
                    csv(action),
                    Integer.toString(quantity),
                    csv(orderType),
                    String.format(Locale.US, "%.6f", requestedPrice),
                    String.format(Locale.US, "%.6f", fillPrice),
                    csv(status),
                    Integer.toString(positionBefore),
                    Integer.toString(positionAfter),
                    csv(note)
                ));
                writer.newLine();
            }
        } catch (IOException exception) {
            System.err.println(">>> [FLOW][ERROR][BACKTEST.ORDER_HISTORY] write failed file=" + path + " reason=" + exception.getMessage());
        }
    }

    private void initializeOrderHistoryFile() {
        Path path = Path.of(getOrderHistoryFile());
        try {
            boolean needsHeader = ensureOutputParentAndHeaderState(path);
            if (needsHeader) {
                try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                    writer.write(orderHistoryHeader());
                    writer.newLine();
                }
            }
        } catch (IOException exception) {
            System.err.println(">>> [FLOW][ERROR][BACKTEST.ORDER_HISTORY] initialize failed file=" + path + " reason=" + exception.getMessage());
        }
    }

    private static boolean ensureOutputParentAndHeaderState(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        return !Files.exists(path) || Files.size(path) == 0L;
    }

    private static String orderHistoryHeader() {
        return "WallTime,MarketTime,OrderId,EventType,Symbol,Action,Quantity,OrderType,RequestedPrice,FillPrice,Status,PositionBefore,PositionAfter,Note";
    }

    private static String positionTransition(int before, int after) {
        if (before == after) {
            return "unchanged";
        }
        if (before == 0 && after != 0) {
            return after > 0 ? "opened_long" : "opened_short";
        }
        if (before != 0 && after == 0) {
            return before > 0 ? "closed_long" : "closed_short";
        }
        if (Integer.signum(before) != Integer.signum(after)) {
            return after > 0 ? "reversed_to_long" : "reversed_to_short";
        }
        return Math.abs(after) > Math.abs(before) ? "increased" : "reduced";
    }

    private static String csv(String value) {
        String safe = value == null ? "" : value;
        if (safe.contains(",") || safe.contains("\"") || safe.contains("\n") || safe.contains("\r")) {
            return "\"" + safe.replace("\"", "\"\"") + "\"";
        }
        return safe;
    }

    private void drainQueue() {
        if (strategy == null) {
            return;
        }
        try {
            while (strategy.getQueueSize() > 0) {
                Thread.sleep(1L);
            }
            while (strategy.hasInFlightOrder()) {
                Thread.sleep(1L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void printSummary() {
        BacktestLifecycleStats.Summary lifecycleSummary = lifecycleStats.summary();
        int closedTradeCount = lifecycleStats.closedTradesSnapshot().size();
        writeTradeLifecycleSummary(lifecycleSummary);
        writeStreamSanityReport();
        System.out.println(">>> [FLOW][INFO][BACKTEST] ==============================================");
        System.out.println(">>> [FLOW][INFO][BACKTEST] DATABENTO HISTORICAL STREAM BACKTEST COMPLETE");
        System.out.println(">>> [FLOW][INFO][BACKTEST] Symbol: " + backtestSymbol);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Equity bars: " + processedEquityBars);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Option bars: " + processedOptionBars);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Skipped events: " + skippedEvents);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Final position: " + (strategy == null ? 0 : strategy.getCurrentPosition()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] Simulated broker position: " + simulatedBrokerPosition);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Total trades: " + closedTradeCount);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Current session trade counter: " + (strategy == null ? 0 : strategy.getTradeCount()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] Total PnL: " + (strategy == null ? 0.0 : strategy.getTotalNetPnL()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] arms_total: " + lifecycleSummary.armsTotal());
        System.out.println(">>> [FLOW][INFO][BACKTEST] arms_long: " + lifecycleSummary.armsLong());
        System.out.println(">>> [FLOW][INFO][BACKTEST] arms_short: " + lifecycleSummary.armsShort());
        System.out.println(">>> [FLOW][INFO][BACKTEST] arm_confirmations: " + lifecycleSummary.armConfirmations());
        System.out.println(">>> [FLOW][INFO][BACKTEST] arm_expirations: " + lifecycleSummary.armExpirations());
        System.out.println(">>> [FLOW][INFO][BACKTEST] arm_conversion_rate: " + formatDouble(lifecycleSummary.armConversionRate()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] guard_evaluations: " + lifecycleSummary.guardEvaluations());
        System.out.println(">>> [FLOW][INFO][BACKTEST] guard_fires: " + lifecycleSummary.guardFires());
        System.out.println(">>> [FLOW][INFO][BACKTEST] lifecycle_exits: " + lifecycleSummary.lifecycleExits());
        System.out.println(">>> [FLOW][INFO][BACKTEST] hard_risk_exits: " + lifecycleSummary.hardRiskExits());
        System.out.println(">>> [FLOW][INFO][BACKTEST] eod_exits: " + lifecycleSummary.eodExits());
        System.out.println(">>> [FLOW][INFO][BACKTEST] avg_setup_to_fill_s: " + formatDouble(lifecycleSummary.avgSetupToFillSeconds()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] avg_mfe_r: " + formatDouble(lifecycleSummary.avgMfeR()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] avg_mae_r: " + formatDouble(lifecycleSummary.avgMaeR()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] exit_reason_dist: " + lifecycleSummary.exitReasonDistribution());
        printAiDecisionDiagnostics();
        System.out.println(">>> [FLOW][INFO][BACKTEST] Trade log: " + getTradeLogFile());
        System.out.println(">>> [FLOW][INFO][BACKTEST] Order history: " + getOrderHistoryFile());
        System.out.println(">>> [FLOW][INFO][BACKTEST] Trade lifecycle summary: " + getTradeLifecycleSummaryFile());
        System.out.println(">>> [FLOW][INFO][BACKTEST] Stream sanity report: " + getStreamSanityReportFile());
        System.out.println(">>> [FLOW][INFO][BACKTEST] ==============================================");
    }

    private void writeStreamSanityReport() {
        Path path = Path.of(getStreamSanityReportFile());
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Map<String, Object> report = streamSanityStats.report(
                backtestSymbol,
                expectedEventSchemaVersion(),
                sanityMinQualityScore(),
                processedEquityBars,
                processedOptionBars,
                skippedEvents
            );
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), report);
        } catch (IOException exception) {
            System.err.println(">>> [FLOW][ERROR][BACKTEST.SANITY] write failed file=" + path + " reason=" + exception.getMessage());
        }
    }

    private static String expectedEventSchemaVersion() {
        return System.getProperty("backtest.databento.sanity.expectedEventSchemaVersion", "databento_ndjson_v2").trim();
    }

    private static double sanityMinQualityScore() {
        return parsePositiveDouble(System.getProperty("backtest.databento.sanity.minQualityScore", "0.50"), 0.50);
    }

    private void printAiDecisionDiagnostics() {
        if (strategy == null) {
            return;
        }
        PingPongStrategy.AiDecisionDiagnostics diagnostics = strategy.getAiDecisionDiagnostics();
        System.out.println(
            ">>> [FLOW][INFO][BACKTEST.NO_TRADE_DIAG] ai_evaluations=" + diagnostics.aiEvaluations()
                + " flat_entry_evaluations=" + diagnostics.flatEntryEvaluations()
                + " entry_gate_open=" + diagnostics.entryGateOpen()
                + " entry_gate_closed=" + diagnostics.entryGateClosed()
                + " missing_market_time=" + diagnostics.missingMarketTime()
                + " pre_market_blocked=" + diagnostics.preMarketBlocked()
                + " missing_previous_close=" + diagnostics.missingPreviousClose()
                + " variance_blocked=" + diagnostics.varianceBlocked()
                + " position_open_skipped=" + diagnostics.positionOpenSkipped()
        );
        System.out.println(
            ">>> [FLOW][INFO][BACKTEST.NO_TRADE_DIAG] entry_gate_blockers"
                + " allow_new_entries=" + diagnostics.allowNewEntriesBlocked()
                + " max_trades=" + diagnostics.maxTradesBlocked()
                + " position_sync=" + diagnostics.positionSyncBlocked()
                + " hard_stop_cooldown=" + diagnostics.hardStopCooldownBlocked()
                + " hard_stop_budget=" + diagnostics.hardStopBudgetBlocked()
                + " buy_qty=" + diagnostics.buyQuantityBlocked()
                + " sell_qty=" + diagnostics.sellQuantityBlocked()
        );
        System.out.println(
            ">>> [FLOW][INFO][BACKTEST.NO_TRADE_DIAG] long_entry"
                + " model_evals=" + diagnostics.longEntryModelEvaluations()
                + " passes=" + diagnostics.longEntryPasses()
                + " near_misses=" + diagnostics.longNearMisses()
                + " rsi_blocked=" + diagnostics.longRsiGateBlocked()
                + " model_unavailable=" + diagnostics.longModelUnavailable()
                + " max_prob=" + formatDiagnosticDouble(diagnostics.maxLongEntryProbability())
                + " max_threshold=" + formatDiagnosticDouble(diagnostics.maxLongEntryThreshold())
                + " max_margin=" + formatDiagnosticDouble(diagnostics.maxLongEntryMargin())
                + " max_epoch=" + diagnostics.maxLongEntryEpoch()
                + " max_time=" + nullToEmpty(diagnostics.maxLongEntryTime())
        );
        System.out.println(
            ">>> [FLOW][INFO][BACKTEST.NO_TRADE_DIAG] short_entry"
                + " model_evals=" + diagnostics.shortEntryModelEvaluations()
                + " passes=" + diagnostics.shortEntryPasses()
                + " near_misses=" + diagnostics.shortNearMisses()
                + " rsi_blocked=" + diagnostics.shortRsiGateBlocked()
                + " model_unavailable=" + diagnostics.shortModelUnavailable()
                + " max_prob=" + formatDiagnosticDouble(diagnostics.maxShortEntryProbability())
                + " max_threshold=" + formatDiagnosticDouble(diagnostics.maxShortEntryThreshold())
                + " max_margin=" + formatDiagnosticDouble(diagnostics.maxShortEntryMargin())
                + " max_epoch=" + diagnostics.maxShortEntryEpoch()
                + " max_time=" + nullToEmpty(diagnostics.maxShortEntryTime())
        );
        int rank = 1;
        for (PingPongStrategy.SetupCandidateDiagnostic candidate : diagnostics.closestSetupEvents()) {
            System.out.println(
                ">>> [FLOW][INFO][BACKTEST.NO_TRADE_DIAG] closest_setup"
                    + " rank=" + rank++
                    + " side=" + candidate.side()
                    + " epoch=" + candidate.epoch()
                    + " time=" + nullToEmpty(candidate.marketTime())
                    + " prob=" + formatDiagnosticDouble(candidate.probability())
                    + " threshold=" + formatDiagnosticDouble(candidate.threshold())
                    + " margin=" + formatDiagnosticDouble(candidate.margin())
                    + " rsi=" + formatDiagnosticDouble(candidate.rsi())
                    + " referencePrice=" + formatDiagnosticDouble(candidate.referencePrice())
                    + " qty=" + candidate.quantity()
                    + " regime=" + candidate.regime()
            );
        }
    }

    private void writeTradeLifecycleSummary(BacktestLifecycleStats.Summary summary) {
        Path path = Path.of(getTradeLifecycleSummaryFile());
        List<BacktestLifecycleStats.ClosedTrade> closedTrades = lifecycleStats.closedTradesSnapshot();
        String runCompletedAt = Instant.now().toString();
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                writer.write(tradeLifecycleSummaryHeader());
                writer.newLine();
                if (closedTrades.isEmpty()) {
                    writer.write(tradeLifecycleSummaryRow(runCompletedAt, summary, null));
                    writer.newLine();
                } else {
                    for (BacktestLifecycleStats.ClosedTrade trade : closedTrades) {
                        writer.write(tradeLifecycleSummaryRow(runCompletedAt, summary, trade));
                        writer.newLine();
                    }
                }
            }
        } catch (IOException exception) {
            System.err.println(">>> [FLOW][ERROR][BACKTEST.LIFECYCLE_SUMMARY] write failed file=" + path + " reason=" + exception.getMessage());
        }
    }

    private static String tradeLifecycleSummaryHeader() {
        return "RunCompletedAt,Symbol,ClosedTradeIndex,TradeSide,EntryTime,ExitTime,Quantity,EntryPrice,ExitPrice,TradePnL,RealizedR,MfeR,MaeR,SetupArmTime,SetupToFillSeconds,EntryRoute,ExitReason,EntryProb,EntryThreshold,EntryThresholdMargin,ArmsTotal,ArmsLong,ArmsShort,ArmConfirmations,ArmExpirations,ArmConversionRate,GuardEvaluations,GuardFires,LifecycleExits,HardRiskExits,EodExits,AvgSetupToFillSeconds,AvgMfeR,AvgMaeR,ExitReasonDistribution";
    }

    private String tradeLifecycleSummaryRow(String runCompletedAt, BacktestLifecycleStats.Summary summary,
                                            BacktestLifecycleStats.ClosedTrade trade) {
        return String.join(",",
            csv(runCompletedAt),
            csv(backtestSymbol),
            trade == null ? "" : Integer.toString(trade.index()),
            csv(trade == null ? "" : trade.side()),
            csv(trade == null ? "" : trade.entryTime()),
            csv(trade == null ? "" : trade.exitTime()),
            trade == null ? "" : Integer.toString(trade.quantity()),
            formatDouble(trade == null ? Double.NaN : trade.entryPrice()),
            formatDouble(trade == null ? Double.NaN : trade.exitPrice()),
            formatDouble(trade == null ? Double.NaN : trade.tradePnl()),
            formatDouble(trade == null ? Double.NaN : trade.realizedR()),
            formatDouble(trade == null ? Double.NaN : trade.mfeR()),
            formatDouble(trade == null ? Double.NaN : trade.maeR()),
            csv(trade == null ? "" : trade.setupArmTime()),
            formatDouble(trade == null ? Double.NaN : trade.setupToFillSeconds()),
            csv(trade == null ? "" : trade.entryRoute()),
            csv(trade == null ? "" : trade.exitReason()),
            formatDouble(trade == null ? Double.NaN : trade.entryProbability()),
            formatDouble(trade == null ? Double.NaN : trade.entryThreshold()),
            formatDouble(trade == null ? Double.NaN : trade.entryThresholdMargin()),
            Long.toString(summary.armsTotal()),
            Long.toString(summary.armsLong()),
            Long.toString(summary.armsShort()),
            Long.toString(summary.armConfirmations()),
            Long.toString(summary.armExpirations()),
            formatDouble(summary.armConversionRate()),
            Long.toString(summary.guardEvaluations()),
            Long.toString(summary.guardFires()),
            Long.toString(summary.lifecycleExits()),
            Long.toString(summary.hardRiskExits()),
            Long.toString(summary.eodExits()),
            formatDouble(summary.avgSetupToFillSeconds()),
            formatDouble(summary.avgMfeR()),
            formatDouble(summary.avgMaeR()),
            csv(summary.exitReasonDistribution())
        );
    }

    private static String formatDouble(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return "";
        }
        return String.format(Locale.US, "%.6f", value);
    }

    private static String formatDiagnosticDouble(double value) {
        String formatted = formatDouble(value);
        return formatted.isBlank() ? "n/a" : formatted;
    }

    private static double lifecycleEntryRiskPct() {
        try {
            double parsed = Double.parseDouble(System.getProperty("strategy.exit.lifecycle.entryRiskPct", "0.0025").trim());
            return parsed > 0.0 && Double.isFinite(parsed) ? parsed : 0.0025;
        } catch (Exception ignored) {
            return 0.0025;
        }
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static final class BacktestLifecycleStats implements PingPongStrategy.LifecycleTelemetryListener {
        private long armsTotal;
        private long armsLong;
        private long armsShort;
        private long armConfirmations;
        private long armExpirations;
        private long guardEvaluations;
        private long guardFires;
        private long lifecycleExits;
        private long hardRiskExits;
        private long eodExits;
        private double setupToFillSecondsSum;
        private long setupToFillSamples;
        private final List<ClosedTrade> closedTrades = new ArrayList<>();
        private final Map<String, Long> exitReasonCounts = new LinkedHashMap<>();
        private OpenTrade openTrade;
        private ConfirmedEntry pendingConfirmedEntry;
        private String pendingExitReason = "";

        BacktestLifecycleStats() {
            exitReasonCounts.put("lifecycle", 0L);
            exitReasonCounts.put("guard", 0L);
            exitReasonCounts.put("hard_stop", 0L);
            exitReasonCounts.put("hard_risk", 0L);
            exitReasonCounts.put("target", 0L);
            exitReasonCounts.put("eod", 0L);
            exitReasonCounts.put("other", 0L);
        }

        @Override
        public synchronized void onMicroEntryArmed(String symbol, String side, long armEpoch, double setupProbability, double setupThreshold) {
            armsTotal++;
            if ("long".equalsIgnoreCase(side)) {
                armsLong++;
            } else if ("short".equalsIgnoreCase(side)) {
                armsShort++;
            }
        }

        @Override
        public synchronized void onMicroEntryArmCleared(String symbol, String side, long armEpoch, String reason) {
            if (reason != null && reason.toLowerCase(Locale.US).startsWith("expired")) {
                armExpirations++;
            }
        }

        @Override
        public synchronized void onMicroEntryConfirmed(String symbol, String side, long armEpoch, long confirmEpoch,
                                                       double probability, double threshold, int quantity,
                                                       double referencePrice) {
            armConfirmations++;
            double setupToFillSeconds = armEpoch > 0L && confirmEpoch > 0L ? Math.max(0L, confirmEpoch - armEpoch) : Double.NaN;
            if (!Double.isNaN(setupToFillSeconds)) {
                setupToFillSecondsSum += setupToFillSeconds;
                setupToFillSamples++;
            }
            pendingConfirmedEntry = new ConfirmedEntry(
                normalizeSide(side),
                armEpoch,
                confirmEpoch,
                setupToFillSeconds,
                probability,
                threshold,
                probability - threshold
            );
        }

        @Override
        public synchronized void onMicroExitGuardEvaluated(String symbol, String side, long epoch, double probability,
                                                           double threshold, boolean fired) {
            guardEvaluations++;
            if (fired) {
                guardFires++;
                pendingExitReason = "guard";
            }
        }

        @Override
        public synchronized void onLifecycleExitEvaluated(String symbol, String side, long epoch, double probability,
                                                          double threshold, boolean fired, double unrealizedR) {
            if (fired) {
                lifecycleExits++;
                pendingExitReason = "lifecycle";
            }
        }

        @Override
        public synchronized void onHardRiskExit(String symbol, String side, String reason) {
            hardRiskExits++;
            String normalized = reason == null ? "hard_risk" : reason.toLowerCase(Locale.US);
            pendingExitReason = normalized.startsWith("hard_stop") ? "hard_stop" : "hard_risk";
        }

        @Override
        public synchronized void onEndOfDayExit(String symbol, String side, long epoch, double executionPrice) {
            eodExits++;
            pendingExitReason = "eod";
        }

        synchronized void onMarketBar(double high, double low, double close) {
            if (openTrade == null || openTrade.entryPrice <= 0.0) {
                return;
            }
            double effectiveHigh = high > 0.0 ? high : close;
            double effectiveLow = low > 0.0 ? low : close;
            double effectiveClose = close > 0.0 ? close : openTrade.entryPrice;
            if (openTrade.sideSign > 0) {
                double favR = ((Math.max(effectiveHigh, effectiveClose) - openTrade.entryPrice) / openTrade.entryPrice) / Math.max(openTrade.riskPct, 1.0e-9);
                double advR = ((Math.min(effectiveLow, effectiveClose) - openTrade.entryPrice) / openTrade.entryPrice) / Math.max(openTrade.riskPct, 1.0e-9);
                openTrade.mfeR = Math.max(openTrade.mfeR, favR);
                openTrade.maeR = Math.min(openTrade.maeR, advR);
            } else if (openTrade.sideSign < 0) {
                double favR = ((openTrade.entryPrice - Math.min(effectiveLow, effectiveClose)) / openTrade.entryPrice) / Math.max(openTrade.riskPct, 1.0e-9);
                double advR = ((openTrade.entryPrice - Math.max(effectiveHigh, effectiveClose)) / openTrade.entryPrice) / Math.max(openTrade.riskPct, 1.0e-9);
                openTrade.mfeR = Math.max(openTrade.mfeR, favR);
                openTrade.maeR = Math.min(openTrade.maeR, advR);
            }
        }

        synchronized void onSimulatedFill(String action, int quantity, double fillPrice, int positionBefore, int positionAfter,
                                          LocalDateTime marketTime, double riskPct) {
            if (quantity <= 0 || fillPrice <= 0.0) {
                return;
            }
            int beforeSign = Integer.signum(positionBefore);
            int afterSign = Integer.signum(positionAfter);
            int beforeAbs = Math.abs(positionBefore);
            int afterAbs = Math.abs(positionAfter);
            double adjustedFillPrice = adjustedFillPrice(action, fillPrice);

            boolean reducedOrReversed = beforeAbs > 0 && (afterAbs < beforeAbs || (afterSign != 0 && afterSign != beforeSign));
            if (reducedOrReversed) {
                int closedQuantity = Math.min(quantity, beforeAbs);
                closeTrade(closedQuantity, adjustedFillPrice, marketTime, riskPct);
            }

            boolean openedOrIncreased = afterAbs > 0 && (beforeAbs == 0 || afterSign != beforeSign || afterAbs > beforeAbs);
            if (openedOrIncreased) {
                int openedQuantity = (beforeAbs == 0 || afterSign != beforeSign) ? afterAbs : afterAbs - beforeAbs;
                openOrIncreaseTrade(afterSign, openedQuantity, adjustedFillPrice, marketTime, riskPct);
            }
        }

        synchronized Summary summary() {
            long closedCount = closedTrades.size();
            double mfeSum = 0.0;
            double maeSum = 0.0;
            for (ClosedTrade trade : closedTrades) {
                mfeSum += trade.mfeR();
                maeSum += trade.maeR();
            }
            return new Summary(
                armsTotal,
                armsLong,
                armsShort,
                armConfirmations,
                armExpirations,
                armsTotal > 0L ? (double) armConfirmations / (double) armsTotal : 0.0,
                guardEvaluations,
                guardFires,
                lifecycleExits,
                hardRiskExits,
                eodExits,
                setupToFillSamples > 0L ? setupToFillSecondsSum / (double) setupToFillSamples : 0.0,
                closedCount > 0L ? mfeSum / (double) closedCount : 0.0,
                closedCount > 0L ? maeSum / (double) closedCount : 0.0,
                exitReasonDistribution()
            );
        }

        synchronized List<ClosedTrade> closedTradesSnapshot() {
            return new ArrayList<>(closedTrades);
        }

        private void openOrIncreaseTrade(int sideSign, int quantity, double adjustedEntryPrice,
                                         LocalDateTime marketTime, double riskPct) {
            if (quantity <= 0 || sideSign == 0) {
                return;
            }
            String side = sideSign > 0 ? "long" : "short";
            ConfirmedEntry confirmedEntry = consumePendingConfirmedEntry(side);
            if (openTrade == null || openTrade.sideSign != sideSign) {
                openTrade = new OpenTrade();
                openTrade.side = side;
                openTrade.sideSign = sideSign;
                openTrade.quantity = quantity;
                openTrade.entryPrice = adjustedEntryPrice;
                openTrade.entryTime = formatMarketTime(marketTime);
                openTrade.riskPct = riskPct > 0.0 ? riskPct : 0.0025;
                if (confirmedEntry != null) {
                    openTrade.entryRoute = "micro_entry";
                    openTrade.setupArmTime = formatEpoch(confirmedEntry.armEpoch());
                    openTrade.setupToFillSeconds = confirmedEntry.setupToFillSeconds();
                    openTrade.entryProbability = confirmedEntry.probability();
                    openTrade.entryThreshold = confirmedEntry.threshold();
                    openTrade.entryThresholdMargin = confirmedEntry.thresholdMargin();
                } else {
                    openTrade.entryRoute = "direct_30s";
                }
                return;
            }

            int combinedQuantity = openTrade.quantity + quantity;
            if (combinedQuantity > 0) {
                openTrade.entryPrice = ((openTrade.entryPrice * openTrade.quantity) + (adjustedEntryPrice * quantity)) / combinedQuantity;
                openTrade.quantity = combinedQuantity;
            }
        }

        private void closeTrade(int closedQuantity, double adjustedExitPrice, LocalDateTime marketTime,
                                double fallbackRiskPct) {
            if (closedQuantity <= 0) {
                return;
            }
            String exitReason = consumePendingExitReason();
            incrementExitReason(exitReason);
            if (openTrade == null || openTrade.entryPrice <= 0.0 || openTrade.sideSign == 0) {
                closedTrades.add(new ClosedTrade(
                    closedTrades.size() + 1,
                    "unknown",
                    "",
                    formatMarketTime(marketTime),
                    closedQuantity,
                    Double.NaN,
                    adjustedExitPrice,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    "",
                    Double.NaN,
                    "unknown",
                    exitReason,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN
                ));
                return;
            }

            double riskPct = openTrade.riskPct > 0.0 ? openTrade.riskPct : Math.max(fallbackRiskPct, 0.0025);
            double perSharePnl = openTrade.sideSign > 0
                ? adjustedExitPrice - openTrade.entryPrice
                : openTrade.entryPrice - adjustedExitPrice;
            double tradePnl = perSharePnl * closedQuantity;
            double realizedR = (perSharePnl / openTrade.entryPrice) / riskPct;
            closedTrades.add(new ClosedTrade(
                closedTrades.size() + 1,
                openTrade.side,
                openTrade.entryTime,
                formatMarketTime(marketTime),
                closedQuantity,
                openTrade.entryPrice,
                adjustedExitPrice,
                tradePnl,
                realizedR,
                openTrade.mfeR,
                openTrade.maeR,
                openTrade.setupArmTime,
                openTrade.setupToFillSeconds,
                openTrade.entryRoute,
                exitReason,
                openTrade.entryProbability,
                openTrade.entryThreshold,
                openTrade.entryThresholdMargin
            ));

            openTrade.quantity -= closedQuantity;
            if (openTrade.quantity <= 0) {
                openTrade = null;
            }
        }

        private ConfirmedEntry consumePendingConfirmedEntry(String side) {
            if (pendingConfirmedEntry == null || !pendingConfirmedEntry.side().equalsIgnoreCase(side)) {
                return null;
            }
            ConfirmedEntry consumed = pendingConfirmedEntry;
            pendingConfirmedEntry = null;
            return consumed;
        }

        private String consumePendingExitReason() {
            String reason = pendingExitReason == null || pendingExitReason.isBlank() ? "other" : pendingExitReason;
            pendingExitReason = "";
            return reason;
        }

        private void incrementExitReason(String reason) {
            String key = reason == null || reason.isBlank() ? "other" : reason;
            exitReasonCounts.put(key, exitReasonCounts.getOrDefault(key, 0L) + 1L);
        }

        private String exitReasonDistribution() {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, Long> entry : exitReasonCounts.entrySet()) {
                if (builder.length() > 0) {
                    builder.append('|');
                }
                builder.append(entry.getKey()).append('=').append(entry.getValue());
            }
            return builder.toString();
        }

        private static double adjustedFillPrice(String action, double fillPrice) {
            if ("BUY".equalsIgnoreCase(action)) {
                return fillPrice + SIMULATED_SLIPPAGE_PER_SHARE;
            }
            if ("SELL".equalsIgnoreCase(action)) {
                return fillPrice - SIMULATED_SLIPPAGE_PER_SHARE;
            }
            return fillPrice;
        }

        private static String normalizeSide(String side) {
            String normalized = side == null ? "" : side.trim().toLowerCase(Locale.US);
            return ("long".equals(normalized) || "short".equals(normalized)) ? normalized : "";
        }

        private static String formatMarketTime(LocalDateTime marketTime) {
            return marketTime == null ? "" : marketTime.atZone(MARKET_ZONE).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

        private static String formatEpoch(long epoch) {
            if (epoch <= 0L) {
                return "";
            }
            return Instant.ofEpochSecond(epoch).atZone(ZoneOffset.UTC).withZoneSameInstant(MARKET_ZONE).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

        private static final class OpenTrade {
            private String side = "";
            private int sideSign;
            private int quantity;
            private double entryPrice;
            private String entryTime = "";
            private double riskPct = 0.0025;
            private double mfeR;
            private double maeR;
            private String setupArmTime = "";
            private double setupToFillSeconds = Double.NaN;
            private String entryRoute = "direct_30s";
            private double entryProbability = Double.NaN;
            private double entryThreshold = Double.NaN;
            private double entryThresholdMargin = Double.NaN;
        }

        private record ConfirmedEntry(String side, long armEpoch, long confirmEpoch, double setupToFillSeconds,
                                      double probability, double threshold, double thresholdMargin) {}

        private record ClosedTrade(int index, String side, String entryTime, String exitTime, int quantity,
                                   double entryPrice, double exitPrice, double tradePnl, double realizedR,
                                   double mfeR, double maeR, String setupArmTime, double setupToFillSeconds,
                                   String entryRoute, String exitReason, double entryProbability,
                                   double entryThreshold, double entryThresholdMargin) {}

        private record Summary(long armsTotal, long armsLong, long armsShort, long armConfirmations,
                               long armExpirations, double armConversionRate, long guardEvaluations,
                               long guardFires, long lifecycleExits, long hardRiskExits, long eodExits,
                               double avgSetupToFillSeconds, double avgMfeR, double avgMaeR,
                               String exitReasonDistribution) {}
    }

    private static final class StreamSanityStats {
        private long statusEvents;
        private long equityBars;
        private long optionBars;
        private long equityBarsWithContract;
        private long equityBarsMissingContract;
        private long schemaMismatchEquityBars;
        private long lowQualityEquityBars;
        private long entryRejectedEquityBars;
        private double minQualityScore = Double.NaN;
        private double maxQualityScore = Double.NaN;
        private double qualityScoreSum;
        private final Map<String, Long> qualityFlagCounts = new LinkedHashMap<>();

        private void recordStatus(DatabentoEvent event) {
            statusEvents++;
        }

        private void recordOption(DatabentoEvent event) {
            optionBars++;
        }

        private void recordEquity(DatabentoEvent event, double minAllowedQualityScore, String expectedSchemaVersion) {
            equityBars++;
            if (event.hasSanityContract()) {
                equityBarsWithContract++;
            } else {
                equityBarsMissingContract++;
            }
            if (!event.isExpectedEventSchema(expectedSchemaVersion)) {
                schemaMismatchEquityBars++;
            }
            double quality = event.effectiveQualityScore();
            minQualityScore = Double.isNaN(minQualityScore) ? quality : Math.min(minQualityScore, quality);
            maxQualityScore = Double.isNaN(maxQualityScore) ? quality : Math.max(maxQualityScore, quality);
            qualityScoreSum += quality;
            for (String flag : splitFlags(event.dataQualityFlags)) {
                qualityFlagCounts.put(flag, qualityFlagCounts.getOrDefault(flag, 0L) + 1L);
            }
            if (event.hasEntryBlockingQualityFlag() || quality < minAllowedQualityScore) {
                lowQualityEquityBars++;
            }
            if (event.blocksNewEntries(minAllowedQualityScore, expectedSchemaVersion)) {
                entryRejectedEquityBars++;
            }
        }

        private Map<String, Object> report(String symbol,
                                           String expectedSchemaVersion,
                                           double minAllowedQualityScore,
                                           long processedEquityBars,
                                           long processedOptionBars,
                                           long skippedEvents) {
            List<String> errors = new ArrayList<>();
            List<String> warnings = new ArrayList<>();
            if (processedEquityBars <= 0L) {
                errors.add("no_equity_bars_processed");
            }
            if (equityBarsMissingContract > 0L) {
                errors.add("equity_bars_missing_sanity_contract=" + equityBarsMissingContract);
            }
            if (schemaMismatchEquityBars > 0L) {
                errors.add("equity_bars_schema_mismatch=" + schemaMismatchEquityBars);
            }
            if (lowQualityEquityBars > 0L) {
                warnings.add("low_quality_equity_bars=" + lowQualityEquityBars);
            }
            if (entryRejectedEquityBars > 0L) {
                warnings.add("entry_rejected_equity_bars=" + entryRejectedEquityBars);
            }

            Map<String, Object> report = new LinkedHashMap<>();
            report.put("symbol", symbol);
            report.put("expectedEventSchemaVersion", expectedSchemaVersion);
            report.put("minAllowedQualityScore", minAllowedQualityScore);
            report.put("processedEquityBars", processedEquityBars);
            report.put("processedOptionBars", processedOptionBars);
            report.put("skippedEvents", skippedEvents);
            report.put("statusEvents", statusEvents);
            report.put("equityBarsObservedBySanity", equityBars);
            report.put("optionBarsObservedBySanity", optionBars);
            report.put("equityBarsWithContract", equityBarsWithContract);
            report.put("equityBarsMissingContract", equityBarsMissingContract);
            report.put("schemaMismatchEquityBars", schemaMismatchEquityBars);
            report.put("lowQualityEquityBars", lowQualityEquityBars);
            report.put("entryRejectedEquityBars", entryRejectedEquityBars);
            report.put("minQualityScore", Double.isNaN(minQualityScore) ? null : minQualityScore);
            report.put("maxQualityScore", Double.isNaN(maxQualityScore) ? null : maxQualityScore);
            report.put("meanQualityScore", equityBars > 0L ? qualityScoreSum / equityBars : null);
            report.put("qualityFlagCounts", Map.copyOf(qualityFlagCounts));
            report.put("errors", errors);
            report.put("warnings", warnings);
            return report;
        }

        private static List<String> splitFlags(String rawFlags) {
            String value = rawFlags == null || rawFlags.isBlank() ? "none" : rawFlags.trim().toLowerCase(Locale.US).replace(',', '|');
            List<String> out = new ArrayList<>();
            for (String token : value.split("\\|")) {
                String normalized = token == null ? "" : token.trim();
                if (!normalized.isBlank()) {
                    out.add(normalized);
                }
            }
            return out.isEmpty() ? List.of("none") : out;
        }
    }

    private static double parseThreshold(String key, double fallback) {
        try {
            return Math.max(0.0, Math.min(1.0, Double.parseDouble(System.getProperty(key, Double.toString(fallback)).trim())));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static PingPongStrategy.AiThresholdConfig buildAiThresholdConfig() {
        double baseLongEntry = parseThreshold("trading.ai.long-entry-threshold", 0.68);
        double baseShortEntry = parseThreshold("trading.ai.short-entry-threshold", 0.63);
        double baseLongExit = parseThreshold("trading.ai.long-exit-threshold", 0.58);
        double baseShortExit = parseThreshold("trading.ai.short-exit-threshold", 0.60);
        double raisePercent = parseNonNegativeDouble(System.getProperty("trading.ai.entry-threshold-raise-percent", "0.0"), 0.0);
        return new PingPongStrategy.AiThresholdConfig(
            applyEntryThresholdLift(baseLongEntry, raisePercent),
            applyEntryThresholdLift(baseShortEntry, raisePercent),
            baseLongExit,
            baseShortExit,
            applyEntryThresholdLift(parseThreshold("trading.ai.open30.long-entry-threshold", baseLongEntry), raisePercent),
            applyEntryThresholdLift(parseThreshold("trading.ai.open30.short-entry-threshold", baseShortEntry), raisePercent),
            parseThreshold("trading.ai.open30.long-exit-threshold", baseLongExit),
            parseThreshold("trading.ai.open30.short-exit-threshold", baseShortExit),
            applyEntryThresholdLift(parseThreshold("trading.ai.regime.choppy.long-entry-threshold", baseLongEntry), raisePercent),
            applyEntryThresholdLift(parseThreshold("trading.ai.regime.choppy.short-entry-threshold", baseShortEntry), raisePercent),
            parseThreshold("trading.ai.regime.choppy.long-exit-threshold", baseLongExit),
            parseThreshold("trading.ai.regime.choppy.short-exit-threshold", baseShortExit),
            applyEntryThresholdLift(parseThreshold("trading.ai.regime.trend.long-entry-threshold", baseLongEntry), raisePercent),
            applyEntryThresholdLift(parseThreshold("trading.ai.regime.trend.short-entry-threshold", baseShortEntry), raisePercent),
            parseThreshold("trading.ai.regime.trend.long-exit-threshold", baseLongExit),
            parseThreshold("trading.ai.regime.trend.short-exit-threshold", baseShortExit),
            applyEntryThresholdLift(parseThreshold("trading.ai.regime.volatile.long-entry-threshold", baseLongEntry), raisePercent),
            applyEntryThresholdLift(parseThreshold("trading.ai.regime.volatile.short-entry-threshold", baseShortEntry), raisePercent),
            parseThreshold("trading.ai.regime.volatile.long-exit-threshold", baseLongExit),
            parseThreshold("trading.ai.regime.volatile.short-exit-threshold", baseShortExit),
            parseThreshold("trading.ai.regime-threshold", 0.50)
        );
    }

    private static double applyEntryThresholdLift(double threshold, double raisePercent) {
        if (!Double.isFinite(threshold) || raisePercent <= 0.0) {
            return threshold;
        }
        double lifted = threshold * (1.0 + (raisePercent / 100.0));
        return Math.max(0.0, Math.min(0.99, lifted));
    }

    private static double parseNonNegativeDouble(String raw, double fallback) {
        try {
            double parsed = Double.parseDouble(raw == null ? "" : raw.trim());
            return parsed >= 0.0 && Double.isFinite(parsed) ? parsed : fallback;
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static double parsePositiveDouble(String raw, double fallback) {
        try {
            double parsed = Double.parseDouble(raw == null ? "" : raw.trim());
            return parsed > 0.0 && Double.isFinite(parsed) ? parsed : fallback;
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static String normalizeSymbol(String raw) {
        String value = raw == null ? "" : raw.trim().toUpperCase(Locale.US);
        return value.isBlank() ? "TSLA" : value;
    }
}
