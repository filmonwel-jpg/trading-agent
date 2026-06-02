package com.calgary.fili.trader.testers;

import com.calgary.fili.trader.bot.strategy.PingPongStrategy;
import com.calgary.fili.trader.bot.trader.DatabentoEvent;
import com.calgary.fili.trader.bot.trader.DatabentoLiveGateway;
import com.calgary.fili.trader.bot.trader.IBKRTrader;
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

    private final AtomicInteger simulatedOrderId = new AtomicInteger(1);
    private final AtomicInteger streamExitCode = new AtomicInteger(Integer.MIN_VALUE);
    private final CountDownLatch streamDone = new CountDownLatch(1);
    private PingPongStrategy strategy;
    private DatabentoLiveGateway gateway;
    private String backtestSymbol = "TSLA";
    private String tradeLogFileName;
    private String orderHistoryFileName;
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
            Integer.parseInt(System.getProperty("backtest.strategy.tradeAmount", "100000")),
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
        strategy.setAiThresholds(
            parseThreshold("trading.ai.long-entry-threshold", 0.68),
            parseThreshold("trading.ai.short-entry-threshold", 0.63),
            parseThreshold("trading.ai.long-exit-threshold", 0.58),
            parseThreshold("trading.ai.short-exit-threshold", 0.60),
            parseThreshold("trading.ai.regime-threshold", 0.50)
        );
        strategy.setMaxVolatilityPercent(10.0);
        strategy.setPositionSynced(true);
    }

    private List<String> buildHistoricalStreamCommand(String symbol) {
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("backtest.databento.python", System.getProperty("trading.databento.python-bin", "python3")));
        command.add(System.getProperty("backtest.databento.streamer", "scripts/databento_historical_streamer.py"));
        command.add("--source");
        command.add("api");
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

    private void handleHistoricalDatabentoEvent(DatabentoEvent event) {
        try {
            if (event == null) {
                skippedEvents++;
                return;
            }
            if (event.isStatus()) {
                System.out.println(">>> [FLOW][INFO][DATABENTO] " + (event.message == null ? "status" : event.message));
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

    private void handleOptionBar(DatabentoEvent event) {
        String underlying = normalizeSymbol(event.underlying == null || event.underlying.isBlank() ? event.symbol : event.underlying);
        if (!backtestSymbol.equals(underlying) || event.volume <= 0L) {
            return;
        }
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
        ZonedDateTime barTs = Instant.ofEpochSecond(event.barEpochSec).atZone(ZoneOffset.UTC).withZoneSameInstant(MARKET_ZONE);
        applySessionState(barTs);
        latestBid = event.bid > 0.0 ? event.bid : latestBid;
        latestAsk = event.ask > 0.0 ? event.ask : latestAsk;
        latestBidSize = event.bidSize > 0L ? event.bidSize : latestBidSize;
        latestAskSize = event.askSize > 0L ? event.askSize : latestAskSize;
        lastClose = event.close;
        latestMarketTime = barTs.toLocalDateTime();
        processedEquityBars++;

        strategy.setCurrentMarketTime(latestMarketTime);
        strategy.onQuoteSnapshot(latestBid, latestAsk, latestBidSize, latestAskSize, 0.0);
        if (event.atBidVol > 0L || event.atAskVol > 0L) {
            strategy.onOrderFlowSnapshot(event.atBidVol, event.atAskVol);
        }
        strategy.onTickForExitsOnly(event.close);
        strategy.onSourceBar(event.barEpochSec, event.open, event.high, event.low, event.close, event.volume, event.wap > 0.0 ? event.wap : event.close);
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
        strategy.onOrderSubmitted(orderId, action, finalQty);
        strategy.onOrderProgress(orderId, action, finalQty, 0, currentPrice);
        strategy.onOrderClosed(orderId, "Filled");
    }

    @Override
    protected int getMaxShareCap() {
        return Integer.parseInt(System.getProperty("backtest.strategy.maxShareCap", "500"));
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
            String timestamp = LocalDateTime.now(MARKET_ZONE).format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            tradeLogFileName = System.getProperty("backtest.tradeLogFile", "runtime/backtests/databento-stream-" + backtestSymbol.toLowerCase(Locale.US) + "-" + timestamp + ".csv");
        }
        return tradeLogFileName;
    }

    public String getOrderHistoryFile() {
        if (orderHistoryFileName == null) {
            String timestamp = LocalDateTime.now(MARKET_ZONE).format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            orderHistoryFileName = System.getProperty("backtest.orderHistoryFile", "runtime/backtests/databento-stream-" + backtestSymbol.toLowerCase(Locale.US) + "-" + timestamp + "-orders.csv");
        }
        return orderHistoryFileName;
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
        System.out.println(">>> [FLOW][INFO][BACKTEST] ==============================================");
        System.out.println(">>> [FLOW][INFO][BACKTEST] DATABENTO HISTORICAL STREAM BACKTEST COMPLETE");
        System.out.println(">>> [FLOW][INFO][BACKTEST] Symbol: " + backtestSymbol);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Equity bars: " + processedEquityBars);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Option bars: " + processedOptionBars);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Skipped events: " + skippedEvents);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Final position: " + (strategy == null ? 0 : strategy.getCurrentPosition()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] Simulated broker position: " + simulatedBrokerPosition);
        System.out.println(">>> [FLOW][INFO][BACKTEST] Total trades: " + (strategy == null ? 0 : strategy.getTradeCount()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] Total PnL: " + (strategy == null ? 0.0 : strategy.getTotalNetPnL()));
        System.out.println(">>> [FLOW][INFO][BACKTEST] Trade log: " + getTradeLogFile());
        System.out.println(">>> [FLOW][INFO][BACKTEST] Order history: " + getOrderHistoryFile());
        System.out.println(">>> [FLOW][INFO][BACKTEST] ==============================================");
    }

    private static double parseThreshold(String key, double fallback) {
        try {
            return Math.max(0.0, Math.min(1.0, Double.parseDouble(System.getProperty(key, Double.toString(fallback)).trim())));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static String normalizeSymbol(String raw) {
        String value = raw == null ? "" : raw.trim().toUpperCase(Locale.US);
        return value.isBlank() ? "TSLA" : value;
    }
}
