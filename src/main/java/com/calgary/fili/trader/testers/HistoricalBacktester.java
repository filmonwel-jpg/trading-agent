package com.calgary.fili.trader.testers;

import com.calgary.fili.trader.bot.trader.IBKRTrader;
import com.calgary.fili.trader.bot.strategy.PingPongStrategy;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HistoricalBacktester extends IBKRTrader {

    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final DateTimeFormatter BACKTEST_TS_LEGACY = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter BACKTEST_TS_MARKET = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV");

    private PingPongStrategy testStrategy;
    private final AtomicInteger mockOrderId = new AtomicInteger(1);
    
    // Caches the output filename so it stays the same throughout the entire run
    private String tradeLogFileName = null; 

    public HistoricalBacktester() {
        super(new SimpleMeterRegistry());
    } 

    public static void main(String[] args) {
        HistoricalBacktester backtester = new HistoricalBacktester();

        String overrideSymbol = "TSLA";
        String csvFile = null;

        if (args != null && args.length > 0 && args[0] != null && !args[0].isBlank()) {
            String[] firstArgParts = splitSymbolAndCsv(args[0]);
            overrideSymbol = firstArgParts[0];
            if (firstArgParts[1] != null && !firstArgParts[1].isBlank()) {
                csvFile = firstArgParts[1];
            }
        }

        if (args != null && args.length > 1 && args[1] != null && !args[1].isBlank()) {
            String[] secondArgParts = splitSymbolAndCsv(args[1]);
            if (secondArgParts[1] != null && !secondArgParts[1].isBlank()) {
                csvFile = secondArgParts[1];
            } else {
                csvFile = secondArgParts[0];
            }
        }

        if (csvFile == null || csvFile.isBlank()) {
            csvFile = overrideSymbol + "_5_Sec_Data.csv";
        }

        backtester.initializeStrategyFromProperties(overrideSymbol);
        backtester.runBacktest(csvFile);
    }

    private static String[] splitSymbolAndCsv(String rawArg) {
        String raw = rawArg == null ? "" : rawArg.trim();
        if (!raw.contains(",")) {
            return new String[]{raw.toUpperCase(), null};
        }

        String[] parts = raw.split(",", 2);
        String symbol = parts[0].trim();
        String csv = parts.length > 1 ? parts[1].trim() : null;

        if (symbol.isBlank()) {
            symbol = "TSLA";
        }

        return new String[]{symbol.toUpperCase(), (csv == null || csv.isBlank()) ? null : csv};
    }

    public void runBacktest(String csvFilePath) {
        flowInfo("BACKTEST", "==============================================");
        flowInfo("BACKTEST", "INITIATING 5-SECOND HISTORICAL BACKTEST");
        flowInfo("BACKTEST", "Reading data from: " + csvFilePath);
        flowInfo("BACKTEST", "Writing trades to: " + getTradeLogFile());
        flowInfo("BACKTEST", "==============================================");

        if (testStrategy == null) {
            initializeStrategyFromProperties(null);
        }

        boolean parseErrorPrinted = false;
        int processedRows = 0;
        int skippedRows = 0;
        int aiInputRows = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            boolean isHeader = true;
            Map<String, Integer> columnIndex = new HashMap<>();
            LocalDate previousDate = null;
            Double previousDayFinalClose = null;
            double lastSeenClose = 0.0;

            // Market timezone is ET; align EOD rules to 16:00 ET close.
            LocalTime hardCutoffTime = LocalTime.of(15, 55, 0);
            LocalTime eodFlattenStart = LocalTime.of(15, 59, 50);
            LocalTime eodFlattenEnd = LocalTime.of(16, 0, 0);

            while ((line = br.readLine()) != null) {
                if (isHeader) {
                    String[] headers = line.split(",");
                    for (int i = 0; i < headers.length; i++) {
                        String key = headers[i] == null ? "" : headers[i].trim().toLowerCase();
                        if (!key.isEmpty()) {
                            columnIndex.put(key, i);
                        }
                    }
                    isHeader = false;
                    continue;
                }

                boolean queuedStrategyWork = false;

                try {

                String[] values = line.split(",");
                if (values.length < 8) {
                    skippedRows++;
                    flowCondition("BACKTEST.ROW", "COLUMN_COUNT>=8", false, "row=" + (processedRows + skippedRows) + " cols=" + values.length);
                    continue;
                }

                int tsIndex = getColumnIndex(columnIndex, "timestamp", 0);
                int openIndex = getColumnIndex(columnIndex, "open", 1);
                int highIndex = getColumnIndex(columnIndex, "high", 2);
                int lowIndex = getColumnIndex(columnIndex, "low", 3);
                int closeIndex = getColumnIndex(columnIndex, "close", 4);
                int volumeIndex = getColumnIndex(columnIndex, "volume", 5);
                int wapIndex = getColumnIndex(columnIndex, "wap", 6);
                int yCloseIndex = getColumnIndex(columnIndex, "yesterdayclose", 8);
                int bidIndex = getColumnIndex(columnIndex, "bid", 9);
                int askIndex = getColumnIndex(columnIndex, "ask", 10);
                int bidSizeIndex = getColumnIndex(columnIndex, "bidsize", 11);
                int askSizeIndex = getColumnIndex(columnIndex, "asksize", 12);
                int putVolIndex = getColumnIndex(columnIndex, "putvol", 13);
                int callVolIndex = getColumnIndex(columnIndex, "callvol", 14);
                int shortableIndex = getColumnIndex(columnIndex, "shortableshares", 15);

                LocalDateTime rowDateTime;
                try {
                    rowDateTime = parseInputTimestamp(getCell(values, tsIndex));
                } catch (Exception exception) {
                    if (!parseErrorPrinted) {
                        flowError("BACKTEST.PARSE", "Date parse error on row timestamp='" + getCell(values, tsIndex) + "' | " + exception.getMessage());
                        parseErrorPrinted = true;
                    }
                    skippedRows++;
                    flowCondition("BACKTEST.ROW", "TIMESTAMP_PARSE", false, "raw='" + getCell(values, tsIndex) + "'");
                    continue;
                }

                processedRows++;
                flowCondition("BACKTEST.ROW", "TIMESTAMP_PARSE", true, "timestampEt=" + rowDateTime);

                LocalDate currentDate = rowDateTime.toLocalDate();
                LocalTime currentTime = rowDateTime.toLocalTime();
                
                testStrategy.setCurrentMarketTime(rowDateTime);
                queuedStrategyWork = true;

                // Skip the opening 30 seconds if desired (NYSE open: 09:30 ET)
                if (currentTime.getHour() == 9 && currentTime.getMinute() == 30 && currentTime.getSecond() < 30) {
                    skippedRows++;
                    flowCondition("BACKTEST.FILTER", "OPENING_30S_SKIP", false, "timestamp=" + rowDateTime);
                    continue;
                }

                double open = parseDoubleOrDefault(values, openIndex, 0.0);
                double high = parseDoubleOrDefault(values, highIndex, 0.0);
                double low = parseDoubleOrDefault(values, lowIndex, 0.0);
                double close = parseDoubleOrDefault(values, closeIndex, 0.0);
                long volume = parseLongOrDefault(values, volumeIndex, 0L);
                double wap = parseDoubleOrDefault(values, wapIndex, close);

                double bid = parseDoubleOrDefault(values, bidIndex, 0.0);
                double ask = parseDoubleOrDefault(values, askIndex, 0.0);
                long bidSize = parseLongOrDefault(values, bidSizeIndex, 0L);
                long askSize = parseLongOrDefault(values, askSizeIndex, 0L);
                long putVol = parseLongOrDefault(values, putVolIndex, 0L);
                long callVol = parseLongOrDefault(values, callVolIndex, 0L);
                double shortableShares = parseDoubleOrDefault(values, shortableIndex, 0.0);
                
                // If the CSV has Yesterday's Close as the 9th column (index 8), parse it. Otherwise use the rolling close.
                if (hasUsableCell(values, yCloseIndex)) {
                    double yClose = parseDoubleOrDefault(values, yCloseIndex, 0.0);
                    flowCondition("BACKTEST.DATA", "YESTERDAY_CLOSE_VALID", yClose > 0.0, "value=" + yClose);
                    testStrategy.setYesterdayClose(yClose);
                    queuedStrategyWork = true;
                }

                if (putVol > 0L || callVol > 0L) {
                    testStrategy.onOptionVolumeUpdate(putVol, callVol);
                    queuedStrategyWork = true;
                }

                if (bid > 0.0 || ask > 0.0 || bidSize > 0L || askSize > 0L || shortableShares > 0.0) {
                    testStrategy.onQuoteSnapshot(bid, ask, bidSize, askSize, shortableShares);
                    queuedStrategyWork = true;
                }

                double yesterday = testStrategy.getYesterdayClose();

                if (yesterday > 0 && Math.abs(close - yesterday) / yesterday > 0.10) {
                    skippedRows++;
                    flowCondition("BACKTEST.SAFETY", "OUTLIER_FILTER", false, "timestamp=" + rowDateTime + " close=" + close + " yesterday=" + yesterday);
                    continue;
                }
                flowCondition("BACKTEST.SAFETY", "OUTLIER_FILTER", true, "timestamp=" + rowDateTime + " close=" + close);

                if (previousDate == null) {
                    previousDate = currentDate;
                } else if (!currentDate.equals(previousDate)) {
                    previousDayFinalClose = lastSeenClose > 0.0 ? lastSeenClose : previousDayFinalClose;
                    testStrategy.resetForNewDay();
                    testStrategy.setEnabled(true);
                    queuedStrategyWork = true;
                    if (previousDayFinalClose != null && previousDayFinalClose > 0.0 && values.length <= 8) {
                        testStrategy.setYesterdayClose(previousDayFinalClose);
                        queuedStrategyWork = true;
                    }
                    previousDate = currentDate;
                }

                lastSeenClose = close;

                // Restrict late-day entries
                if (!currentTime.isBefore(hardCutoffTime) && currentTime.isBefore(eodFlattenStart)) {
                    flowCondition("BACKTEST.RULE", "NEW_ENTRIES_ALLOWED", false, "timestamp=" + rowDateTime + " reason=hardCutoff");
                    testStrategy.setAllowNewEntries(false);
                    queuedStrategyWork = true;
                } else {
                    flowCondition("BACKTEST.RULE", "NEW_ENTRIES_ALLOWED", true, "timestamp=" + rowDateTime);
                }

                // Force EOD Flattening
                if (!currentTime.isBefore(eodFlattenStart) && currentTime.isBefore(eodFlattenEnd)) {
                    flowCondition("BACKTEST.RULE", "EOD_FLATTEN_WINDOW", true, "timestamp=" + rowDateTime + " close=" + close);
                    testStrategy.forceEndOfDayFlatten(close);
                    testStrategy.setEnabled(false);
                    queuedStrategyWork = true;
                    continue;
                }
                flowCondition("BACKTEST.RULE", "EOD_FLATTEN_WINDOW", false, "timestamp=" + rowDateTime);

                // Push the 5-second bar to the strategy
                long epochSeconds = rowDateTime.atZone(MARKET_ZONE).toEpochSecond();
                aiInputRows++;
                flowData("BACKTEST->AI", "barTs=" + rowDateTime + " epoch=" + epochSeconds + " ohlc=" + open + "/" + high + "/" + low + "/" + close + " vol=" + volume + " wap=" + wap);
                testStrategy.on5SecondBar(epochSeconds, open, high, low, close, volume, wap);
                queuedStrategyWork = true;
                } finally {
                    if (queuedStrategyWork) {
                        drainQueue();
                    }
                }
            }

            flowInfo("BACKTEST", "==============================================");
            flowInfo("BACKTEST", "BACKTEST COMPLETE");
            flowInfo("BACKTEST", "Final Position: " + testStrategy.getCurrentPosition() + " shares");
            flowInfo("BACKTEST", "Total Trades Executed: " + testStrategy.getTradeCount());
            flowInfo("BACKTEST", "Rows processed=" + processedRows + " skipped=" + skippedRows + " aiInputRows=" + aiInputRows);
            flowInfo("BACKTEST", "Log saved to: " + getTradeLogFile());
            flowInfo("BACKTEST", "==============================================");

            // NEW FIX: Shut down the Strategy Thread and exit the JVM
            testStrategy.stop();
            System.exit(0);

        } catch (Exception e) {
            flowError("BACKTEST", "Error reading CSV: " + e.getMessage());
        }
    }

    @Override
    public String getTradeLogFile() {
        if (this.tradeLogFileName == null) {
            LocalDate tradeDate = LocalDate.now(MARKET_ZONE);
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
            String timestamp = LocalDateTime.now().format(dtf);
            String sym = (testStrategy != null ? testStrategy.getSymbol().toLowerCase() : "unknown");
            this.tradeLogFileName = "backtest_trades_log_" + sym + "_trade_date_" + tradeDate + "_" + timestamp + ".csv";
        }
        return this.tradeLogFileName;
    }

    private void initializeStrategyFromProperties(String overrideSymbol) {
        String symbol = (overrideSymbol != null && !overrideSymbol.isBlank()) 
            ? overrideSymbol.trim().toUpperCase() 
            : "TSLA";

        System.out.println(">>> [WARNING] HYPER-AGGRESSIVE GOD MODE ACTIVATED.");

        testStrategy = new PingPongStrategy(
            this,
            symbol,
            0.0025,     // gapPercentage
            100000,     // tradeAmount: FORCE $100,000 per trade
            2000,       // maxTrades: keep high; trade frequency controlled by RSI pre-gates and AI
            true,       // autoRegimeEnabled: ON
            300,        // regimeWindowTicks
            14,         // rsiPeriod
            0.001,      // reversalPercentage: 0.1% profit target
            0.0030,     // stopLossPercentage: 0.30% hard stop (aligned to ENTRY_RISK_PCT)
            5000.0,     // maxDailyDrawdown
            1.20,       // minDirectionalMove
            0.70        // trendStrengthThreshold
        );
        testStrategy.setAiThresholds(
            parseThresholdProperty("trading.ai.long-entry-threshold", 0.68),
            parseThresholdProperty("trading.ai.short-entry-threshold", 0.63),
            parseThresholdProperty("trading.ai.long-exit-threshold", 0.61),
            parseThresholdProperty("trading.ai.short-exit-threshold", 0.63),
            parseThresholdProperty("trading.ai.regime-threshold", 0.50)
        );
        testStrategy.setMaxVolatilityPercent(10.0);
        testStrategy.setPositionSynced(true);
    }

    private static double parseThresholdProperty(String key, double fallback) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            double parsed = Double.parseDouble(raw.trim());
            return Math.max(0.0, Math.min(1.0, parsed));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    @Override
    public void placeTrade(String symbol, String action, double currentPrice, int quantity, String orderType) {
        int safetyCap = getMaxShareCap();
        int finalQty = Math.min(quantity, safetyCap);
        if (quantity > safetyCap) {
            flowCondition("BACKTEST.ORDER", "QTY_WITHIN_CAP", false, "requested=" + quantity + " cap=" + safetyCap + " final=" + finalQty);
        } else {
            flowCondition("BACKTEST.ORDER", "QTY_WITHIN_CAP", true, "requested=" + quantity + " final=" + finalQty);
        }

        int orderId = mockOrderId.getAndIncrement();
        flowData("BACKTEST.ORDER", "simulated orderId=" + orderId + " action=" + action + " qty=" + finalQty + " symbol=" + symbol + " price=" + currentPrice + " type=" + orderType);

        testStrategy.onOrderSubmitted(orderId, action, finalQty);
        testStrategy.onOrderProgress(orderId, action, finalQty, 0, currentPrice);
        testStrategy.onOrderClosed(orderId, "Filled");
    }

    private LocalDateTime parseInputTimestamp(String rawTimestamp) {
        String raw = rawTimestamp == null ? "" : rawTimestamp.trim();
        try {
            return LocalDateTime.parse(raw, BACKTEST_TS_LEGACY);
        } catch (DateTimeParseException ignored) {
        }

        ZonedDateTime marketTs = ZonedDateTime.parse(raw, BACKTEST_TS_MARKET);
        return marketTs.withZoneSameInstant(MARKET_ZONE).toLocalDateTime();
    }

    private static int getColumnIndex(Map<String, Integer> index, String key, int fallback) {
        Integer found = index.get(key);
        return found != null ? found : fallback;
    }

    private static String getCell(String[] values, int index) {
        if (index < 0 || index >= values.length) {
            return "";
        }
        return values[index] == null ? "" : values[index].trim();
    }

    private static boolean hasUsableCell(String[] values, int index) {
        return index >= 0 && index < values.length && values[index] != null && !values[index].trim().isEmpty();
    }

    private static double parseDoubleOrDefault(String[] values, int index, double fallback) {
        if (!hasUsableCell(values, index)) {
            return fallback;
        }
        try {
            return Double.parseDouble(values[index].trim());
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static long parseLongOrDefault(String[] values, int index, long fallback) {
        if (!hasUsableCell(values, index)) {
            return fallback;
        }
        try {
            return (long) Double.parseDouble(values[index].trim());
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    // Forces the CSV loop to perfectly match the AI's processing speed
    private void drainQueue() {
        try {
            // 1. Wait for the Strategy to finish processing all pending ticks/bars
            while (testStrategy.getQueueSize() > 0) {
                Thread.sleep(1);
            }

            // 2. If the Strategy just fired an order, wait for the Simulated Broker events to process
            while (testStrategy.hasInFlightOrder()) {
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void flowInfo(String stage, String message) {
        System.out.println(">>> [FLOW][INFO][" + stage + "] " + message);
    }

    private void flowData(String stage, String message) {
        System.out.println(">>> [FLOW][DATA][" + stage + "] " + message);
    }

    private void flowError(String stage, String message) {
        System.err.println(">>> [FLOW][ERROR][" + stage + "] " + message);
    }

    private void flowCondition(String stage, String conditionName, boolean passed, String details) {
        String verdict = passed ? "PASS" : "FAIL";
        System.out.println(">>> [FLOW][COND][" + stage + "] " + conditionName + "=" + verdict + " | " + details);
    }
    //./mvnw spring-boot:run "-Dspring-boot.run.main-class=com.calgary.fili.trader.testers.HistoricalBacktester" "-Dspring-boot.run.arguments=TSLA,TSLA_Historical_Full.csv"
}