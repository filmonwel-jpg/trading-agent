package com.calgary.fili.trader.testers;

import com.calgary.fili.trader.bot.trader.DatabentoEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Historical Databento replay helper for combined CSV outputs produced by the
 * Python multi-cadence builder.
 *
 * <p>This is intentionally a small foundation class: it gives backtests a single
 * timestamp-ordered event stream abstraction while keeping the current
 * {@link HistoricalBacktester} CSV path intact. The next integration step is to
 * have HistoricalBacktester consume {@link ReplayBar} events directly instead of
 * reading one CSV at a time.</p>
 */
public final class DatabentoHistoricalReplayProvider {
    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final DateTimeFormatter MARKET_TS = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV", Locale.US);
    private static final DateTimeFormatter LEGACY_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.US);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public record ReplayBar(
        String cadence,
        String symbol,
        LocalDateTime timestamp,
        double open,
        double high,
        double low,
        double close,
        long volume,
        double wap,
        double bid,
        double ask,
        long bidSize,
        long askSize,
        long putVolume,
        long callVolume
    ) {}

    private final List<ReplayBar> events;

    private DatabentoHistoricalReplayProvider(List<ReplayBar> events) {
        this.events = List.copyOf(events);
    }

    public static DatabentoHistoricalReplayProvider fromCsv(String cadence, String csvPath) throws IOException {
        List<ReplayBar> out = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvPath))) {
            String header = reader.readLine();
            if (header == null || header.isBlank()) {
                return new DatabentoHistoricalReplayProvider(out);
            }
            Map<String, Integer> index = headerIndex(header);
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                String[] values = line.split(",", -1);
                out.add(parseBar(cadence, values, index));
            }
        }
        out.sort(Comparator.comparing(ReplayBar::timestamp).thenComparing(ReplayBar::symbol).thenComparing(ReplayBar::cadence));
        return new DatabentoHistoricalReplayProvider(out);
    }

    public static DatabentoHistoricalReplayProvider fromDatabentoHistoricalStream(
        String cadence,
        List<String> command,
        Path workingDirectory,
        String symbolFilter
    ) throws IOException, InterruptedException {
        int cadenceSeconds = parseCadenceSeconds(cadence);
        ProcessBuilder builder = new ProcessBuilder(command);
        if (workingDirectory != null) {
            builder.directory(workingDirectory.toFile());
        }
        Process process = builder.start();
        StringBuilder stderr = new StringBuilder();
        Thread stderrThread = new Thread(() -> readStderr(process, stderr), "databento-historical-stream-stderr");
        stderrThread.setDaemon(true);
        stderrThread.start();

        Map<String, BarAccumulator> accumulators = new LinkedHashMap<>();
        String wantedSymbol = symbolFilter == null ? "" : symbolFilter.trim().toUpperCase(Locale.US);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String payload = line.trim();
                if (payload.isEmpty()) {
                    continue;
                }
                DatabentoEvent event = OBJECT_MAPPER.readValue(payload, DatabentoEvent.class);
                if (event.isStatus()) {
                    continue;
                }
                if (event.isOptionBar()) {
                    String eventUnderlying = event.underlying == null || event.underlying.isBlank() ? event.symbol : event.underlying;
                    String underlying = normalizeSymbol(eventUnderlying);
                    if (!wantedSymbol.isBlank() && !wantedSymbol.equals(underlying)) {
                        continue;
                    }
                    accumulator(accumulators, cadence, cadenceSeconds, underlying, event.barEpochSec)
                        .addOption(event.right, event.volume);
                } else if (event.isEquityBar()) {
                    String symbol = normalizeSymbol(event.symbol);
                    if (!wantedSymbol.isBlank() && !wantedSymbol.equals(symbol)) {
                        continue;
                    }
                    accumulator(accumulators, cadence, cadenceSeconds, symbol, event.barEpochSec).addEquity(event);
                }
            }
        }

        boolean exited = process.waitFor(30, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            throw new IOException("Databento historical stream did not exit cleanly within timeout");
        }
        stderrThread.join(1000L);
        int exitCode = process.exitValue();
        if (exitCode != 0) {
            throw new IOException("Databento historical stream exited with code=" + exitCode + " stderr=" + stderr);
        }

        List<ReplayBar> out = accumulators.values().stream()
            .filter(BarAccumulator::hasEquity)
            .map(BarAccumulator::toReplayBar)
            .sorted(Comparator.comparing(ReplayBar::timestamp).thenComparing(ReplayBar::symbol).thenComparing(ReplayBar::cadence))
            .toList();
        return new DatabentoHistoricalReplayProvider(out);
    }

    public List<ReplayBar> events() {
        return events;
    }

    private static void readStderr(Process process, StringBuilder stderr) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (stderr.length() < 16_384) {
                    stderr.append(line).append(System.lineSeparator());
                }
            }
        } catch (IOException ignored) {
        }
    }

    private static BarAccumulator accumulator(Map<String, BarAccumulator> accumulators, String cadence, int cadenceSeconds, String symbol, long epochSeconds) {
        long bucketEpoch = Math.floorDiv(epochSeconds, cadenceSeconds) * (long) cadenceSeconds;
        String key = symbol + "|" + bucketEpoch;
        return accumulators.computeIfAbsent(key, ignored -> new BarAccumulator(cadence, symbol, bucketEpoch));
    }

    private static String normalizeSymbol(String raw) {
        return raw == null ? "" : raw.trim().toUpperCase(Locale.US);
    }

    private static int parseCadenceSeconds(String cadence) {
        String value = cadence == null ? "5s" : cadence.trim().toLowerCase(Locale.US);
        if (value.endsWith("s")) {
            return Math.max(1, Integer.parseInt(value.substring(0, value.length() - 1)));
        }
        if (value.endsWith("m")) {
            return Math.max(1, Integer.parseInt(value.substring(0, value.length() - 1)) * 60);
        }
        return Math.max(1, Integer.parseInt(value));
    }

    private static final class BarAccumulator {
        private final String cadence;
        private final String symbol;
        private final long bucketEpoch;
        private boolean hasEquity;
        private double open;
        private double high;
        private double low;
        private double close;
        private long volume;
        private double wapNumerator;
        private double wapFallback;
        private double bid;
        private double ask;
        private long bidSize;
        private long askSize;
        private long putVolume;
        private long callVolume;

        private BarAccumulator(String cadence, String symbol, long bucketEpoch) {
            this.cadence = cadence;
            this.symbol = symbol;
            this.bucketEpoch = bucketEpoch;
        }

        private void addEquity(DatabentoEvent event) {
            double eventClose = event.close > 0.0 ? event.close : Math.max(event.bid, event.ask);
            if (eventClose <= 0.0) {
                return;
            }
            double eventOpen = event.open > 0.0 ? event.open : eventClose;
            double eventHigh = event.high > 0.0 ? event.high : eventClose;
            double eventLow = event.low > 0.0 ? event.low : eventClose;
            if (!hasEquity) {
                open = eventOpen;
                high = eventHigh;
                low = eventLow;
                hasEquity = true;
            } else {
                high = Math.max(high, eventHigh);
                low = Math.min(low, eventLow);
            }
            close = eventClose;
            volume += Math.max(0L, event.volume);
            double eventWap = event.wap > 0.0 ? event.wap : eventClose;
            wapNumerator += eventWap * Math.max(0L, event.volume);
            wapFallback = eventWap;
            bid = event.bid;
            ask = event.ask;
            bidSize = event.bidSize;
            askSize = event.askSize;
        }

        private void addOption(String right, long optionVolume) {
            String normalizedRight = right == null ? "" : right.trim().toUpperCase(Locale.US);
            long safeVolume = Math.max(0L, optionVolume);
            if ("P".equals(normalizedRight)) {
                putVolume += safeVolume;
            } else if ("C".equals(normalizedRight)) {
                callVolume += safeVolume;
            }
        }

        private boolean hasEquity() {
            return hasEquity;
        }

        private ReplayBar toReplayBar() {
            double wap = volume > 0L ? wapNumerator / volume : (wapFallback > 0.0 ? wapFallback : close);
            LocalDateTime timestamp = LocalDateTime.ofInstant(Instant.ofEpochSecond(bucketEpoch), MARKET_ZONE);
            return new ReplayBar(cadence, symbol, timestamp, open, high, low, close, volume, wap, bid, ask, bidSize, askSize, putVolume, callVolume);
        }
    }

    private static Map<String, Integer> headerIndex(String header) {
        String[] parts = header.split(",", -1);
        Map<String, Integer> index = new HashMap<>();
        for (int i = 0; i < parts.length; i++) {
            String key = parts[i] == null ? "" : parts[i].trim().toLowerCase(Locale.US);
            if (!key.isBlank()) {
                index.put(key, i);
            }
        }
        return index;
    }

    private static ReplayBar parseBar(String cadence, String[] values, Map<String, Integer> index) {
        return new ReplayBar(
            cadence,
            get(values, index, "symbol", "SINGLE").toUpperCase(Locale.US),
            parseTimestamp(get(values, index, "timestamp", "")),
            parseDouble(values, index, "open", 0.0),
            parseDouble(values, index, "high", 0.0),
            parseDouble(values, index, "low", 0.0),
            parseDouble(values, index, "close", 0.0),
            parseLong(values, index, "volume", 0L),
            parseDouble(values, index, "wap", parseDouble(values, index, "close", 0.0)),
            parseDouble(values, index, "bid", 0.0),
            parseDouble(values, index, "ask", 0.0),
            parseLong(values, index, "bidsize", 0L),
            parseLong(values, index, "asksize", 0L),
            parseLong(values, index, "putvol", 0L),
            parseLong(values, index, "callvol", 0L)
        );
    }

    private static LocalDateTime parseTimestamp(String raw) {
        String value = raw == null ? "" : raw.trim();
        if (value.isBlank()) {
            throw new IllegalArgumentException("Blank replay timestamp");
        }
        try {
            return ZonedDateTime.parse(value, MARKET_TS).withZoneSameInstant(MARKET_ZONE).toLocalDateTime();
        } catch (DateTimeParseException ignored) {
            return LocalDateTime.parse(value.replace(" America/New_York", ""), LEGACY_TS);
        }
    }

    private static String get(String[] values, Map<String, Integer> index, String key, String fallback) {
        Integer i = index.get(key);
        if (i == null || i < 0 || i >= values.length) {
            return fallback;
        }
        String value = values[i] == null ? "" : values[i].trim();
        return value.isBlank() ? fallback : value;
    }

    private static double parseDouble(String[] values, Map<String, Integer> index, String key, double fallback) {
        try {
            return Double.parseDouble(get(values, index, key, Double.toString(fallback)));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static long parseLong(String[] values, Map<String, Integer> index, String key, long fallback) {
        try {
            return Math.round(Double.parseDouble(get(values, index, key, Long.toString(fallback))));
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }
}

