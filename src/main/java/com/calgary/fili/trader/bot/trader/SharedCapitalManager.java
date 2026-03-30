package com.calgary.fili.trader.bot.trader;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class SharedCapitalManager {
    private static final String TOTAL_NOTIONAL_KEY = "total.notional";
    private static final String UPDATED_AT_KEY = "updated.at";
    private static final String RESERVATION_PREFIX = "reservation.";
    private static final double EPSILON = 0.0001;

    private final boolean enabled;
    private final Path stateFile;
    private final double configuredTotalNotional;

    public record Snapshot(boolean enabled, Path stateFile, double totalNotional, double reservedNotional,
                           double availableNotional, Map<String, Double> reservations) {}

    public record ReservationDecision(boolean allowed, String message, Snapshot snapshot) {}

    public SharedCapitalManager(boolean enabled, Path stateFile, double configuredTotalNotional) {
        this.enabled = enabled;
        this.stateFile = stateFile == null ? Path.of("runtime", "shared-capital.properties") : stateFile;
        this.configuredTotalNotional = Math.max(0.0, configuredTotalNotional);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Path getStateFile() {
        return stateFile;
    }

    public ReservationDecision tryReserve(String symbol, double requestedNotional) {
        if (!enabled) {
            Snapshot snapshot = snapshot();
            return new ReservationDecision(true, "shared-capital-disabled", snapshot);
        }

        String normalizedSymbol = normalizeSymbol(symbol);
        double normalizedRequest = Math.max(0.0, requestedNotional);
        if (normalizedSymbol.isBlank() || normalizedRequest <= EPSILON) {
            Snapshot snapshot = snapshot();
            return new ReservationDecision(false, "invalid-symbol-or-request", snapshot);
        }

        try {
            return withLockedState(state -> {
                double currentReservation = state.reservations().getOrDefault(normalizedSymbol, 0.0);
                double availableIncludingCurrent = state.totalNotional() - state.reservedExcluding(normalizedSymbol);
                if (normalizedRequest <= availableIncludingCurrent + EPSILON) {
                    state.reservations().put(normalizedSymbol, normalizedRequest);
                    state.persist();
                    return new ReservationDecision(
                        true,
                        currentReservation > EPSILON ? "reservation-updated" : "reservation-created",
                        state.snapshot()
                    );
                }
                return new ReservationDecision(
                    false,
                    "insufficient-shared-capital requested=" + format(normalizedRequest)
                        + " available=" + format(Math.max(0.0, availableIncludingCurrent)),
                    state.snapshot()
                );
            });
        } catch (IOException e) {
            return new ReservationDecision(false, "shared-capital-io-error: " + e.getMessage(), snapshot());
        }
    }

    public ReservationDecision release(String symbol) {
        if (!enabled) {
            Snapshot snapshot = snapshot();
            return new ReservationDecision(true, "shared-capital-disabled", snapshot);
        }

        String normalizedSymbol = normalizeSymbol(symbol);
        if (normalizedSymbol.isBlank()) {
            Snapshot snapshot = snapshot();
            return new ReservationDecision(false, "invalid-symbol", snapshot);
        }

        try {
            return withLockedState(state -> {
                Double removed = state.reservations().remove(normalizedSymbol);
                state.persist();
                return new ReservationDecision(true, removed == null ? "already-released" : "reservation-released", state.snapshot());
            });
        } catch (IOException e) {
            return new ReservationDecision(false, "shared-capital-io-error: " + e.getMessage(), snapshot());
        }
    }

    public ReservationDecision resetAll() {
        if (!enabled) {
            Snapshot snapshot = snapshot();
            return new ReservationDecision(true, "shared-capital-disabled", snapshot);
        }

        try {
            return withLockedState(state -> {
                state.reservations().clear();
                state.persist();
                return new ReservationDecision(true, "all-reservations-cleared", state.snapshot());
            });
        } catch (IOException e) {
            return new ReservationDecision(false, "shared-capital-io-error: " + e.getMessage(), snapshot());
        }
    }

    public ReservationDecision reconcilePosition(String symbol, int position, double referencePrice, double fallbackNotional) {
        if (!enabled) {
            Snapshot snapshot = snapshot();
            return new ReservationDecision(true, "shared-capital-disabled", snapshot);
        }

        String normalizedSymbol = normalizeSymbol(symbol);
        if (normalizedSymbol.isBlank()) {
            Snapshot snapshot = snapshot();
            return new ReservationDecision(false, "invalid-symbol", snapshot);
        }

        int absolutePosition = Math.abs(position);
        if (absolutePosition == 0) {
            return release(normalizedSymbol);
        }

        double price = referencePrice > EPSILON ? referencePrice : 0.0;
        double derivedNotional = price > EPSILON ? absolutePosition * price : 0.0;
        double computedReservation = Math.max(derivedNotional, Math.max(0.0, fallbackNotional));
        if (computedReservation <= EPSILON) {
            computedReservation = Math.max(1.0, Math.max(0.0, fallbackNotional));
        }
        final double normalizedReservation = computedReservation;

        try {
            return withLockedState(state -> {
                state.reservations().put(normalizedSymbol, normalizedReservation);
                state.persist();
                return new ReservationDecision(true, "position-reconciled", state.snapshot());
            });
        } catch (IOException e) {
            return new ReservationDecision(false, "shared-capital-io-error: " + e.getMessage(), snapshot());
        }
    }

    public Snapshot snapshot() {
        if (!enabled) {
            return new Snapshot(false, stateFile, configuredTotalNotional, 0.0, configuredTotalNotional, Map.of());
        }

        try {
            return withLockedState(State::snapshot);
        } catch (IOException e) {
            return new Snapshot(true, stateFile, configuredTotalNotional, 0.0, configuredTotalNotional, Map.of());
        }
    }

    private <T> T withLockedState(StateFunction<T> action) throws IOException {
        Path parent = stateFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        try (FileChannel channel = FileChannel.open(stateFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
             FileLock ignored = channel.lock()) {
            State state = State.load(channel, stateFile, configuredTotalNotional);
            return action.apply(state);
        }
    }

    private static String normalizeSymbol(String symbol) {
        return symbol == null ? "" : symbol.trim().toUpperCase();
    }

    private static String format(double value) {
        return String.format("%.2f", value);
    }

    @FunctionalInterface
    private interface StateFunction<T> {
        T apply(State state) throws IOException;
    }

    private static final class State {
        private final FileChannel channel;
        private final Path stateFile;
        private final Properties props;
        private final double totalNotional;
        private final Map<String, Double> reservations;

        private State(FileChannel channel, Path stateFile, Properties props, double totalNotional, Map<String, Double> reservations) {
            this.channel = channel;
            this.stateFile = stateFile;
            this.props = props;
            this.totalNotional = totalNotional;
            this.reservations = reservations;
        }

        static State load(FileChannel channel, Path stateFile, double configuredTotalNotional) throws IOException {
            Properties props = new Properties();
            channel.position(0L);
            if (channel.size() > 0) {
                props.load(Channels.newInputStream(channel));
            }

            double fileTotal = parseDouble(props.getProperty(TOTAL_NOTIONAL_KEY), configuredTotalNotional);
            double totalNotional = configuredTotalNotional > EPSILON ? configuredTotalNotional : Math.max(0.0, fileTotal);
            Map<String, Double> reservations = new LinkedHashMap<>();
            for (String key : props.stringPropertyNames()) {
                if (!key.startsWith(RESERVATION_PREFIX)) {
                    continue;
                }
                String symbol = normalizeSymbol(key.substring(RESERVATION_PREFIX.length()));
                double value = Math.max(0.0, parseDouble(props.getProperty(key), 0.0));
                if (!symbol.isBlank() && value > EPSILON) {
                    reservations.put(symbol, value);
                }
            }
            return new State(channel, stateFile, props, totalNotional, reservations);
        }

        double totalNotional() {
            return totalNotional;
        }

        Map<String, Double> reservations() {
            return reservations;
        }

        double reservedExcluding(String excludedSymbol) {
            double total = 0.0;
            for (Map.Entry<String, Double> entry : reservations.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(excludedSymbol)) {
                    continue;
                }
                total += Math.max(0.0, entry.getValue());
            }
            return total;
        }

        Snapshot snapshot() {
            double reserved = 0.0;
            Map<String, Double> copy = new LinkedHashMap<>();
            for (Map.Entry<String, Double> entry : reservations.entrySet()) {
                double amount = Math.max(0.0, entry.getValue());
                if (amount <= EPSILON) {
                    continue;
                }
                copy.put(entry.getKey(), amount);
                reserved += amount;
            }
            double available = Math.max(0.0, totalNotional - reserved);
            return new Snapshot(true, stateFile, totalNotional, reserved, available, Map.copyOf(copy));
        }

        void persist() throws IOException {
            Properties next = new Properties();
            next.putAll(props);
            next.setProperty(TOTAL_NOTIONAL_KEY, format(totalNotional));
            next.setProperty(UPDATED_AT_KEY, String.valueOf(System.currentTimeMillis()));

            for (String key : props.stringPropertyNames()) {
                if (key.startsWith(RESERVATION_PREFIX)) {
                    next.remove(key);
                }
            }
            for (Map.Entry<String, Double> entry : reservations.entrySet()) {
                double amount = Math.max(0.0, entry.getValue());
                if (amount <= EPSILON) {
                    continue;
                }
                next.setProperty(RESERVATION_PREFIX + normalizeSymbol(entry.getKey()), format(amount));
            }

            channel.truncate(0L);
            channel.position(0L);
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                next.store(os, "Shared Capital Reservations");
                channel.write(ByteBuffer.wrap(os.toByteArray()));
            }
            channel.force(true);
            props.clear();
            props.putAll(next);
        }
    }

    private static double parseDouble(String value, double fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }
}





