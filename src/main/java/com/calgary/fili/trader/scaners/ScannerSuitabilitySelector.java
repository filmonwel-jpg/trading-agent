package com.calgary.fili.trader.scaners;

import java.util.Comparator;
import java.util.List;

public final class ScannerSuitabilitySelector {

    private ScannerSuitabilitySelector() {
    }

    public record ScannerCandidate(String symbol, int rank, Double absDistance) {}

    public static Double parseAbsDistance(String rawDistance) {
        if (rawDistance == null) {
            return null;
        }
        String normalized = rawDistance.replace("%", "").replace("+", "").trim();
        if (normalized.isBlank()) {
            return null;
        }
        try {
            return Math.abs(Double.parseDouble(normalized));
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    public static boolean isSuitable(boolean scannerRequireDistance,
                                     double scannerMinAbsDistance,
                                     double scannerMaxAbsDistance,
                                     Double absDistance) {
        if (!scannerRequireDistance) {
            return true;
        }
        if (absDistance == null) {
            return false;
        }
        return absDistance >= scannerMinAbsDistance && absDistance <= scannerMaxAbsDistance;
    }

    public static ScannerCandidate selectBest(List<ScannerCandidate> candidates,
                                              String currentSymbol,
                                              boolean preferLowDistance) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }

        return candidates.stream()
            .filter(candidate -> candidate.symbol() != null && !candidate.symbol().equals(currentSymbol))
            .sorted(preferLowDistance
                ? Comparator.comparing((ScannerCandidate candidate) -> candidate.absDistance() == null ? Double.MAX_VALUE : candidate.absDistance())
                    .thenComparingInt(ScannerCandidate::rank)
                : Comparator.comparingInt(ScannerCandidate::rank))
            .findFirst()
            .orElse(null);
    }
}
