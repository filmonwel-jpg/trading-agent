package com.calgary.fili.trader.bot.trader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ModelBundleResolver {

    public record ResolvedBundle(String symbol, String jobName, String variant, String modelDir, String note) {}

    private record RoutingDecision(String symbol, String jobName, String variant, String modelDir, String note) {}

    private final String defaultModelDir;
    private final Path symbolPlanCsv;
    private final Path routingCsv;
    private final Map<String, String> cohortBySymbol;
    private final Map<String, RoutingDecision> routingBySymbol;
    private final Map<String, RoutingDecision> routingByJob;

    public ModelBundleResolver(String defaultModelDir, String symbolPlanCsv, String routingCsv) {
        this.defaultModelDir = defaultModelDir == null ? "" : defaultModelDir.trim();
        this.symbolPlanCsv = normalizePath(symbolPlanCsv);
        this.routingCsv = normalizePath(routingCsv);
        this.cohortBySymbol = loadCohortBySymbol(this.symbolPlanCsv);
        this.routingBySymbol = new LinkedHashMap<>();
        this.routingByJob = new LinkedHashMap<>();
        loadRoutingDecisions(this.routingCsv, routingBySymbol, routingByJob);
    }

    public ResolvedBundle resolveForSymbol(String rawSymbol) {
        String symbol = normalizeSymbol(rawSymbol);
        if (symbol.isBlank()) {
            return new ResolvedBundle("", "", "default", defaultModelDir, "blank-symbol");
        }

        RoutingDecision bySymbol = routingBySymbol.get(symbol);
        if (bySymbol != null && !bySymbol.modelDir().isBlank()) {
            return new ResolvedBundle(symbol, nonBlank(bySymbol.jobName(), inferJobName(symbol)), nonBlank(bySymbol.variant(), "mixed"), bySymbol.modelDir(), nonBlank(bySymbol.note(), "routing-csv-symbol"));
        }

        String inferredJob = inferJobName(symbol);
        RoutingDecision byJob = routingByJob.get(inferredJob);
        if (byJob != null && !byJob.modelDir().isBlank()) {
            return new ResolvedBundle(symbol, inferredJob, nonBlank(byJob.variant(), "mixed"), byJob.modelDir(), nonBlank(byJob.note(), "routing-csv-job"));
        }

        return new ResolvedBundle(symbol, inferredJob, "default", defaultModelDir, "fallback-default-model-dir");
    }

    private String inferJobName(String symbol) {
        String cohort = cohortBySymbol.getOrDefault(symbol, "specialized");
        return switch (cohort.toLowerCase(Locale.US)) {
            case "mega_liquid" -> "mega_liquid";
            case "liquid" -> "liquid";
            default -> "specialized_" + symbol.toLowerCase(Locale.US);
        };
    }

    private static Map<String, String> loadCohortBySymbol(Path csvPath) {
        Map<String, String> out = new HashMap<>();
        if (csvPath == null || !Files.exists(csvPath)) {
            return out;
        }
        try {
            List<String> lines = Files.readAllLines(csvPath);
            if (lines.isEmpty()) {
                return out;
            }
            List<String> header = parseCsvLine(lines.getFirst());
            int symbolIdx = header.indexOf("Symbol");
            int cohortIdx = header.indexOf("modeling_cohort");
            if (symbolIdx < 0 || cohortIdx < 0) {
                return out;
            }
            for (int i = 1; i < lines.size(); i++) {
                List<String> row = parseCsvLine(lines.get(i));
                if (row.size() <= Math.max(symbolIdx, cohortIdx)) {
                    continue;
                }
                String symbol = normalizeSymbol(row.get(symbolIdx));
                String cohort = row.get(cohortIdx) == null ? "" : row.get(cohortIdx).trim();
                if (!symbol.isBlank() && !cohort.isBlank()) {
                    out.put(symbol, cohort);
                }
            }
        } catch (IOException ignored) {
        }
        return out;
    }

    private static void loadRoutingDecisions(Path csvPath,
                                             Map<String, RoutingDecision> bySymbol,
                                             Map<String, RoutingDecision> byJob) {
        if (csvPath == null || !Files.exists(csvPath)) {
            return;
        }
        try {
            List<String> lines = Files.readAllLines(csvPath);
            if (lines.isEmpty()) {
                return;
            }
            List<String> header = parseCsvLine(lines.getFirst());
            int symbolIdx = header.indexOf("symbol");
            int jobIdx = header.indexOf("job_name");
            int variantIdx = header.indexOf("variant");
            int modelDirIdx = header.indexOf("model_dir");
            int noteIdx = header.indexOf("note");
            for (int i = 1; i < lines.size(); i++) {
                List<String> row = parseCsvLine(lines.get(i));
                if (row.isEmpty()) {
                    continue;
                }
                String symbol = symbolIdx >= 0 && row.size() > symbolIdx ? normalizeSymbol(row.get(symbolIdx)) : "";
                String jobName = jobIdx >= 0 && row.size() > jobIdx ? safeCell(row.get(jobIdx)) : "";
                String variant = variantIdx >= 0 && row.size() > variantIdx ? safeCell(row.get(variantIdx)) : "";
                String modelDir = modelDirIdx >= 0 && row.size() > modelDirIdx ? safeCell(row.get(modelDirIdx)) : "";
                String note = noteIdx >= 0 && row.size() > noteIdx ? safeCell(row.get(noteIdx)) : "";
                RoutingDecision decision = new RoutingDecision(symbol, jobName, variant, modelDir, note);
                if (!symbol.isBlank()) {
                    bySymbol.put(symbol, decision);
                }
                if (!jobName.isBlank()) {
                    byJob.put(jobName, decision);
                }
            }
        } catch (IOException ignored) {
        }
    }

    private static Path normalizePath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return null;
        }
        Path candidate = Paths.get(rawPath.trim());
        return candidate.isAbsolute() ? candidate.normalize() : candidate.toAbsolutePath().normalize();
    }

    private static String nonBlank(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String safeCell(String value) {
        return value == null ? "" : value.trim();
    }

    private static String normalizeSymbol(String rawSymbol) {
        return rawSymbol == null ? "" : rawSymbol.trim().toUpperCase(Locale.US);
    }

    private static List<String> parseCsvLine(String line) {
        List<String> cells = new ArrayList<>();
        if (line == null || line.isEmpty()) {
            return cells;
        }

        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                cells.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }
        cells.add(current.toString().trim());
        return cells;
    }
}

