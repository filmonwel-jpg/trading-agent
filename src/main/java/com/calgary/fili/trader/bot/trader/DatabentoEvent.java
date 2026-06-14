package com.calgary.fili.trader.bot.trader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;

import java.util.Locale;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabentoEvent {
    public String event = "";
    public String symbol = "";
    public String underlying = "";
    public String right = "";
    public long barEpochSec = 0L;
    public long tsEventNs = 0L;
    public double open = 0.0;
    public double high = 0.0;
    public double low = 0.0;
    public double close = 0.0;
    public double wap = 0.0;
    public long volume = 0L;
    public long tradeCount = 0L;
    public long quoteCount = 0L;
    public double bid = 0.0;
    public double ask = 0.0;
    public long bidSize = 0L;
    public long askSize = 0L;
    public long atBidVol = 0L;
    public long atAskVol = 0L;
    public double previousClose = 0.0;
    public String sessionDate = "";
    public String message = "";
    public boolean historical = false;

    @JsonAlias({"EventSchemaVersion", "event_schema_version"})
    public String eventSchemaVersion = "";
    @JsonAlias({"EventSource", "event_source"})
    public String eventSource = "";
    @JsonAlias({"EventDataset", "event_dataset"})
    public String eventDataset = "";
    @JsonAlias({"EventSchema", "event_schema"})
    public String eventSchema = "";
    @JsonAlias({"EventStypeIn", "event_stype_in"})
    public String eventStypeIn = "";
    @JsonAlias({"EventTsEventNs", "event_ts_event_ns"})
    public long eventTsEventNs = 0L;
    @JsonAlias({"EventGeneratedAtMs", "event_generated_at_ms"})
    public long eventGeneratedAtMs = 0L;

    @JsonAlias({"DataQualityFlags", "data_quality_flags"})
    public String dataQualityFlags = "";
    @JsonAlias({"ChildDataQualityFlagUnion", "child_data_quality_flag_union"})
    public String childDataQualityFlagUnion = "";
    @JsonAlias({"TradeSecondsPresent", "trade_seconds_present"})
    public double tradeSecondsPresent = Double.NaN;
    @JsonAlias({"QuoteUpdateSecondsPresent", "quote_update_seconds_present"})
    public double quoteUpdateSecondsPresent = Double.NaN;
    @JsonAlias({"QuoteStateSecondsValid", "quote_state_seconds_valid"})
    public double quoteStateSecondsValid = Double.NaN;
    @JsonAlias({"SyntheticSeconds", "synthetic_seconds"})
    public double syntheticSeconds = Double.NaN;
    @JsonAlias({"TradeCoverage", "trade_coverage"})
    public double tradeCoverage = Double.NaN;
    @JsonAlias({"QuoteUpdateCoverage", "quote_update_coverage"})
    public double quoteUpdateCoverage = Double.NaN;
    @JsonAlias({"QuoteStateCoverage", "quote_state_coverage"})
    public double quoteStateCoverage = Double.NaN;
    @JsonAlias({"SyntheticCoverage", "synthetic_coverage"})
    public double syntheticCoverage = Double.NaN;
    @JsonAlias({"QuoteAgeMsMean", "quote_age_ms_mean"})
    public double quoteAgeMsMean = Double.NaN;
    @JsonAlias({"QuoteAgeMsMax", "quote_age_ms_max"})
    public double quoteAgeMsMax = Double.NaN;
    @JsonAlias({"ValidSpreadCoverage", "valid_spread_coverage"})
    public double validSpreadCoverage = Double.NaN;
    @JsonAlias({"LockedCrossedSeconds", "locked_crossed_seconds"})
    public double lockedCrossedSeconds = Double.NaN;
    @JsonAlias({"QualityScore", "quality_score"})
    public double qualityScore = Double.NaN;

    public boolean isEquityBar() {
        return "equity_bar".equalsIgnoreCase(event);
    }

    public boolean isOptionBar() {
        return "option_bar".equalsIgnoreCase(event);
    }

    public boolean isPreviousClose() {
        return "previous_close".equalsIgnoreCase(event);
    }

    public boolean isStatus() {
        return "status".equalsIgnoreCase(event);
    }

    public boolean hasSanityContract() {
        return notBlank(eventSchemaVersion)
            || notBlank(dataQualityFlags)
            || Double.isFinite(qualityScore);
    }

    public boolean isExpectedEventSchema(String expectedVersion) {
        String expected = expectedVersion == null ? "" : expectedVersion.trim();
        if (expected.isBlank()) {
            return true;
        }
        return expected.equalsIgnoreCase(eventSchemaVersion == null ? "" : eventSchemaVersion.trim());
    }

    public double effectiveQualityScore() {
        return Double.isFinite(qualityScore) ? qualityScore : 1.0;
    }

    public boolean hasDataQualityFlag(String flag) {
        String wanted = normalizeFlag(flag);
        if (wanted.isBlank()) {
            return false;
        }
        for (String token : normalizedFlagString(dataQualityFlags).split("\\|")) {
            if (wanted.equals(token)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasEntryBlockingQualityFlag() {
        return hasDataQualityFlag("no_quote")
            || hasDataQualityFlag("synthetic_ohlc")
            || hasDataQualityFlag("stale_quote")
            || hasDataQualityFlag("locked_crossed");
    }

    public boolean blocksNewEntries(double minQualityScore, String expectedVersion) {
        return !hasSanityContract()
            || !isExpectedEventSchema(expectedVersion)
            || effectiveQualityScore() < minQualityScore
            || hasEntryBlockingQualityFlag();
    }

    private static boolean notBlank(String value) {
        return value != null && !value.trim().isBlank();
    }

    private static String normalizedFlagString(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.US).replace(',', '|');
        return normalized.isBlank() ? "none" : normalized;
    }

    private static String normalizeFlag(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.US);
    }
}

