package com.calgary.fili.trader.bot.trader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class DatabentoEventTest {

    @Test
    void identifiesPreviousCloseContextEvent() {
        DatabentoEvent event = new DatabentoEvent();
        event.event = "previous_close";
        event.symbol = "TSLA";
        event.previousClose = 420.12;

        assertTrue(event.isPreviousClose());
        assertFalse(event.isEquityBar());
        assertFalse(event.isOptionBar());
        assertFalse(event.isStatus());
    }

    @Test
    void parsesStepTenQualityAliasesAndBlocksUnsafeEntries() throws Exception {
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        DatabentoEvent event = mapper.readValue("""
            {
              "event": "equity_bar",
              "symbol": "TSLA",
              "barEpochSec": 1800000000,
              "EventSchemaVersion": "databento_ndjson_v2",
              "DataQualityFlags": "no_quote|synthetic_ohlc",
              "TradeCoverage": 0.0,
              "QuoteStateCoverage": 0.0,
              "QualityScore": 0.35
            }
            """, DatabentoEvent.class);

        assertTrue(event.hasSanityContract());
        assertEquals("databento_ndjson_v2", event.eventSchemaVersion);
        assertEquals("no_quote|synthetic_ohlc", event.dataQualityFlags);
        assertTrue(event.hasDataQualityFlag("no_quote"));
        assertTrue(event.blocksNewEntries(0.50, "databento_ndjson_v2"));
    }

    @Test
    void capturesEventCarriedEnrichedSnapshotFields() throws Exception {
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        DatabentoEvent event = mapper.readValue("""
            {
              "event": "equity_bar",
              "symbol": "TQQQ",
              "barEpochSec": 1778767029,
              "FeatureSnapshotEpochSec": 1778767050,
              "FeatureSnapshotSchemaVersion": "downstream_setup_filter_onnx_research_v1",
              "FeatureSnapshotSource": "silver_30s",
              "ImbalanceStd5s": 177024.8363,
              "AskSizeLast": "1975",
              "ignored_text": "not numeric",
              "enriched_features": {
                "CallVolDelta5s": 300,
                "PutVolDelta5s": 62,
                "NaNValue": "NaN"
              }
            }
            """, DatabentoEvent.class);

        assertTrue(event.hasEnrichedNumericFields());
        assertEquals(1778767050L, event.effectiveFeatureSnapshotEpochSec());
        assertEquals("downstream_setup_filter_onnx_research_v1", event.featureSnapshotSchemaVersion);
        assertEquals("silver_30s", event.featureSnapshotSource);
        assertEquals(177024.84f, event.getEnrichedNumericFields().get("ImbalanceStd5s"), 1.0e-2f);
        assertEquals(1975.0f, event.getEnrichedNumericFields().get("AskSizeLast"), 1.0e-6f);
        assertEquals(300.0f, event.getEnrichedNumericFields().get("CallVolDelta5s"), 1.0e-6f);
        assertEquals(62.0f, event.getEnrichedNumericFields().get("PutVolDelta5s"), 1.0e-6f);
        assertFalse(event.getEnrichedNumericFields().containsKey("ignored_text"));
        assertFalse(event.getEnrichedNumericFields().containsKey("NaNValue"));
    }
}
