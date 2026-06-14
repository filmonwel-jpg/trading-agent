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
}
