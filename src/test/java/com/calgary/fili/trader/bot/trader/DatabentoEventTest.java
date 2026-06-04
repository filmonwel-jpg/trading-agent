package com.calgary.fili.trader.bot.trader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
