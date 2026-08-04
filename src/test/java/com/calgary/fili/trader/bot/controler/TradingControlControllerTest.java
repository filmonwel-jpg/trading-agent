package com.calgary.fili.trader.bot.controler;

import com.calgary.fili.trader.bot.trader.IBKRTrader;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TradingControlControllerTest {

    @Test
    void positionSyncEndpointRequestsBrokerSyncAndReturnsCurrentStatus() {
        IBKRTrader trader = mock(IBKRTrader.class);
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("positionSyncComplete", false);
        status.put("positionSyncState", "syncing:preserve-bar-forwarding:shared-gateway");
        when(trader.controlStatus()).thenReturn(status);

        TradingControlController controller = new TradingControlController(trader);
        Map<String, Object> response = controller.positionSync(" operator-retry ");

        verify(trader).requestPositions("operator-retry");
        assertEquals("position-sync-requested reason=operator-retry", response.get("message"));
        assertSame(status, response.get("status"));
    }
}

