package com.calgary.fili.trader.bot.trader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SharedCapitalManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void deniesSecondReservationWhenSharedCapitalIsInsufficient() {
        Path stateFile = tempDir.resolve("shared-capital.properties");
        SharedCapitalManager first = new SharedCapitalManager(true, stateFile, 70000.0);
        SharedCapitalManager second = new SharedCapitalManager(true, stateFile, 70000.0);

        SharedCapitalManager.ReservationDecision firstDecision = first.tryReserve("TSLA", 69000.0);
        assertTrue(firstDecision.allowed());
        assertEquals(1000.0, firstDecision.snapshot().availableNotional(), 0.01);

        SharedCapitalManager.ReservationDecision secondDecision = second.tryReserve("NVDA", 50000.0);
        assertFalse(secondDecision.allowed());
        assertEquals(1000.0, secondDecision.snapshot().availableNotional(), 0.01);
    }

    @Test
    void releaseAndReconcileToFlatRestoresAvailableCapital() {
        Path stateFile = tempDir.resolve("shared-capital.properties");
        SharedCapitalManager manager = new SharedCapitalManager(true, stateFile, 70000.0);

        assertTrue(manager.tryReserve("AMD", 50000.0).allowed());
        assertEquals(20000.0, manager.snapshot().availableNotional(), 0.01);

        SharedCapitalManager.ReservationDecision releaseDecision = manager.release("AMD");
        assertTrue(releaseDecision.allowed());
        assertEquals(70000.0, releaseDecision.snapshot().availableNotional(), 0.01);

        assertTrue(manager.tryReserve("AMD", 50000.0).allowed());
        SharedCapitalManager.ReservationDecision flatDecision = manager.reconcilePosition("AMD", 0, 0.0, 50000.0);
        assertTrue(flatDecision.allowed());
        assertEquals(70000.0, flatDecision.snapshot().availableNotional(), 0.01);
    }

    @Test
    void resetAllClearsReservationsButKeepsTotalNotional() {
        Path stateFile = tempDir.resolve("shared-capital.properties");
        SharedCapitalManager manager = new SharedCapitalManager(true, stateFile, 70000.0);

        assertTrue(manager.tryReserve("TSLA", 30000.0).allowed());
        assertTrue(manager.tryReserve("AMD", 20000.0).allowed());
        assertEquals(20000.0, manager.snapshot().availableNotional(), 0.01);

        SharedCapitalManager.ReservationDecision resetDecision = manager.resetAll();
        assertTrue(resetDecision.allowed());
        assertEquals(70000.0, resetDecision.snapshot().totalNotional(), 0.01);
        assertEquals(70000.0, resetDecision.snapshot().availableNotional(), 0.01);
        assertTrue(resetDecision.snapshot().reservations().isEmpty());
    }
}


