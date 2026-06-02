package com.calgary.fili.trader.bot.strategy;

import com.calgary.fili.trader.bot.trader.IBKRTrader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PingPongStrategyBucketFinalizationTest {

    @Test
    void handle5SecondBarFinalizesImmediatelyOnTerminalSecond() {
        PingPongStrategy strategy = newStrategy();
        long bucketStart = 1_740_000_000L;
        try {
            ReflectionTestUtils.setField(strategy, "featureSessionDate", etDate(bucketStart));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart, 100.0, 101.0, 99.5, 100.5, 10L, 100.2);
            assertEquals(bucketStart, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 29L, 100.6, 101.4, 100.2, 101.2, 15L, 101.0);

            assertEquals(-1L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
            assertEquals(bucketStart, ReflectionTestUtils.getField(strategy, "lastFinalizedBucketStartEpoch"));
            Double barClose = (Double) ReflectionTestUtils.getField(strategy, "barClose");
            assertNotNull(barClose);
            assertEquals(101.2, barClose, 1e-9);
            assertEquals(25L, ReflectionTestUtils.getField(strategy, "barVolume"));
            assertNotNull(ReflectionTestUtils.getField(strategy, "currentMarketTime"));
        } finally {
            strategy.stop();
        }
    }

    @Test
    void handle5SecondBarStartsNextBucketNormallyAfterImmediateFinalize() {
        PingPongStrategy strategy = newStrategy();
        long bucketStart = 1_740_000_000L;
        try {
            ReflectionTestUtils.setField(strategy, "featureSessionDate", etDate(bucketStart));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart, 100.0, 100.5, 99.8, 100.1, 10L, 100.1);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 29L, 100.2, 100.8, 100.0, 100.7, 11L, 100.6);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 30L, 100.9, 101.0, 100.7, 100.95, 12L, 100.9);

            assertEquals(bucketStart + 30L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
            assertEquals(bucketStart, ReflectionTestUtils.getField(strategy, "lastFinalizedBucketStartEpoch"));
            Double bucketClose = (Double) ReflectionTestUtils.getField(strategy, "bucketClose");
            assertNotNull(bucketClose);
            assertEquals(100.95, bucketClose, 1e-9);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void handle5SecondBarDropsLateBarForAlreadyFinalizedBucket() {
        PingPongStrategy strategy = newStrategy();
        long bucketStart = 1_740_000_000L;
        try {
            ReflectionTestUtils.setField(strategy, "featureSessionDate", etDate(bucketStart));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart, 100.0, 100.5, 99.9, 100.2, 10L, 100.1);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 29L, 100.3, 100.9, 100.1, 100.8, 20L, 100.7);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 28L, 99.9, 100.0, 99.7, 99.8, 30L, 99.85);

            assertEquals(-1L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
            assertEquals(bucketStart, ReflectionTestUtils.getField(strategy, "lastFinalizedBucketStartEpoch"));
            Double barClose = (Double) ReflectionTestUtils.getField(strategy, "barClose");
            assertNotNull(barClose);
            assertEquals(100.8, barClose, 1e-9);
            assertEquals(30L, ReflectionTestUtils.getField(strategy, "barVolume"));
        } finally {
            strategy.stop();
        }
    }

    @Test
    void handle5SecondBarTriggersLongHardStopFromBarLow() {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        long bucketStart = 1_740_000_000L;
        try {
            ReflectionTestUtils.setField(strategy, "featureSessionDate", etDate(bucketStart));
            ReflectionTestUtils.setField(strategy, "currentPosition", 12);
            ReflectionTestUtils.setField(strategy, "avgEntryPrice", 100.0);

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart, 100.0, 100.2, 99.6, 99.9, 10L, 99.9);

            verify(parent).placeTrade("AAPL", "SELL", 99.9, 12, "MKT");
        } finally {
            strategy.stop();
        }
    }

    @Test
    void handle5SecondBarTriggersShortHardStopFromBarHigh() {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        long bucketStart = 1_740_000_000L;
        try {
            ReflectionTestUtils.setField(strategy, "featureSessionDate", etDate(bucketStart));
            ReflectionTestUtils.setField(strategy, "currentPosition", -8);
            ReflectionTestUtils.setField(strategy, "avgEntryPrice", 100.0);

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart, 100.0, 100.4, 99.9, 100.2, 10L, 100.2);

            verify(parent).placeTrade("AAPL", "BUY", 100.2, 8, "MKT");
        } finally {
            strategy.stop();
        }
    }

    private static PingPongStrategy newStrategy() {
        return newStrategy(mock(IBKRTrader.class));
    }

    private static PingPongStrategy newStrategy(IBKRTrader parent) {
        return new PingPongStrategy(
            parent,
            "AAPL",
            0.003,
            40_000,
            20,
            true,
            12,
            14,
            0.0005,
            0.004,
            500.0,
            1.20,
            0.70,
            null
        );
    }

    private static LocalDate etDate(long epochSec) {
        return Instant.ofEpochSecond(epochSec)
            .atZone(ZoneOffset.UTC)
            .withZoneSameInstant(ZoneId.of("America/New_York"))
            .toLocalDate();
    }
}


