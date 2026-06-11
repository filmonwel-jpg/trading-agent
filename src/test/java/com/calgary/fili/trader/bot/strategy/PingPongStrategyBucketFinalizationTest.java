package com.calgary.fili.trader.bot.strategy;

import com.calgary.fili.trader.bot.trader.IBKRTrader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Deque;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PingPongStrategyBucketFinalizationTest {

    @Test
    void handle5SecondBarWaitsForAlignedInitial30SecondBoundary() {
        PingPongStrategy strategy = newStrategy();
        long bucketStart = 1_740_000_000L;
        try {
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 7L, 100.0, 100.4, 99.9, 100.1, 10L, 100.1);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 12L, 100.1, 100.5, 100.0, 100.2, 11L, 100.2);

            assertEquals(-1L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
            assertEquals(-1L, ReflectionTestUtils.getField(strategy, "lastFinalizedBucketStartEpoch"));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 30L, 100.3, 100.7, 100.2, 100.6, 12L, 100.5);

            assertEquals(bucketStart + 30L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
            assertEquals(12L, ReflectionTestUtils.getField(strategy, "bucketVolume"));
            Double bucketOpen = (Double) ReflectionTestUtils.getField(strategy, "bucketOpen");
            assertNotNull(bucketOpen);
            assertEquals(100.3, bucketOpen, 1e-9);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void handle5SecondBarStartsImmediatelyAtExactMarketOpenBoundary() {
        PingPongStrategy strategy = newStrategy();
        long marketOpen = LocalDateTime.of(2026, 6, 10, 9, 30, 0)
            .atZone(ZoneId.of("America/New_York"))
            .toEpochSecond();
        try {
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen, 100.0, 100.2, 99.8, 100.1, 10L, 100.1);

            assertEquals(0L, marketOpen % 30L);
            assertEquals(marketOpen, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
        } finally {
            strategy.stop();
        }
    }

    @Test
    void independentMicroBucketWaitsForAlignedInitialFiveSecondBoundary() {
        PingPongStrategy strategy = newStrategy();
        long marketOpen = LocalDateTime.of(2026, 6, 10, 9, 30, 0)
            .atZone(ZoneId.of("America/New_York"))
            .toEpochSecond();
        try {
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen + 2L, 100.0, 100.2, 99.8, 100.1, 10L, 100.1);

            assertEquals(-1L, ReflectionTestUtils.getField(strategy, "micro5sBucketStartEpoch"));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen + 5L, 100.1, 100.4, 100.0, 100.3, 12L, 100.25);

            assertEquals(marketOpen + 5L, ReflectionTestUtils.getField(strategy, "micro5sBucketStartEpoch"));
        } finally {
            strategy.stop();
        }
    }

    @Test
    void independentMicroBucketsUseAbsoluteFiveSecondStartLabels() {
        PingPongStrategy strategy = newStrategy();
        long marketOpen = LocalDateTime.of(2026, 6, 10, 9, 30, 0)
            .atZone(ZoneId.of("America/New_York"))
            .toEpochSecond();
        try {
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen, 100.0, 100.2, 99.8, 100.1, 10L, 100.1);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen + 1L, 100.1, 100.3, 100.0, 100.2, 11L, 100.2);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen + 5L, 100.2, 100.5, 100.1, 100.4, 12L, 100.35);

            assertEquals(marketOpen + 5L, ReflectionTestUtils.getField(strategy, "micro5sBucketStartEpoch"));
            @SuppressWarnings("unchecked")
            Deque<Object> micro5sWindow = (Deque<Object>) ReflectionTestUtils.getField(strategy, "micro5sWindow");
            assertNotNull(micro5sWindow);
            assertEquals(1, micro5sWindow.size());
            Object finalizedMicroBar = micro5sWindow.peekLast();
            assertNotNull(finalizedMicroBar);
            assertEquals(marketOpen, ReflectionTestUtils.getField(finalizedMicroBar, "epoch"));
            assertEquals(21L, ReflectionTestUtils.getField(finalizedMicroBar, "volume"));
        } finally {
            strategy.stop();
        }
    }

    @Test
    void lifecycleBarsAndMicroArmEpochsUseThirtySecondClockBoundaries() {
        PingPongStrategy strategy = newStrategy();
        long marketOpen = LocalDateTime.of(2026, 6, 10, 9, 30, 0)
            .atZone(ZoneId.of("America/New_York"))
            .toEpochSecond();
        try {
            ReflectionTestUtils.setField(strategy, "featureSessionDate", etDate(marketOpen));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen, 100.0, 100.5, 99.8, 100.1, 10L, 100.1);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen + 29L, 100.2, 100.8, 100.0, 100.7, 11L, 100.6);

            assertEquals(marketOpen, ReflectionTestUtils.getField(strategy, "lastTraining30sEpoch"));
            assertEquals(marketOpen + 30L, ReflectionTestUtils.getField(strategy, "current30sAiDecisionEpoch"));
            assertEquals(LocalDateTime.of(2026, 6, 10, 9, 30, 0), ReflectionTestUtils.getField(strategy, "currentMarketTime"));
            Long firstArmEpoch = ReflectionTestUtils.invokeMethod(strategy, "currentMicroArmEpoch");
            assertNotNull(firstArmEpoch);
            assertEquals(marketOpen + 30L, firstArmEpoch.longValue());

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen + 30L, 100.8, 101.0, 100.7, 100.9, 12L, 100.85);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen + 59L, 100.9, 101.2, 100.8, 101.1, 13L, 101.0);

            assertEquals(marketOpen + 30L, ReflectionTestUtils.getField(strategy, "lastTraining30sEpoch"));
            assertEquals(marketOpen + 60L, ReflectionTestUtils.getField(strategy, "current30sAiDecisionEpoch"));
            assertEquals(LocalDateTime.of(2026, 6, 10, 9, 30, 30), ReflectionTestUtils.getField(strategy, "currentMarketTime"));
            Long secondArmEpoch = ReflectionTestUtils.invokeMethod(strategy, "currentMicroArmEpoch");
            assertNotNull(secondArmEpoch);
            assertEquals(marketOpen + 60L, secondArmEpoch.longValue());
        } finally {
            strategy.stop();
        }
    }

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
            assertEquals(bucketStart + 30L, ReflectionTestUtils.getField(strategy, "current30sAiDecisionEpoch"));
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
    void handle5SecondBarWaitsForNextBoundaryWhenRestartingAfterGap() {
        PingPongStrategy strategy = newStrategy();
        long bucketStart = 1_740_000_000L;
        try {
            ReflectionTestUtils.setField(strategy, "featureSessionDate", etDate(bucketStart));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart, 100.0, 100.5, 99.8, 100.1, 10L, 100.1);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 29L, 100.2, 100.8, 100.0, 100.7, 11L, 100.6);
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 35L, 100.9, 101.0, 100.7, 100.95, 12L, 100.9);

            assertEquals(-1L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
            assertEquals(bucketStart, ReflectionTestUtils.getField(strategy, "lastFinalizedBucketStartEpoch"));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", bucketStart + 60L, 101.0, 101.3, 100.9, 101.2, 13L, 101.1);

            assertEquals(bucketStart + 60L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
            assertEquals(13L, ReflectionTestUtils.getField(strategy, "bucketVolume"));
        } finally {
            strategy.stop();
        }
    }

    @Test
    void resetForNewDayClearsPartialThirtySecondBucketBeforeRealigningAtOpen() {
        PingPongStrategy strategy = newStrategy();
        long marketOpen = LocalDateTime.of(2026, 6, 10, 9, 30, 0)
            .atZone(ZoneId.of("America/New_York"))
            .toEpochSecond();
        try {
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen - 30L, 100.0, 100.2, 99.8, 100.1, 10L, 100.1);
            assertEquals(marketOpen - 30L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));

            strategy.resetForNewDayAndWait(1_000L);

            assertEquals(-1L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen - 5L, 100.1, 100.3, 100.0, 100.2, 11L, 100.2);
            assertEquals(-1L, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));

            ReflectionTestUtils.invokeMethod(strategy, "handle5SecondBar", marketOpen, 100.3, 100.6, 100.2, 100.5, 12L, 100.4);
            assertEquals(marketOpen, ReflectionTestUtils.getField(strategy, "bucketStartEpoch"));
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


