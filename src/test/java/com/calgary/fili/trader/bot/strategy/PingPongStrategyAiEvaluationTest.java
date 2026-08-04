package com.calgary.fili.trader.bot.strategy;

import com.calgary.fili.trader.bot.trader.IBKRTrader;
import java.lang.reflect.Constructor;
import java.time.LocalDateTime;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PingPongStrategyAiEvaluationTest {

    @Test
    void askArtificialIntelligenceDoesNotEvaluateLegacyExitModelsWhileFlatByDefault() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            seedFeatureState(strategy);
            ReflectionTestUtils.setField(strategy, "allowNewEntries", false);
            ReflectionTestUtils.setField(strategy, "currentPosition", 0);

            AiPredictor longEntry = mock(AiPredictor.class);
            AiPredictor shortEntry = mock(AiPredictor.class);
            AiPredictor longExit = mock(AiPredictor.class);
            AiPredictor shortExit = mock(AiPredictor.class);
            when(longExit.predictProbability(any(float[].class))).thenReturn(0.92);
            when(shortExit.predictProbability(any(float[].class))).thenReturn(0.94);
            configureBasePredictors(strategy, longEntry, shortEntry, longExit, shortExit);

            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            verify(longExit, never()).predictProbability(any(float[].class));
            verify(shortExit, never()).predictProbability(any(float[].class));
            verify(parent, never()).placeTrade(anyString(), anyString(), anyDouble(), anyInt(), anyString());
        } finally {
            strategy.stop();
        }
    }

    @Test
    void askArtificialIntelligenceDoesNotUseLegacyLongExitByDefault() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            seedFeatureState(strategy);
            ReflectionTestUtils.setField(strategy, "currentPosition", 37);

            AiPredictor longEntry = mock(AiPredictor.class);
            AiPredictor shortEntry = mock(AiPredictor.class);
            AiPredictor longExit = mock(AiPredictor.class);
            AiPredictor shortExit = mock(AiPredictor.class);
            when(longExit.predictProbability(any(float[].class))).thenReturn(0.95);
            configureBasePredictors(strategy, longEntry, shortEntry, longExit, shortExit);

            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            verify(longExit, never()).predictProbability(any(float[].class));
            verify(shortExit, never()).predictProbability(any(float[].class));
            verify(parent, never()).placeTrade(anyString(), anyString(), anyDouble(), anyInt(), anyString());
        } finally {
            strategy.stop();
        }
    }

    @Test
    void askArtificialIntelligenceDoesNotUseLegacyShortExitByDefault() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            seedFeatureState(strategy);
            ReflectionTestUtils.setField(strategy, "currentPosition", -23);

            AiPredictor longEntry = mock(AiPredictor.class);
            AiPredictor shortEntry = mock(AiPredictor.class);
            AiPredictor longExit = mock(AiPredictor.class);
            AiPredictor shortExit = mock(AiPredictor.class);
            when(shortExit.predictProbability(any(float[].class))).thenReturn(0.97);
            configureBasePredictors(strategy, longEntry, shortEntry, longExit, shortExit);

            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            verify(longExit, never()).predictProbability(any(float[].class));
            verify(shortExit, never()).predictProbability(any(float[].class));
            verify(parent, never()).placeTrade(anyString(), anyString(), anyDouble(), anyInt(), anyString());
        } finally {
            strategy.stop();
        }
    }

    @Test
    void askArtificialIntelligenceBlocksNewEntriesWhenPositionSyncIsNotConfirmed() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            seedFeatureState(strategy);
            ReflectionTestUtils.setField(strategy, "allowNewEntries", true);
            ReflectionTestUtils.setField(strategy, "positionSynced", false);
            ReflectionTestUtils.setField(strategy, "currentPosition", 0);

            AiPredictor longEntry = mock(AiPredictor.class);
            AiPredictor shortEntry = mock(AiPredictor.class);
            AiPredictor longExit = mock(AiPredictor.class);
            AiPredictor shortExit = mock(AiPredictor.class);
            when(longExit.predictProbability(any(float[].class))).thenReturn(0.92);
            when(shortExit.predictProbability(any(float[].class))).thenReturn(0.94);
            when(longEntry.predictProbability(any(float[].class))).thenReturn(0.99);
            when(shortEntry.predictProbability(any(float[].class))).thenReturn(0.99);
            configureBasePredictors(strategy, longEntry, shortEntry, longExit, shortExit);

            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            verify(longExit, never()).predictProbability(any(float[].class));
            verify(shortExit, never()).predictProbability(any(float[].class));
            verify(longEntry, never()).predictProbability(any(float[].class));
            verify(shortEntry, never()).predictProbability(any(float[].class));
            verify(parent, never()).placeTrade(anyString(), anyString(), anyDouble(), anyInt(), anyString());
        } finally {
            strategy.stop();
        }
    }

    @Test
    void dataQualityEntryGateBlocksAndRecoversWithoutMutatingScheduleGate() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            seedFeatureState(strategy);
            ReflectionTestUtils.setField(strategy, "allowNewEntries", true);
            ReflectionTestUtils.setField(strategy, "positionSynced", true);
            ReflectionTestUtils.setField(strategy, "currentPosition", 0);
            strategy.setAiThresholds(0.68, 0.63, 0.58, 0.60);

            AiPredictor longEntry = mock(AiPredictor.class);
            AiPredictor shortEntry = mock(AiPredictor.class);
            AiPredictor longExit = mock(AiPredictor.class);
            AiPredictor shortExit = mock(AiPredictor.class);
            when(longEntry.predictProbability(any(float[].class))).thenReturn(0.90);
            when(shortEntry.predictProbability(any(float[].class))).thenReturn(0.10);
            configureBasePredictors(strategy, longEntry, shortEntry, longExit, shortExit);

            ReflectionTestUtils.invokeMethod(strategy, "handleSetDataQualityAllowsNewEntries", false);
            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            assertTrue((Boolean) ReflectionTestUtils.getField(strategy, "allowNewEntries"));
            assertFalse((Boolean) ReflectionTestUtils.getField(strategy, "dataQualityAllowsNewEntries"));
            verify(longEntry, never()).predictProbability(any(float[].class));
            verify(shortEntry, never()).predictProbability(any(float[].class));
            verify(parent, never()).placeTrade(anyString(), anyString(), anyDouble(), anyInt(), anyString());
            assertEquals(1L, strategy.getAiDecisionDiagnostics().dataQualityNewEntriesBlocked());

            ReflectionTestUtils.invokeMethod(strategy, "handleSetDataQualityAllowsNewEntries", true);
            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            assertTrue((Boolean) ReflectionTestUtils.getField(strategy, "allowNewEntries"));
            assertTrue((Boolean) ReflectionTestUtils.getField(strategy, "dataQualityAllowsNewEntries"));
            verify(longEntry, times(1)).predictProbability(any(float[].class));
            verify(shortEntry, times(1)).predictProbability(any(float[].class));
            verify(parent, times(1)).placeTrade("AAPL", "BUY", 100.05, 399, "FAST_LMT");
        } finally {
            strategy.stop();
        }
    }

    @Test
    void aiDecisionDiagnosticsCaptureBelowThresholdEntryScores() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            seedFeatureState(strategy);
            ReflectionTestUtils.setField(strategy, "allowNewEntries", true);
            ReflectionTestUtils.setField(strategy, "positionSynced", true);
            ReflectionTestUtils.setField(strategy, "currentPosition", 0);
            strategy.setAiThresholds(0.68, 0.63, 0.58, 0.60);

            AiPredictor longEntry = mock(AiPredictor.class);
            AiPredictor shortEntry = mock(AiPredictor.class);
            AiPredictor longExit = mock(AiPredictor.class);
            AiPredictor shortExit = mock(AiPredictor.class);
            when(longEntry.predictProbability(any(float[].class))).thenReturn(0.62);
            when(shortEntry.predictProbability(any(float[].class))).thenReturn(0.61);
            configureBasePredictors(strategy, longEntry, shortEntry, longExit, shortExit);

            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            PingPongStrategy.AiDecisionDiagnostics diagnostics = strategy.getAiDecisionDiagnostics();
            assertEquals(1L, diagnostics.aiEvaluations());
            assertEquals(1L, diagnostics.flatEntryEvaluations());
            assertEquals(1L, diagnostics.entryGateOpen());
            assertEquals(1L, diagnostics.longEntryModelEvaluations());
            assertEquals(1L, diagnostics.shortEntryModelEvaluations());
            assertEquals(0L, diagnostics.longEntryPasses());
            assertEquals(0L, diagnostics.shortEntryPasses());
            assertEquals(0.62, diagnostics.maxLongEntryProbability(), 1.0e-9);
            assertEquals(0.68, diagnostics.maxLongEntryThreshold(), 1.0e-9);
            assertEquals(-0.06, diagnostics.maxLongEntryMargin(), 1.0e-9);
            assertEquals(0.61, diagnostics.maxShortEntryProbability(), 1.0e-9);
            assertEquals(0.63, diagnostics.maxShortEntryThreshold(), 1.0e-9);
            assertEquals(-0.02, diagnostics.maxShortEntryMargin(), 1.0e-9);
            assertEquals(2, diagnostics.closestSetupEvents().size());
            assertEquals("short", diagnostics.closestSetupEvents().get(0).side());
            verify(parent, never()).placeTrade(anyString(), anyString(), anyDouble(), anyInt(), anyString());
        } finally {
            strategy.stop();
        }
    }

    @Test
    void askArtificialIntelligenceChoosesHigherMarginSideAfterEvaluatingBothEntryModels() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            seedFeatureState(strategy);
            ReflectionTestUtils.setField(strategy, "allowNewEntries", true);
            ReflectionTestUtils.setField(strategy, "positionSynced", true);
            ReflectionTestUtils.setField(strategy, "currentPosition", 0);
            strategy.setAiThresholds(0.68, 0.63, 0.58, 0.60);

            AiPredictor longEntry = mock(AiPredictor.class);
            AiPredictor shortEntry = mock(AiPredictor.class);
            AiPredictor longExit = mock(AiPredictor.class);
            AiPredictor shortExit = mock(AiPredictor.class);
            when(longEntry.predictProbability(any(float[].class))).thenReturn(0.70);
            when(shortEntry.predictProbability(any(float[].class))).thenReturn(0.90);
            configureBasePredictors(strategy, longEntry, shortEntry, longExit, shortExit);

            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            verify(longEntry, times(1)).predictProbability(any(float[].class));
            verify(shortEntry, times(1)).predictProbability(any(float[].class));
            verify(parent, times(1)).placeTrade("AAPL", "SELL", 99.95, 400, "FAST_LMT");
        } finally {
            strategy.stop();
        }
    }

    @Test
    void askArtificialIntelligenceDoesNotTradeWhenBothSidesPassWithoutClearMarginEdge() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            seedFeatureState(strategy);
            ReflectionTestUtils.setField(strategy, "allowNewEntries", true);
            ReflectionTestUtils.setField(strategy, "positionSynced", true);
            ReflectionTestUtils.setField(strategy, "currentPosition", 0);
            strategy.setAiThresholds(0.50, 0.50, 0.58, 0.60);

            AiPredictor longEntry = mock(AiPredictor.class);
            AiPredictor shortEntry = mock(AiPredictor.class);
            AiPredictor longExit = mock(AiPredictor.class);
            AiPredictor shortExit = mock(AiPredictor.class);
            when(longEntry.predictProbability(any(float[].class))).thenReturn(0.80);
            when(shortEntry.predictProbability(any(float[].class))).thenReturn(0.80);
            configureBasePredictors(strategy, longEntry, shortEntry, longExit, shortExit);

            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            verify(longEntry, times(1)).predictProbability(any(float[].class));
            verify(shortEntry, times(1)).predictProbability(any(float[].class));
            verify(parent, never()).placeTrade(anyString(), anyString(), anyDouble(), anyInt(), anyString());
        } finally {
            strategy.stop();
        }
    }

    @Test
    void downstreamSetupFilterRunsAfterNormalSetupArmAndBeforeEntryExecution() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            seedFeatureState(strategy);
            ReflectionTestUtils.setField(strategy, "allowNewEntries", true);
            ReflectionTestUtils.setField(strategy, "positionSynced", true);
            ReflectionTestUtils.setField(strategy, "currentPosition", 0);
            strategy.setAiThresholds(0.68, 0.63, 0.58, 0.60);

            AtomicBoolean setupModelEvaluated = new AtomicBoolean(false);
            AiPredictor longEntry = mock(AiPredictor.class);
            AiPredictor shortEntry = mock(AiPredictor.class);
            AiPredictor longExit = mock(AiPredictor.class);
            AiPredictor shortExit = mock(AiPredictor.class);
            when(longEntry.predictProbability(any(float[].class))).thenAnswer(invocation -> {
                setupModelEvaluated.set(true);
                return 0.74;
            });
            when(shortEntry.predictProbability(any(float[].class))).thenReturn(0.61);
            configureBasePredictors(strategy, longEntry, shortEntry, longExit, shortExit);

            BiFunction<String, Map<String, Float>, DownstreamSetupFilter.Decision> blockingFilter = (side, features) -> {
                assertTrue(setupModelEvaluated.get(), "normal setup model should evaluate before downstream filter");
                assertEquals("long", side);
                assertEquals(0.74f, features.get("SetupProb"), 1.0e-6f);
                assertEquals(0.68f, features.get("SetupThreshold"), 1.0e-6f);
                assertEquals(0.06f, features.get("SetupThresholdMargin"), 1.0e-6f);
                assertEquals(1.0f, features.get("SetupArbitrationReason_only_long_passed"), 1.0e-6f);
                return new DownstreamSetupFilter.Decision(false, 0.49, 0.50, 139, "unitTestDownstreamSetupFilter");
            };
            ReflectionTestUtils.setField(strategy, "downstreamSetupFilterScorer", blockingFilter);

            ReflectionTestUtils.invokeMethod(strategy, "askArtificialIntelligence");

            verify(longEntry, times(1)).predictProbability(any(float[].class));
            verify(shortEntry, times(1)).predictProbability(any(float[].class));
            verify(parent, never()).placeTrade(anyString(), anyString(), anyDouble(), anyInt(), anyString());
            assertFalse((Boolean) ReflectionTestUtils.getField(strategy, "microLongEntryArmed"));
            assertFalse((Boolean) ReflectionTestUtils.getField(strategy, "microShortEntryArmed"));
        } finally {
            strategy.stop();
        }
    }

    @Test
    void watchdogKeepsInFlightLockWhileReconcilingStaleExitOrder() {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            ReflectionTestUtils.setField(strategy, "currentPosition", 66);
            ReflectionTestUtils.setField(strategy, "avgEntryPrice", 138.40);
            ReflectionTestUtils.setField(strategy, "latestBidPrice", 133.30);
            ReflectionTestUtils.setField(strategy, "inFlightOrder", true);
            ReflectionTestUtils.setField(strategy, "pendingOrderId", 21003134);
            ReflectionTestUtils.setField(strategy, "lastOrderSubmitTime", System.currentTimeMillis() - 4_000L);

            ReflectionTestUtils.invokeMethod(strategy, "handleTickForExitsOnly", 133.30);
            ReflectionTestUtils.invokeMethod(strategy, "handleTickForExitsOnly", 133.20);

            verify(parent, times(1)).cancelStaleOrder(21003134);
            verify(parent, times(1)).requestPositions();
            verify(parent, times(1)).requestOpenOrdersSync();
            verify(parent, never()).placeTrade(anyString(), anyString(), anyDouble(), anyInt(), anyString());
        } finally {
            strategy.stop();
        }
    }

    @Test
    void modelForTrendRegimeFallsBackToChoppyWhenTrendModelIsUnavailable() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            Object base = lazyPredictor("long_entry.onnx", mock(AiPredictor.class));
            Object choppy = lazyPredictor("choppy_long_entry.onnx", mock(AiPredictor.class));
            Object missingTrend = unavailableLazyPredictor("trend_long_entry.onnx");
            Object volatileModel = lazyPredictor("volatile_long_entry.onnx", mock(AiPredictor.class));

            Object selected = ReflectionTestUtils.invokeMethod(
                strategy,
                "modelForRegime",
                marketRegime("TREND"),
                base,
                choppy,
                missingTrend,
                volatileModel
            );

            assertSame(choppy, selected);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void modelForVolatileRegimeFallsBackToChoppyWhenVolatileModelIsUnavailable() throws Exception {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            Object base = lazyPredictor("short_entry.onnx", mock(AiPredictor.class));
            Object choppy = lazyPredictor("choppy_short_entry.onnx", mock(AiPredictor.class));
            Object trend = lazyPredictor("trend_short_entry.onnx", mock(AiPredictor.class));
            Object missingVolatile = unavailableLazyPredictor("volatile_short_entry.onnx");

            Object selected = ReflectionTestUtils.invokeMethod(
                strategy,
                "modelForRegime",
                marketRegime("VOLATILE"),
                base,
                choppy,
                trend,
                missingVolatile
            );

            assertSame(choppy, selected);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void microEntryDefaultTtlIsThirtySecondsAfterThirtySecondAiDecisionTime() {
        Long ttlSeconds = (Long) ReflectionTestUtils.getField(PingPongStrategy.class, "MICRO_ARM_TTL_SECONDS");

        assertEquals(30L, ttlSeconds);
    }

    @Test
    void microEntryArmEpochUsesThirtySecondAiDecisionTime() {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            ReflectionTestUtils.setField(strategy, "lastTraining30sEpoch", 1_779_894_150L);
            ReflectionTestUtils.setField(strategy, "latestSourceBarEpoch", 1_779_894_184L);
            ReflectionTestUtils.setField(strategy, "current30sAiDecisionEpoch", 1_779_894_184L);

            Long armEpoch = ReflectionTestUtils.invokeMethod(strategy, "currentMicroArmEpoch");

            assertEquals(1_779_894_184L, armEpoch);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void microEntryArmEpochFallsBackToThirtySecondBucketCloseWhenSourceTimeMissing() {
        IBKRTrader parent = mock(IBKRTrader.class);
        PingPongStrategy strategy = newStrategy(parent);
        try {
            ReflectionTestUtils.setField(strategy, "lastTraining30sEpoch", 1_779_894_150L);
            ReflectionTestUtils.setField(strategy, "latestSourceBarEpoch", 0L);
            ReflectionTestUtils.setField(strategy, "current30sAiDecisionEpoch", 0L);

            Long armEpoch = ReflectionTestUtils.invokeMethod(strategy, "currentMicroArmEpoch");

            assertEquals(1_779_894_180L, armEpoch);
        } finally {
            strategy.stop();
        }
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

    private static void configureBasePredictors(PingPongStrategy strategy,
                                                AiPredictor longEntry,
                                                AiPredictor shortEntry,
                                                AiPredictor longExit,
                                                AiPredictor shortExit) throws Exception {
        ReflectionTestUtils.setField(strategy, "longEntryAi", lazyPredictor("long_entry.onnx", longEntry));
        ReflectionTestUtils.setField(strategy, "shortEntryAi", lazyPredictor("short_entry.onnx", shortEntry));
        ReflectionTestUtils.setField(strategy, "longExitAi", lazyPredictor("long_exit.onnx", longExit));
        ReflectionTestUtils.setField(strategy, "shortExitAi", lazyPredictor("short_exit.onnx", shortExit));
        ReflectionTestUtils.setField(strategy, "regimeClassifierAi", null);
        ReflectionTestUtils.setField(strategy, "choppyLongEntryAi", null);
        ReflectionTestUtils.setField(strategy, "choppyShortEntryAi", null);
        ReflectionTestUtils.setField(strategy, "choppyLongExitAi", null);
        ReflectionTestUtils.setField(strategy, "choppyShortExitAi", null);
        ReflectionTestUtils.setField(strategy, "trendLongEntryAi", null);
        ReflectionTestUtils.setField(strategy, "trendShortEntryAi", null);
        ReflectionTestUtils.setField(strategy, "trendLongExitAi", null);
        ReflectionTestUtils.setField(strategy, "trendShortExitAi", null);
        ReflectionTestUtils.setField(strategy, "volatileLongEntryAi", null);
        ReflectionTestUtils.setField(strategy, "volatileShortEntryAi", null);
        ReflectionTestUtils.setField(strategy, "volatileLongExitAi", null);
        ReflectionTestUtils.setField(strategy, "volatileShortExitAi", null);
        ReflectionTestUtils.setField(strategy, "open30LongEntryAi", null);
        ReflectionTestUtils.setField(strategy, "open30ShortEntryAi", null);
        ReflectionTestUtils.setField(strategy, "open30LongExitAi", null);
        ReflectionTestUtils.setField(strategy, "open30ShortExitAi", null);
    }

    private static Object lazyPredictor(String modelName, AiPredictor delegate) throws Exception {
        Class<?> lazyClass = Class.forName("com.calgary.fili.trader.bot.strategy.PingPongStrategy$LazyAiPredictor");
        Constructor<?> constructor = lazyClass.getDeclaredConstructor(String.class, String.class, String.class);
        constructor.setAccessible(true);
        Object predictor = constructor.newInstance(modelName, null, "unit-test");
        ReflectionTestUtils.setField(predictor, "delegate", delegate);
        ReflectionTestUtils.setField(predictor, "attemptedLoad", true);
        return predictor;
    }

    private static Object unavailableLazyPredictor(String modelName) throws Exception {
        Class<?> lazyClass = Class.forName("com.calgary.fili.trader.bot.strategy.PingPongStrategy$LazyAiPredictor");
        Constructor<?> constructor = lazyClass.getDeclaredConstructor(String.class, String.class, String.class);
        constructor.setAccessible(true);
        Object predictor = constructor.newInstance(modelName, null, "unit-test");
        ReflectionTestUtils.setField(predictor, "attemptedLoad", true);
        return predictor;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Object marketRegime(String name) throws Exception {
        Class<? extends Enum> regimeClass = Class.forName("com.calgary.fili.trader.bot.strategy.PingPongStrategy$MarketRegime").asSubclass(Enum.class);
        return Enum.valueOf(regimeClass, name);
    }

    private static void seedFeatureState(PingPongStrategy strategy) {
        ReflectionTestUtils.setField(strategy, "currentMarketTime", LocalDateTime.of(2026, 4, 10, 10, 0));
        ReflectionTestUtils.setField(strategy, "barOpen", 99.5);
        ReflectionTestUtils.setField(strategy, "barHigh", 101.0);
        ReflectionTestUtils.setField(strategy, "barLow", 99.0);
        ReflectionTestUtils.setField(strategy, "barClose", 100.0);
        ReflectionTestUtils.setField(strategy, "barVolume", 1_000L);
        ReflectionTestUtils.setField(strategy, "vwap", 99.8);
        ReflectionTestUtils.setField(strategy, "atr12", 1.5);
        ReflectionTestUtils.setField(strategy, "macdDiff", 0.42);
        ReflectionTestUtils.setField(strategy, "dayHigh", 102.0);
        ReflectionTestUtils.setField(strategy, "dayLow", 98.0);
        ReflectionTestUtils.setField(strategy, "greenStreak", 3);
        ReflectionTestUtils.setField(strategy, "redStreak", 0);
        ReflectionTestUtils.setField(strategy, "currentPutCallRatio", 1.25f);
        ReflectionTestUtils.setField(strategy, "currentBarVolAsk", 600L);
        ReflectionTestUtils.setField(strategy, "currentBarVolBid", 400L);
        ReflectionTestUtils.setField(strategy, "latestBidPrice", 99.95);
        ReflectionTestUtils.setField(strategy, "latestAskPrice", 100.05);
        ReflectionTestUtils.setField(strategy, "latestBidSize", 1_200L);
        ReflectionTestUtils.setField(strategy, "latestAskSize", 800L);
        ReflectionTestUtils.setField(strategy, "openingRangeHigh", 101.5);
        ReflectionTestUtils.setField(strategy, "openingRangeLow", 98.5);
        ReflectionTestUtils.setField(strategy, "yesterdayClose", 98.0);

        @SuppressWarnings("unchecked")
        Deque<Double> bbWindow = (Deque<Double>) ReflectionTestUtils.getField(strategy, "bbWindow");
        @SuppressWarnings("unchecked")
        Deque<Double> smaWindow = (Deque<Double>) ReflectionTestUtils.getField(strategy, "smaWindow");
        @SuppressWarnings("unchecked")
        Deque<Double> highWindow = (Deque<Double>) ReflectionTestUtils.getField(strategy, "highWindow");
        @SuppressWarnings("unchecked")
        Deque<Double> lowWindow = (Deque<Double>) ReflectionTestUtils.getField(strategy, "lowWindow");
        @SuppressWarnings("unchecked")
        Deque<Double> returnWindow20 = (Deque<Double>) ReflectionTestUtils.getField(strategy, "returnWindow20");
        @SuppressWarnings("unchecked")
        Deque<Double> realizedVolWindow100 = (Deque<Double>) ReflectionTestUtils.getField(strategy, "realizedVolWindow100");
        @SuppressWarnings("unchecked")
        Deque<Double> spreadWindow100 = (Deque<Double>) ReflectionTestUtils.getField(strategy, "spreadWindow100");
        @SuppressWarnings("unchecked")
        Deque<Double> volumeWindow = (Deque<Double>) ReflectionTestUtils.getField(strategy, "volumeWindow");
        @SuppressWarnings("unchecked")
        Map<Integer, Double> minuteVolumeBaseline = (Map<Integer, Double>) ReflectionTestUtils.getField(strategy, "minuteVolumeBaseline");

        resetDeque(bbWindow, 99.1, 99.4, 99.6, 99.2, 99.8, 100.1, 100.4, 100.2, 100.7, 100.5, 99.9, 100.0);
        resetDeque(
            smaWindow,
            96.4, 96.8, 97.1, 97.4, 97.8, 98.0, 98.3, 98.7, 99.0, 99.4,
            99.6, 99.9, 100.1, 100.5, 100.8, 101.0, 100.7, 100.3, 99.9, 99.6,
            99.3, 99.1, 98.9, 99.2, 99.5, 99.7, 100.0, 100.2, 100.5, 100.9,
            101.2, 101.0, 100.6, 100.3, 100.1, 99.8, 99.5, 99.2, 98.9, 98.7,
            98.5, 98.8, 99.0, 99.3, 99.6, 99.9, 100.2, 100.4, 100.7, 100.9,
            101.1, 100.8, 100.4, 100.0, 99.7, 99.4, 99.2, 99.5, 99.8, 100.0
        );
        resetDeque(
            highWindow,
            96.9, 97.2, 97.6, 97.8, 98.2, 98.5, 98.7, 99.0, 99.3, 99.7,
            100.0, 100.3, 100.5, 100.9, 101.2, 101.4, 101.1, 100.7, 100.3, 100.0,
            99.8, 99.5, 99.2, 99.6, 99.9, 100.1, 100.4, 100.6, 100.9, 101.3,
            101.6, 101.4, 101.0, 100.7, 100.4, 100.1, 99.8, 99.5, 99.2, 99.0,
            98.8, 99.1, 99.4, 99.7, 100.0, 100.3, 100.6, 100.8, 101.1, 101.3,
            101.5, 101.2, 100.8, 100.4, 100.1, 99.8, 99.6, 99.9, 100.2, 101.0
        );
        resetDeque(
            lowWindow,
            95.9, 96.2, 96.6, 96.8, 97.2, 97.5, 97.8, 98.1, 98.5, 98.9,
            99.1, 99.4, 99.7, 100.0, 100.3, 100.5, 100.1, 99.8, 99.4, 99.1,
            98.8, 98.6, 98.3, 98.7, 99.0, 99.2, 99.5, 99.7, 100.0, 100.4,
            100.7, 100.5, 100.1, 99.8, 99.6, 99.3, 99.0, 98.7, 98.4, 98.2,
            98.0, 98.3, 98.6, 98.9, 99.2, 99.5, 99.8, 100.0, 100.3, 100.5,
            100.7, 100.4, 100.0, 99.6, 99.3, 99.0, 98.8, 99.1, 99.4, 99.0
        );
        resetDeque(returnWindow20, 0.010, -0.004, 0.007, 0.003, 0.006, -0.002, 0.005, 0.004);
        resetDeque(realizedVolWindow100, 0.011, 0.014, 0.017, 0.013, 0.015, 0.012, 0.016, 0.014);
        resetDeque(spreadWindow100, 0.06, 0.08, 0.10, 0.07, 0.09, 0.08, 0.07, 0.06);
        resetDeque(
            volumeWindow,
            720.0, 760.0, 810.0, 845.0, 790.0, 830.0, 880.0, 905.0, 940.0, 975.0,
            1010.0, 980.0, 960.0, 995.0, 1025.0, 1055.0, 990.0, 970.0, 950.0, 930.0,
            915.0, 900.0, 885.0, 905.0, 925.0, 945.0, 965.0, 985.0, 1005.0, 1030.0,
            1060.0, 1040.0, 1015.0, 990.0, 970.0, 950.0, 930.0, 910.0, 890.0, 870.0,
            850.0, 865.0, 885.0, 905.0, 925.0, 945.0, 965.0, 985.0, 1005.0, 1025.0,
            1045.0, 1020.0, 995.0, 970.0, 950.0, 930.0, 910.0, 935.0, 960.0, 1000.0
        );
        minuteVolumeBaseline.clear();
        minuteVolumeBaseline.put(10 * 60, 800.0);
    }

    private static void resetDeque(Deque<Double> deque, double... values) {
        deque.clear();
        for (double value : values) {
            deque.addLast(value);
        }
    }
}

