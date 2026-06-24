package com.calgary.fili.trader.bot.strategy;

import com.calgary.fili.trader.bot.trader.IBKRTrader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class PingPongStrategyFeatureSchemaTest {

    private static final List<String> EXPECTED_ENHANCED_MAIN_SCHEMA = List.of(
        "f_dist_vwap", "f_bb_lower_dist", "f_bb_upper_dist", "f_macd_diff",
        "f_body_size", "f_lower_wick", "f_upper_wick", "f_atr_norm",
        "f_dist_sma", "f_dist_high", "f_dist_low", "f_rsi", "f_gap_from_prev_close",
        "f_time_of_day", "f_dist_swing_high", "f_dist_swing_low", "f_is_new_high",
        "f_is_new_low", "f_dist_whole_num", "f_is_green", "f_green_streak",
        "f_red_streak", "f_put_call_ratio", "f_vol_ask_ratio", "f_vol_bid_ratio",
        "f_rel_volume_30s", "f_realized_vol_20", "f_realized_vol_z",
        "f_dist_or_high_atr", "f_dist_or_low_atr",
        "f_news_intensity_60s", "f_news_intensity_300s", "f_news_freshness",
        "f_news_provider_breadth", "f_news_confidence", "f_news_sentiment_level",
        "f_news_sentiment_shift", "f_news_sentiment_dispersion", "f_news_coverage",
        "f_news_relevance", "f_news_surprise", "f_news_directional_impulse",
        "f_news_event_earnings", "f_news_event_analyst", "f_news_event_legal",
        "f_news_event_product", "f_news_event_macro", "f_news_model_relevance",
        "f_news_model_impact", "f_news_model_novelty", "f_news_directional_conviction",
        "f_news_alpha_bias_60s", "f_news_alpha_bias_300s", "f_news_alpha_ret_60s_norm",
        "f_news_alpha_ret_300s_norm", "f_news_vol_shock", "f_news_event_strength",
        "tsm_ret_30s_p50", "tsm_ret_120s_p50", "tsm_ret_30s_p10", "tsm_ret_30s_p90",
        "tsm_up_prob_30s", "tsm_vol_forecast_120s", "tsm_uncertainty",
        "regime_trend_prob", "regime_chop_prob", "regime_volatile_prob", "regime_transition_prob",
        "news_event_earnings", "news_event_analyst", "news_event_legal",
        "news_event_earnings_beat_miss", "news_event_analyst_upgrade_downgrade",
        "news_event_legal_regulatory", "news_event_product_capex", "news_event_macro_spillover",
        "news_novelty_score", "news_relevance_score", "news_embedding_cluster",
        "seq_lstm_up_prob_30s", "seq_tcn_up_prob_30s", "seq_transformer_up_prob_30s",
        "seq_patchtst_up_prob_30s", "seq_model_consensus_up_prob_30s",
        "setup_breakout_prob", "setup_pullback_continuation_prob", "setup_reversal_prob",
        "setup_trend_exhaustion_prob", "setup_failed_breakout_prob",
        "f_regime_prob_choppy", "f_regime_prob_trend", "f_regime_prob_volatile", "f_regime_prob_entropy"
    );

    private static final List<String> EXPECTED_ENHANCED_REGIME_SCHEMA = List.of(
        "f_dist_vwap", "f_bb_lower_dist", "f_bb_upper_dist", "f_body_size", "f_lower_wick", "f_upper_wick",
        "f_dist_high", "f_dist_low", "f_gap_from_prev_close", "f_time_of_day", "f_dist_swing_high",
        "f_dist_swing_low", "f_is_new_high", "f_is_new_low", "f_dist_whole_num", "f_is_green",
        "f_green_streak", "f_red_streak", "f_put_call_ratio", "f_vol_ask_ratio", "f_vol_bid_ratio",
        "f_rel_volume_30s", "f_dist_or_high_atr", "f_dist_or_low_atr",
        "f_news_intensity_60s", "f_news_intensity_300s", "f_news_freshness", "f_news_provider_breadth",
        "f_news_confidence", "f_news_sentiment_level", "f_news_sentiment_shift", "f_news_sentiment_dispersion",
        "f_news_coverage", "f_news_relevance", "f_news_surprise", "f_news_directional_impulse",
        "f_news_event_earnings", "f_news_event_analyst", "f_news_event_legal", "f_news_event_product",
        "f_news_event_macro", "f_news_model_relevance", "f_news_model_impact", "f_news_model_novelty",
        "f_news_directional_conviction", "f_news_alpha_bias_60s", "f_news_alpha_bias_300s",
        "f_news_alpha_ret_60s_norm", "f_news_alpha_ret_300s_norm", "f_news_vol_shock", "f_news_event_strength",
        "tsm_ret_30s_p50", "tsm_ret_120s_p50", "tsm_ret_30s_p10", "tsm_ret_30s_p90", "tsm_up_prob_30s",
        "tsm_vol_forecast_120s", "tsm_uncertainty", "regime_trend_prob", "regime_chop_prob",
        "regime_volatile_prob", "regime_transition_prob", "news_event_earnings", "news_event_analyst",
        "news_event_legal", "news_event_earnings_beat_miss", "news_event_analyst_upgrade_downgrade",
        "news_event_legal_regulatory", "news_event_product_capex", "news_event_macro_spillover",
        "news_novelty_score", "news_relevance_score", "news_embedding_cluster", "seq_lstm_up_prob_30s",
        "seq_tcn_up_prob_30s", "seq_transformer_up_prob_30s", "seq_patchtst_up_prob_30s",
        "seq_model_consensus_up_prob_30s", "setup_breakout_prob", "setup_pullback_continuation_prob",
        "setup_reversal_prob", "setup_trend_exhaustion_prob", "setup_failed_breakout_prob"
    );
    private static final List<String> EXPECTED_BASE_PLUS_REGIME_PROBABILITY_SCHEMA = basePlusRegimeProbabilitySchema();
    private static final List<String> EXPECTED_BASE_REGIME_SCHEMA = EXPECTED_ENHANCED_REGIME_SCHEMA.subList(0, 24);

    @Test
    void buildFeatureVectorForExpectedCountMatchesSupportedLiveSchemas() {
        PingPongStrategy strategy = newStrategy();
        try {
            seedFeatureState(strategy);
            @SuppressWarnings("unchecked")
            Map<String, Float> featureValues = ReflectionTestUtils.invokeMethod(strategy, "constructFeatureValueMap", 55.0);

            float[] legacyFeatures = ReflectionTestUtils.invokeMethod(strategy, "buildFeatureVectorForExpectedCount", 25, featureValues);
            float[] baseFeatures = ReflectionTestUtils.invokeMethod(strategy, "buildFeatureVectorForExpectedCount", 30, featureValues);
            float[] extendedFeatures = ReflectionTestUtils.invokeMethod(strategy, "buildFeatureVectorForExpectedCount", 34, featureValues);
            float[] mainFeaturesWithNewsAndRegime = ReflectionTestUtils.invokeMethod(strategy, "buildFeatureVectorForExpectedCount", 61, featureValues);

            assertEquals(25, legacyFeatures.length);
            assertEquals(30, baseFeatures.length);
            assertEquals(34, extendedFeatures.length);
            assertEquals(61, mainFeaturesWithNewsAndRegime.length);
            assertArrayEquals(legacyFeatures, Arrays.copyOf(baseFeatures, 25), 1.0e-6f);
            assertArrayEquals(baseFeatures, Arrays.copyOf(extendedFeatures, 30), 1.0e-6f);
            assertArrayEquals(baseFeatures, Arrays.copyOf(mainFeaturesWithNewsAndRegime, 30), 1.0e-6f);
            assertEquals(0.0f, mainFeaturesWithNewsAndRegime[57], 1.0e-6f);
            assertEquals(0.0f, mainFeaturesWithNewsAndRegime[58], 1.0e-6f);
            assertEquals(0.0f, mainFeaturesWithNewsAndRegime[59], 1.0e-6f);
            assertEquals(0.0f, mainFeaturesWithNewsAndRegime[60], 1.0e-6f);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void buildRegimeFeatureVectorUsesTrainingSubsetForEachSchema() {
        PingPongStrategy strategy = newStrategy();
        try {
            seedFeatureState(strategy);
            @SuppressWarnings("unchecked")
            Map<String, Float> featureValues = ReflectionTestUtils.invokeMethod(strategy, "constructFeatureValueMap", 55.0);

            float[] legacyRegime = ReflectionTestUtils.invokeMethod(strategy, "buildRegimeClassifierFeaturesForExpectedCount", 25, featureValues);
            float[] baseRegime = ReflectionTestUtils.invokeMethod(strategy, "buildRegimeClassifierFeaturesForExpectedCount", 30, featureValues);
            float[] extendedRegime = ReflectionTestUtils.invokeMethod(strategy, "buildRegimeClassifierFeaturesForExpectedCount", 34, featureValues);
            float[] newsAwareRegime = ReflectionTestUtils.invokeMethod(strategy, "buildRegimeClassifierFeaturesForExpectedCount", 51, featureValues);
            float[] extendedFeatures = ReflectionTestUtils.invokeMethod(strategy, "buildFeatureVectorForExpectedCount", 34, featureValues);

            assertEquals(21, legacyRegime.length);
            assertEquals(24, baseRegime.length);
            assertEquals(27, extendedRegime.length);
            assertEquals(51, newsAwareRegime.length);
            assertEquals(extendedFeatures[30], extendedRegime[24], 1.0e-6f);
            assertEquals(extendedFeatures[32], extendedRegime[25], 1.0e-6f);
            assertEquals(extendedFeatures[33], extendedRegime[26], 1.0e-6f);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void setupManifestColumnsOverrideAmbiguousThirtyFourFeatureEntrySchema() {
        PingPongStrategy strategy = newStrategy();
        try {
            seedFeatureState(strategy);
            @SuppressWarnings("unchecked")
            Map<String, Float> featureValues = ReflectionTestUtils.invokeMethod(strategy, "constructFeatureValueMap", 55.0);
            featureValues.put("f_regime_prob_choppy", 0.20f);
            featureValues.put("f_regime_prob_trend", 0.30f);
            featureValues.put("f_regime_prob_volatile", 0.40f);
            featureValues.put("f_regime_prob_entropy", 0.90f);
            ReflectionTestUtils.setField(strategy, "setupFeatureColumns", EXPECTED_BASE_PLUS_REGIME_PROBABILITY_SCHEMA);

            float[] setupFeatures = ReflectionTestUtils.invokeMethod(strategy, "buildSetupFeatureVectorForExpectedCount", 34, featureValues);
            float[] countBasedFeatures = ReflectionTestUtils.invokeMethod(strategy, "buildFeatureVectorForExpectedCount", 34, featureValues);

            assertEquals(34, setupFeatures.length);
            assertEquals(34, countBasedFeatures.length);
            assertEquals(0.20f, setupFeatures[30], 1.0e-6f);
            assertEquals(0.30f, setupFeatures[31], 1.0e-6f);
            assertEquals(0.40f, setupFeatures[32], 1.0e-6f);
            assertEquals(0.90f, setupFeatures[33], 1.0e-6f);
            assertEquals(featureValues.get("f_spread_pct"), countBasedFeatures[30], 1.0e-6f);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void setupManifestRegimeColumnsOverrideAmbiguousTwentyFourFeatureRegimeSchema() {
        PingPongStrategy strategy = newStrategy();
        try {
            Map<String, Float> orderedValues = indexedFeatureValues(EXPECTED_ENHANCED_MAIN_SCHEMA);
            ReflectionTestUtils.setField(strategy, "setupRegimeFeatureColumns", EXPECTED_BASE_REGIME_SCHEMA);

            float[] regimeFeatures = ReflectionTestUtils.invokeMethod(strategy, "buildRegimeClassifierFeaturesForExpectedCount", 24, orderedValues);

            assertEquals(24, regimeFeatures.length);
            assertArrayEquals(expectedVector(EXPECTED_BASE_REGIME_SCHEMA, orderedValues), regimeFeatures, 1.0e-6f);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void enhancedMainSchemaUsesExactTrainingColumnOrder() {
        PingPongStrategy strategy = newStrategy();
        try {
            Map<String, Float> orderedValues = indexedFeatureValues(EXPECTED_ENHANCED_MAIN_SCHEMA);
            float[] enhancedFeatures = ReflectionTestUtils.invokeMethod(strategy, "buildFeatureVectorForExpectedCount", 93, orderedValues);

            assertEquals(93, enhancedFeatures.length);
            assertArrayEquals(expectedVector(EXPECTED_ENHANCED_MAIN_SCHEMA, orderedValues), enhancedFeatures, 1.0e-6f);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void enhancedRegimeSchemaUsesExactTrainingSubsetOrder() {
        PingPongStrategy strategy = newStrategy();
        try {
            Map<String, Float> orderedValues = indexedFeatureValues(EXPECTED_ENHANCED_MAIN_SCHEMA);
            float[] enhancedRegimeFeatures = ReflectionTestUtils.invokeMethod(strategy, "buildRegimeClassifierFeaturesForExpectedCount", 83, orderedValues);

            assertEquals(83, enhancedRegimeFeatures.length);
            assertArrayEquals(expectedVector(EXPECTED_ENHANCED_REGIME_SCHEMA, orderedValues), enhancedRegimeFeatures, 1.0e-6f);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void constructFeatureValueMapPopulatesEnhancedMetaProducerColumns() {
        PingPongStrategy strategy = newStrategy();
        try {
            seedFeatureState(strategy);
            @SuppressWarnings("unchecked")
            Map<String, Float> featureValues = ReflectionTestUtils.invokeMethod(strategy, "constructFeatureValueMap", 55.0);

            assertEquals(32, EnhancedLiveFeatureProducer.META_PRODUCER_FEATURE_COLUMNS.size());
            for (String column : EnhancedLiveFeatureProducer.META_PRODUCER_FEATURE_COLUMNS) {
                assertTrue(featureValues.containsKey(column), "missing enhanced meta column: " + column);
            }

            float trend = featureValues.get("regime_trend_prob");
            float chop = featureValues.get("regime_chop_prob");
            float volatileProb = featureValues.get("regime_volatile_prob");
            assertEquals(1.0f, trend + chop + volatileProb, 1.0e-4f);
            assertTrue(featureValues.get("tsm_ret_30s_p10") <= featureValues.get("tsm_ret_30s_p50"));
            assertTrue(featureValues.get("tsm_ret_30s_p50") <= featureValues.get("tsm_ret_30s_p90"));
            assertEquals(
                (featureValues.get("seq_lstm_up_prob_30s")
                    + featureValues.get("seq_tcn_up_prob_30s")
                    + featureValues.get("seq_transformer_up_prob_30s")
                    + featureValues.get("seq_patchtst_up_prob_30s")) / 4.0f,
                featureValues.get("seq_model_consensus_up_prob_30s"),
                1.0e-6f
            );
            assertEquals(2.0f, featureValues.get("news_embedding_cluster"), 1.0e-6f);
            assertTrue(featureValues.get("setup_breakout_prob") >= 0.0f && featureValues.get("setup_breakout_prob") <= 1.0f);
            assertTrue(featureValues.get("setup_failed_breakout_prob") >= 0.0f && featureValues.get("setup_failed_breakout_prob") <= 1.0f);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void lifecycleMicroProbabilityFeaturesUseStoredArmAndEntryProbabilities() {
        PingPongStrategy strategy = newStrategy();
        try {
            Map<String, Float> contextFeatures = new LinkedHashMap<>();
            contextFeatures.put("f_30s_body_pct", 0.25f);

            ReflectionTestUtils.invokeMethod(strategy, "armMicroEntry", "long", contextFeatures, 1_700_000_030L, 0.73d, 0.62d);
            assertEquals(0.73d, (Double) ReflectionTestUtils.getField(strategy, "armedSetupProbability"), 1.0e-9);
            assertEquals(0.62d, (Double) ReflectionTestUtils.getField(strategy, "armedSetupThreshold"), 1.0e-9);
            assertEquals(0.11d, (Double) ReflectionTestUtils.getField(strategy, "armedSetupThresholdMargin"), 1.0e-9);

            ReflectionTestUtils.invokeMethod(strategy, "clearMicroEntryArms", "test-clear");
            assertEquals(0.0d, (Double) ReflectionTestUtils.getField(strategy, "armedSetupProbability"), 1.0e-9);
            assertEquals(0.0d, (Double) ReflectionTestUtils.getField(strategy, "armedSetupThreshold"), 1.0e-9);
            assertEquals(0.0d, (Double) ReflectionTestUtils.getField(strategy, "armedSetupThresholdMargin"), 1.0e-9);

            ReflectionTestUtils.setField(strategy, "positionEntryPrice", 100.0);
            ReflectionTestUtils.setField(strategy, "positionEntrySide", 1);
            ReflectionTestUtils.setField(strategy, "positionEntryProbability", 0.61d);
            ReflectionTestUtils.setField(strategy, "positionEntryThreshold", 0.55d);
            ReflectionTestUtils.setField(strategy, "positionEntryThresholdMargin", 0.06d);

            @SuppressWarnings("unchecked")
            Map<String, Float> positionFeatures = ReflectionTestUtils.invokeMethod(strategy, "positionFeatureValues", 101.0, true);

            assertEquals(0.61f, positionFeatures.get("f_entry_score_proxy"), 1.0e-6f);
            assertEquals(0.61f, positionFeatures.get("f_entry_prob"), 1.0e-6f);
            assertEquals(0.55f, positionFeatures.get("f_entry_threshold"), 1.0e-6f);
            assertEquals(0.06f, positionFeatures.get("f_entry_threshold_margin"), 1.0e-6f);
        } finally {
            strategy.stop();
        }
    }

    @Test
    void orderProgressTransfersPendingEntryProbabilityIntoLifecyclePositionFeatures() {
        PingPongStrategy strategy = newStrategy();
        try {
            ReflectionTestUtils.setField(strategy, "pendingEntryProbability", 0.82d);
            ReflectionTestUtils.setField(strategy, "pendingEntryThreshold", 0.64d);
            ReflectionTestUtils.setField(strategy, "pendingEntryThresholdMargin", 0.18d);

            ReflectionTestUtils.invokeMethod(strategy, "handleOrderProgress", 101, "BUY", 10, 0, 100.0d);

            assertEquals(0.82d, (Double) ReflectionTestUtils.getField(strategy, "positionEntryProbability"), 1.0e-9);
            assertEquals(0.64d, (Double) ReflectionTestUtils.getField(strategy, "positionEntryThreshold"), 1.0e-9);
            assertEquals(0.18d, (Double) ReflectionTestUtils.getField(strategy, "positionEntryThresholdMargin"), 1.0e-9);
            assertEquals(0.0d, (Double) ReflectionTestUtils.getField(strategy, "pendingEntryProbability"), 1.0e-9);
            assertEquals(0.0d, (Double) ReflectionTestUtils.getField(strategy, "pendingEntryThreshold"), 1.0e-9);
            assertEquals(0.0d, (Double) ReflectionTestUtils.getField(strategy, "pendingEntryThresholdMargin"), 1.0e-9);

            @SuppressWarnings("unchecked")
            Map<String, Float> positionFeatures = ReflectionTestUtils.invokeMethod(strategy, "positionFeatureValues", 101.0, true);

            assertEquals(0.82f, positionFeatures.get("f_entry_score_proxy"), 1.0e-6f);
            assertEquals(0.82f, positionFeatures.get("f_entry_prob"), 1.0e-6f);
            assertEquals(0.64f, positionFeatures.get("f_entry_threshold"), 1.0e-6f);
            assertEquals(0.18f, positionFeatures.get("f_entry_threshold_margin"), 1.0e-6f);
        } finally {
            strategy.stop();
        }
    }

    private static PingPongStrategy newStrategy() {
        return new PingPongStrategy(
            mock(IBKRTrader.class),
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

    private static Map<String, Float> indexedFeatureValues(List<String> orderedColumns) {
        Map<String, Float> values = new LinkedHashMap<>();
        List<String> distinctColumns = new ArrayList<>();
        for (String column : orderedColumns) {
            if (!distinctColumns.contains(column)) {
                distinctColumns.add(column);
            }
        }
        for (int i = 0; i < distinctColumns.size(); i++) {
            values.put(distinctColumns.get(i), i + 1.0f);
        }
        return values;
    }

    private static List<String> basePlusRegimeProbabilitySchema() {
        List<String> columns = new ArrayList<>(EXPECTED_ENHANCED_MAIN_SCHEMA.subList(0, 30));
        columns.addAll(EXPECTED_ENHANCED_MAIN_SCHEMA.subList(EXPECTED_ENHANCED_MAIN_SCHEMA.size() - 4, EXPECTED_ENHANCED_MAIN_SCHEMA.size()));
        return List.copyOf(columns);
    }

    private static float[] expectedVector(List<String> schema, Map<String, Float> values) {
        float[] out = new float[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            out[i] = values.get(schema.get(i));
        }
        return out;
    }

    private static void resetDeque(Deque<Double> deque, double... values) {
        deque.clear();
        for (double value : values) {
            deque.addLast(value);
        }
    }
}

