package com.calgary.fili.trader.bot.strategy;

import ai.onnxruntime.OnnxValue;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AiPredictorProbabilityExtractionTest {

    @Test
    void extractsCatBoostZipMapProbabilityFromLongKeySequence() {
        Map<Long, Float> classProbabilities = new LinkedHashMap<>();
        classProbabilities.put(0L, 0.17f);
        classProbabilities.put(1L, 0.83f);

        Double probability = AiPredictor.extractPositiveClassProbabilityFromRawValue(List.of(classProbabilities));

        assertEquals(0.83, probability, 1.0e-6);
    }

    @Test
    void extractsCatBoostZipMapProbabilityFromNestedScalarValue() {
        Map<String, Object> classProbabilities = new LinkedHashMap<>();
        classProbabilities.put("0", new float[]{0.22f});
        classProbabilities.put("1", new float[]{0.78f});

        Double probability = AiPredictor.extractPositiveClassProbabilityFromRawValue(List.of(classProbabilities));

        assertEquals(0.78, probability, 1.0e-6);
    }

    @Test
    void extractsCatBoostZipMapProbabilityFromOnnxValueInsideSequence() throws Exception {
        Map<Long, Float> classProbabilities = new LinkedHashMap<>();
        classProbabilities.put(0L, 0.12f);
        classProbabilities.put(1L, 0.88f);
        OnnxValue onnxMapLikeValue = mock(OnnxValue.class);
        when(onnxMapLikeValue.getValue()).thenReturn(classProbabilities);

        Double probability = AiPredictor.extractPositiveClassProbabilityFromRawValue(List.of(onnxMapLikeValue));

        assertEquals(0.88, probability, 1.0e-6);
    }

    @Test
    void extractsClassProbabilitiesFromOnnxValueInsideSequence() throws Exception {
        Map<Long, Float> classProbabilities = new LinkedHashMap<>();
        classProbabilities.put(0L, 0.31f);
        classProbabilities.put(1L, 0.69f);
        OnnxValue onnxMapLikeValue = mock(OnnxValue.class);
        when(onnxMapLikeValue.getValue()).thenReturn(classProbabilities);

        Map<Integer, Double> extracted = AiPredictor.extractClassProbabilitiesFromRawValue(List.of(onnxMapLikeValue));

        assertEquals(0.31, extracted.get(0), 1.0e-6);
        assertEquals(0.69, extracted.get(1), 1.0e-6);
    }

    @Test
    void doesNotTreatIntegerLabelTensorAsProbability() {
        assertNull(AiPredictor.extractPositiveClassProbabilityFromRawValue(new long[]{1L}));
    }
}

