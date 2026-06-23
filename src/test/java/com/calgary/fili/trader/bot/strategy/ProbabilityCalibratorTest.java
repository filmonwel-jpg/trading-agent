package com.calgary.fili.trader.bot.strategy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProbabilityCalibratorTest {

    @Test
    void identityCalibratorClipsRawProbability() {
        ProbabilityCalibrator calibrator = ProbabilityCalibrator.identity("longMicroEntryAi");

        assertEquals(0.0, calibrator.apply(-0.25), 1.0e-12);
        assertEquals(0.37, calibrator.apply(0.37), 1.0e-12);
        assertEquals(1.0, calibrator.apply(1.25), 1.0e-12);
        assertEquals(0.0, calibrator.apply(Double.NaN), 1.0e-12);
        assertTrue(calibrator.isIdentity());
    }

    @Test
    void sigmoidCalibratorAppliesStablePlattScaling() {
        ProbabilityCalibrator calibrator = ProbabilityCalibrator.fromPosthocExportRow(Map.of(
            "model", "longMicroEntryAi",
            "filename", "long_micro_entry_5s.onnx",
            "feature_schema_sha256", "abc",
            "holdout_fingerprint_sha256", "def",
            "selected", true,
            "method", "sigmoid",
            "parameters", Map.of("method", "sigmoid", "coef", 2.0, "intercept", -1.0)
        ));

        assertEquals("sigmoid", calibrator.methodName());
        assertEquals(0.5, calibrator.apply(0.5), 1.0e-12);
        assertEquals(0.2689414213699951, calibrator.apply(0.0), 1.0e-12);
        assertEquals(0.7310585786300049, calibrator.apply(1.0), 1.0e-12);
    }

    @Test
    void isotonicCalibratorInterpolatesAndClipsOutOfBounds() {
        ProbabilityCalibrator calibrator = ProbabilityCalibrator.fromPosthocExportRow(Map.of(
            "model", "shortMicroExitGuardAi",
            "selected", true,
            "method", "isotonic",
            "parameters", Map.of(
                "method", "isotonic",
                "x_thresholds", List.of(0.0, 0.5, 1.0),
                "y_thresholds", List.of(0.1, 0.4, 0.9)
            )
        ));

        assertEquals("isotonic", calibrator.methodName());
        assertEquals(0.1, calibrator.apply(-1.0), 1.0e-12);
        assertEquals(0.25, calibrator.apply(0.25), 1.0e-12);
        assertEquals(0.65, calibrator.apply(0.75), 1.0e-12);
        assertEquals(0.9, calibrator.apply(2.0), 1.0e-12);
    }

    @Test
    void invalidExportRowsFailClosed() {
        Map<String, Object> unsupportedMethod = Map.of(
            "model", "longMicroEntryAi",
            "selected", true,
            "method", "unknown",
            "parameters", Map.of()
        );
        Map<String, Object> unsortedIsotonic = Map.of(
            "model", "longMicroEntryAi",
            "selected", true,
            "method", "isotonic",
            "parameters", Map.of(
                "method", "isotonic",
                "x_thresholds", List.of(0.5, 0.25),
                "y_thresholds", List.of(0.4, 0.2)
            )
        );

        assertThrows(IllegalArgumentException.class, () -> ProbabilityCalibrator.fromPosthocExportRow(unsupportedMethod));
        assertThrows(IllegalArgumentException.class, () -> ProbabilityCalibrator.fromPosthocExportRow(unsortedIsotonic));
    }
}
