package com.calgary.fili.trader.bot.strategy;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.TensorInfo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AiPredictor {
    private static final Logger log = LoggerFactory.getLogger(AiPredictor.class);
    private static final int DEFAULT_EXPECTED_FEATURES = 25;
    private static final boolean SETUP_PROBABILITY_GUARD_ENABLED = Boolean.parseBoolean(System.getProperty("strategy.ai.setupProbabilityGuardEnabled", "true"));
    private static final boolean SETUP_BINARY_PROBABILITY_FAIL_CLOSED = Boolean.parseBoolean(System.getProperty("strategy.ai.setupBinaryProbabilityFailClosed", "true"));
    private static final int SETUP_BINARY_PROBABILITY_MIN_SAMPLES = Math.max(1, Integer.getInteger("strategy.ai.setupBinaryProbabilityMinSamples", 50));
    private static final double BINARY_PROBABILITY_EPSILON = Double.parseDouble(System.getProperty("strategy.ai.binaryProbabilityEpsilon", "1.0e-9"));
    private static final Set<String> CALIBRATED_SETUP_MODEL_NAMES = Set.of(
        "long_entry.onnx",
        "short_entry.onnx",
        "open30_long_entry.onnx",
        "open30_short_entry.onnx",
        "choppy_long_entry.onnx",
        "choppy_short_entry.onnx",
        "trend_long_entry.onnx",
        "trend_short_entry.onnx",
        "volatile_long_entry.onnx",
        "volatile_short_entry.onnx"
    );

    private final OrtEnvironment env;
    private final OrtSession session;
    private final String modelFileName;
    private final String inputName;
    private final int expectedFeatureCount;
    private final String modelSource;
    private final boolean calibratedSetupRoute;
    private long setupProbabilitySampleCount = 0L;
    private long setupBinaryProbabilitySampleCount = 0L;
    private long setupNonBinaryProbabilitySampleCount = 0L;
    private boolean setupBinaryProbabilityGuardTripped = false;

    public record PredictionOutcome(boolean predictedPositive, double positiveProbability) {}
    public record ClassPredictionOutcome(int classLabel, double confidence) {}
    public record MultiClassPredictionOutcome(int classLabel, double confidence, Map<Integer, Double> classProbabilities) {}

    public AiPredictor(String modelFileName) throws Exception {
        this(modelFileName, null);
    }

    public AiPredictor(String modelFileName, String modelDir) throws Exception {
        this.modelFileName = modelFileName;
        this.calibratedSetupRoute = isCalibratedSetupRouteName(modelFileName);
        flowInfo("AI.INIT", "Booting ONNX Runtime Environment model=" + modelFileName);
        this.env = OrtEnvironment.getEnvironment();

        ModelBytes modelBytes = resolveModelBytes(modelFileName, modelDir);
        this.modelSource = modelBytes.sourceDescription();
        this.session = env.createSession(modelBytes.bytes(), new OrtSession.SessionOptions());

        Map<String, NodeInfo> inputs = session.getInputInfo();
        if (inputs.isEmpty()) {
            throw new IllegalStateException("ONNX model has no inputs.");
        }

        this.inputName = inputs.keySet().iterator().next();
        NodeInfo nodeInfo = inputs.get(inputName);
        int detectedFeatures = -1;

        if (nodeInfo != null && nodeInfo.getInfo() instanceof TensorInfo tensorInfo) {
            long[] shape = tensorInfo.getShape();
            if (shape != null && shape.length >= 2 && shape[1] > 0) {
                detectedFeatures = (int) shape[1];
            }
        }

        this.expectedFeatureCount = detectedFeatures > 0 ? detectedFeatures : DEFAULT_EXPECTED_FEATURES;
        flowInfo("AI.INIT", "Successfully loaded model=" + modelFileName + " source=" + modelSource);
        flowData("AI.INIT", "model=" + modelFileName + " input=" + inputName + " expectedFeatures=" + expectedFeatureCount + " source=" + modelSource);
        flowData("AI.INIT", "model=" + modelFileName + " outputs=" + session.getOutputNames() + " calibratedSetupRoute=" + calibratedSetupRoute);
        if (expectedFeatureCount == 24
            || expectedFeatureCount == 25
            || expectedFeatureCount == 30
            || expectedFeatureCount == 31
            || expectedFeatureCount == 34
            || expectedFeatureCount == 41
            || expectedFeatureCount == 44
            || expectedFeatureCount == 50
            || expectedFeatureCount == 51
            || expectedFeatureCount == 57
            || expectedFeatureCount == 61
            || expectedFeatureCount == 65
            || expectedFeatureCount == 83
            || expectedFeatureCount == 93) {
            flowCondition("AI.INIT", "FEATURE_COUNT_SUPPORTED", true, "model=" + modelFileName + " expected=" + expectedFeatureCount);
        } else {
            flowCondition("AI.INIT", "FEATURE_COUNT_SUPPORTED", false, "model=" + modelFileName + " expected=" + expectedFeatureCount + " note=will trim/pad from strategy vector");
        }
    }

    private record ModelBytes(byte[] bytes, String sourceDescription) {}

    private ModelBytes resolveModelBytes(String modelFileName, String modelDir) throws IOException {
        String normalizedDir = modelDir == null ? "" : modelDir.trim();
        if (!normalizedDir.isEmpty()) {
            Path candidate = Paths.get(normalizedDir).resolve(modelFileName).normalize();
            if (Files.exists(candidate) && Files.isRegularFile(candidate)) {
                flowInfo("AI.INIT", "Loading model from filesystem path=" + candidate);
                return new ModelBytes(Files.readAllBytes(candidate), candidate.toAbsolutePath().toString());
            }
            flowInfo("AI.INIT", "Filesystem model not found; falling back to classpath model=" + modelFileName + " dir=" + normalizedDir);
        }

        try (InputStream is = getClass().getClassLoader().getResourceAsStream(modelFileName)) {
            if (is == null) {
                throw new RuntimeException("Could not find " + modelFileName + " in resources or modelDir=" + normalizedDir + "!");
            }
            return new ModelBytes(is.readAllBytes(), "classpath:" + modelFileName);
        }
    }

    public boolean predict(float[] features) {
        return predict(features, 0.50);
    }

    public boolean predict(float[] features, double threshold) {
        PredictionOutcome outcome = predictOutcome(features);
        boolean decision = outcome.positiveProbability() >= threshold;
        flowCondition(
            "AI.RESPONSE",
            "PREDICTION_POSITIVE",
            decision,
            "model=" + modelFileName
                + " prob=" + String.format("%.4f", outcome.positiveProbability())
                + " threshold=" + String.format("%.4f", threshold)
        );
        return decision;
    }

    public double predictProbability(float[] features) {
        return predictOutcome(features).positiveProbability();
    }

    public int predictClassLabel(float[] features, int fallbackLabel) {
        boolean validFeatures = features != null && features.length > 0;
        flowCondition("AI.INPUT", "FEATURE_VECTOR_PRESENT", validFeatures, "model=" + modelFileName + " featureCount=" + (features == null ? 0 : features.length));
        if (!validFeatures) {
            flowError("AI.INPUT", "Features cannot be null or empty model=" + modelFileName + " usingFallbackLabel=" + fallbackLabel);
            return fallbackLabel;
        }

        try {
            float[] normalizedFeatures = normalizeFeatures(features);
            float[][] inputMatrix = new float[][]{normalizedFeatures};
            try (OnnxTensor tensor = OnnxTensor.createTensor(env, inputMatrix);
                 OrtSession.Result result = session.run(Collections.singletonMap(inputName, tensor))) {
                long label = extractLabel(result);
                if (label >= 0) {
                    flowData("AI.RESPONSE", "model=" + modelFileName + " classLabel=" + label);
                    return (int) label;
                }
                flowError("AI.RESPONSE", "Missing class label model=" + modelFileName + " usingFallbackLabel=" + fallbackLabel);
            }
        } catch (Exception e) {
            flowError("AI.RESPONSE", "Class prediction failed model=" + modelFileName + " reason=" + e.getMessage() + " usingFallbackLabel=" + fallbackLabel);
        }

        return fallbackLabel;
    }

    public ClassPredictionOutcome predictClassWithConfidence(float[] features, int fallbackLabel) {
        MultiClassPredictionOutcome outcome = predictMultiClassOutcome(features, fallbackLabel);
        return new ClassPredictionOutcome(outcome.classLabel(), outcome.confidence());
    }

    public MultiClassPredictionOutcome predictMultiClassOutcome(float[] features, int fallbackLabel) {
        boolean validFeatures = features != null && features.length > 0;
        flowCondition("AI.INPUT", "FEATURE_VECTOR_PRESENT", validFeatures, "model=" + modelFileName + " featureCount=" + (features == null ? 0 : features.length));
        if (!validFeatures) {
            flowError("AI.INPUT", "Features cannot be null or empty model=" + modelFileName + " usingFallbackLabel=" + fallbackLabel);
            return new MultiClassPredictionOutcome(fallbackLabel, 0.0, Collections.emptyMap());
        }

        try {
            float[] normalizedFeatures = normalizeFeatures(features);
            float[][] inputMatrix = new float[][]{normalizedFeatures};
            try (OnnxTensor tensor = OnnxTensor.createTensor(env, inputMatrix);
                 OrtSession.Result result = session.run(Collections.singletonMap(inputName, tensor))) {
                long label = extractLabel(result);
                Map<Integer, Double> classProbabilities = extractClassProbabilities(result);

                int predictedLabel = label >= 0 ? (int) label : fallbackLabel;
                double confidence = 0.0;

                if (!classProbabilities.isEmpty()) {
                    if (classProbabilities.containsKey(predictedLabel)) {
                        confidence = clampProbability(classProbabilities.get(predictedLabel));
                    } else {
                        Map.Entry<Integer, Double> best = classProbabilities.entrySet().stream()
                            .max(Map.Entry.comparingByValue())
                            .orElse(null);
                        if (best != null) {
                            predictedLabel = best.getKey();
                            confidence = clampProbability(best.getValue());
                        }
                    }
                } else if (label >= 0) {
                    // If model does not expose probabilities, treat the emitted label as confident.
                    confidence = 1.0;
                }

                flowData(
                    "AI.RESPONSE",
                    "model=" + modelFileName
                        + " classLabel=" + predictedLabel
                        + " confidence=" + String.format("%.4f", confidence)
                );
                return new MultiClassPredictionOutcome(predictedLabel, confidence, classProbabilities);
            }
        } catch (Exception e) {
            flowError("AI.RESPONSE", "Class prediction failed model=" + modelFileName + " reason=" + e.getMessage() + " usingFallbackLabel=" + fallbackLabel);
            return new MultiClassPredictionOutcome(fallbackLabel, 0.0, Collections.emptyMap());
        }
    }

    public PredictionOutcome predictOutcome(float[] features) {
        boolean validFeatures = features != null && features.length > 0;
        flowCondition("AI.INPUT", "FEATURE_VECTOR_PRESENT", validFeatures, "model=" + modelFileName + " featureCount=" + (features == null ? 0 : features.length));
        if (!validFeatures) {
            flowError("AI.INPUT", "Features cannot be null or empty model=" + modelFileName);
            return new PredictionOutcome(false, 0.0);
        }

        try {
            float[] normalizedFeatures = normalizeFeatures(features);
            float[][] inputMatrix = new float[][]{normalizedFeatures};
            try (OnnxTensor tensor = OnnxTensor.createTensor(env, inputMatrix)) {
                flowAnalyze("AI.REQUEST", "model=" + modelFileName + " running prediction featureCount=" + normalizedFeatures.length);
                flowData("AI.REQUEST", "model=" + modelFileName + " features=" + Arrays.toString(normalizedFeatures));
                try (OrtSession.Result result = session.run(Collections.singletonMap(inputName, tensor))) {
                    long label = extractLabel(result);
                    Double prob = extractPositiveClassProbability(result);

                    // Fallback for models that only emit labels. Calibrated setup routes must not silently
                    // degrade to label-derived 0/1 probabilities because setup thresholds and downstream
                    // micro features require a real probability contract.
                    double positiveProb;
                    boolean probabilityExtracted = prob != null;
                    if (prob != null) {
                        positiveProb = clampProbability(prob);
                    } else if (calibratedSetupRoute && SETUP_PROBABILITY_GUARD_ENABLED) {
                        flowCondition(
                            "AI.RESPONSE",
                            "SETUP_PROBABILITY_OUTPUT_EXTRACTED",
                            false,
                            "model=" + modelFileName + " label=" + label + " action=fail_closed"
                        );
                        positiveProb = 0.0;
                    } else if (label >= 0) {
                        positiveProb = (label == 1L) ? 1.0 : 0.0;
                    } else {
                        positiveProb = 0.0;
                    }
                    positiveProb = applySetupProbabilityGuard(positiveProb, probabilityExtracted);

                    boolean decision = positiveProb >= 0.50;
                    flowCondition("AI.RESPONSE", "PREDICTION_LABEL_AVAILABLE", label >= 0, "model=" + modelFileName + " label=" + label);
                    flowData("AI.RESPONSE", "model=" + modelFileName + " positiveProb=" + String.format("%.4f", positiveProb));
                    return new PredictionOutcome(decision, positiveProb);
                }
            }
        } catch (Exception e) {
            flowError("AI.RESPONSE", "Prediction failed model=" + modelFileName + " reason=" + e.getMessage());
            return new PredictionOutcome(false, 0.0);
        }
    }

    public boolean shouldBuyDip(float[] features) {
        return predict(features);
    }

    public int getExpectedFeatureCount() {
        return expectedFeatureCount;
    }

    private long extractLabel(OrtSession.Result result) {
        long namedLabel = extractLabel(result, true);
        return namedLabel >= 0 ? namedLabel : extractLabel(result, false);
    }

    private long extractLabel(OrtSession.Result result, boolean labelOutputsOnly) {
        int idx = 0;
        for (Map.Entry<String, OnnxValue> entry : result) {
            String outputName = entry.getKey();
            if (labelOutputsOnly && !looksLikeLabelOutput(outputName)) {
                idx++;
                continue;
            }
            if (!labelOutputsOnly && looksLikeProbabilityOutput(outputName)) {
                idx++;
                continue;
            }
            Object value = readOnnxValue("label", outputName, idx, entry.getValue());
            long label = extractLabelFromValue(value);
            if (label >= 0) {
                return label;
            }
            idx++;
        }
        return -1;
    }

    private Double extractPositiveClassProbability(OrtSession.Result result) {
        Double namedProbability = extractPositiveClassProbability(result, true);
        return namedProbability != null ? namedProbability : extractPositiveClassProbability(result, false);
    }

    private Double extractPositiveClassProbability(OrtSession.Result result, boolean probabilityOutputsOnly) {
        int idx = 0;
        for (Map.Entry<String, OnnxValue> entry : result) {
            String outputName = entry.getKey();
            if (probabilityOutputsOnly && !looksLikeProbabilityOutput(outputName)) {
                idx++;
                continue;
            }
            if (!probabilityOutputsOnly && looksLikeLabelOutput(outputName)) {
                idx++;
                continue;
            }
            Object value = readOnnxValue("probability", outputName, idx, entry.getValue());
            Double extracted = extractProbabilityFromValue(value);
            if (extracted != null) {
                return extracted;
            }
            idx++;
        }
        return null;
    }

    private Map<Integer, Double> extractClassProbabilities(OrtSession.Result result) {
        Map<Integer, Double> namedProbabilities = extractClassProbabilities(result, true);
        return namedProbabilities.isEmpty() ? extractClassProbabilities(result, false) : namedProbabilities;
    }

    private Map<Integer, Double> extractClassProbabilities(OrtSession.Result result, boolean probabilityOutputsOnly) {
        int idx = 0;
        for (Map.Entry<String, OnnxValue> entry : result) {
            String outputName = entry.getKey();
            if (probabilityOutputsOnly && !looksLikeProbabilityOutput(outputName)) {
                idx++;
                continue;
            }
            if (!probabilityOutputsOnly && looksLikeLabelOutput(outputName)) {
                idx++;
                continue;
            }
            Object value = readOnnxValue("class probabilities", outputName, idx, entry.getValue());
            Map<Integer, Double> extracted = extractClassProbabilitiesFromValue(value);
            if (!extracted.isEmpty()) {
                return extracted;
            }
            idx++;
        }
        return Collections.emptyMap();
    }

    private Object readOnnxValue(String outputRole, String outputName, int idx, OnnxValue onnxValue) {
        try {
            return onnxValue == null ? null : onnxValue.getValue();
        } catch (Exception e) {
            flowError("AI.RESPONSE", "Failed reading ONNX " + outputRole + " output model=" + modelFileName + " output=" + outputName + " idx=" + idx + " reason=" + e.getMessage());
            return null;
        }
    }

    private long extractLabelFromValue(Object value) {
        if (value instanceof long[] labels && labels.length > 0) {
            return labels[0];
        }
        if (value instanceof Long[] labels && labels.length > 0 && labels[0] != null) {
            return labels[0];
        }
        if (value instanceof int[] labels && labels.length > 0) {
            return labels[0];
        }
        if (value instanceof Integer[] labels && labels.length > 0 && labels[0] != null) {
            return labels[0];
        }
        if (value instanceof long[][] labels && labels.length > 0 && labels[0].length > 0) {
            return labels[0][0];
        }
        if (value instanceof int[][] labels && labels.length > 0 && labels[0].length > 0) {
            return labels[0][0];
        }
        return -1;
    }

    private static Map<Integer, Double> extractClassProbabilitiesFromValue(Object value) {
        if (value == null) {
            return Collections.emptyMap();
        }

        if (value instanceof OnnxValue onnxValue) {
            Object unwrapped = readRawOnnxValue(onnxValue);
            return unwrapped == null || unwrapped == value ? Collections.emptyMap() : extractClassProbabilitiesFromValue(unwrapped);
        }

        if (value instanceof float[][] probs2d && probs2d.length > 0 && probs2d[0].length > 0) {
            return indexedProbabilities(probs2d[0]);
        }
        if (value instanceof double[][] probs2d && probs2d.length > 0 && probs2d[0].length > 0) {
            return indexedProbabilities(probs2d[0]);
        }
        if (value instanceof float[] probs1d && probs1d.length > 0) {
            return indexedProbabilities(probs1d);
        }
        if (value instanceof double[] probs1d && probs1d.length > 0) {
            return indexedProbabilities(probs1d);
        }

        if (value instanceof List<?> list && !list.isEmpty()) {
            Object first = list.get(0);
            if (first instanceof Map<?, ?> map) {
                return extractClassProbabilitiesFromMap(map);
            }
            return extractClassProbabilitiesFromValue(first);
        }

        if (value instanceof Object[] array && array.length > 0) {
            if (array[0] instanceof Map<?, ?> map) {
                return extractClassProbabilitiesFromMap(map);
            }
            return extractClassProbabilitiesFromValue(array[0]);
        }

        if (value instanceof Map<?, ?> map) {
            return extractClassProbabilitiesFromMap(map);
        }

        return Collections.emptyMap();
    }

    private static Map<Integer, Double> indexedProbabilities(float[] probs) {
        Map<Integer, Double> out = new HashMap<>();
        for (int i = 0; i < probs.length; i++) {
            out.put(i, clampProbability(probs[i]));
        }
        return out;
    }

    private static Map<Integer, Double> indexedProbabilities(double[] probs) {
        Map<Integer, Double> out = new HashMap<>();
        for (int i = 0; i < probs.length; i++) {
            out.put(i, clampProbability(probs[i]));
        }
        return out;
    }

    private static Map<Integer, Double> extractClassProbabilitiesFromMap(Map<?, ?> map) {
        Map<Integer, Double> out = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Integer key = parseClassKey(entry.getKey());
            Double value = extractScalarProbability(entry.getValue());
            if (key == null || value == null) {
                continue;
            }
            out.put(key, clampProbability(value));
        }
        return out;
    }

    private static Integer parseClassKey(Object rawKey) {
        if (rawKey instanceof Number n) {
            return n.intValue();
        }
        if (rawKey != null) {
            try {
                return Integer.parseInt(String.valueOf(rawKey));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    static Double extractPositiveClassProbabilityFromRawValue(Object value) {
        return extractProbabilityFromValue(value);
    }

    static Map<Integer, Double> extractClassProbabilitiesFromRawValue(Object value) {
        return extractClassProbabilitiesFromValue(value);
    }

    private static Double extractProbabilityFromValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof OnnxValue onnxValue) {
            Object unwrapped = readRawOnnxValue(onnxValue);
            return unwrapped == null || unwrapped == value ? null : extractProbabilityFromValue(unwrapped);
        }
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        if (value instanceof float[][] probs2d && probs2d.length > 0 && probs2d[0].length > 0) {
            return probs2d[0].length > 1 ? (double) probs2d[0][1] : (double) probs2d[0][0];
        }
        if (value instanceof double[][] probs2d && probs2d.length > 0 && probs2d[0].length > 0) {
            return probs2d[0].length > 1 ? probs2d[0][1] : probs2d[0][0];
        }
        if (value instanceof float[] probs1d && probs1d.length > 0) {
            return probs1d.length > 1 ? (double) probs1d[1] : (double) probs1d[0];
        }
        if (value instanceof double[] probs1d && probs1d.length > 0) {
            return probs1d.length > 1 ? probs1d[1] : probs1d[0];
        }

        if (value instanceof List<?> list && !list.isEmpty()) {
            Object first = list.get(0);
            if (first instanceof Map<?, ?> map) {
                return extractProbabilityFromMap(map);
            }
            if (allNumbers(list)) {
                Object selected = list.size() > 1 ? list.get(1) : list.get(0);
                return selected instanceof Number n ? n.doubleValue() : null;
            }
            return extractProbabilityFromValue(first);
        }

        if (value instanceof Object[] array && array.length > 0) {
            if (array[0] instanceof Map<?, ?> map) {
                return extractProbabilityFromMap(map);
            }
            if (allNumbers(array)) {
                Object selected = array.length > 1 ? array[1] : array[0];
                return selected instanceof Number n ? n.doubleValue() : null;
            }
            return extractProbabilityFromValue(array[0]);
        }

        if (value instanceof Map<?, ?> map) {
            return extractProbabilityFromMap(map);
        }

        return null;
    }

    private static Double extractProbabilityFromMap(Map<?, ?> map) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Integer key = parseClassKey(entry.getKey());
            if (key != null && key == 1) {
                return extractScalarProbability(entry.getValue());
            }
        }
        return null;
    }

    private static Double extractScalarProbability(Object value) {
        if (value instanceof OnnxValue onnxValue) {
            Object unwrapped = readRawOnnxValue(onnxValue);
            return unwrapped == null || unwrapped == value ? null : extractScalarProbability(unwrapped);
        }
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        if (value instanceof float[] arr && arr.length > 0) {
            return (double) arr[0];
        }
        if (value instanceof double[] arr && arr.length > 0) {
            return arr[0];
        }
        if (value instanceof Float[] arr && arr.length > 0 && arr[0] != null) {
            return arr[0].doubleValue();
        }
        if (value instanceof Double[] arr && arr.length > 0 && arr[0] != null) {
            return arr[0];
        }
        if (value instanceof List<?> list && !list.isEmpty()) {
            return extractScalarProbability(list.get(0));
        }
        if (value instanceof Object[] arr && arr.length > 0) {
            return extractScalarProbability(arr[0]);
        }
        return null;
    }

    private static Object readRawOnnxValue(OnnxValue onnxValue) {
        try {
            return onnxValue == null ? null : onnxValue.getValue();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static boolean allNumbers(List<?> values) {
        return values.stream().allMatch(Number.class::isInstance);
    }

    private static boolean allNumbers(Object[] values) {
        return Arrays.stream(values).allMatch(Number.class::isInstance);
    }

    private double applySetupProbabilityGuard(double positiveProb, boolean probabilityExtracted) {
        if (!calibratedSetupRoute || !SETUP_PROBABILITY_GUARD_ENABLED) {
            return positiveProb;
        }
        if (!probabilityExtracted) {
            return 0.0;
        }

        setupProbabilitySampleCount++;
        if (isBinaryProbability(positiveProb)) {
            setupBinaryProbabilitySampleCount++;
        } else {
            setupNonBinaryProbabilitySampleCount++;
        }

        boolean binaryOnlyAfterSample = setupProbabilitySampleCount >= SETUP_BINARY_PROBABILITY_MIN_SAMPLES
            && setupNonBinaryProbabilitySampleCount == 0L
            && setupBinaryProbabilitySampleCount == setupProbabilitySampleCount;
        if (binaryOnlyAfterSample) {
            if (!setupBinaryProbabilityGuardTripped) {
                flowCondition(
                    "AI.RESPONSE",
                    "SETUP_PROBABILITY_NON_BINARY_SAMPLE",
                    false,
                    "model=" + modelFileName
                        + " samples=" + setupProbabilitySampleCount
                        + " binarySamples=" + setupBinaryProbabilitySampleCount
                        + " nonBinarySamples=" + setupNonBinaryProbabilitySampleCount
                        + " action=" + (SETUP_BINARY_PROBABILITY_FAIL_CLOSED ? "fail_closed" : "warn_only")
                );
                setupBinaryProbabilityGuardTripped = true;
            }
            return SETUP_BINARY_PROBABILITY_FAIL_CLOSED ? 0.0 : positiveProb;
        }

        if (setupBinaryProbabilityGuardTripped && setupNonBinaryProbabilitySampleCount > 0L) {
            flowCondition(
                "AI.RESPONSE",
                "SETUP_PROBABILITY_NON_BINARY_SAMPLE",
                true,
                "model=" + modelFileName
                    + " samples=" + setupProbabilitySampleCount
                    + " binarySamples=" + setupBinaryProbabilitySampleCount
                    + " nonBinarySamples=" + setupNonBinaryProbabilitySampleCount
                    + " action=recovered"
            );
            setupBinaryProbabilityGuardTripped = false;
        }
        return positiveProb;
    }

    private boolean isBinaryProbability(double probability) {
        return Math.abs(probability) <= BINARY_PROBABILITY_EPSILON
            || Math.abs(1.0 - probability) <= BINARY_PROBABILITY_EPSILON;
    }

    private static boolean isCalibratedSetupRouteName(String modelFileName) {
        if (modelFileName == null || modelFileName.isBlank()) {
            return false;
        }
        Path fileName = Paths.get(modelFileName).getFileName();
        String normalized = (fileName == null ? modelFileName : fileName.toString()).trim().toLowerCase();
        return CALIBRATED_SETUP_MODEL_NAMES.contains(normalized);
    }

    private static boolean looksLikeProbabilityOutput(String outputName) {
        String normalized = outputName == null ? "" : outputName.trim().toLowerCase();
        return normalized.contains("prob") || normalized.contains("score");
    }

    private static boolean looksLikeLabelOutput(String outputName) {
        String normalized = outputName == null ? "" : outputName.trim().toLowerCase();
        return normalized.equals("label") || normalized.contains("label") || normalized.contains("class");
    }

    private static double clampProbability(double p) {
        if (Double.isNaN(p) || Double.isInfinite(p)) {
            return 0.0;
        }
        return Math.max(0.0, Math.min(1.0, p));
    }

    private float[] normalizeFeatures(float[] features) {
        if (features.length == expectedFeatureCount) {
            flowCondition("AI.NORMALIZE", "FEATURE_COUNT_MATCH", true, "model=" + modelFileName + " expected=" + expectedFeatureCount + " actual=" + features.length);
            return features;
        }

        float[] adjusted = new float[expectedFeatureCount];
        int copyLen = Math.min(features.length, expectedFeatureCount);
        System.arraycopy(features, 0, adjusted, 0, copyLen);

        if (features.length > expectedFeatureCount) {
            flowCondition("AI.NORMALIZE", "FEATURE_COUNT_MATCH", false, "model=" + modelFileName + " expected=" + expectedFeatureCount + " actual=" + features.length + " mode=trim");
            if (features.length - expectedFeatureCount >= 1) {
                float[] dropped = Arrays.copyOfRange(features, expectedFeatureCount, features.length);
                flowAnalyze("AI.NORMALIZE", "model=" + modelFileName + " droppedTailFeatures=" + Arrays.toString(dropped));
            }
        } else {
            flowCondition("AI.NORMALIZE", "FEATURE_COUNT_MATCH", false, "model=" + modelFileName + " expected=" + expectedFeatureCount + " actual=" + features.length + " mode=pad");
        }

        return adjusted;
    }
    
    public void close() {
        try {
            if (session != null) session.close();
            if (env != null) env.close();
            flowInfo("AI.CLOSE", "Closed ONNX resources model=" + modelFileName);
        } catch (Exception e) {
            flowError("AI.CLOSE", "Failed to close model=" + modelFileName + " reason=" + e.getMessage());
        }
    }

    private void flowInfo(String stage, String message) {
        log.info(">>> [FLOW][INFO][{}] {}", stage, message);
    }

    private void flowData(String stage, String message) {
        log.info(">>> [FLOW][DATA][{}] {}", stage, message);
    }

    private void flowAnalyze(String stage, String message) {
        log.info(">>> [FLOW][ANALYZE][{}] {}", stage, message);
    }

    private void flowError(String stage, String message) {
        log.error(">>> [FLOW][ERROR][{}] {}", stage, message);
    }

    private void flowCondition(String stage, String conditionName, boolean passed, String details) {
        String verdict = passed ? "PASS" : "FAIL";
        log.info(">>> [FLOW][COND][{}] {}={} | {}", stage, conditionName, verdict, details);
    }
}
