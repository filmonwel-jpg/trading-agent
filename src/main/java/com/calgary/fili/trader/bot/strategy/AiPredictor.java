package com.calgary.fili.trader.bot.strategy;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.TensorInfo;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AiPredictor {
    private static final Logger log = LoggerFactory.getLogger(AiPredictor.class);
    private static final int DEFAULT_EXPECTED_FEATURES = 25;
    
    private final OrtEnvironment env;
    private final OrtSession session;
    private final String modelFileName;
    private final String inputName;
    private final int expectedFeatureCount;

    public record PredictionOutcome(boolean predictedPositive, double positiveProbability) {}

    public AiPredictor(String modelFileName) throws Exception {
        this.modelFileName = modelFileName;
        flowInfo("AI.INIT", "Booting ONNX Runtime Environment model=" + modelFileName);
        this.env = OrtEnvironment.getEnvironment();
        
        // Load the AI model file from the resources folder
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(modelFileName)) {
            if (is == null) {
                throw new RuntimeException("Could not find " + modelFileName + " in resources!");
            }
            byte[] modelBytes = is.readAllBytes();
            this.session = env.createSession(modelBytes, new OrtSession.SessionOptions());

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
            flowInfo("AI.INIT", "Successfully loaded model=" + modelFileName);
            flowData("AI.INIT", "model=" + modelFileName + " input=" + inputName + " expectedFeatures=" + expectedFeatureCount);
            if (expectedFeatureCount == 25 || expectedFeatureCount == 34) {
                flowCondition("AI.INIT", "FEATURE_COUNT_SUPPORTED", true, "model=" + modelFileName + " expected=" + expectedFeatureCount);
            } else {
                flowCondition("AI.INIT", "FEATURE_COUNT_SUPPORTED", false, "model=" + modelFileName + " expected=" + expectedFeatureCount + " note=will trim/pad from strategy vector");
            }
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

                    // Fallback for models that only emit labels.
                    double positiveProb;
                    if (prob != null) {
                        positiveProb = clampProbability(prob);
                    } else if (label >= 0) {
                        positiveProb = (label == 1L) ? 1.0 : 0.0;
                    } else {
                        positiveProb = 0.0;
                    }

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

    private long extractLabel(OrtSession.Result result) {
        for (int i = 0; i < result.size(); i++) {
            Object value;
            try {
                value = result.get(i).getValue();
            } catch (Exception e) {
                flowError("AI.RESPONSE", "Failed reading ONNX label output model=" + modelFileName + " idx=" + i + " reason=" + e.getMessage());
                continue;
            }
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
        }
        return -1;
    }

    private Double extractPositiveClassProbability(OrtSession.Result result) {
        for (int i = 0; i < result.size(); i++) {
            Object value;
            try {
                value = result.get(i).getValue();
            } catch (Exception e) {
                flowError("AI.RESPONSE", "Failed reading ONNX probability output model=" + modelFileName + " idx=" + i + " reason=" + e.getMessage());
                continue;
            }
            Double extracted = extractProbabilityFromValue(value);
            if (extracted != null) {
                return extracted;
            }
        }
        return null;
    }

    private Double extractProbabilityFromValue(Object value) {
        if (value == null) {
            return null;
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
        }

        if (value instanceof Map<?, ?> map) {
            return extractProbabilityFromMap(map);
        }

        return null;
    }

    private Double extractProbabilityFromMap(Map<?, ?> map) {
        Object classOneVal = null;

        if (map.containsKey(1L)) {
            classOneVal = map.get(1L);
        } else if (map.containsKey(1)) {
            classOneVal = map.get(1);
        } else if (map.containsKey("1")) {
            classOneVal = map.get("1");
        }

        if (classOneVal instanceof Number n) {
            return n.doubleValue();
        }

        if (map.size() == 2) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey();
                Object val = entry.getValue();
                if (val instanceof Number n) {
                    if ("1".equals(String.valueOf(key))) {
                        return n.doubleValue();
                    }
                }
            }
        }

        return null;
    }

    private double clampProbability(double p) {
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