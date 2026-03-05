package com.calgary.fili.trader.bot.strategy;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.TensorInfo;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AiPredictor {
    private static final Logger log = LoggerFactory.getLogger(AiPredictor.class);
    private static final int DEFAULT_EXPECTED_FEATURES = 23;
    
    private final OrtEnvironment env;
    private final OrtSession session;
    private final String modelFileName;
    private final String inputName;
    private final int expectedFeatureCount;

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
            if (expectedFeatureCount == 23 || expectedFeatureCount == 32) {
                flowCondition("AI.INIT", "FEATURE_COUNT_SUPPORTED", true, "model=" + modelFileName + " expected=" + expectedFeatureCount);
            } else {
                flowCondition("AI.INIT", "FEATURE_COUNT_SUPPORTED", false, "model=" + modelFileName + " expected=" + expectedFeatureCount + " note=will trim/pad from strategy vector");
            }
        }
    }

    public boolean predict(float[] features) {
        boolean validFeatures = features != null && features.length > 0;
        flowCondition("AI.INPUT", "FEATURE_VECTOR_PRESENT", validFeatures, "model=" + modelFileName + " featureCount=" + (features == null ? 0 : features.length));
        if (!validFeatures) {
            flowError("AI.INPUT", "Features cannot be null or empty model=" + modelFileName);
            return false;
        }

        try {
            float[] normalizedFeatures = normalizeFeatures(features);
            float[][] inputMatrix = new float[][]{normalizedFeatures};
            try (OnnxTensor tensor = OnnxTensor.createTensor(env, inputMatrix)) {
                flowAnalyze("AI.REQUEST", "model=" + modelFileName + " running prediction featureCount=" + normalizedFeatures.length);
                flowData("AI.REQUEST", "model=" + modelFileName + " features=" + Arrays.toString(normalizedFeatures));
                OrtSession.Result result = session.run(Collections.singletonMap(inputName, tensor));
                long[] labels = (long[]) result.get(0).getValue();
                boolean decision = labels.length > 0 && labels[0] == 1L;
                flowCondition("AI.RESPONSE", "PREDICTION_LABEL_AVAILABLE", labels.length > 0, "model=" + modelFileName + " labelsLength=" + labels.length);
                flowCondition("AI.RESPONSE", "PREDICTION_POSITIVE", decision, "model=" + modelFileName + " label=" + (labels.length > 0 ? labels[0] : -1));
                return decision;
            }
        } catch (Exception e) {
            flowError("AI.RESPONSE", "Prediction failed model=" + modelFileName + " reason=" + e.getMessage());
            return false;
        }
    }

    public boolean shouldBuyDip(float[] features) {
        return predict(features);
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