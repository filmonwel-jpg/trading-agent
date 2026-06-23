package com.calgary.fili.trader.bot.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class ProbabilityCalibrator {
    private enum Method {
        RAW,
        SIGMOID,
        ISOTONIC
    }

    private final String modelName;
    private final String filename;
    private final String featureSchemaSha256;
    private final String holdoutFingerprintSha256;
    private final boolean selected;
    private final Method method;
    private final double sigmoidCoef;
    private final double sigmoidIntercept;
    private final double[] isotonicX;
    private final double[] isotonicY;

    private ProbabilityCalibrator(
        String modelName,
        String filename,
        String featureSchemaSha256,
        String holdoutFingerprintSha256,
        boolean selected,
        Method method,
        double sigmoidCoef,
        double sigmoidIntercept,
        double[] isotonicX,
        double[] isotonicY
    ) {
        this.modelName = modelName == null ? "" : modelName.trim();
        this.filename = filename == null ? "" : filename.trim();
        this.featureSchemaSha256 = featureSchemaSha256 == null ? "" : featureSchemaSha256.trim();
        this.holdoutFingerprintSha256 = holdoutFingerprintSha256 == null ? "" : holdoutFingerprintSha256.trim();
        this.selected = selected;
        this.method = method;
        this.sigmoidCoef = sigmoidCoef;
        this.sigmoidIntercept = sigmoidIntercept;
        this.isotonicX = isotonicX == null ? new double[0] : isotonicX.clone();
        this.isotonicY = isotonicY == null ? new double[0] : isotonicY.clone();
    }

    static ProbabilityCalibrator identity(String modelName) {
        return new ProbabilityCalibrator(modelName, "", "", "", true, Method.RAW, 0.0, 0.0, null, null);
    }

    @SuppressWarnings("unchecked")
    static ProbabilityCalibrator fromPosthocExportRow(Map<String, Object> row) {
        if (row == null) {
            throw new IllegalArgumentException("calibrator row is null");
        }
        String modelName = requiredString(row, "model");
        String filename = optionalString(row, "filename");
        String featureSchemaSha256 = optionalString(row, "feature_schema_sha256");
        String holdoutFingerprintSha256 = optionalString(row, "holdout_fingerprint_sha256");
        boolean selected = booleanValue(row.get("selected"));
        Method method = parseMethod(optionalString(row, "method"));
        Object rawParameters = row.get("parameters");
        Map<String, Object> parameters = rawParameters instanceof Map<?, ?> rawMap
            ? (Map<String, Object>) rawMap
            : Map.of();
        String parameterMethod = optionalString(parameters, "method");
        if (!parameterMethod.isBlank() && parseMethod(parameterMethod) != method) {
            throw new IllegalArgumentException("calibrator method mismatch model=" + modelName + " row=" + methodName(method) + " parameters=" + parameterMethod);
        }
        return switch (method) {
            case RAW -> new ProbabilityCalibrator(modelName, filename, featureSchemaSha256, holdoutFingerprintSha256, selected, Method.RAW, 0.0, 0.0, null, null);
            case SIGMOID -> new ProbabilityCalibrator(
                modelName,
                filename,
                featureSchemaSha256,
                holdoutFingerprintSha256,
                selected,
                Method.SIGMOID,
                requiredFiniteDouble(parameters, "coef", modelName),
                requiredFiniteDouble(parameters, "intercept", modelName),
                null,
                null
            );
            case ISOTONIC -> {
                double[] x = requiredFiniteDoubleArray(parameters, "x_thresholds", modelName);
                double[] y = requiredFiniteDoubleArray(parameters, "y_thresholds", modelName);
                validateIsotonicThresholds(modelName, x, y);
                yield new ProbabilityCalibrator(modelName, filename, featureSchemaSha256, holdoutFingerprintSha256, selected, Method.ISOTONIC, 0.0, 0.0, x, y);
            }
        };
    }

    double apply(double rawProbability) {
        double raw = clipProbability(rawProbability);
        return switch (method) {
            case RAW -> raw;
            case SIGMOID -> clipProbability(stableSigmoid(sigmoidCoef * raw + sigmoidIntercept));
            case ISOTONIC -> clipProbability(interpolate(raw));
        };
    }

    String modelName() {
        return modelName;
    }

    String filename() {
        return filename;
    }

    String featureSchemaSha256() {
        return featureSchemaSha256;
    }

    String holdoutFingerprintSha256() {
        return holdoutFingerprintSha256;
    }

    boolean selected() {
        return selected;
    }

    String methodName() {
        return methodName(method);
    }

    boolean isIdentity() {
        return method == Method.RAW;
    }

    static String normalizeMethodName(String methodName) {
        return methodName(parseMethod(methodName));
    }

    static boolean isSupportedMethodName(String methodName) {
        try {
            parseMethod(methodName);
            return true;
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    static double clipProbability(double value) {
        if (!Double.isFinite(value)) {
            return 0.0;
        }
        return Math.max(0.0, Math.min(1.0, value));
    }

    private double interpolate(double raw) {
        if (raw <= isotonicX[0]) {
            return isotonicY[0];
        }
        int last = isotonicX.length - 1;
        if (raw >= isotonicX[last]) {
            return isotonicY[last];
        }
        int hi = 1;
        while (hi < isotonicX.length && raw > isotonicX[hi]) {
            hi++;
        }
        int lo = hi - 1;
        double width = isotonicX[hi] - isotonicX[lo];
        if (width <= 0.0) {
            return isotonicY[hi];
        }
        double t = (raw - isotonicX[lo]) / width;
        return isotonicY[lo] + t * (isotonicY[hi] - isotonicY[lo]);
    }

    private static double stableSigmoid(double value) {
        if (value >= 0.0) {
            double exp = Math.exp(-value);
            return 1.0 / (1.0 + exp);
        }
        double exp = Math.exp(value);
        return exp / (1.0 + exp);
    }

    private static Method parseMethod(String rawMethod) {
        String method = rawMethod == null ? "" : rawMethod.trim().toLowerCase(Locale.US);
        if (method.isBlank()
            || "raw".equals(method)
            || "identity".equals(method)
            || method.contains("identity_raw_probability")
            || method.contains("no_posthoc_calibrator")) {
            return Method.RAW;
        }
        if ("sigmoid".equals(method) || method.contains("platt")) {
            return Method.SIGMOID;
        }
        if ("isotonic".equals(method)) {
            return Method.ISOTONIC;
        }
        throw new IllegalArgumentException("unsupported posthoc calibration method=" + rawMethod);
    }

    private static String methodName(Method method) {
        return switch (method) {
            case RAW -> "raw";
            case SIGMOID -> "sigmoid";
            case ISOTONIC -> "isotonic";
        };
    }

    private static String requiredString(Map<String, Object> row, String key) {
        String value = optionalString(row, key);
        if (value.isBlank()) {
            throw new IllegalArgumentException("missing required calibrator field=" + key);
        }
        return value;
    }

    private static String optionalString(Map<String, Object> row, String key) {
        Object value = row == null ? null : row.get(key);
        return value == null ? "" : value.toString().trim();
    }

    private static boolean booleanValue(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        return value != null && Boolean.parseBoolean(value.toString());
    }

    private static double requiredFiniteDouble(Map<String, Object> row, String key, String modelName) {
        Object value = row.get(key);
        if (!(value instanceof Number)) {
            try {
                value = value == null ? null : Double.parseDouble(value.toString());
            } catch (NumberFormatException exception) {
                value = null;
            }
        }
        if (value instanceof Number number) {
            double parsed = number.doubleValue();
            if (Double.isFinite(parsed)) {
                return parsed;
            }
        }
        throw new IllegalArgumentException("invalid finite double field=" + key + " model=" + modelName);
    }

    private static double[] requiredFiniteDoubleArray(Map<String, Object> row, String key, String modelName) {
        Object value = row.get(key);
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException("missing numeric array field=" + key + " model=" + modelName);
        }
        List<Double> out = new ArrayList<>();
        for (Object item : list) {
            double parsed;
            if (item instanceof Number number) {
                parsed = number.doubleValue();
            } else {
                try {
                    parsed = Double.parseDouble(item == null ? "" : item.toString());
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException("non-numeric value in " + key + " model=" + modelName);
                }
            }
            if (!Double.isFinite(parsed)) {
                throw new IllegalArgumentException("non-finite value in " + key + " model=" + modelName);
            }
            out.add(parsed);
        }
        double[] values = new double[out.size()];
        for (int i = 0; i < out.size(); i++) {
            values[i] = out.get(i);
        }
        return values;
    }

    private static void validateIsotonicThresholds(String modelName, double[] x, double[] y) {
        if (x.length == 0 || x.length != y.length) {
            throw new IllegalArgumentException("isotonic thresholds must be non-empty and equal length model=" + modelName);
        }
        for (int i = 1; i < x.length; i++) {
            if (x[i] < x[i - 1]) {
                throw new IllegalArgumentException("isotonic x_thresholds must be sorted model=" + modelName);
            }
        }
    }
}
