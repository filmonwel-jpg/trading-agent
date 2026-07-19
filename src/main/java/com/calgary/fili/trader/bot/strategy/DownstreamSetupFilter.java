package com.calgary.fili.trader.bot.strategy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Research-only downstream setup-arm quality filter used by controlled Java replay.
 *
 * <p>The manifest is produced by {@code scripts/export_downstream_setup_filter_onnx.py}. Each route resolves its
 * ONNX model and feature schema relative to the route manifest directory. The feature vector is rebuilt in the exact
 * schema order and missing/unavailable values are represented as {@code 0.0f}, matching the research exporter contract
 * for unseen one-hot categories and missing numeric fields.</p>
 */
final class DownstreamSetupFilter implements Closeable {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	record Decision(boolean passed, double probability, double threshold, int featureCount, String routeName) {}

	private record Route(String routeName, String side, AiPredictor predictor, List<String> featureColumns, double threshold) {}

	private final Path manifestPath;
	private final Map<String, Route> routeBySide;

	private DownstreamSetupFilter(Path manifestPath, Map<String, Route> routeBySide) {
		this.manifestPath = manifestPath;
		this.routeBySide = Map.copyOf(routeBySide);
	}

	static DownstreamSetupFilter load(Path manifestPath) throws Exception {
		Path normalizedManifest = manifestPath.toAbsolutePath().normalize();
		Path manifestDir = normalizedManifest.getParent();
		if (manifestDir == null || !Files.isRegularFile(normalizedManifest)) {
			throw new IOException("missing downstream setup filter route manifest: " + normalizedManifest);
		}
		Map<String, Object> manifest = OBJECT_MAPPER.readValue(normalizedManifest.toFile(), new TypeReference<>() {});
		Object rawRoutes = manifest.get("routes");
		if (!(rawRoutes instanceof List<?> routes)) {
			throw new IOException("downstream setup filter manifest missing routes list: " + normalizedManifest);
		}

		Map<String, Route> loaded = new HashMap<>();
		for (Object rawRoute : routes) {
			if (!(rawRoute instanceof Map<?, ?> rawRouteMap)) {
				continue;
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> routeMap = (Map<String, Object>) rawRouteMap;
			Route route = loadRoute(manifestDir, routeMap);
			if (route != null) {
				loaded.put(route.side(), route);
			}
		}
		if (!loaded.containsKey("long") || !loaded.containsKey("short")) {
			throw new IOException("downstream setup filter manifest must contain long and short routes: " + normalizedManifest);
		}
		return new DownstreamSetupFilter(normalizedManifest, loaded);
	}

	private static Route loadRoute(Path manifestDir, Map<String, Object> routeMap) throws Exception {
		String routeName = stringValue(routeMap.get("route_name"));
		String side = stringValue(routeMap.get("side")).toLowerCase(Locale.US);
		if (routeName.isBlank() || side.isBlank()) {
			return null;
		}
		String modelFileName = stringValue(routeMap.get("onnx_model_filename"));
		if (modelFileName.isBlank()) {
			modelFileName = fileNameFromPathString(stringValue(routeMap.get("onnx_model")));
		}
		String schemaFileName = stringValue(routeMap.get("feature_schema_filename"));
		Path schemaPath = resolveManifestRelative(manifestDir, schemaFileName, stringValue(routeMap.get("feature_schema")));
		if (modelFileName.isBlank() || !Files.isRegularFile(schemaPath)) {
			throw new IOException("invalid downstream setup filter route=" + routeName + " model=" + modelFileName + " schema=" + schemaPath);
		}
		List<String> featureColumns = loadFeatureColumns(schemaPath);
		int routeFeatureCount = intValue(routeMap.get("feature_count"), featureColumns.size());
		if (routeFeatureCount != featureColumns.size()) {
			throw new IOException("downstream setup filter feature_count mismatch route=" + routeName + " manifest=" + routeFeatureCount + " schema=" + featureColumns.size());
		}
		double threshold = probabilityValue(routeMap.get("selected_threshold"));
		AiPredictor predictor = new AiPredictor(modelFileName, manifestDir.toString());
		if (predictor.getExpectedFeatureCount() != featureColumns.size()) {
			predictor.close();
			throw new IOException("downstream setup filter ONNX input count mismatch route=" + routeName + " onnx=" + predictor.getExpectedFeatureCount() + " schema=" + featureColumns.size());
		}
		return new Route(routeName, side, predictor, List.copyOf(featureColumns), threshold);
	}

	static Path resolveManifestRelative(Path manifestDir, String fileName, String fallbackPath) {
		if (fileName != null && !fileName.isBlank()) {
			return manifestDir.resolve(fileName).normalize();
		}
		Path fallback = Path.of(fallbackPath == null ? "" : fallbackPath.trim());
		if (fallback.isAbsolute()) {
			return fallback.normalize();
		}
		Path relativeToManifest = manifestDir.resolve(fallback).normalize();
		if (Files.isRegularFile(relativeToManifest)) {
			return relativeToManifest;
		}
		return fallback.normalize();
	}

	private static List<String> loadFeatureColumns(Path schemaPath) throws IOException {
		Map<String, Object> schema = OBJECT_MAPPER.readValue(schemaPath.toFile(), new TypeReference<>() {});
		Object rawInput = schema.get("input");
		if (!(rawInput instanceof Map<?, ?> input)) {
			throw new IOException("downstream setup filter schema missing input object: " + schemaPath);
		}
		Object rawColumns = input.get("feature_columns");
		if (!(rawColumns instanceof List<?> columns) || columns.isEmpty()) {
			throw new IOException("downstream setup filter schema missing feature_columns: " + schemaPath);
		}
		return columns.stream().map(Object::toString).toList();
	}

	Decision score(String side, Map<String, Float> featureValues) {
		Route route = routeBySide.get(side == null ? "" : side.toLowerCase(Locale.US));
		if (route == null) {
			return new Decision(false, 0.0, 1.0, 0, "missingRoute");
		}
		float[] features = buildFeatureVector(route.featureColumns(), featureValues);
		double probability = route.predictor().predictProbability(features);
		return new Decision(probability >= route.threshold(), probability, route.threshold(), features.length, route.routeName());
	}

	Path manifestPath() {
		return manifestPath;
	}

	@Override
	public void close() {
		for (Route route : routeBySide.values()) {
			route.predictor().close();
		}
	}

	static float[] buildFeatureVector(List<String> featureColumns, Map<String, Float> featureValues) {
		float[] vector = new float[featureColumns.size()];
		for (int i = 0; i < featureColumns.size(); i++) {
			Float value = featureValues == null ? null : featureValues.get(featureColumns.get(i));
			vector[i] = value == null || !Float.isFinite(value) ? 0.0f : value;
		}
		return vector;
	}

	static String fileNameFromPathString(String rawPath) {
		if (rawPath == null || rawPath.isBlank()) {
			return "";
		}
		Path path = Path.of(rawPath.trim());
		Path fileName = path.getFileName();
		return fileName == null ? "" : fileName.toString();
	}

	private static String stringValue(Object value) {
		return value == null ? "" : value.toString().trim();
	}

	private static int intValue(Object value, int fallback) {
		return value instanceof Number number ? number.intValue() : fallback;
	}

	private static double probabilityValue(Object value) throws IOException {
		double parsed;
		if (value instanceof Number number) {
			parsed = number.doubleValue();
		} else if (value != null) {
			try {
				parsed = Double.parseDouble(value.toString());
			} catch (NumberFormatException ignored) {
				parsed = Double.NaN;
			}
		} else {
			parsed = Double.NaN;
		}
		if (!Double.isFinite(parsed) || parsed < 0.0 || parsed > 1.0) {
			throw new IOException("downstream setup filter selected_threshold must be a finite probability: " + value);
		}
		return parsed;
	}
}

