package com.calgary.fili.trader.bot.strategy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DownstreamSetupFilterTest {

	@TempDir
	Path tempDir;

	@Test
	void buildFeatureVectorUsesExactSchemaOrderAndZeroFillsMissingValues() {
		List<String> featureColumns = List.of(
			"SetupProb",
			"SetupThresholdMargin",
			"Symbol_TSLA",
			"MissingResearchOnlyField",
			"SessionBucket_open"
		);
		Map<String, Float> values = Map.of(
			"SetupProb", 0.77f,
			"SetupThresholdMargin", 0.12f,
			"Symbol_TSLA", 1.0f,
			"SessionBucket_open", 1.0f
		);

		float[] vector = DownstreamSetupFilter.buildFeatureVector(featureColumns, values);

		assertArrayEquals(new float[]{0.77f, 0.12f, 1.0f, 0.0f, 1.0f}, vector, 1.0e-6f);
	}

	@Test
	void buildFeatureVectorZeroFillsNullAndNonFiniteValues() {
		List<String> featureColumns = List.of("Finite", "NullValue", "NaNValue", "PositiveInfinity", "Absent");
		Map<String, Float> values = new HashMap<>();
		values.put("Finite", -0.25f);
		values.put("NullValue", null);
		values.put("NaNValue", Float.NaN);
		values.put("PositiveInfinity", Float.POSITIVE_INFINITY);

		float[] vector = DownstreamSetupFilter.buildFeatureVector(featureColumns, values);

		assertArrayEquals(new float[]{-0.25f, 0.0f, 0.0f, 0.0f, 0.0f}, vector, 1.0e-6f);
		assertArrayEquals(new float[]{0.0f, 0.0f, 0.0f, 0.0f, 0.0f}, DownstreamSetupFilter.buildFeatureVector(featureColumns, null), 1.0e-6f);
	}

	@Test
	void manifestHelpersResolveRouteArtifactsRelativeToManifestDirectory() throws Exception {
		Path manifestDir = tempDir.resolve("bundle");
		Files.createDirectories(manifestDir);
		Path schema = manifestDir.resolve("long_downstream_setup_filter_feature_schema.json");
		Files.writeString(schema, "{}");

		Path resolvedByFilename = DownstreamSetupFilter.resolveManifestRelative(
			manifestDir,
			"long_downstream_setup_filter_feature_schema.json",
			"/stale/export/path/long_downstream_setup_filter_feature_schema.json"
		);
		Path resolvedByFallback = DownstreamSetupFilter.resolveManifestRelative(
			manifestDir,
			"",
			"long_downstream_setup_filter_feature_schema.json"
		);

		assertEquals(schema.normalize(), resolvedByFilename);
		assertEquals(schema.normalize(), resolvedByFallback);
		assertEquals("long_downstream_setup_filter.onnx", DownstreamSetupFilter.fileNameFromPathString("/stale/export/path/long_downstream_setup_filter.onnx"));
	}
}

