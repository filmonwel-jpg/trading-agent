package com.calgary.fili.trader.bot.strategy;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Research-only replay sidecar for downstream setup-filter features.
 *
 * <p>The downstream setup filter was trained on enriched 30-second research rows. Normal live/replay strategy state
 * can populate causal market/quote/option aliases, but research producers such as sequence/meta/regime/setup-screen
 * columns are not present in the normalized Databento event stream. This store lets controlled replay opt in to an
 * exact setup-arm CSV sidecar so Java scores the same setup-arm feature distribution as the offline policy screen.</p>
 */
final class DownstreamSetupFeatureStore {
	private record Key(String symbol, String side, long setupEpochSec) {}

	private final Path sourceCsv;
	private final Map<Key, Map<String, Float>> rowsByArm;

	private DownstreamSetupFeatureStore(Path sourceCsv, Map<Key, Map<String, Float>> rowsByArm) {
		this.sourceCsv = sourceCsv;
		this.rowsByArm = Map.copyOf(rowsByArm);
	}

	static DownstreamSetupFeatureStore load(Path sourceCsv) throws IOException {
		Path normalized = sourceCsv.toAbsolutePath().normalize();
		if (!Files.isRegularFile(normalized)) {
			throw new IOException("missing downstream setup feature sidecar CSV: " + normalized);
		}

		Map<Key, Map<String, Float>> rows = new LinkedHashMap<>();
		try (BufferedReader reader = Files.newBufferedReader(normalized, StandardCharsets.UTF_8)) {
			String headerLine = reader.readLine();
			if (headerLine == null || headerLine.isBlank()) {
				throw new IOException("empty downstream setup feature sidecar CSV: " + normalized);
			}
			List<String> header = parseCsvLine(headerLine);
			Map<String, Integer> index = headerIndex(header);
			long lineNumber = 1L;
			String line;
			while ((line = reader.readLine()) != null) {
				lineNumber++;
				if (line.isBlank()) {
					continue;
				}
				List<String> cells = parseCsvLine(line);
				Key key = keyFromRow(cells, index);
				if (key == null) {
					throw new IOException("missing Symbol/SetupSide/SetupEpochSec at line=" + lineNumber + " file=" + normalized);
				}
				rows.put(key, Map.copyOf(valuesFromRow(header, cells, index, key.symbol())));
			}
		}
		return new DownstreamSetupFeatureStore(normalized, rows);
	}

	Map<String, Float> lookup(String symbol, String side, long setupEpochSec) {
		Map<String, Float> row = rowsByArm.get(new Key(normalizeSymbol(symbol), normalizeSide(side), setupEpochSec));
		return row == null ? Map.of() : row;
	}

	int rowCount() {
		return rowsByArm.size();
	}

	Path sourceCsv() {
		return sourceCsv;
	}

	private static void addCategoricalOneHots(Map<String, Float> values, String symbol, String arbitrationReason, String sessionBucket) {
		if (symbol != null && !symbol.isBlank()) {
			values.put("Symbol_" + normalizeSymbol(symbol), 1.0f);
		}
		if (arbitrationReason != null && !arbitrationReason.isBlank()) {
			values.put("SetupArbitrationReason_" + arbitrationReason.trim(), 1.0f);
		}
		if (sessionBucket != null && !sessionBucket.isBlank()) {
			values.put("SessionBucket_" + sessionBucket.trim().toLowerCase(Locale.US), 1.0f);
		}
	}

	private static Key keyFromRow(List<String> cells, Map<String, Integer> index) {
		String symbol = normalizeSymbol(cell(cells, index, "Symbol"));
		if (symbol.isBlank()) {
			symbol = normalizeSymbol(cell(cells, index, "Symbol_30s"));
		}
		String side = normalizeSide(cell(cells, index, "SetupSide"));
		Long setupEpoch = parseLong(cell(cells, index, "SetupEpochSec"));
		if (setupEpoch == null) {
			setupEpoch = parseLong(cell(cells, index, "BarEpochSec"));
		}
		if (symbol.isBlank() || side.isBlank() || setupEpoch == null) {
			return null;
		}
		return new Key(symbol, side, setupEpoch);
	}

	private static Map<String, Float> valuesFromRow(List<String> header, List<String> cells, Map<String, Integer> index, String symbol) {
		Map<String, Float> values = new HashMap<>();
		for (int i = 0; i < header.size(); i++) {
			String column = header.get(i);
			if (column == null || column.isBlank()) {
				continue;
			}
			String raw = i < cells.size() ? cells.get(i) : "";
			Float value = parseFloat(raw);
			if (value != null) {
				values.put(column, value);
			}
		}
		addCategoricalOneHots(
			values,
			symbol,
			cell(cells, index, "SetupArbitrationReason"),
			cell(cells, index, "SessionBucket")
		);
		return values;
	}

	private static Map<String, Integer> headerIndex(List<String> header) {
		Map<String, Integer> index = new HashMap<>();
		for (int i = 0; i < header.size(); i++) {
			String column = header.get(i) == null ? "" : header.get(i).trim();
			if (!column.isBlank()) {
				index.put(column, i);
			}
		}
		return index;
	}

	private static String cell(List<String> cells, Map<String, Integer> index, String key) {
		Integer i = index.get(key);
		if (i == null || i < 0 || i >= cells.size()) {
			return "";
		}
		String value = cells.get(i);
		return value == null ? "" : value.trim();
	}

	private static Float parseFloat(String raw) {
		if (raw == null || raw.isBlank()) {
			return null;
		}
		try {
			float value = Float.parseFloat(raw.trim());
			return Float.isFinite(value) ? value : null;
		} catch (NumberFormatException ignored) {
			return null;
		}
	}

	private static Long parseLong(String raw) {
		if (raw == null || raw.isBlank()) {
			return null;
		}
		try {
			return Math.round(Double.parseDouble(raw.trim()));
		} catch (NumberFormatException ignored) {
			return null;
		}
	}

	private static String normalizeSymbol(String raw) {
		return raw == null ? "" : raw.trim().toUpperCase(Locale.US);
	}

	private static String normalizeSide(String raw) {
		return raw == null ? "" : raw.trim().toLowerCase(Locale.US);
	}

	static List<String> parseCsvLine(String line) {
		List<String> out = new ArrayList<>();
		if (line == null || line.isEmpty()) {
			return out;
		}
		StringBuilder cell = new StringBuilder();
		boolean inQuotes = false;
		int i = 0;
		while (i < line.length()) {
			char ch = line.charAt(i);
			if (ch == '"') {
				if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
					cell.append('"');
					i += 2;
					continue;
				} else {
					inQuotes = !inQuotes;
				}
			} else if (ch == ',' && !inQuotes) {
				out.add(cell.toString().trim());
				cell.setLength(0);
			} else {
				cell.append(ch);
			}
			i++;
		}
		out.add(cell.toString().trim());
		return out;
	}
}


