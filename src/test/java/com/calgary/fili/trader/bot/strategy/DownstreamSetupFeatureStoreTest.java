package com.calgary.fili.trader.bot.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DownstreamSetupFeatureStoreTest {
	@TempDir
	Path tempDir;

	@Test
	void parseCsvLineHandlesQuotedCommasAndEscapedQuotes() {
		List<String> values = DownstreamSetupFeatureStore.parseCsvLine("NVDA,\"best, threshold\",\"a \"\"quote\"\"\",1.25");

		assertEquals(List.of("NVDA", "best, threshold", "a \"quote\"", "1.25"), values);
	}

	@Test
	void loadIndexesRowsBySymbolSideAndSetupEpochAndAddsCategoricalOneHots() throws Exception {
		Path csv = tempDir.resolve("setup_rows.csv");
		Files.writeString(
			csv,
			String.join("\n",
				"Symbol,SetupSide,SetupEpochSec,SetupProb,tsm_up_prob_30s,SetupArbitrationReason,SessionBucket,ignored_text",
				"NVDA,long,1777376400,0.74,0.61,only_long_passed,open,not_numeric",
				"TQQQ,short,1777376430,0.66,0.42,only_short_passed,midday,not_numeric"
			) + "\n"
		);

		DownstreamSetupFeatureStore store = DownstreamSetupFeatureStore.load(csv);

		assertEquals(2, store.rowCount());
		Map<String, Float> row = store.lookup("nvda", "LONG", 1777376400L);
		assertEquals(0.74f, row.get("SetupProb"), 1.0e-6f);
		assertEquals(0.61f, row.get("tsm_up_prob_30s"), 1.0e-6f);
		assertEquals(1.0f, row.get("Symbol_NVDA"), 1.0e-6f);
		assertEquals(1.0f, row.get("SetupArbitrationReason_only_long_passed"), 1.0e-6f);
		assertEquals(1.0f, row.get("SessionBucket_open"), 1.0e-6f);
		assertFalse(row.containsKey("ignored_text"));
		assertTrue(store.lookup("NVDA", "short", 1777376400L).isEmpty());
	}
}


