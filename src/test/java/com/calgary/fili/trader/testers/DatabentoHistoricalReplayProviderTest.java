package com.calgary.fili.trader.testers;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatabentoHistoricalReplayProviderTest {
    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");

    @TempDir
    Path tempDir;

    @Test
    void aggregatesNormalizedHistoricalStreamIntoCadenceBars() throws Exception {
        long firstEpoch = 1_716_218_200L;
        long secondEpoch = firstEpoch + 1L;
        long nextBucketEpoch = firstEpoch + 5L;
        Path script = tempDir.resolve("emit_stream.py");
        Files.writeString(script, """
            import json
            events = [
                {"event":"status","message":"begin"},
                {"event":"option_bar","underlying":"TSLA","right":"P","barEpochSec":%d,"volume":7},
                {"event":"option_bar","underlying":"TSLA","right":"C","barEpochSec":%d,"volume":11},
                {"event":"equity_bar","symbol":"TSLA","barEpochSec":%d,"open":100.0,"high":101.0,"low":99.5,"close":100.5,"wap":100.25,"volume":10,"bid":100.4,"ask":100.6,"bidSize":2,"askSize":3},
                {"event":"equity_bar","symbol":"TSLA","barEpochSec":%d,"open":100.5,"high":102.0,"low":100.0,"close":101.5,"wap":101.0,"volume":30,"bid":101.4,"ask":101.6,"bidSize":4,"askSize":5},
                {"event":"equity_bar","symbol":"TSLA","barEpochSec":%d,"open":102.0,"high":103.0,"low":101.0,"close":102.5,"wap":102.4,"volume":20,"bid":102.4,"ask":102.6,"bidSize":6,"askSize":7}
            ]
            for event in events:
                print(json.dumps(event), flush=True)
            """.formatted(firstEpoch, secondEpoch, firstEpoch, secondEpoch, nextBucketEpoch), StandardCharsets.UTF_8);

        DatabentoHistoricalReplayProvider provider = DatabentoHistoricalReplayProvider.fromDatabentoHistoricalStream(
            "5s",
            List.of("python3", script.toString()),
            tempDir,
            "TSLA"
        );

        assertThat(provider.events()).hasSize(2);
        DatabentoHistoricalReplayProvider.ReplayBar first = provider.events().getFirst();
        assertThat(first.symbol()).isEqualTo("TSLA");
        assertThat(first.timestamp()).isEqualTo(LocalDateTime.ofInstant(Instant.ofEpochSecond(firstEpoch), MARKET_ZONE));
        assertThat(first.open()).isEqualTo(100.0);
        assertThat(first.high()).isEqualTo(102.0);
        assertThat(first.low()).isEqualTo(99.5);
        assertThat(first.close()).isEqualTo(101.5);
        assertThat(first.volume()).isEqualTo(40L);
        assertThat(first.wap()).isEqualTo((100.25 * 10.0 + 101.0 * 30.0) / 40.0);
        assertThat(first.putVolume()).isEqualTo(7L);
        assertThat(first.callVolume()).isEqualTo(11L);
        assertThat(first.bid()).isEqualTo(101.4);
        assertThat(first.ask()).isEqualTo(101.6);

        DatabentoHistoricalReplayProvider.ReplayBar second = provider.events().get(1);
        assertThat(second.timestamp()).isEqualTo(LocalDateTime.ofInstant(Instant.ofEpochSecond(nextBucketEpoch), MARKET_ZONE));
        assertThat(second.volume()).isEqualTo(20L);
        assertThat(second.putVolume()).isZero();
        assertThat(second.callVolume()).isZero();
    }
}

