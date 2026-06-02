package com.calgary.fili.trader.bot.trader;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DatabentoLiveGatewayTest {

    @Test
    void startPassesEnvironmentOverridesAndWorkingDirectoryToChildProcess() throws Exception {
        String pythonBin = resolvePython3();
        assumeTrue(pythonBin != null && !pythonBin.isBlank(), "python3 is required for this test");

        Path workingDirectory = Files.createTempDirectory("databento-gateway-test-");
        BlockingQueue<DatabentoEvent> events = new LinkedBlockingQueue<>();
        BlockingQueue<Integer> exitCodes = new LinkedBlockingQueue<>();
        AtomicReference<String> stderrLine = new AtomicReference<>();

        DatabentoLiveGateway gateway = new DatabentoLiveGateway(
            List.of(
                pythonBin,
                "-c",
                "import json, os, pathlib; print(json.dumps({'event':'status','message': os.getenv('DATABENTO_API_KEY', '') + '|' + pathlib.Path.cwd().name}))"
            ),
            workingDirectory,
            Map.of("DATABENTO_API_KEY", "unit-test-key"),
            events::add,
            stderrLine::set,
            (exitCode, unexpected) -> exitCodes.add(exitCode)
        );

        gateway.start();

        DatabentoEvent event = events.poll(5, TimeUnit.SECONDS);
        Integer exitCode = exitCodes.poll(5, TimeUnit.SECONDS);

        assertNotNull(event);
        assertTrue(event.isStatus());
        assertEquals("unit-test-key|" + workingDirectory.getFileName(), event.message);
        assertEquals(0, exitCode);
        assertNull(stderrLine.get());
    }

    private String resolvePython3() {
        ProcessBuilder builder = new ProcessBuilder("python3", "-c", "print('ok')");
        try {
            Process process = builder.start();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return null;
            }
            return process.exitValue() == 0 ? "python3" : null;
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}


