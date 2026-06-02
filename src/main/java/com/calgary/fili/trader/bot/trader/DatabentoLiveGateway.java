package com.calgary.fili.trader.bot.trader;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Small lifecycle wrapper around the live Databento event source.
 *
 * <p>This class intentionally hides two runtime modes behind the same interface:</p>
 * <ul>
 *   <li><strong>relay mode</strong>: connect to the shared local relay over TCP and consume already-normalized JSON</li>
 *   <li><strong>subprocess mode</strong>: launch the Python normalizer directly and read its stdout</li>
 * </ul>
 *
 * <p>The rest of the Java trading stack should not need to care where the JSON line came from. It should only
 * care that each line can be parsed into a {@link DatabentoEvent} and forwarded to the owning symbol trader.</p>
 *
 * <p>Threading model:</p>
 * <ul>
 *   <li>one reader thread consumes stdout / relay socket lines</li>
 *   <li>one optional stderr thread consumes subprocess stderr</li>
 *   <li>one wait thread turns process/socket termination into an exit callback</li>
 * </ul>
 */
public class DatabentoLiveGateway {

    private static final Logger log = LoggerFactory.getLogger(DatabentoLiveGateway.class);

    private final List<String> command;
    private final Path workingDirectory;
    private final Map<String, String> environmentOverrides;
    private final String relayHost;
    private final int relayPort;
    private final String relaySymbol;
    private final String relayClientKey;
    private final Consumer<DatabentoEvent> eventConsumer;
    private final Consumer<String> stderrConsumer;
    private final BiConsumer<Integer, Boolean> exitConsumer;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Process process;
    private volatile Socket relaySocket;
    private volatile BufferedWriter relayWriter;
    private volatile Thread stdoutThread;
    private volatile Thread stderrThread;
    private volatile Thread waitThread;

    public DatabentoLiveGateway(List<String> command,
                                Consumer<DatabentoEvent> eventConsumer,
                                Consumer<String> stderrConsumer,
                                BiConsumer<Integer, Boolean> exitConsumer) {
        this(command, null, Map.of(), eventConsumer, stderrConsumer, exitConsumer);
    }

    public DatabentoLiveGateway(List<String> command,
                                Path workingDirectory,
                                Map<String, String> environmentOverrides,
                                Consumer<DatabentoEvent> eventConsumer,
                                Consumer<String> stderrConsumer,
                                BiConsumer<Integer, Boolean> exitConsumer) {
        this(command, workingDirectory, environmentOverrides, null, 0, null, null, eventConsumer, stderrConsumer, exitConsumer);
    }

    public static DatabentoLiveGateway forRelay(String relayHost,
                                                int relayPort,
                                                String relaySymbol,
                                                String relayClientKey,
                                                Consumer<DatabentoEvent> eventConsumer,
                                                Consumer<String> stderrConsumer,
                                                BiConsumer<Integer, Boolean> exitConsumer) {
        return new DatabentoLiveGateway(null, null, Map.of(), relayHost, relayPort, relaySymbol, relayClientKey, eventConsumer, stderrConsumer, exitConsumer);
    }

    private DatabentoLiveGateway(List<String> command,
                                 Path workingDirectory,
                                 Map<String, String> environmentOverrides,
                                 String relayHost,
                                 int relayPort,
                                 String relaySymbol,
                                 String relayClientKey,
                                 Consumer<DatabentoEvent> eventConsumer,
                                 Consumer<String> stderrConsumer,
                                 BiConsumer<Integer, Boolean> exitConsumer) {
        this.command = command == null ? null : List.copyOf(command);
        this.workingDirectory = workingDirectory;
        this.environmentOverrides = environmentOverrides == null ? Map.of() : Map.copyOf(environmentOverrides);
        this.relayHost = relayHost;
        this.relayPort = relayPort;
        this.relaySymbol = relaySymbol == null ? "" : relaySymbol.trim();
        this.relayClientKey = relayClientKey == null ? "" : relayClientKey.trim();
        this.eventConsumer = eventConsumer;
        this.stderrConsumer = stderrConsumer;
        this.exitConsumer = exitConsumer;
        this.objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private boolean relayMode() {
        return relayHost != null && !relayHost.isBlank() && relayPort > 0;
    }

    public synchronized void start() throws IOException {
        // The gateway is start-once per lifecycle instance. Callers that want a restart should stop and create/assign
        // a fresh instance at the trader level so stream teardown and health state are explicit.
        if (running.get()) {
            log.info(">>> [FLOW][INFO][DATABENTO] Live gateway already running");
            return;
        }

        if (relayMode()) {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(relayHost, relayPort), 3000);
            socket.setTcpNoDelay(true);
            relaySocket = socket;
            relayWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            registerRelayClient();
            running.set(true);

            stdoutThread = new Thread(() -> {
                try {
                    readStdout(socket.getInputStream());
                } catch (IOException e) {
                    if (running.get()) {
                        log.error(">>> [ERROR][DATABENTO] relay input stream failed reason={}", e.getMessage(), e);
                    }
                }
            }, "databento-live-relay-stdout");
            waitThread = new Thread(this::waitForRelayExit, "databento-live-relay-wait");

            stdoutThread.start();
            waitThread.start();

            log.info(">>> [FLOW][INFO][DATABENTO] Connected to shared relay host={} port={} symbol={} clientKey={}", relayHost, relayPort, relaySymbol, relayClientKey.isBlank() ? relaySymbol : relayClientKey);
            return;
        }

        ProcessBuilder builder = new ProcessBuilder(command);
        if (workingDirectory != null) {
            builder.directory(workingDirectory.toFile());
        }
        if (!environmentOverrides.isEmpty()) {
            builder.environment().putAll(environmentOverrides);
        }
        builder.redirectErrorStream(false);
        process = builder.start();
        running.set(true);

        stdoutThread = new Thread(() -> readStdout(process.getInputStream()), "databento-live-stdout");
        stderrThread = new Thread(() -> readStderr(process.getErrorStream()), "databento-live-stderr");
        waitThread = new Thread(this::waitForExit, "databento-live-wait");

        stdoutThread.start();
        stderrThread.start();
        waitThread.start();

        log.info(">>> [FLOW][INFO][DATABENTO] Started live gateway command={}", String.join(" ", command));
    }

    public synchronized void stop() {
        // Stop is intentionally aggressive because lingering relay sockets or orphaned Python normalizers can cause
        // duplicate market-data streams, misleading feed-health signals, and confusing reconnect behavior.
        running.set(false);
        Process liveProcess = process;
        if (liveProcess != null && liveProcess.isAlive()) {
            liveProcess.destroy();
            try {
                if (!liveProcess.waitFor(3, java.util.concurrent.TimeUnit.SECONDS)) {
                    liveProcess.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        BufferedWriter liveRelayWriter = relayWriter;
        relayWriter = null;
        if (liveRelayWriter != null) {
            try {
                liveRelayWriter.close();
            } catch (IOException ignored) {
            }
        }
        Socket liveRelaySocket = relaySocket;
        if (liveRelaySocket != null) {
            try {
                liveRelaySocket.close();
            } catch (IOException ignored) {
            }
        }
    }

    public boolean isRunning() {
        if (relayMode()) {
            Socket liveRelaySocket = relaySocket;
            return running.get() && liveRelaySocket != null && liveRelaySocket.isConnected() && !liveRelaySocket.isClosed();
        }
        Process liveProcess = process;
        return running.get() && liveProcess != null && liveProcess.isAlive();
    }

    private void readStdout(InputStream stream) {
        // All live market-data payloads ultimately funnel through here as newline-delimited JSON.
        // Parsing failures are logged but do not kill the reader loop; a single malformed line should not take down
        // the entire market-data stream for the symbol.
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while (running.get() && (line = reader.readLine()) != null) {
                String payload = line.trim();
                if (payload.isEmpty()) {
                    continue;
                }
                try {
                    DatabentoEvent event = objectMapper.readValue(payload, DatabentoEvent.class);
                    eventConsumer.accept(event);
                } catch (Exception e) {
                    log.error(">>> [ERROR][DATABENTO] Failed to parse event payload={} reason={}", payload, e.getMessage(), e);
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                log.error(">>> [ERROR][DATABENTO] stdout reader failed reason={}", e.getMessage(), e);
            }
        }
    }

    private void readStderr(InputStream stream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stderrConsumer.accept(line);
            }
        } catch (IOException e) {
            if (running.get()) {
                log.error(">>> [ERROR][DATABENTO] stderr reader failed reason={}", e.getMessage(), e);
            }
        }
    }

    private void waitForExit() {
        Process liveProcess = process;
        if (liveProcess == null) {
            return;
        }
        try {
            int exitCode = liveProcess.waitFor();
            boolean wasRunning = running.getAndSet(false);
            log.info(">>> [FLOW][INFO][DATABENTO] Live gateway exited code={} running={}", exitCode, wasRunning);
            if (exitConsumer != null) {
                exitConsumer.accept(exitCode, wasRunning);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void waitForRelayExit() {
        // In relay mode there is no child process to wait on. Instead we treat reader termination as relay-stream
        // termination and synthesize a negative exit code so the higher-level trader can reuse the same restart path.
        Thread liveStdoutThread = stdoutThread;
        if (liveStdoutThread == null) {
            return;
        }
        try {
            liveStdoutThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        boolean wasRunning = running.getAndSet(false);
        relaySocket = null;
        relayWriter = null;
        log.info(">>> [FLOW][INFO][DATABENTO] Shared relay stream ended running={}", wasRunning);
        if (exitConsumer != null) {
            exitConsumer.accept(-2, wasRunning);
        }
    }

    private void registerRelayClient() throws IOException {
        BufferedWriter writer = relayWriter;
        if (writer == null) {
            throw new IOException("shared relay writer unavailable during registration");
        }
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("type", "register");
        envelope.put("symbol", relaySymbol);
        envelope.put("clientKey", relayClientKey.isBlank() ? relaySymbol : relayClientKey);
        writer.write(objectMapper.writeValueAsString(envelope));
        writer.newLine();
        writer.flush();
    }
}

