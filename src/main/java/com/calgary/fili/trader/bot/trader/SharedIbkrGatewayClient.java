package com.calgary.fili.trader.bot.trader;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Java-side client for the shared Python IBKR execution gateway.
 *
 * <p>This class exists so many symbol JVMs can share one broker connection indirectly. Commands are sent as
 * newline-delimited JSON, each with a request id, and acknowledgements are matched back via {@code pendingAcks}.
 * Asynchronous broker events (position updates, order status, disconnects) are delivered through the same socket
 * but bypass the ack map and are forwarded to the supplied {@code eventConsumer}.</p>
 *
 * <p>One subtle but important property is that {@code sendCommand(...)} is synchronized. That keeps per-client
 * command ordering simple and ensures request/ack correlation stays predictable even though async events may still
 * arrive between command acknowledgements.</p>
 */
public class SharedIbkrGatewayClient {

    private static final Logger log = LoggerFactory.getLogger(SharedIbkrGatewayClient.class);

    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final int ackTimeoutMs;
    private final Consumer<SharedIbkrGatewayMessage> eventConsumer;
    private final ObjectMapper objectMapper;
    private final Map<String, CompletableFuture<SharedIbkrGatewayMessage>> pendingAcks = new ConcurrentHashMap<>();

    private volatile Socket socket;
    private volatile BufferedReader reader;
    private volatile BufferedWriter writer;
    private volatile Thread readerThread;
    private volatile boolean running = false;

    public SharedIbkrGatewayClient(String host,
                                   int port,
                                   Duration connectTimeout,
                                   Duration ackTimeout,
                                   Consumer<SharedIbkrGatewayMessage> eventConsumer) {
        this.host = host;
        this.port = port;
        this.connectTimeoutMs = (int) Math.max(1000L, connectTimeout == null ? 5000L : connectTimeout.toMillis());
        this.ackTimeoutMs = (int) Math.max(1000L, ackTimeout == null ? 5000L : ackTimeout.toMillis());
        this.eventConsumer = eventConsumer == null ? message -> {} : eventConsumer;
        this.objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public synchronized void connect() throws IOException {
        // Connect also performs an immediate ping so callers learn about broken sockets or protocol mismatches
        // before they start routing live orders through this client.
        if (isConnected()) {
            return;
        }
        Socket connectedSocket = new Socket();
        connectedSocket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        connectedSocket.setTcpNoDelay(true);
        socket = connectedSocket;
        reader = new BufferedReader(new InputStreamReader(connectedSocket.getInputStream(), StandardCharsets.UTF_8));
        writer = new BufferedWriter(new OutputStreamWriter(connectedSocket.getOutputStream(), StandardCharsets.UTF_8));
        running = true;
        readerThread = new Thread(this::readerLoop, "shared-ibkr-gateway-reader");
        readerThread.start();
        SharedIbkrGatewayMessage ack = ping();
        if (!ack.ok) {
            throw new IOException("shared IBKR gateway ping failed detail=" + ack.detail);
        }
    }

    public synchronized void disconnect() {
        running = false;
        Socket liveSocket = socket;
        socket = null;
        if (liveSocket != null) {
            try {
                liveSocket.close();
            } catch (IOException ignored) {
            }
        }
        BufferedReader liveReader = reader;
        reader = null;
        if (liveReader != null) {
            try {
                liveReader.close();
            } catch (IOException ignored) {
            }
        }
        BufferedWriter liveWriter = writer;
        writer = null;
        if (liveWriter != null) {
            try {
                liveWriter.close();
            } catch (IOException ignored) {
            }
        }
        RuntimeException disconnected = new RuntimeException("shared IBKR gateway disconnected");
        pendingAcks.values().forEach(future -> future.completeExceptionally(disconnected));
        pendingAcks.clear();
    }

    public boolean isConnected() {
        Socket liveSocket = socket;
        return running && liveSocket != null && liveSocket.isConnected() && !liveSocket.isClosed();
    }

    public SharedIbkrGatewayMessage ping() throws IOException {
        return sendCommand("ping", "", Map.of());
    }

    public SharedIbkrGatewayMessage registerSymbol(String symbol, String strategyId) throws IOException {
        return sendCommand(
            "register_symbol",
            symbol,
            Map.of("strategyId", strategyId == null ? "" : strategyId)
        );
    }

    public SharedIbkrGatewayMessage requestPositionSync(String reason) throws IOException {
        return sendCommand("request_position_sync", "", Map.of("reason", reason == null ? "" : reason));
    }

    public SharedIbkrGatewayMessage requestOpenOrdersSync(String reason) throws IOException {
        return sendCommand("request_open_orders_sync", "", Map.of("reason", reason == null ? "" : reason));
    }

    public SharedIbkrGatewayMessage submitOrder(String symbol,
                                                String action,
                                                int quantity,
                                                String orderType,
                                                double referencePrice,
                                                Double limitPrice,
                                                String reason,
                                                Map<String, Object> metadata) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("action", action == null ? "" : action);
        payload.put("quantity", quantity);
        payload.put("orderType", orderType == null ? "MKT" : orderType);
        payload.put("referencePrice", referencePrice);
        if (limitPrice != null) {
            payload.put("limitPrice", limitPrice);
        }
        payload.put("tif", "IOC");
        payload.put("reason", reason == null ? "" : reason);
        payload.put("metadata", metadata == null ? Map.of() : metadata);
        return sendCommand("submit_order", symbol, payload);
    }

    public SharedIbkrGatewayMessage cancelSymbolOrders(String symbol, String reason) throws IOException {
        return sendCommand("cancel_symbol_orders", symbol, Map.of("reason", reason == null ? "" : reason));
    }

    public SharedIbkrGatewayMessage flattenSymbol(String symbol,
                                                  double referencePrice,
                                                  Integer quantityOverride,
                                                  String reason) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("referencePrice", referencePrice);
        if (quantityOverride != null) {
            payload.put("quantityOverride", quantityOverride);
        }
        payload.put("reason", reason == null ? "" : reason);
        return sendCommand("flatten_symbol", symbol, payload);
    }

    public SharedIbkrGatewayMessage snapshot() throws IOException {
        return sendCommand("snapshot", "", Map.of());
    }

    public synchronized SharedIbkrGatewayMessage sendCommand(String command,
                                                             String symbol,
                                                             Map<String, Object> payload) throws IOException {
        // All protocol commands are wrapped in the same request envelope so the Python server can treat them
        // uniformly. The returned future is completed either by the matching ack or exceptionally on disconnect.
        if (!isConnected()) {
            throw new IOException("shared IBKR gateway is not connected");
        }
        String requestId = UUID.randomUUID().toString();
        CompletableFuture<SharedIbkrGatewayMessage> future = new CompletableFuture<>();
        pendingAcks.put(requestId, future);

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("type", "command");
        envelope.put("requestId", requestId);
        envelope.put("command", command);
        if (symbol != null && !symbol.isBlank()) {
            envelope.put("symbol", symbol);
        }
        envelope.put("payload", payload == null ? Map.of() : payload);

        try {
            BufferedWriter liveWriter = writer;
            if (liveWriter == null) {
                throw new IOException("shared IBKR gateway writer is unavailable");
            }
            liveWriter.write(objectMapper.writeValueAsString(envelope));
            liveWriter.newLine();
            liveWriter.flush();
            return future.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            pendingAcks.remove(requestId);
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("shared IBKR gateway command failed command=" + command + " reason=" + formatFailureReason(e), e);
        }
    }

    private String formatFailureReason(Exception e) {
        if (e instanceof TimeoutException) {
            return "TimeoutException after " + ackTimeoutMs + "ms waiting for ack";
        }
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            return e.getClass().getSimpleName();
        }
        return e.getClass().getSimpleName() + ": " + message;
    }

    private void readerLoop() {
        // Single reader thread for the socket. Acks complete blocked command futures; events are pushed upstream to
        // the trader so Java-side state can stay synchronized with the shared broker connection.
        try {
            String line;
            BufferedReader liveReader = reader;
            if (liveReader == null) {
                return;
            }
            while (running && (line = liveReader.readLine()) != null) {
                String payload = line.trim();
                if (payload.isEmpty()) {
                    continue;
                }
                SharedIbkrGatewayMessage message = objectMapper.readValue(payload, SharedIbkrGatewayMessage.class);
                if (message.isAck()) {
                    CompletableFuture<SharedIbkrGatewayMessage> future = pendingAcks.remove(message.requestId);
                    if (future != null) {
                        future.complete(message);
                    }
                } else if (message.isEvent()) {
                    eventConsumer.accept(message);
                } else {
                    log.info(">>> [FLOW][INFO][IBKR.GATEWAY] unclassified message payload={}", payload);
                }
            }
        } catch (Exception e) {
            if (running) {
                log.error(">>> [ERROR][IBKR.GATEWAY] reader failed reason={}", e.getMessage(), e);
            }
        } finally {
            disconnect();
        }
    }
}

