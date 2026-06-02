package com.calgary.fili.trader.bot.trader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedIbkrGatewayClientTest {

    @Test
    void clientSendsCommandAndReceivesAckAndEvent() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        BlockingQueue<SharedIbkrGatewayMessage> events = new LinkedBlockingQueue<>();

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            Thread serverThread = new Thread(() -> runStubGateway(serverSocket, objectMapper));
            serverThread.start();

            SharedIbkrGatewayClient client = new SharedIbkrGatewayClient(
                "127.0.0.1",
                serverSocket.getLocalPort(),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2),
                events::add
            );

            client.connect();
            SharedIbkrGatewayMessage ack = client.registerSymbol("AAPL", "AAPL:9082");
            SharedIbkrGatewayMessage event = events.poll(5, TimeUnit.SECONDS);
            client.disconnect();
            serverThread.join(5000);

            assertNotNull(ack);
            assertTrue(ack.ok);
            assertEquals("symbol-registered", ack.detail);
            assertEquals(101, ack.payloadInt("reqId"));
            assertNotNull(event);
            assertTrue(event.isEvent());
            assertEquals("position_updated", event.eventType);
            assertEquals("AAPL", event.symbol);
            assertEquals(5, event.position);
        }
    }

    @Test
    void timeoutErrorsIncludeExceptionTypeInsteadOfNullReason() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            Thread serverThread = new Thread(() -> runTimeoutGateway(serverSocket, objectMapper));
            serverThread.start();

            SharedIbkrGatewayClient client = new SharedIbkrGatewayClient(
                "127.0.0.1",
                serverSocket.getLocalPort(),
                Duration.ofSeconds(2),
                Duration.ofSeconds(1),
                message -> {}
            );

            client.connect();
            IOException error = assertThrows(IOException.class, () -> client.registerSymbol("AAPL", "AAPL:9082"));
            client.disconnect();
            serverThread.join(5000);

            assertTrue(error.getMessage().contains("TimeoutException after 1000ms waiting for ack"));
        }
    }

    private void runStubGateway(ServerSocket serverSocket, ObjectMapper objectMapper) {
        try (Socket socket = serverSocket.accept();
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            String pingLine = reader.readLine();
            Map<?, ?> ping = objectMapper.readValue(pingLine, Map.class);
            writer.write(objectMapper.writeValueAsString(Map.of(
                "type", "ack",
                "requestId", ping.get("requestId"),
                "ok", true,
                "detail", "pong",
                "payload", Map.of("connected", true)
            )));
            writer.newLine();
            writer.flush();

            String registerLine = reader.readLine();
            Map<?, ?> register = objectMapper.readValue(registerLine, Map.class);
            writer.write(objectMapper.writeValueAsString(Map.of(
                "type", "ack",
                "requestId", register.get("requestId"),
                "ok", true,
                "detail", "symbol-registered",
                "payload", Map.of("reqId", 101)
            )));
            writer.newLine();
            writer.write(objectMapper.writeValueAsString(Map.of(
                "type", "event",
                "eventType", "position_updated",
                "symbol", "AAPL",
                "position", 5,
                "avgCost", 100.5,
                "detail", "stub-position",
                "payload", Map.of("source", "stub")
            )));
            writer.newLine();
            writer.flush();
        } catch (Exception ignored) {
        }
    }

    private void runTimeoutGateway(ServerSocket serverSocket, ObjectMapper objectMapper) {
        try (Socket socket = serverSocket.accept();
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            String pingLine = reader.readLine();
            Map<?, ?> ping = objectMapper.readValue(pingLine, Map.class);
            writer.write(objectMapper.writeValueAsString(Map.of(
                "type", "ack",
                "requestId", ping.get("requestId"),
                "ok", true,
                "detail", "pong",
                "payload", Map.of("connected", true)
            )));
            writer.newLine();
            writer.flush();

            reader.readLine();
            Thread.sleep(1500L);
        } catch (Exception ignored) {
        }
    }
}

