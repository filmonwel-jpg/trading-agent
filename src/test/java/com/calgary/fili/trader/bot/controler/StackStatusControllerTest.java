package com.calgary.fili.trader.bot.controler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StackStatusControllerTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    @Test
    void readSymbolStatusIncludesPositionSyncFieldsFromControlStatus() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/control/status", exchange -> respondJson(exchange, """
            {
              "connected": true,
              "marketDataProvider": "databento",
              "strategyEnabled": true,
              "killSwitch": false,
              "currentPosition": 15,
              "openOrders": 2,
              "orderInFlight": true,
              "positionSyncState": "failed:shared-gateway-request:ack-timeout",
              "positionSyncComplete": false,
              "lastOrderAction": "SELL",
              "databentoFeedHealthy": true
            }
            """));
        server.createContext("/api/control/feed-health", exchange -> respondJson(exchange, """
            {
              "healthy": true,
              "gatewayRunning": true,
              "restartCount": 3,
              "withinStartupGrace": false,
              "marketDataExpectedNow": true
            }
            """));
        server.createContext("/actuator/health", exchange -> respondJson(exchange, "{" +
            "\"status\":\"DOWN\"" +
            "}"));
        server.start();

        int port = server.getAddress().getPort();
        Path properties = Files.createTempFile("trading-aapl-", ".properties");
        Files.writeString(properties, String.join(System.lineSeparator(),
            "trading.symbol=AAPL",
            "server.port=" + port,
            "trading.client-id=77",
            "trading.model.dir=/tmp/models/aapl",
            "logging.file.name=/tmp/aapl.log"
        ));

        StackStatusController controller = new StackStatusController(new ObjectMapper());
        Map<String, Object> status = ReflectionTestUtils.invokeMethod(controller, "readSymbolStatus", properties);

        assertNotNull(status);
        assertEquals("AAPL", status.get("symbol"));
        assertEquals(port, status.get("port"));
        assertEquals(true, status.get("connected"));
        assertEquals(15, status.get("position"));
        assertEquals(2, status.get("openOrders"));
        assertEquals(true, status.get("orderInFlight"));
        assertEquals("failed:shared-gateway-request:ack-timeout", status.get("positionSyncState"));
        assertEquals(false, status.get("positionSyncComplete"));
        assertEquals("SELL", status.get("lastOrderAction"));
        assertEquals(true, status.get("databentoFeedHealthy"));
        assertEquals(true, status.get("databentoGatewayRunning"));
        assertEquals(3, status.get("databentoRestartCount"));
        assertEquals("DOWN", status.get("healthStatus"));
        assertEquals(true, status.get("healthOk"));
        assertEquals(true, status.get("controlReachable"));
    }

    @Test
    void readSymbolStatusDefaultsPositionSyncFieldsWhenControlStatusIsUnavailable() throws Exception {
        Path properties = Files.createTempFile("trading-msft-", ".properties");
        Files.writeString(properties, String.join(System.lineSeparator(),
            "trading.symbol=MSFT",
            "server.port=0"
        ));

        StackStatusController controller = new StackStatusController(new ObjectMapper());
        Map<String, Object> status = ReflectionTestUtils.invokeMethod(controller, "readSymbolStatus", properties);

        assertNotNull(status);
        assertEquals("MSFT", status.get("symbol"));
        assertEquals(0, status.get("port"));
        assertEquals("unknown", status.get("positionSyncState"));
        assertEquals(false, status.get("positionSyncComplete"));
        assertEquals(false, status.get("connected"));
        assertEquals(false, status.get("controlReachable"));
    }

    @Test
    void listSymbolPropertyFilesPrefersDatabentoBotsAndSkipsTemplates() throws Exception {
        Path runtimeDir = Files.createTempDirectory("stack-runtime-");
        Path databentoBots = Files.createDirectories(runtimeDir.resolve("databento").resolve("bots"));
        Path rootLegacy = runtimeDir.resolve("trading-tsla.properties");
        Path rootTemplate = runtimeDir.resolve("trading-databento-template.properties");
        Path aapl = databentoBots.resolve("trading-aapl.properties");
        Path spy = databentoBots.resolve("trading-spy.properties");
        Path botTemplate = databentoBots.resolve("trading-databento-template.properties");

        Files.writeString(rootLegacy, "trading.symbol=TSLA\nserver.port=8081\n");
        Files.writeString(rootTemplate, "trading.symbol=SPY\nserver.port=9081\n");
        Files.writeString(aapl, "trading.symbol=AAPL\nserver.port=9082\n");
        Files.writeString(spy, "trading.symbol=SPY\nserver.port=9081\n");
        Files.writeString(botTemplate, "trading.symbol=SPY\nserver.port=9081\n");

        StackStatusController controller = new StackStatusController(new ObjectMapper());
        List<Path> paths = ReflectionTestUtils.invokeMethod(controller, "listSymbolPropertyFiles", runtimeDir);

        assertEquals(List.of(aapl, spy), paths);
    }

    @Test
    void listSymbolPropertyFilesFallsBackToLegacyRootWhenDatabentoBotsAreMissingOrTemplatesOnly() throws Exception {
        Path runtimeDir = Files.createTempDirectory("stack-runtime-");
        Path databentoBots = Files.createDirectories(runtimeDir.resolve("databento").resolve("bots"));
        Path botTemplate = databentoBots.resolve("trading-databento-template.properties");
        Path nvda = runtimeDir.resolve("trading-nvda.properties");
        Path tsla = runtimeDir.resolve("trading-tsla.properties");

        Files.writeString(botTemplate, "trading.symbol=SPY\nserver.port=9081\n");
        Files.writeString(nvda, "trading.symbol=NVDA\nserver.port=8082\n");
        Files.writeString(tsla, "trading.symbol=TSLA\nserver.port=8081\n");

        StackStatusController controller = new StackStatusController(new ObjectMapper());
        List<Path> paths = ReflectionTestUtils.invokeMethod(controller, "listSymbolPropertyFiles", runtimeDir);

        assertEquals(List.of(nvda, tsla), paths);
    }

    private void respondJson(HttpExchange exchange, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, payload.length);
        try (exchange; OutputStream output = exchange.getResponseBody()) {
            output.write(payload);
        }
    }
}




