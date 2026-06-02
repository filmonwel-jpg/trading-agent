package com.calgary.fili.trader.bot.controler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/api/stack")
public class StackStatusController {

    private static final int SOCKET_TIMEOUT_MS = 1500;
    private static final Pattern NOTIFICATION_PATTERN = Pattern.compile(
        "^\\[STACK-NOTIFY]\\[(?<timestamp>[^]]+)] event=(?<event>\\S+) status=(?<status>\\S+) title=(?<title>.*?) message=(?<message>.*)$"
    );

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    @Value("${server.port:8080}")
    private int serverPort;

    @Value("${trading.symbol:TSLA}")
    private String currentSymbol;

    @Value("${trading.host:127.0.0.1}")
    private String ibHost;

    @Value("${trading.port:7497}")
    private int ibPort;

    public StackStatusController(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    }

    @GetMapping("/overview")
    public Map<String, Object> overview() {
        List<Map<String, Object>> symbols = collectSymbolStatuses();
        Map<String, Object> harvester = collectHarvesterStatus();
        Map<String, Object> ibkr = collectIbkrStatus();
        Map<String, Object> lastNotification = readJsonFile(runtimeDir().resolve("stack-last-status.json"));
        Map<String, Object> notificationHistory = collectNotificationHistory();

        long connectedCount = symbols.stream().filter(symbol -> Boolean.TRUE.equals(symbol.get("connected"))).count();
        long healthyCount = symbols.stream().filter(symbol -> Boolean.TRUE.equals(symbol.get("healthOk"))).count();
        boolean symbolsOk = !symbols.isEmpty() && connectedCount == symbols.size();
        boolean overallOk = Boolean.TRUE.equals(ibkr.get("reachable"))
            && Boolean.TRUE.equals(harvester.get("running"))
            && symbolsOk;

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("symbolCount", symbols.size());
        summary.put("connectedCount", connectedCount);
        summary.put("healthyCount", healthyCount);
        summary.put("overallOk", overallOk);
        summary.put("status", overallOk ? "ok" : "attention");

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("generatedAt", Instant.now().toString());
        response.put("host", hostName());
        response.put("servingPort", serverPort);
        response.put("servingSymbol", currentSymbol);
        response.put("summary", summary);
        response.put("ibkr", ibkr);
        response.put("harvester", harvester);
        response.put("lastNotification", lastNotification);
        response.put("lastSuccessfulStartup", notificationHistory.getOrDefault("lastSuccessfulStartup", Map.of()));
        response.put("lastSuccessfulShutdown", notificationHistory.getOrDefault("lastSuccessfulShutdown", Map.of()));
        response.put("latestMorningLog", latestMatchingFileName("morning-launch-*.log"));
        response.put("latestAfternoonLog", latestMatchingFileName("afternoon-flatten-*.log"));
        response.put("symbols", symbols);
        return response;
    }

    private List<Map<String, Object>> collectSymbolStatuses() {
        List<Map<String, Object>> statuses = new ArrayList<>();
        for (Path path : listSymbolPropertyFiles()) {
            statuses.add(readSymbolStatus(path));
        }
        return statuses;
    }

    private List<Path> listSymbolPropertyFiles() {
        return listSymbolPropertyFiles(runtimeDir());
    }

    private List<Path> listSymbolPropertyFiles(Path rootRuntime) {
        Path databentoBots = rootRuntime.resolve("databento").resolve("bots");
        List<Path> databentoBotFiles = listTradingPropertyFiles(databentoBots);
        if (!databentoBotFiles.isEmpty()) {
            return databentoBotFiles;
        }
        return listTradingPropertyFiles(rootRuntime);
    }

    private List<Path> listTradingPropertyFiles(Path directory) {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        try (Stream<Path> paths = Files.list(directory)) {
            return paths
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().startsWith("trading-"))
                .filter(path -> path.getFileName().toString().endsWith(".properties"))
                .filter(path -> !isTemplatePropertyFile(path))
                .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                .toList();
        } catch (IOException ignored) {
            // Best-effort endpoint.
            return List.of();
        }
    }

    private boolean isTemplatePropertyFile(Path path) {
        String fileName = path.getFileName().toString().toLowerCase(Locale.US);
        return fileName.contains("template");
    }

    private Map<String, Object> readSymbolStatus(Path propertiesPath) {
        Map<String, String> props = readProperties(propertiesPath);
        String symbol = props.getOrDefault("trading.symbol", deriveSymbolFromPath(propertiesPath));
        int port = parseInt(props.get("server.port"));
        String statusBaseUrl = port > 0 ? "http://127.0.0.1:" + port : "";
        Map<String, Object> control = statusBaseUrl.isEmpty() ? Map.of() : getJson(statusBaseUrl + "/api/control/status");
        Map<String, Object> feedHealth = statusBaseUrl.isEmpty() ? Map.of() : getJson(statusBaseUrl + "/api/control/feed-health");
        Map<String, Object> health = statusBaseUrl.isEmpty() ? Map.of() : getJson(statusBaseUrl + "/actuator/health");
        boolean healthOk = "UP".equalsIgnoreCase(String.valueOf(health.getOrDefault("status", "")));
        if (!healthOk) {
            healthOk = databentoFeedHealthOk(control, feedHealth);
        }
        boolean connected = Boolean.TRUE.equals(control.get("connected"));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("symbol", symbol);
        result.put("port", port);
        result.put("clientId", props.getOrDefault("trading.client-id", ""));
        result.put("modelDir", props.getOrDefault("trading.model.dir", ""));
        result.put("appLogFile", props.getOrDefault("logging.file.name", ""));
        result.put("statusBaseUrl", statusBaseUrl);
        result.put("healthOk", healthOk);
        result.put("healthStatus", healthOk && health.isEmpty() ? "UP (feed-health-fallback)" : health.getOrDefault("status", "unknown"));
        result.put("connected", connected);
        result.put("marketDataProvider", control.getOrDefault("marketDataProvider", props.getOrDefault("trading.market-data.provider", "ibkr")));
        result.put("strategyEnabled", control.getOrDefault("strategyEnabled", false));
        result.put("killSwitch", control.getOrDefault("killSwitch", false));
        result.put("position", control.containsKey("currentPosition") ? control.get("currentPosition") : control.getOrDefault("position", 0));
        result.put("openOrders", control.getOrDefault("openOrders", 0));
        result.put("orderInFlight", control.getOrDefault("orderInFlight", false));
        result.put("positionSyncState", control.getOrDefault("positionSyncState", "unknown"));
        result.put("positionSyncComplete", control.getOrDefault("positionSyncComplete", false));
        result.put("lastOrderAction", control.getOrDefault("lastOrderAction", ""));
        result.put("databentoFeedHealthy", control.getOrDefault("databentoFeedHealthy", feedHealth.getOrDefault("healthy", false)));
        result.put("databentoGatewayRunning", feedHealth.getOrDefault("gatewayRunning", false));
        result.put("databentoRestartCount", feedHealth.getOrDefault("restartCount", 0));
        result.put("databentoFeed", feedHealth);
        result.put("controlReachable", !control.isEmpty());
        return result;
    }

    private boolean databentoFeedHealthOk(Map<String, Object> control, Map<String, Object> feedHealth) {
        if (control == null || control.isEmpty() || feedHealth == null || feedHealth.isEmpty()) {
            return false;
        }
        String provider = String.valueOf(control.getOrDefault("marketDataProvider", "")).trim().toLowerCase(Locale.US);
        if (!"databento".equals(provider)) {
            return false;
        }
        if (Boolean.TRUE.equals(feedHealth.get("healthy"))) {
            return true;
        }
        if (Boolean.TRUE.equals(feedHealth.get("withinStartupGrace"))) {
            return true;
        }
        return Boolean.FALSE.equals(feedHealth.get("marketDataExpectedNow"));
    }

    private Map<String, Object> collectHarvesterStatus() {
        List<Long> pids = ProcessHandle.allProcesses()
            .filter(process -> process.info().commandLine().orElse("").contains("harvester.py"))
            .map(ProcessHandle::pid)
            .toList();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("running", !pids.isEmpty());
        result.put("pidCount", pids.size());
        result.put("pids", pids);
        result.put("latestLog", latestMatchingFileName("harvester-scheduled-*.log"));
        return result;
    }

    private Map<String, Object> collectIbkrStatus() {
        boolean reachable = isSocketReachable(ibHost, ibPort);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("host", ibHost);
        result.put("port", ibPort);
        result.put("reachable", reachable);
        result.put("status", reachable ? "up" : "down");
        return result;
    }

    private Map<String, Object> collectNotificationHistory() {
        Path logPath = runtimeDir().resolve("stack-notifications.log");
        if (!Files.exists(logPath)) {
            return Map.of(
                "lastSuccessfulStartup", Map.of(),
                "lastSuccessfulShutdown", Map.of()
            );
        }

        Map<String, Object> lastSuccessfulStartup = Map.of();
        Map<String, Object> lastSuccessfulShutdown = Map.of();
        Map<String, Object> pending = null;

        try {
            for (String rawLine : Files.readAllLines(logPath)) {
                String line = rawLine.trim();
                Matcher matcher = NOTIFICATION_PATTERN.matcher(line);
                if (matcher.matches()) {
                    Map<String, Object> parsed = new LinkedHashMap<>();
                    parsed.put("timestamp", matcher.group("timestamp"));
                    parsed.put("event", matcher.group("event"));
                    parsed.put("status", matcher.group("status"));
                    parsed.put("title", matcher.group("title"));
                    parsed.put("message", matcher.group("message"));
                    pending = parsed;

                    if (isSuccessStatus(matcher.group("status"))) {
                        if ("morning-startup".equals(matcher.group("event"))) {
                            lastSuccessfulStartup = new LinkedHashMap<>(parsed);
                        } else if ("afternoon-shutdown".equals(matcher.group("event"))) {
                            lastSuccessfulShutdown = new LinkedHashMap<>(parsed);
                        }
                    }
                    continue;
                }

                if (pending != null && line.startsWith("[STACK-NOTIFY][") && line.contains("] details=")) {
                    String details = line.substring(line.indexOf("] details=") + 10).trim();
                    pending.put("details", details);
                    String event = String.valueOf(pending.getOrDefault("event", ""));
                    String status = String.valueOf(pending.getOrDefault("status", ""));
                    if (isSuccessStatus(status)) {
                        if ("morning-startup".equals(event) && !lastSuccessfulStartup.isEmpty()) {
                            lastSuccessfulStartup = new LinkedHashMap<>(pending);
                        } else if ("afternoon-shutdown".equals(event) && !lastSuccessfulShutdown.isEmpty()) {
                            lastSuccessfulShutdown = new LinkedHashMap<>(pending);
                        }
                    }
                }
            }
        } catch (IOException ignored) {
            return Map.of(
                "lastSuccessfulStartup", Map.of(),
                "lastSuccessfulShutdown", Map.of()
            );
        }

        return Map.of(
            "lastSuccessfulStartup", lastSuccessfulStartup,
            "lastSuccessfulShutdown", lastSuccessfulShutdown
        );
    }

    private boolean isSuccessStatus(String status) {
        return "ok".equalsIgnoreCase(status) || "success".equalsIgnoreCase(status);
    }

    private boolean isSocketReachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), SOCKET_TIMEOUT_MS);
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    private Map<String, Object> getJson(String url) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300 || response.body().isBlank()) {
                return Map.of();
            }
            return objectMapper.readValue(response.body(), new TypeReference<>() {
            });
        } catch (Exception ignored) {
            return Map.of();
        }
    }

    private Map<String, Object> readJsonFile(Path path) {
        try {
            if (!Files.exists(path) || Files.size(path) == 0) {
                return Map.of();
            }
            return objectMapper.readValue(Files.readString(path), new TypeReference<>() {
            });
        } catch (Exception ignored) {
            return Map.of();
        }
    }

    private Map<String, String> readProperties(Path path) {
        Map<String, String> props = new HashMap<>();
        try {
            for (String rawLine : Files.readAllLines(path)) {
                String line = rawLine.trim();
                if (line.startsWith("#") || !line.contains("=")) {
                    continue;
                }
                String[] parts = line.split("=", 2);
                props.put(parts[0].trim(), parts[1].trim());
            }
        } catch (IOException ignored) {
            return Map.of();
        }
        return props;
    }

    private String deriveSymbolFromPath(Path path) {
        String name = path.getFileName().toString();
        String stripped = name.replace("trading-", "").replace(".properties", "");
        return stripped.toUpperCase(Locale.US);
    }

    private int parseInt(String value) {
        try {
            return Integer.parseInt(Optional.ofNullable(value).orElse(""));
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private Path runtimeDir() {
        return Path.of("runtime").toAbsolutePath().normalize();
    }

    private String latestMatchingFileName(String glob) {
        try (Stream<Path> paths = Files.list(runtimeDir())) {
            return paths
                .filter(path -> path.getFileName().toString().matches(globToRegex(glob)))
                .max(Comparator.comparingLong(this::safeLastModified))
                .map(path -> path.toAbsolutePath().toString())
                .orElse("");
        } catch (IOException ignored) {
            return "";
        }
    }

    private long safeLastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException ignored) {
            return 0L;
        }
    }

    private String globToRegex(String glob) {
        return glob.replace(".", "\\.").replace("*", ".*");
    }

    private String hostName() {
        return Optional.ofNullable(System.getenv("HOSTNAME"))
            .filter(name -> !name.isBlank())
            .orElseGet(() -> System.getProperty("user.name", "trading-agent-host"));
    }
}



