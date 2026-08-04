package com.calgary.fili.trader.bot.controler;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.calgary.fili.trader.bot.trader.IBKRTrader;

/**
 * Operational control surface for one symbol bot.
 *
 * <p>This controller is not strategy logic; it is the runtime operator interface. It exposes:</p>
 * <ul>
 *   <li>read-only status and feed-health endpoints for dashboards/health checks</li>
 *   <li>mutating controls such as pause, resume, flatten, cancel-open-orders, and kill switch</li>
 *   <li>SSE log streams for both trade logs and the main application log</li>
 * </ul>
 *
 * <p>Because each bot process owns only one symbol, every endpoint here acts on the single injected
 * {@link IBKRTrader} instance. There is no multi-symbol routing layer inside this controller.</p>
 */
@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/api/control")
public class TradingControlController {

    private final IBKRTrader trader;
    private final ScheduledExecutorService logStreamExecutor = Executors.newScheduledThreadPool(2);
    private final AtomicBoolean logTailerRunning = new AtomicBoolean(false);
    private final AtomicBoolean appLogTailerRunning = new AtomicBoolean(false);

    @Value("${logging.file.name:runtime/trading-agent.log}")
    private String appLogFile;

    public TradingControlController(IBKRTrader trader) {
        this.trader = trader;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return trader.controlStatus();
    }

    @GetMapping("/feed-health")
    public Map<String, Object> feedHealth() {
        return trader.databentoFeedHealthStatus();
    }

    @GetMapping("/logs/db/recent")
    public List<Map<String, Object>> recentDatabaseTradeLogs(
        @RequestParam(name = "symbol", required = false) String symbol,
        @RequestParam(name = "limit", defaultValue = "200") int limit
    ) {
        return trader.recentTradeLogs(symbol, limit);
    }

    @GetMapping("/logs/stream")
    public SseEmitter streamLogs() {
        // Trade-log streaming is implemented as tail-following over the log file rather than polling the database.
        // New subscribers first receive a recent in-memory backlog, then continue on a shared tailer thread.
        SseEmitter emitter = LogStreamHub.registerEmitter();
        String logFile = trader.getTradeLogFile();
        try {
            emitter.send(SseEmitter.event().name("meta").data("streaming=" + logFile));
            for (String line : LogStreamHub.recent(500)) {
                emitter.send(SseEmitter.event().name("log").data(line));
            }
        } catch (IOException ioException) {
            emitter.completeWithError(ioException);
            return emitter;
        }

        startLogTailerIfNeeded(logFile);
        return emitter;
    }

    @GetMapping("/logs/app/stream")
    public SseEmitter streamAppLogs() {
        SseEmitter emitter = AppLogStreamHub.registerEmitter();
        try {
            emitter.send(SseEmitter.event().name("meta").data("streaming=" + appLogFile));
            for (String line : AppLogStreamHub.recent(500)) {
                emitter.send(SseEmitter.event().name("log").data(line));
            }
        } catch (IOException ioException) {
            emitter.completeWithError(ioException);
            return emitter;
        }

        startAppLogTailerIfNeeded(appLogFile);
        return emitter;
    }

    private void startLogTailerIfNeeded(String logFile) {
        // Only one tailer thread per log stream should exist inside the process; otherwise multiple tailers would
        // repeatedly reread the same file and fan out duplicate log lines to all SSE subscribers.
        if (!logTailerRunning.compareAndSet(false, true)) {
            return;
        }

        logStreamExecutor.execute(() -> {
            long filePointer = 0L;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    try (RandomAccessFile reader = new RandomAccessFile(logFile, "r")) {
                        long fileLength = reader.length();
                        if (fileLength < filePointer) {
                            filePointer = 0L;
                        }
                        reader.seek(filePointer);

                        String line;
                        while ((line = reader.readLine()) != null) {
                            LogStreamHub.publish(line);
                        }
                        filePointer = reader.getFilePointer();
                        TimeUnit.MILLISECONDS.sleep(1000);
                    } catch (IOException ioException) {
                        TimeUnit.MILLISECONDS.sleep(1000);
                    }
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            } finally {
                logTailerRunning.set(false);
            }
        });
    }

    private void startAppLogTailerIfNeeded(String logFile) {
        if (!appLogTailerRunning.compareAndSet(false, true)) {
            return;
        }

        logStreamExecutor.execute(() -> {
            long filePointer = 0L;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    try (RandomAccessFile reader = new RandomAccessFile(logFile, "r")) {
                        long fileLength = reader.length();
                        if (fileLength < filePointer) {
                            filePointer = 0L;
                        }
                        reader.seek(filePointer);

                        String line;
                        while ((line = reader.readLine()) != null) {
                            AppLogStreamHub.publish(line);
                        }
                        filePointer = reader.getFilePointer();
                        TimeUnit.MILLISECONDS.sleep(1000);
                    } catch (IOException ioException) {
                        TimeUnit.MILLISECONDS.sleep(1000);
                    }
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            } finally {
                appLogTailerRunning.set(false);
            }
        });
    }

    @PostMapping("/pause")
    public Map<String, Object> pause() {
        boolean paused = trader.pauseTrading();
        return response(paused ? "strategy-paused" : "pause-failed");
    }

    @PostMapping("/resume")
    public Map<String, Object> resume() {
        boolean resumed = trader.resumeTrading();
        return response(resumed ? "strategy-resumed" : "resume-failed");
    }

    @PostMapping("/kill-switch/{enabled}")
    public Map<String, Object> killSwitch(@PathVariable boolean enabled) {
        trader.setRuntimeKillSwitch(enabled);
        return response("runtime-kill-switch=" + enabled);
    }

    @PostMapping("/cancel-open-orders")
    public Map<String, Object> cancelOpenOrders() {
        int cancelled = trader.cancelOpenOrders();
        return response("cancel-requested-for=" + cancelled);
    }

    @GetMapping("/clear-locks")
    public Map<String, Object> clearLocks() {
        Map<String, Object> response = new HashMap<>();
        response.putAll(trader.forceClearLocks());
        response.put("status", trader.controlStatus());
        return response;
    }

    @PostMapping("/position-sync")
    public Map<String, Object> positionSync(@RequestParam(name = "reason", defaultValue = "manual-control") String reason) {
        String normalizedReason = (reason == null || reason.isBlank()) ? "manual-control" : reason.trim();
        trader.requestPositions(normalizedReason);
        return response("position-sync-requested reason=" + normalizedReason);
    }

    @PostMapping("/flatten")
    public Map<String, Object> flatten() {
        // Flatten is intentionally exposed as an operator endpoint because it is the main emergency/manual recovery
        // action when a bot appears stuck or the session is about to close.
        String result = trader.flattenPosition();
        return response(result);
    }

    @PostMapping("/shared-capital/reset")
    public Map<String, Object> resetSharedCapital() {
        return trader.resetSharedCapitalReservations(false);
    }

    @PostMapping("/shared-capital/reset/{force}")
    public Map<String, Object> resetSharedCapital(@PathVariable boolean force) {
        return trader.resetSharedCapitalReservations(force);
    }

    @PostMapping("/state/reset-daily")
    public Map<String, Object> resetDailyStrategyState(@RequestParam(name = "reason", defaultValue = "manual") String reason) {
        return trader.resetDailyStrategyState(reason);
    }

    @PostMapping("/switch/{newSymbol}")
    public Map<String, Object> switchSymbol(@PathVariable String newSymbol) {
        return trader.manualSwitchSymbol(newSymbol);
    }

    private Map<String, Object> response(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("message", message);
        response.put("status", trader.controlStatus());
        return response;
    }

    @PreDestroy
    public void stopLogStreamingExecutor() {
        logStreamExecutor.shutdownNow();
    }
}
