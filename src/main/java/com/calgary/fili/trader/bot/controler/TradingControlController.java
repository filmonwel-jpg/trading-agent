package com.calgary.fili.trader.bot.controler;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.calgary.fili.trader.bot.trader.IBKRTrader;

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

    @GetMapping("/logs/stream")
    public SseEmitter streamLogs() {
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

    @PostMapping("/flatten")
    public Map<String, Object> flatten() {
        String result = trader.flattenPosition();
        return response(result);
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
