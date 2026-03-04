package com.calgary.fili.trader.bot.controler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

public final class AppLogStreamHub {

    private static final int MAX_BUFFER = 2000;
    private static final Deque<String> RECENT = new ArrayDeque<>();
    private static final CopyOnWriteArrayList<SseEmitter> EMITTERS = new CopyOnWriteArrayList<>();
    private static final AtomicLong EVENT_ID = new AtomicLong(0);

    private AppLogStreamHub() {}

    public static SseEmitter registerEmitter() {
        SseEmitter emitter = new SseEmitter(0L);
        EMITTERS.add(emitter);
        emitter.onCompletion(() -> EMITTERS.remove(emitter));
        emitter.onTimeout(() -> EMITTERS.remove(emitter));
        emitter.onError(ex -> EMITTERS.remove(emitter));
        return emitter;
    }

    public static void publish(String line) {
        if (line == null || line.isBlank()) {
            return;
        }

        synchronized (RECENT) {
            RECENT.addLast(line);
            while (RECENT.size() > MAX_BUFFER) {
                RECENT.removeFirst();
            }
        }

        long id = EVENT_ID.incrementAndGet();
        for (SseEmitter emitter : EMITTERS) {
            try {
                emitter.send(SseEmitter.event().id(String.valueOf(id)).name("log").data(line));
            } catch (IOException | IllegalStateException sendError) {
                EMITTERS.remove(emitter);
            }
        }
    }

    public static List<String> recent(int limit) {
        int effective = Math.max(1, Math.min(limit, MAX_BUFFER));
        List<String> snapshot;
        synchronized (RECENT) {
            snapshot = new ArrayList<>(RECENT);
        }

        if (snapshot.size() <= effective) {
            return snapshot;
        }
        return snapshot.subList(snapshot.size() - effective, snapshot.size());
    }
}
