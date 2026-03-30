package com.calgary.fili.trader.bot.storage;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class TradeLogStore {

    private static final Logger log = LoggerFactory.getLogger(TradeLogStore.class);
    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final DateTimeFormatter MARKET_TS_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV");

    private final JdbcTemplate jdbcTemplate;

    public TradeLogStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void ensureSchema() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS live_trade_logs (
                event_key TEXT PRIMARY KEY,
                trade_ts TIMESTAMPTZ NOT NULL,
                market_day DATE NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                exit_price DOUBLE PRECISION NOT NULL,
                trade_pnl DOUBLE PRECISION NOT NULL,
                cumulative_pnl DOUBLE PRECISION NOT NULL,
                log_file TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """);
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_live_trade_logs_symbol_ts ON live_trade_logs (symbol, trade_ts DESC)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_live_trade_logs_market_day_symbol ON live_trade_logs (market_day, symbol)");
    }

    public boolean saveTrade(String formattedTimestamp,
                             String symbol,
                             String action,
                             int quantity,
                             double entryPrice,
                             double exitPrice,
                             double tradePnL,
                             double cumulativePnL,
                             String logFile) {
        try {
            ZonedDateTime tradeTs = ZonedDateTime.parse(formattedTimestamp, MARKET_TS_FORMAT).withZoneSameInstant(MARKET_ZONE);
            LocalDate marketDay = tradeTs.toLocalDate();
            String eventKey = eventKey(tradeTs, symbol, action, quantity, entryPrice, exitPrice, tradePnL, cumulativePnL);
            jdbcTemplate.update(
                """
                INSERT INTO live_trade_logs (
                    event_key, trade_ts, market_day, symbol, action, quantity,
                    entry_price, exit_price, trade_pnl, cumulative_pnl, log_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_key) DO UPDATE SET
                    cumulative_pnl = EXCLUDED.cumulative_pnl,
                    log_file = EXCLUDED.log_file
                """,
                eventKey,
                tradeTs.toOffsetDateTime(),
                marketDay,
                normalizeSymbol(symbol),
                safeText(action),
                Math.max(0, quantity),
                entryPrice,
                exitPrice,
                tradePnL,
                cumulativePnL,
                safeText(logFile)
            );
            return true;
        } catch (Exception exception) {
            log.error("Failed to persist live trade log symbol={} action={} qty={} reason={}", symbol, action, quantity, exception.getMessage(), exception);
            return false;
        }
    }

    public List<Map<String, Object>> recentTrades(String symbol, int limit) {
        String normalizedSymbol = normalizeSymbol(symbol);
        int safeLimit = Math.max(1, Math.min(limit, 1000));
        if (normalizedSymbol.isEmpty()) {
            return jdbcTemplate.queryForList(
                """
                SELECT trade_ts, market_day, symbol, action, quantity,
                       entry_price, exit_price, trade_pnl, cumulative_pnl,
                       log_file, created_at
                FROM live_trade_logs
                ORDER BY trade_ts DESC, created_at DESC
                LIMIT ?
                """,
                safeLimit
            );
        }
        return jdbcTemplate.queryForList(
            """
            SELECT trade_ts, market_day, symbol, action, quantity,
                   entry_price, exit_price, trade_pnl, cumulative_pnl,
                   log_file, created_at
            FROM live_trade_logs
            WHERE symbol = ?
            ORDER BY trade_ts DESC, created_at DESC
            LIMIT ?
            """,
            normalizedSymbol,
            safeLimit
        );
    }

    private String eventKey(ZonedDateTime tradeTs,
                            String symbol,
                            String action,
                            int quantity,
                            double entryPrice,
                            double exitPrice,
                            double tradePnL,
                            double cumulativePnL) throws Exception {
        String raw = tradeTs.toInstant() + "|"
            + normalizeSymbol(symbol) + "|"
            + safeText(action) + "|"
            + quantity + "|"
            + String.format(Locale.US, "%.4f", entryPrice) + "|"
            + String.format(Locale.US, "%.4f", exitPrice) + "|"
            + String.format(Locale.US, "%.4f", tradePnL) + "|"
            + String.format(Locale.US, "%.4f", cumulativePnL);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(raw.getBytes(StandardCharsets.UTF_8)));
    }

    private String normalizeSymbol(String symbol) {
        return symbol == null ? "" : symbol.trim().toUpperCase(Locale.US);
    }

    private String safeText(String value) {
        return value == null ? "" : value.trim();
    }
}

