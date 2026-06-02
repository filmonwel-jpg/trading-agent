package com.calgary.fili.trader.bot.storage;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TradeLogStoreTest {

    @Test
    void ensureSchemaSkipsIndexDdlWhenCurrentUserDoesNotOwnExistingTable() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.query(contains("FROM pg_class c"), any(RowMapper.class)))
            .thenReturn(List.of("table_owner"));
        when(jdbcTemplate.query(contains("SELECT current_user"), any(RowMapper.class)))
            .thenReturn(List.of("runtime_user"));

        TradeLogStore store = new TradeLogStore(jdbcTemplate);
        store.ensureSchema();

        verify(jdbcTemplate, times(1)).execute(contains("CREATE TABLE IF NOT EXISTS databento_live_trade_logs"));
        verify(jdbcTemplate, never()).execute(contains("idx_live_trade_logs_symbol_ts"));
        verify(jdbcTemplate, never()).execute(contains("idx_live_trade_logs_market_day_symbol"));
    }

    @Test
    void ensureSchemaCreatesIndexesWhenCurrentUserOwnsTable() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.query(contains("FROM pg_class c"), any(RowMapper.class)))
            .thenReturn(List.of("runtime_user"));
        when(jdbcTemplate.query(contains("SELECT current_user"), any(RowMapper.class)))
            .thenReturn(List.of("runtime_user"));

        TradeLogStore store = new TradeLogStore(jdbcTemplate);
        store.ensureSchema();

        verify(jdbcTemplate, times(1)).execute(contains("CREATE TABLE IF NOT EXISTS databento_live_trade_logs"));
        verify(jdbcTemplate, times(1)).execute(contains("idx_live_trade_logs_symbol_ts"));
        verify(jdbcTemplate, times(1)).execute(contains("idx_live_trade_logs_market_day_symbol"));
    }
}
