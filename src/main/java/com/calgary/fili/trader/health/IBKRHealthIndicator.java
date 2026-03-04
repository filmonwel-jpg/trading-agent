package com.calgary.fili.trader.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import com.calgary.fili.trader.bot.trader.IBKRTrader;

@Component
public class IBKRHealthIndicator implements HealthIndicator {

    private final IBKRTrader trader;

    public IBKRHealthIndicator(IBKRTrader trader) {
        this.trader = trader;
    }

    @Override
    public Health health() {
        // Check if IBKR client is connected
        boolean connected = trader.isConnected();

        if (connected) {
            return Health.up()
                    .withDetail("connection", "established")
                    .withDetail("clientId", trader.getClientId())
                    .build();
        } else {
            return Health.down()
                    .withDetail("connection", "disconnected")
                    .withDetail("lastError", "IBKR TWS connection lost")
                    .build();
        }
    }
}