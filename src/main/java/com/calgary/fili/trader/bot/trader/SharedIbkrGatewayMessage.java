package com.calgary.fili.trader.bot.trader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SharedIbkrGatewayMessage {
    public String type = "";
    public String requestId = "";
    public String command = "";
    public boolean ok = false;
    public String detail = "";
    public String eventType = "";
    public String symbol = "";
    public Integer gatewayOrderId;
    public Integer ibOrderId;
    public Integer permId;
    public Integer reqId;
    public Integer position;
    public Double avgCost;
    public Map<String, Object> payload = new LinkedHashMap<>();

    public boolean isAck() {
        return "ack".equalsIgnoreCase(type);
    }

    public boolean isEvent() {
        return "event".equalsIgnoreCase(type);
    }

    public String payloadString(String key) {
        Object value = payload == null ? null : payload.get(key);
        return value == null ? "" : String.valueOf(value);
    }

    public Integer payloadInt(String key) {
        Object value = payload == null ? null : payload.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public Double payloadDouble(String key) {
        Object value = payload == null ? null : payload.get(key);
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value == null) {
            return null;
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public Boolean payloadBoolean(String key) {
        Object value = payload == null ? null : payload.get(key);
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim().toLowerCase();
        if ("true".equals(text) || "1".equals(text) || "yes".equals(text) || "on".equals(text)) {
            return true;
        }
        if ("false".equals(text) || "0".equals(text) || "no".equals(text) || "off".equals(text)) {
            return false;
        }
        return null;
    }
}

