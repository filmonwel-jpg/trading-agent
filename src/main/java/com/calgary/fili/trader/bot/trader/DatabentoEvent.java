package com.calgary.fili.trader.bot.trader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabentoEvent {
    public String event = "";
    public String symbol = "";
    public String underlying = "";
    public String right = "";
    public long barEpochSec = 0L;
    public long tsEventNs = 0L;
    public double open = 0.0;
    public double high = 0.0;
    public double low = 0.0;
    public double close = 0.0;
    public double wap = 0.0;
    public long volume = 0L;
    public long tradeCount = 0L;
    public long quoteCount = 0L;
    public double bid = 0.0;
    public double ask = 0.0;
    public long bidSize = 0L;
    public long askSize = 0L;
    public long atBidVol = 0L;
    public long atAskVol = 0L;
    public double previousClose = 0.0;
    public String sessionDate = "";
    public String message = "";

    public boolean isEquityBar() {
        return "equity_bar".equalsIgnoreCase(event);
    }

    public boolean isOptionBar() {
        return "option_bar".equalsIgnoreCase(event);
    }

    public boolean isPreviousClose() {
        return "previous_close".equalsIgnoreCase(event);
    }

    public boolean isStatus() {
        return "status".equalsIgnoreCase(event);
    }
}

