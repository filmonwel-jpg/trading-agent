package com.calgary.fili.trader.bot.trader;

import com.ib.client.*;
import com.calgary.fili.trader.bot.strategy.PingPongStrategy;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;

@Component
public class IBKRTrader implements CommandLineRunner, EWrapper {

    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final Logger log = LoggerFactory.getLogger(IBKRTrader.class);

    private EClientSocket client;
    private EJavaSignal signal = new EJavaSignal();
    private Thread readerThread;
    private volatile boolean isShuttingDown = false;

    private int currentOrderId = -1;
    private PingPongStrategy shopStrategy;
    private volatile boolean positionSyncComplete = false;

    private double currentLastPrice = 0.0;
    private double yesterdayClose = 0.0;
    private double currentBidPrice = 0.0;
    private double currentAskPrice = 0.0;
    private long currentBidSize = 0L;
    private long currentAskSize = 0L;
    private double latestShortableShares = 0.0;
    private long latestPutVolume = 0L;
    private long latestCallVolume = 0L;
    private volatile int lastPlacedOrderId = -1;
    private volatile String lastPlacedOrderAction = "";
    private volatile int lastPlacedOrderQuantity = 0;
    private volatile double lastOrderAvgFillPrice = 0.0;

    private final Map<Integer, OrderContext> orderContextById = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> cumulativeFilledByOrderId = new ConcurrentHashMap<>();
    private final Map<Integer, ScheduledFuture<?>> staleOrderTimeoutByOrderId = new ConcurrentHashMap<>();
    private final Map<Integer, Long> orderSentTimes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService staleOrderScheduler = Executors.newSingleThreadScheduledExecutor();

    private LocalDate riskCounterDate = LocalDate.now(MARKET_ZONE);
    private int dailySubmittedOrders = 0;
    private volatile boolean runtimeKillSwitch = false;

    private volatile boolean isReconnecting = false;
    private ScheduledExecutorService reconnectionScheduler = Executors.newSingleThreadScheduledExecutor();
    private int reconnectionAttempts = 0;
    private long lastReconnectionAttempt = 0;
    private LocalDate lastCloseFlattenDate = null;
    private Boolean lastScheduleAllowNewEntries = null;
    private static final int MAX_RECONNECTION_ATTEMPTS = 10;
    private static final long INITIAL_RECONNECT_DELAY_MS = 1000;
    private static final long MAX_RECONNECT_DELAY_MS = 30000;

    @Value("${trading.host:127.0.0.1}") private String ibHost;
    @Value("${trading.port:7497}") private int ibPort;
    @Value("${trading.client-id:1}") private int clientId;
    @Value("${trading.symbol:}") private String symbol;
    @Value("${trading.trade-amount:40000}") private int tradeAmount;
    @Value("${trading.max-trades:20}") private int maxTrades;
    @Value("${trading.reversal-percentage:0.0005}") private double reversalPercentage;
    @Value("${trading.stop-loss-percentage:0.01}") private double stopLossPercentage;
    @Value("${trading.max-daily-drawdown:500.0}") private double maxDailyDrawdown;
    @Value("${trading.ai.long-entry-threshold:0.68}") private double aiLongEntryThreshold;
    @Value("${trading.ai.short-entry-threshold:0.63}") private double aiShortEntryThreshold;
    @Value("${trading.ai.long-exit-threshold:0.61}") private double aiLongExitThreshold;
    @Value("${trading.ai.short-exit-threshold:0.63}") private double aiShortExitThreshold;
    @Value("${trading.ai.regime-threshold:0.50}") private double aiRegimeThreshold;
    @Value("${trading.market-data-request-id:1001}") private int marketDataRequestId;
    @Value("${trading.risk.max-order-notional:25000}") private double maxOrderNotional;
    @Value("${trading.risk.max-daily-orders:40}") private int maxDailyOrders;
    @Value("${trading.risk.max-share-cap:500}") private int maxShareCap;
    @Value("${trading.state.file:trader-state.properties}") private String stateFile;
    @Value("${trading.log.file:trades.csv}") private String tradeLogFile;
    private String resolvedTradeLogFile;

    private final MeterRegistry meterRegistry;

    private record OrderContext(String symbol, String action, int quantity) {}

    public IBKRTrader(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public double getYesterdayClose() { return this.yesterdayClose; }

    @PostConstruct
    public void postConstruct() {
        flowInfo("BOOT", "IBKRTrader bean initialized");
    }

    @Override
    public void run(String... args) throws Exception {
        flowInfo("BOOT", "SPRING BOOT STARTED: Initializing Trading Bot...");
        client = new EClientSocket(this, signal);
        client.eConnect(ibHost, ibPort, clientId);
        flowData("BOOT", "connect host=" + ibHost + " port=" + ibPort + " clientId=" + clientId + " symbol=" + symbol);
        startReaderLoop();
        Thread.sleep(1000);
        if (client.isConnected()) {
            flowCondition("BOOT", "IBKR_CONNECTED", true, "host=" + ibHost + " port=" + ibPort + " clientId=" + clientId);
            onConnected();
        } else {
            flowCondition("BOOT", "IBKR_CONNECTED", false, "host=" + ibHost + " port=" + ibPort + " clientId=" + clientId);
            flowError("CONNECTION", "Initial connection failed. Starting reconnection...");
            startReconnection();
        }
        Thread.currentThread().join();
    }

    private void onConnected() {
        positionSyncComplete = false;
        lastScheduleAllowNewEntries = null;
        boolean symbolPresent = symbol != null && !symbol.isBlank();
        flowCondition("BOOT", "SYMBOL_PRESENT", symbolPresent, "rawSymbol=" + symbol);
        symbol = symbolPresent ? symbol.trim().toUpperCase() : "TSLA";
        flowData("BOOT", "normalizedSymbol=" + symbol);

        if (shopStrategy == null) {
            shopStrategy = new PingPongStrategy(this, symbol, 0.003, tradeAmount, maxTrades, true, 12, 14, reversalPercentage, stopLossPercentage, maxDailyDrawdown, 1.20, 0.70);
            loadStrategyState();
            meterRegistry.gauge("trading.strategy.stop.queue.depth", shopStrategy, PingPongStrategy::getLastStopQueueDepth);
            meterRegistry.gauge("trading.strategy.stop.ack.latency.last", shopStrategy, PingPongStrategy::getLastStopAckLatencyMs);
            meterRegistry.gauge("trading.strategy.stop.ack.latency.p50", shopStrategy, PingPongStrategy::getStopAckLatencyP50Ms);
            meterRegistry.gauge("trading.strategy.stop.ack.latency.p95", shopStrategy, PingPongStrategy::getStopAckLatencyP95Ms);
            meterRegistry.gauge("trading.strategy.stop.ack.latency.samples", shopStrategy, PingPongStrategy::getStopAckLatencySampleCount);
            meterRegistry.gauge("trading.connection.status", this, IBKRTrader::getConnectionStatus);
            meterRegistry.gauge("trading.reconnection.attempts", this, IBKRTrader::getReconnectionAttempts);
        }

        shopStrategy.setAiThresholds(aiLongEntryThreshold, aiShortEntryThreshold, aiLongExitThreshold, aiShortExitThreshold, aiRegimeThreshold);

        if (this.yesterdayClose > 0) {
            shopStrategy.setYesterdayClose(this.yesterdayClose);
            flowCondition("IBKR.SYNC", "YESTERDAY_CLOSE_AVAILABLE", true, "value=" + this.yesterdayClose);
        } else {
            flowCondition("IBKR.SYNC", "YESTERDAY_CLOSE_AVAILABLE", false, "value=" + this.yesterdayClose);
        }

        client.reqPositions();
        subscribeToMarketData();

        Contract contract = buildStockContract();
        String queryTime = LocalDate.now(MARKET_ZONE).atStartOfDay(MARKET_ZONE).minusSeconds(1).format(DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss"));
        client.reqHistoricalData(marketDataRequestId + 2, contract, queryTime, "1 D", "1 day", "TRADES", 1, 1, false, null);
    }

    private void subscribeToMarketData() {
        Contract contract = buildStockContract();
        flowData("IBKR.SUBSCRIBE", "contract symbol=" + symbol + " secType=STK exchange=SMART currency=USD");
        client.reqMktData(marketDataRequestId, contract, "100,104,236", false, false, null);
        client.reqRealTimeBars(marketDataRequestId + 1, contract, 5, "TRADES", false, null);
        client.reqTickByTickData(marketDataRequestId + 3, contract, "AllLast", 0, false);
        client.reqTickByTickData(marketDataRequestId + 4, contract, "BidAsk", 0, false);
        flowInfo("IBKR.SUBSCRIBE", "Subscribed streams mktDataReqId=" + marketDataRequestId + " barReqId=" + (marketDataRequestId + 1) + " allLastReqId=" + (marketDataRequestId + 3) + " bidAskReqId=" + (marketDataRequestId + 4));
    }

    private Contract buildStockContract() {
        Contract contract = new Contract();
        contract.symbol(symbol);
        contract.secType("STK");
        contract.currency("USD");
        contract.exchange("SMART");
        return contract;
    }

    private void handleDisconnection() {
        if (shopStrategy != null) {
            shopStrategy.setEnabled(false);
            flowInfo("CONNECTION", "Strategy paused during disconnection.");
        }
        orderContextById.clear();
        cumulativeFilledByOrderId.clear();
        staleOrderTimeoutByOrderId.values().forEach(f -> f.cancel(false));
        staleOrderTimeoutByOrderId.clear();
        orderSentTimes.clear();
        startReconnection();
    }

    private void startReconnection() {
        if (isReconnecting) {
            flowCondition("CONNECTION", "RECONNECT_NOT_ALREADY_RUNNING", false, "isReconnecting=" + isReconnecting);
            return;
        }
        flowCondition("CONNECTION", "RECONNECT_NOT_ALREADY_RUNNING", true, "isReconnecting=" + isReconnecting);
        isReconnecting = true;
        reconnectionAttempts = 0;
        scheduleReconnectAttempt();
    }

    private void scheduleReconnectAttempt() {
        if (reconnectionAttempts >= MAX_RECONNECTION_ATTEMPTS) {
            flowCondition("CONNECTION", "RECONNECT_ATTEMPTS_WITHIN_LIMIT", false, "attempts=" + reconnectionAttempts + " max=" + MAX_RECONNECTION_ATTEMPTS);
            flowError("CONNECTION", "Max reconnection attempts reached. Giving up.");
            isReconnecting = false;
            return;
        }
        flowCondition("CONNECTION", "RECONNECT_ATTEMPTS_WITHIN_LIMIT", true, "attempts=" + reconnectionAttempts + " max=" + MAX_RECONNECTION_ATTEMPTS);
        long delay = Math.min(INITIAL_RECONNECT_DELAY_MS * (1L << reconnectionAttempts), MAX_RECONNECT_DELAY_MS);
        reconnectionAttempts++;
        lastReconnectionAttempt = System.currentTimeMillis();
        flowData("CONNECTION", "Scheduling reconnection attempt=" + reconnectionAttempts + " delayMs=" + delay);
        reconnectionScheduler.schedule(this::attemptReconnect, delay, TimeUnit.MILLISECONDS);
    }

    private void attemptReconnect() {
        if (isShuttingDown || isConnected()) {
            flowCondition("CONNECTION", "RECONNECT_NEEDED", false, "isShuttingDown=" + isShuttingDown + " isConnected=" + isConnected());
            isReconnecting = false;
            return;
        }
        flowCondition("CONNECTION", "RECONNECT_NEEDED", true, "isShuttingDown=" + isShuttingDown + " isConnected=" + isConnected());
        flowAnalyze("CONNECTION", "Attempting reconnect host=" + ibHost + " port=" + ibPort + " clientId=" + clientId);
        try {
            client.eConnect(ibHost, ibPort, clientId);
            Thread.sleep(1000);
            if (client.isConnected()) {
                flowCondition("CONNECTION", "RECONNECT_SUCCESS", true, "attempt=" + reconnectionAttempts);
                onReconnected();
            } else {
                flowCondition("CONNECTION", "RECONNECT_SUCCESS", false, "attempt=" + reconnectionAttempts);
                scheduleReconnectAttempt();
            }
        } catch (Exception e) {
            flowError("CONNECTION", "Reconnection error: " + e.getMessage());
            scheduleReconnectAttempt();
        }
    }

    private void onReconnected() {
        isReconnecting = false;
        reconnectionAttempts = 0;
        positionSyncComplete = false;
        lastScheduleAllowNewEntries = null;
        client.reqPositions();
        client.reqOpenOrders();
        subscribeToMarketData();
        if (shopStrategy != null && !runtimeKillSwitch) {
            shopStrategy.setEnabled(true);
            flowCondition("CONNECTION", "STRATEGY_RESUMED_AFTER_RECONNECT", true, "killSwitch=" + runtimeKillSwitch);
        } else {
            flowCondition("CONNECTION", "STRATEGY_RESUMED_AFTER_RECONNECT", false, "strategyReady=" + (shopStrategy != null) + " killSwitch=" + runtimeKillSwitch);
        }
    }

    @Override
    public void tickPrice(int tickerId, int field, double price, TickAttrib attribs) {
        boolean expectedTicker = tickerId == marketDataRequestId;
        flowCondition("IBKR.TICK", "EXPECTED_TICKER_ID", expectedTicker, "tickerId=" + tickerId + " expected=" + marketDataRequestId + " field=" + field);
        if (!expectedTicker) return;

        if (field == 9) {
            this.yesterdayClose = price;
            flowCondition("IBKR.TICK", "YESTERDAY_CLOSE_VALID", price > 0.0, "value=" + price);
            flowData("IBKR.TICK", "field=9 yesterdayClose=" + price);
            if (shopStrategy != null) {
                shopStrategy.setYesterdayClose(price);
            }
            persistStrategyState();
            return;
        }

        if (field == 1) {
            this.currentBidPrice = price;
            flowData("IBKR.TICK", "field=1 bid=" + price);
            if (shopStrategy != null) {
                shopStrategy.onQuoteSnapshot(currentBidPrice, currentAskPrice, currentBidSize, currentAskSize, latestShortableShares);
            }
            return;
        }

        if (field == 2) {
            this.currentAskPrice = price;
            flowData("IBKR.TICK", "field=2 ask=" + price);
            if (shopStrategy != null) {
                shopStrategy.onQuoteSnapshot(currentBidPrice, currentAskPrice, currentBidSize, currentAskSize, latestShortableShares);
            }
            return;
        }

        if (field == 4) {
            if (Math.abs(price - this.currentLastPrice) > 0.001) {
                this.currentLastPrice = price;
                boolean canForward = shopStrategy != null && positionSyncComplete;
                flowCondition("IBKR->AI.TICK", "FORWARD_TICK_TO_STRATEGY", canForward, "lastPrice=" + price + " positionSyncComplete=" + positionSyncComplete + " strategyReady=" + (shopStrategy != null));
                if (canForward) {
                    shopStrategy.onTickForExitsOnly(price);
                }
            }
        }
    }

    @Override
    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, Decimal volume, Decimal wap, int count) {
        boolean expectedReq = reqId == marketDataRequestId + 1;
        boolean canForward = expectedReq && shopStrategy != null && positionSyncComplete;

        ZonedDateTime barTs = null;
        if (expectedReq) {
            barTs = Instant.ofEpochSecond(time).atZone(ZoneOffset.UTC).withZoneSameInstant(MARKET_ZONE);
            flowData("IBKR.BAR", "reqId=" + reqId + " tsEt=" + barTs + " ohlc=" + open + "/" + high + "/" + low + "/" + close + " count=" + count);
            flowCondition("IBKR->AI.BAR", "FORWARD_BAR_TO_STRATEGY", canForward, "positionSyncComplete=" + positionSyncComplete + " strategyReady=" + (shopStrategy != null));
        }

        if (expectedReq && shopStrategy != null && barTs != null) {
            LocalTime timeEt = barTs.toLocalTime();
            LocalDate barDateEt = barTs.toLocalDate();

            boolean allowEntriesByClock = !timeEt.isBefore(LocalTime.of(9, 30, 0)) && timeEt.isBefore(LocalTime.of(15, 50, 0));
            if (lastScheduleAllowNewEntries == null || lastScheduleAllowNewEntries.booleanValue() != allowEntriesByClock) {
                shopStrategy.setAllowNewEntries(allowEntriesByClock);
                lastScheduleAllowNewEntries = allowEntriesByClock;
                flowInfo("SCHEDULE", "Clock gate set allowNewEntries=" + allowEntriesByClock + " at " + timeEt);
            }

            if (timeEt.isAfter(LocalTime.of(9, 29, 55)) && timeEt.isBefore(LocalTime.of(9, 31, 0))) {
                if (!shopStrategy.isEnabled()) {
                    shopStrategy.resetForNewDay();
                    shopStrategy.setEnabled(true);
                    flowInfo("SCHEDULE", "9:30 AM ET: Market Open. Strategy awake and indicators reset.");
                }
            }

            if (timeEt.isAfter(LocalTime.of(15, 59, 45)) && timeEt.isBefore(LocalTime.of(16, 0, 0))) {
                boolean closeActionNotDoneToday = !barDateEt.equals(lastCloseFlattenDate);
                if (shopStrategy.isEnabled() && closeActionNotDoneToday) {
                    flowInfo("SCHEDULE", "3:59 PM ET: Market closing. Flattening positions and going to sleep.");
                    flattenPosition();
                    shopStrategy.setEnabled(false);
                    lastCloseFlattenDate = barDateEt;
                }
            }
        }

        if (canForward) {
            long vol = 0L;
            if (volume != null && volume.value() != null) {
                vol = volume.value().longValue();
            }

            double vwapVal = close;
            if (wap != null && wap.value() != null) {
                vwapVal = wap.value().doubleValue();
                if (vwapVal <= 0) vwapVal = close;
            }

            shopStrategy.on5SecondBar(time, open, high, low, close, vol, vwapVal);
        }
    }

    public void placeTrade(String symbol, String action, double currentPrice, int quantity, String orderType) {
        rollRiskCountersIfNeeded();
        if (currentOrderId < 0 || isKillSwitchActive()) {
            flowCondition("ORDER.GATE", "READY_TO_PLACE_ORDER", false, "currentOrderId=" + currentOrderId + " killSwitch=" + isKillSwitchActive());
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }
        flowCondition("ORDER.GATE", "READY_TO_PLACE_ORDER", true, "currentOrderId=" + currentOrderId + " killSwitch=" + isKillSwitchActive());

        if (quantity <= 0 || currentPrice <= 0.0) {
            flowCondition("ORDER.GATE", "VALID_QTY_PRICE", false, "qty=" + quantity + " price=" + currentPrice);
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }
        flowCondition("ORDER.GATE", "VALID_QTY_PRICE", true, "qty=" + quantity + " price=" + currentPrice);

        if (orderType == null || orderType.isBlank()) {
            flowCondition("ORDER.GATE", "ORDER_TYPE_PRESENT", false, "orderType=" + orderType);
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }

        String normalizedOrderType = orderType.trim().toUpperCase();
        int finalQty = Math.min(quantity, getMaxShareCap());

        boolean isClosingTrade = false;
        if (shopStrategy != null) {
            int currentPos = shopStrategy.getCurrentPosition();
            if (("SELL".equals(action) && currentPos > 0) || ("BUY".equals(action) && currentPos < 0)) {
                isClosingTrade = true;
                flowInfo("RISK", "Trade identified as EXIT. Bypassing Notional Limits.");
            }
        }

        if (!isClosingTrade && (currentPrice * finalQty) > maxOrderNotional) {
            flowError("RISK", "Order blocked: notional exceeds limit");
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }
        if (dailySubmittedOrders >= maxDailyOrders) {
            flowError("RISK", "Order blocked: max daily order limit reached");
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }

        Contract contract = new Contract();
        contract.symbol(symbol);
        contract.secType("STK");
        contract.currency("USD");
        contract.exchange("SMART");

        Order order = new Order();
        order.action(action);
        order.totalQuantity(Decimal.parse(String.valueOf(finalQty)));
        order.outsideRth(true);

        if ("FAST_LMT".equals(normalizedOrderType)) {
            order.orderType("LMT");
            double aggressiveOffset = 0.05;
            double fastPrice = "BUY".equals(action) ? (currentPrice + aggressiveOffset) : (currentPrice - aggressiveOffset);
            order.lmtPrice(Math.round(fastPrice * 100.0) / 100.0);
            order.tif("IOC");
        } else if ("LMT".equals(normalizedOrderType)) {
            order.orderType("LMT");
            order.lmtPrice(currentPrice);
            order.tif("IOC");
        } else {
            order.orderType("MKT");
            order.tif("IOC");
        }

        flowData("ORDER.SEND", "orderId=" + currentOrderId + " action=" + action + " type=" + normalizedOrderType);
        client.placeOrder(currentOrderId, contract, order);
        lastPlacedOrderId = currentOrderId;
        lastPlacedOrderAction = action;
        lastPlacedOrderQuantity = finalQty;

        if (shopStrategy != null) shopStrategy.onOrderSubmitted(currentOrderId, action, finalQty);
        dailySubmittedOrders++;
        currentOrderId++;
    }

    protected int getMaxShareCap() {
        return Math.max(500, maxShareCap);
    }

    protected void rollRiskCountersIfNeeded() {
        LocalDate today = LocalDate.now(MARKET_ZONE);
        if (!today.equals(riskCounterDate)) {
            riskCounterDate = today;
            dailySubmittedOrders = 0;
        }
    }

    private void loadStrategyState() {
        Path path = Paths.get(stateFile);
        boolean stateExists = Files.exists(path);
        flowCondition("STATE", "STATE_FILE_EXISTS", stateExists, "path=" + stateFile);
        if (!stateExists) return;
        Properties props = new Properties();
        try (InputStream is = Files.newInputStream(path)) {
            props.load(is);
            double rPrice = Double.parseDouble(props.getProperty("strategy.lastPrice", "0.0"));
            int rTrades = Integer.parseInt(props.getProperty("strategy.tradeCount", "0"));
            this.yesterdayClose = Double.parseDouble(props.getProperty("strategy.yesterdayClose", "0.0"));
            shopStrategy.restoreState(rPrice, rTrades, true);
            flowData("STATE", "Restored state lastPrice=" + rPrice + " trades=" + rTrades + " yesterdayClose=" + yesterdayClose);
        } catch (Exception e) {
            flowError("STATE", "Restore failed: " + e.getMessage());
        }
    }

    protected void persistStrategyState() {
        if (shopStrategy == null) {
            flowCondition("STATE", "STRATEGY_PRESENT_FOR_PERSIST", false, "strategyReady=false");
            return;
        }
        flowCondition("STATE", "STRATEGY_PRESENT_FOR_PERSIST", true, "strategyReady=true");
        Properties props = new Properties();
        props.setProperty("strategy.lastPrice", String.valueOf(shopStrategy.getLastPrice()));
        props.setProperty("strategy.tradeCount", String.valueOf(shopStrategy.getTradeCount()));
        props.setProperty("strategy.yesterdayClose", String.valueOf(this.yesterdayClose));
        try (OutputStream os = Files.newOutputStream(Paths.get(stateFile))) {
            props.store(os, "Trader State");
            flowData("STATE", "Persisted state file=" + stateFile + " lastPrice=" + shopStrategy.getLastPrice() + " tradeCount=" + shopStrategy.getTradeCount() + " yesterdayClose=" + this.yesterdayClose);
        } catch (Exception e) {
            flowError("STATE", "Save failed: " + e.getMessage());
        }
    }

    private void startReaderLoop() {
        final EReader reader = new EReader(client, signal);
        reader.start();
        readerThread = new Thread(() -> {
            while (!isShuttingDown) {
                signal.waitForSignal();
                try { reader.processMsgs(); } catch (Exception ignored) {}
            }
        });
        readerThread.start();
    }

    @Override public void nextValidId(int orderId) { this.currentOrderId = orderId; flowData("IBKR.SYNC", "nextValidId=" + orderId); }
    @Override public void positionEnd() {
        positionSyncComplete = true;
        if (shopStrategy != null) {
            shopStrategy.setPositionSynced(true);
        }
        flowCondition("IBKR.SYNC", "POSITION_SYNC_COMPLETE", true, "symbol=" + symbol);
    }
    @Override public void connectAck() {}
    @Override public void error(Exception e) { flowError("IBKR.ERROR", e.getMessage()); }
    @Override public void error(String str) { flowError("IBKR.ERROR", str); }
    @Override public void error(int id, long errorCode, int errorVersion, String errorString, String errorExplain) {
        if (errorCode == 2104 || errorCode == 2106 || errorCode == 2158) {
            flowInfo("IBKR.HEALTH", "id=" + id + " code=" + errorCode + " msg=" + errorString);
            return;
        }
        flowError("IBKR.ERROR", "id=" + id + " code=" + errorCode + " msg=" + errorString + " explain=" + errorExplain);
    }

    @Override public void connectionClosed() {
        if (!isShuttingDown) {
            flowError("CONNECTION", "Lost connection to IBKR. Starting reconnection...");
            handleDisconnection();
        }
    }

    @Override public void position(String account, Contract contract, Decimal pos, double avgCost) {
        if (contract.symbol().equals(symbol)) {
            if (shopStrategy != null) shopStrategy.syncPosition(pos.value().intValue(), avgCost);
        }
    }

    @Override
    public void orderStatus(int orderId, String status, Decimal filled, Decimal remaining, double avgFillPrice, long permId, int parentId, double lastFillPrice, int clientId, String whyHeld, double mktCapPrice) {
        if (avgFillPrice > 0.0) {
            lastOrderAvgFillPrice = avgFillPrice;
        }
        if (shopStrategy != null) {
            if ("Filled".equalsIgnoreCase(status) || "Cancelled".equalsIgnoreCase(status) || remaining.value().intValue() == 0) {
                shopStrategy.onOrderClosed(orderId, status);
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        isShuttingDown = true;
        persistStrategyState();
        if (client != null) client.eDisconnect();
        staleOrderScheduler.shutdown();
        reconnectionScheduler.shutdown();
    }

    public void cancelStaleOrder(int orderIdToCancel) {
        if (client != null && orderIdToCancel >= 0) {
            client.cancelOrder(orderIdToCancel, new OrderCancel());
        }
    }

    public void requestPositions() {
        if (client != null && client.isConnected()) {
            client.reqPositions();
        }
    }

    public Map<String, Object> forceClearLocks() {
        Map<String, Object> response = new LinkedHashMap<>();
        if (shopStrategy != null) {
            shopStrategy.onOrderClosed(-1, "Cancelled");
            response.put("message", "SUCCESS: Artificial 'Cancelled' signal sent. Strategy locks cleared.");
        }
        return response;
    }

    public boolean isConnected() { return client != null && client.isConnected(); }
    public int getClientId() { return clientId; }
    public boolean isStrategyEnabled() { return shopStrategy != null && shopStrategy.isEnabled(); }
    public int getOpenOrdersCount() { return orderContextById.size(); }
    public int getDailySubmittedOrders() { rollRiskCountersIfNeeded(); return dailySubmittedOrders; }
    public boolean isOrderInFlight() { return shopStrategy != null && shopStrategy.hasInFlightOrder(); }
    public synchronized String getTradeLogFile() {
        if (resolvedTradeLogFile != null && !resolvedTradeLogFile.isBlank()) {
            return resolvedTradeLogFile;
        }

        String configuredFile = tradeLogFile == null ? "" : tradeLogFile.trim();
        if (!configuredFile.isBlank() && !"trades.csv".equalsIgnoreCase(configuredFile)) {
            resolvedTradeLogFile = configuredFile;
            return resolvedTradeLogFile;
        }

        LocalDate tradeDate = LocalDate.now(MARKET_ZONE);
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
        String timestamp = LocalDateTime.now(MARKET_ZONE).format(dtf);
        String sym = (symbol != null && !symbol.isBlank()) ? symbol.trim().toLowerCase() : "unknown";
        resolvedTradeLogFile = "trades_log_" + sym + "_trade_date_" + tradeDate + "_" + timestamp + ".csv";
        return resolvedTradeLogFile;
    }
    public int getStrategyStopQueueDepth() { return shopStrategy != null ? shopStrategy.getLastStopQueueDepth() : 0; }
    public double getStrategyStopAckLatencyMs() { return shopStrategy != null ? shopStrategy.getLastStopAckLatencyMs() : 0.0; }
    public double getStrategyStopAckLatencyP50Ms() { return shopStrategy != null ? shopStrategy.getStopAckLatencyP50Ms() : 0.0; }
    public double getStrategyStopAckLatencyP95Ms() { return shopStrategy != null ? shopStrategy.getStopAckLatencyP95Ms() : 0.0; }
    public int getStrategyStopAckLatencySamples() { return shopStrategy != null ? shopStrategy.getStopAckLatencySampleCount() : 0; }
    public double getConnectionStatus() { return isConnected() ? 1.0 : 0.0; }
    public int getReconnectionAttempts() { return reconnectionAttempts; }

    public Map<String, Object> controlStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        int currentPosition = shopStrategy != null ? shopStrategy.getCurrentPosition() : 0;
        int submittedToday = getDailySubmittedOrders();
        status.put("connected", isConnected());
        status.put("symbol", symbol);
        status.put("strategyEnabled", isStrategyEnabled());
        status.put("killSwitch", runtimeKillSwitch);
        status.put("openOrders", getOpenOrdersCount());
        status.put("dailySubmittedOrders", submittedToday);
        status.put("currentPosition", currentPosition);
        status.put("position", currentPosition);
        status.put("orderInFlight", isOrderInFlight());
        status.put("orderPlaced", submittedToday > 0);
        status.put("ordersPlacedToday", submittedToday);
        status.put("lastPlacedOrderId", lastPlacedOrderId);
        status.put("lastPlacedOrderAction", lastPlacedOrderAction);
        status.put("lastPlacedOrderQuantity", lastPlacedOrderQuantity);
        status.put("lastOrderAvgFillPrice", lastOrderAvgFillPrice);
        status.put("stopQueueDepthLast", getStrategyStopQueueDepth());
        status.put("stopAckLatencyMsLast", getStrategyStopAckLatencyMs());
        status.put("stopAckLatencyMsP50", getStrategyStopAckLatencyP50Ms());
        status.put("stopAckLatencyMsP95", getStrategyStopAckLatencyP95Ms());
        status.put("stopAckLatencySamples", getStrategyStopAckLatencySamples());
        status.put("reconnecting", isReconnecting);
        status.put("reconnectionAttempts", reconnectionAttempts);
        return status;
    }

    public boolean pauseTrading() {
        if (shopStrategy == null) return false;
        shopStrategy.setEnabled(false);
        return true;
    }

    public boolean resumeTrading() {
        if (shopStrategy == null || runtimeKillSwitch) return false;
        shopStrategy.setEnabled(true);
        return true;
    }

    public void setRuntimeKillSwitch(boolean enabled) {
        this.runtimeKillSwitch = enabled;
        if (enabled && shopStrategy != null) {
            shopStrategy.setEnabled(false);
        }
    }

    public int cancelOpenOrders() {
        if (!isConnected()) return 0;
        int openCount = orderContextById.size();
        client.reqGlobalCancel(new OrderCancel());
        return openCount;
    }

    public String flattenPosition() {
        if (shopStrategy == null) return "strategy-not-ready";
        int position = shopStrategy.getCurrentPosition();
        if (position == 0) return "already-flat";
        if (currentLastPrice <= 0.0) return "flatten-failed-no-price";
        String action = (position > 0) ? "SELL" : "BUY";
        placeTrade(symbol, action, currentLastPrice, Math.abs(position), "MKT");
        return "flatten-requested";
    }

    public Map<String, Object> manualSwitchSymbol(String newSymbol) {
        Map<String, Object> response = new LinkedHashMap<>();
        if (newSymbol == null || newSymbol.isBlank()) {
            response.put("message", "invalid-symbol");
            response.put("status", controlStatus());
            return response;
        }

        String nextSymbol = newSymbol.trim().toUpperCase();
        this.symbol = nextSymbol;
        response.put("message", "Switched symbol to " + nextSymbol);
        return response;
    }

    public boolean isKillSwitchActive() {
        return runtimeKillSwitch;
    }

    protected PingPongStrategy getShopStrategy() {
        return shopStrategy;
    }

    protected void setShopStrategy(PingPongStrategy strategy) {
        this.shopStrategy = strategy;
    }

    // Logger Helpers
    private void flowInfo(String tag, String msg) { log.info(">>> [FLOW][INFO][{}] {}", tag, msg); }
    private void flowData(String tag, String msg) { log.info(">>> [FLOW][DATA][{}] {}", tag, msg); }
    private void flowCondition(String tag, String check, boolean pass, String context) { log.info(">>> [FLOW][COND][{}] {}={} | {}", tag, check, pass ? "PASS" : "FAIL", context); }
    private void flowAnalyze(String tag, String msg) { log.info(">>> [FLOW][ANALYZE][{}] {}", tag, msg); }
    private void flowError(String tag, String msg) { log.error(">>> [ERROR][{}] {}", tag, msg); }

    // ===== Full EWrapper boilerplate =====
    @Override
    public void tickSize(int tickerId, int field, Decimal size) {
        if (tickerId != marketDataRequestId) return;

        long sizeVal = 0L;
        if (size != null && size.value() != null) {
            sizeVal = Math.max(0L, size.value().longValue());
        }

        // IB standard tickType ids used directly for resilience.
        if (field == 0) { // BID_SIZE
            currentBidSize = sizeVal;
        } else if (field == 3) { // ASK_SIZE
            currentAskSize = sizeVal;
        } else if (field == 29) { // OPTION_CALL_VOLUME
            latestCallVolume = sizeVal;
            if (shopStrategy != null) shopStrategy.onOptionVolumeUpdate(latestPutVolume, latestCallVolume);
        } else if (field == 30) { // OPTION_PUT_VOLUME
            latestPutVolume = sizeVal;
            if (shopStrategy != null) shopStrategy.onOptionVolumeUpdate(latestPutVolume, latestCallVolume);
        }

        if (shopStrategy != null) {
            shopStrategy.onQuoteSnapshot(currentBidPrice, currentAskPrice, currentBidSize, currentAskSize, latestShortableShares);
        }
    }
    @Override
    public void tickString(int tickerId, int tickType, String value) {
        if (tickerId != marketDataRequestId) return;

        // RT_VOLUME fallback payload: price;size;time;totalVolume;vwap;singleTrade
        if (tickType == 48 && value != null && !value.isBlank() && shopStrategy != null) {
            try {
                String[] parts = value.split(";");
                if (parts.length >= 2) {
                    double price = Double.parseDouble(parts[0]);
                    long size = (long) Double.parseDouble(parts[1]);
                    if (price > 0.0 && size > 0L) {
                        shopStrategy.onTapeTrade(price, size, currentBidPrice, currentAskPrice);
                    }
                }
            } catch (Exception ignored) {
                flowCondition("IBKR.TICK", "RT_VOLUME_PARSE", false, "value=" + value);
            }
        }
    }

    @Override
    public void tickGeneric(int tickerId, int tickType, double value) {
        if (tickerId != marketDataRequestId) return;

        // SHORTABLE is commonly delivered as generic tick type 46.
        if (tickType == 46 && value > 0.0) {
            latestShortableShares = value;
            if (shopStrategy != null) {
                shopStrategy.onQuoteSnapshot(currentBidPrice, currentAskPrice, currentBidSize, currentAskSize, latestShortableShares);
            }
        }
    }
    @Override public void tickOptionComputation(int tickerId, int field, int tickAttrib, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) {}
    @Override public void tickEFP(int tickerId, int tickType, double basisPoints, String formattedBasisPoints, double impliedFuture, int holdDays, String futureLastTradeDate, double dividendImpact, double dividendsToLastTradeDate) {}
    @Override public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {}
    @Override public void openOrderEnd() {}
    @Override public void updateAccountValue(String key, String value, String currency, String accountName) {}
    @Override public void updatePortfolio(Contract contract, Decimal position, double marketPrice, double marketValue, double averageCost, double unrealizedPNL, double realizedPNL, String accountName) {}
    @Override public void updateAccountTime(String timeStamp) {}
    @Override public void accountDownloadEnd(String accountName) {}
    @Override public void contractDetails(int reqId, ContractDetails contractDetails) {}
    @Override public void bondContractDetails(int reqId, ContractDetails contractDetails) {}
    @Override public void contractDetailsEnd(int reqId) {}
    @Override public void execDetails(int reqId, Contract contract, Execution execution) {}
    @Override public void execDetailsEnd(int reqId) {}
    @Override public void updateMktDepth(int tickerId, int position, int operation, int side, double price, Decimal size) {}
    @Override public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation, int side, double price, Decimal size, boolean isSmartDepth) {}
    @Override public void updateNewsBulletin(int msgId, int msgType, String message, String origExchange) {}
    @Override public void managedAccounts(String accountsList) {}
    @Override public void receiveFA(int faDataType, String xml) {}
    @Override public void scannerParameters(String xml) {}
    @Override public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance, String benchmark, String projection, String legsStr) {}
    @Override public void scannerDataEnd(int reqId) {}
    @Override public void currentTime(long time) {}
    @Override public void currentTimeInMillis(long timeInMillis) {}
    @Override public void fundamentalData(int reqId, String data) {}
    @Override public void deltaNeutralValidation(int reqId, DeltaNeutralContract deltaNeutralContract) {}
    @Override public void tickSnapshotEnd(int reqId) {}
    @Override public void marketDataType(int reqId, int marketDataType) {}
    @Override public void commissionAndFeesReport(CommissionAndFeesReport commissionAndFeesReport) {}
    @Override public void accountSummary(int reqId, String account, String tag, String value, String currency) {}
    @Override public void accountSummaryEnd(int reqId) {}
    @Override public void verifyMessageAPI(String apiData) {}
    @Override public void verifyCompleted(boolean isSuccessful, String errorText) {}
    @Override public void verifyAndAuthMessageAPI(String apiData, String xyzChallenge) {}
    @Override public void verifyAndAuthCompleted(boolean isSuccessful, String errorText) {}
    @Override public void displayGroupList(int reqId, String groups) {}
    @Override public void displayGroupUpdated(int reqId, String contractInfo) {}
    @Override public void positionMulti(int reqId, String account, String modelCode, Contract contract, Decimal pos, double avgCost) {}
    @Override public void positionMultiEnd(int reqId) {}
    @Override public void accountUpdateMulti(int reqId, String account, String modelCode, String key, String value, String currency) {}
    @Override public void accountUpdateMultiEnd(int reqId) {}
    @Override public void securityDefinitionOptionalParameter(int reqId, String exchange, int underlyingConId, String tradingClass, String multiplier, java.util.Set<String> expirations, java.util.Set<Double> strikes) {}
    @Override public void securityDefinitionOptionalParameterEnd(int reqId) {}
    @Override public void softDollarTiers(int reqId, SoftDollarTier[] tiers) {}
    @Override public void familyCodes(FamilyCode[] familyCodes) {}
    @Override public void symbolSamples(int reqId, ContractDescription[] contractDescriptions) {}
    @Override public void mktDepthExchanges(DepthMktDataDescription[] depthMktDataDescriptions) {}
    @Override public void tickNews(int tickerId, long timeStamp, String providerCode, String articleId, String headline, String extraData) {}
    @Override public void smartComponents(int reqId, java.util.Map<Integer, java.util.Map.Entry<String, Character>> theMap) {}
    @Override public void tickReqParams(int tickerId, double minTick, String bboExchange, int snapshotPermissions) {}
    @Override public void newsProviders(NewsProvider[] newsProviders) {}
    @Override public void newsArticle(int requestId, int articleType, String articleText) {}
    @Override public void historicalNews(int requestId, String time, String providerCode, String articleId, String headline) {}
    @Override public void historicalNewsEnd(int requestId, boolean hasMore) {}
    @Override public void headTimestamp(int reqId, String headTimestamp) {}
    @Override public void histogramData(int reqId, java.util.List<HistogramEntry> items) {}
    @Override public void historicalData(int reqId, Bar bar) {}
    @Override public void historicalDataUpdate(int reqId, Bar bar) {}
    @Override public void historicalDataEnd(int reqId, String startDateStr, String endDateStr) {}
    @Override public void rerouteMktDataReq(int reqId, int conId, String exchange) {}
    @Override public void rerouteMktDepthReq(int reqId, int conId, String exchange) {}
    @Override public void marketRule(int marketRuleId, PriceIncrement[] priceIncrements) {}
    @Override public void pnl(int reqId, double dailyPnL, double unrealizedPnL, double realizedPnL) {}
    @Override public void pnlSingle(int reqId, Decimal pos, double dailyPnL, double unrealizedPnL, double realizedPnL, double value) {}
    @Override public void historicalTicks(int reqId, java.util.List<HistoricalTick> ticks, boolean done) {}
    @Override public void historicalTicksBidAsk(int reqId, java.util.List<HistoricalTickBidAsk> ticks, boolean done) {}
    @Override public void historicalTicksLast(int reqId, java.util.List<HistoricalTickLast> ticks, boolean done) {}
    @Override
    public void tickByTickAllLast(int reqId, int tickType, long time, double price, Decimal size, TickAttribLast tickAttribLast, String exchange, String specialConditions) {
        if (reqId != marketDataRequestId + 3) return;
        if (shopStrategy == null || !positionSyncComplete) return;

        long tradeSize = 0L;
        if (size != null && size.value() != null) {
            tradeSize = Math.max(0L, size.value().longValue());
        }
        if (price <= 0.0 || tradeSize <= 0L) return;

        shopStrategy.onTapeTrade(price, tradeSize, currentBidPrice, currentAskPrice);
    }

    @Override
    public void tickByTickBidAsk(int reqId, long time, double bidPrice, double askPrice, Decimal bidSize, Decimal askSize, TickAttribBidAsk tickAttribBidAsk) {
        if (reqId != marketDataRequestId + 4) return;

        currentBidPrice = bidPrice > 0.0 ? bidPrice : currentBidPrice;
        currentAskPrice = askPrice > 0.0 ? askPrice : currentAskPrice;

        if (bidSize != null && bidSize.value() != null) {
            currentBidSize = Math.max(0L, bidSize.value().longValue());
        }
        if (askSize != null && askSize.value() != null) {
            currentAskSize = Math.max(0L, askSize.value().longValue());
        }

        if (shopStrategy != null) {
            shopStrategy.onQuoteSnapshot(currentBidPrice, currentAskPrice, currentBidSize, currentAskSize, latestShortableShares);
        }
    }
    @Override public void tickByTickMidPoint(int reqId, long time, double midPoint) {}
    @Override public void orderBound(long orderId, int apiClientId, int apiParentId) {}
    @Override public void completedOrder(Contract contract, Order order, OrderState orderState) {}
    @Override public void completedOrdersEnd() {}
    @Override public void replaceFAEnd(int reqId, String text) {}
    @Override public void wshMetaData(int reqId, String dataJson) {}
    @Override public void wshEventData(int reqId, String dataJson) {}
    @Override public void historicalSchedule(int reqId, String startDateTime, String endDateTime, String timeZone, java.util.List<HistoricalSession> sessions) {}
    @Override public void userInfo(int reqId, String whiteBrandingId) {}
    @Override public void configResponseProtoBuf(com.ib.client.protobuf.ConfigResponseProto.ConfigResponse configResponse) {}
    @Override public void updateConfigResponseProtoBuf(com.ib.client.protobuf.UpdateConfigResponseProto.UpdateConfigResponse updateConfigResponse) {}
    @Override public void wshMetaDataProtoBuf(com.ib.client.protobuf.WshMetaDataProto.WshMetaData wshMetaData) {}
    @Override public void tickReqParamsProtoBuf(com.ib.client.protobuf.TickReqParamsProto.TickReqParams tickReqParams) {}
    @Override public void scannerParametersProtoBuf(com.ib.client.protobuf.ScannerParametersProto.ScannerParameters scannerParameters) {}
    @Override public void historicalNewsProtoBuf(com.ib.client.protobuf.HistoricalNewsProto.HistoricalNews historicalNews) {}
    @Override public void accountSummaryEndProtoBuf(com.ib.client.protobuf.AccountSummaryEndProto.AccountSummaryEnd accountSummaryEnd) {}
    @Override public void managedAccountsProtoBuf(com.ib.client.protobuf.ManagedAccountsProto.ManagedAccounts managedAccounts) {}
    @Override public void tickOptionComputationProtoBuf(com.ib.client.protobuf.TickOptionComputationProto.TickOptionComputation tickOptionComputation) {}
    @Override public void updateAccountTimeProtoBuf(com.ib.client.protobuf.AccountUpdateTimeProto.AccountUpdateTime accountUpdateTime) {}
    @Override public void historicalNewsEndProtoBuf(com.ib.client.protobuf.HistoricalNewsEndProto.HistoricalNewsEnd historicalNewsEnd) {}
    @Override public void historicalTicksLastProtoBuf(com.ib.client.protobuf.HistoricalTicksLastProto.HistoricalTicksLast historicalTicksLast) {}
    @Override public void softDollarTiersProtoBuf(com.ib.client.protobuf.SoftDollarTiersProto.SoftDollarTiers softDollarTiers) {}
    @Override public void execDetailsProtoBuf(com.ib.client.protobuf.ExecutionDetailsProto.ExecutionDetails executionDetails) {}
    @Override public void pnlSingleProtoBuf(com.ib.client.protobuf.PnLSingleProto.PnLSingle pnlSingle) {}
    @Override public void tickNewsProtoBuf(com.ib.client.protobuf.TickNewsProto.TickNews tickNews) {}
    @Override public void updateMarketDepthL2ProtoBuf(com.ib.client.protobuf.MarketDepthL2Proto.MarketDepthL2 marketDepthL2) {}
    @Override public void completedOrdersEndProtoBuf(com.ib.client.protobuf.CompletedOrdersEndProto.CompletedOrdersEnd completedOrdersEnd) {}
    @Override public void completedOrderProtoBuf(com.ib.client.protobuf.CompletedOrderProto.CompletedOrder completedOrder) {}
    @Override public void bondContractDataProtoBuf(com.ib.client.protobuf.ContractDataProto.ContractData contractData) {}
    @Override public void contractDataEndProtoBuf(com.ib.client.protobuf.ContractDataEndProto.ContractDataEnd contractDataEnd) {}
    @Override public void pnlProtoBuf(com.ib.client.protobuf.PnLProto.PnL pnl) {}
    @Override public void tickSizeProtoBuf(com.ib.client.protobuf.TickSizeProto.TickSize tickSize) {}
    @Override public void tickGenericProtoBuf(com.ib.client.protobuf.TickGenericProto.TickGeneric tickGeneric) {}
    @Override public void wshEventDataProtoBuf(com.ib.client.protobuf.WshEventDataProto.WshEventData wshEventData) {}
    @Override public void marketDepthExchangesProtoBuf(com.ib.client.protobuf.MarketDepthExchangesProto.MarketDepthExchanges marketDepthExchanges) {}
    @Override public void currentTimeInMillisProtoBuf(com.ib.client.protobuf.CurrentTimeInMillisProto.CurrentTimeInMillis currentTimeInMillis) {}
    @Override public void historicalDataEndProtoBuf(com.ib.client.protobuf.HistoricalDataEndProto.HistoricalDataEnd historicalDataEnd) {}
    @Override public void updateNewsBulletinProtoBuf(com.ib.client.protobuf.NewsBulletinProto.NewsBulletin newsBulletin) {}
    @Override public void secDefOptParameterProtoBuf(com.ib.client.protobuf.SecDefOptParameterProto.SecDefOptParameter secDefOptParameter) {}
    @Override public void rerouteMarketDataRequestProtoBuf(com.ib.client.protobuf.RerouteMarketDataRequestProto.RerouteMarketDataRequest rerouteMarketDataRequest) {}
    @Override public void historicalTicksProtoBuf(com.ib.client.protobuf.HistoricalTicksProto.HistoricalTicks historicalTicks) {}
    @Override public void verifyMessageApiProtoBuf(com.ib.client.protobuf.VerifyMessageApiProto.VerifyMessageApi verifyMessageApi) {}
    @Override public void errorProtoBuf(com.ib.client.protobuf.ErrorMessageProto.ErrorMessage errorMessage) {}
    @Override public void tickByTickDataProtoBuf(com.ib.client.protobuf.TickByTickDataProto.TickByTickData tickByTickData) {}
    @Override public void tickStringProtoBuf(com.ib.client.protobuf.TickStringProto.TickString tickString) {}
    @Override public void smartComponentsProtoBuf(com.ib.client.protobuf.SmartComponentsProto.SmartComponents smartComponents) {}
    @Override public void updateAccountValueProtoBuf(com.ib.client.protobuf.AccountValueProto.AccountValue accountValue) {}
    @Override public void orderStatusProtoBuf(com.ib.client.protobuf.OrderStatusProto.OrderStatus orderStatus) {}
    @Override public void verifyCompletedProtoBuf(com.ib.client.protobuf.VerifyCompletedProto.VerifyCompleted verifyCompleted) {}
    @Override public void openOrderProtoBuf(com.ib.client.protobuf.OpenOrderProto.OpenOrder openOrder) {}
    @Override public void openOrdersEndProtoBuf(com.ib.client.protobuf.OpenOrdersEndProto.OpenOrdersEnd openOrdersEnd) {}
    @Override public void positionMultiProtoBuf(com.ib.client.protobuf.PositionMultiProto.PositionMulti positionMulti) {}
    @Override public void commissionAndFeesReportProtoBuf(com.ib.client.protobuf.CommissionAndFeesReportProto.CommissionAndFeesReport commissionAndFeesReport) {}
    @Override public void positionProtoBuf(com.ib.client.protobuf.PositionProto.Position position) {}
    @Override public void historicalDataUpdateProtoBuf(com.ib.client.protobuf.HistoricalDataUpdateProto.HistoricalDataUpdate historicalDataUpdate) {}
    @Override public void headTimestampProtoBuf(com.ib.client.protobuf.HeadTimestampProto.HeadTimestamp headTimestamp) {}
    @Override public void orderBoundProtoBuf(com.ib.client.protobuf.OrderBoundProto.OrderBound orderBound) {}
    @Override public void receiveFAProtoBuf(com.ib.client.protobuf.ReceiveFAProto.ReceiveFA receiveFA) {}
    @Override public void displayGroupListProtoBuf(com.ib.client.protobuf.DisplayGroupListProto.DisplayGroupList displayGroupList) {}
    @Override public void rerouteMarketDepthRequestProtoBuf(com.ib.client.protobuf.RerouteMarketDepthRequestProto.RerouteMarketDepthRequest rerouteMarketDepthRequest) {}
    @Override public void updateMarketDepthProtoBuf(com.ib.client.protobuf.MarketDepthProto.MarketDepth marketDepth) {}
    @Override public void histogramDataProtoBuf(com.ib.client.protobuf.HistogramDataProto.HistogramData histogramData) {}
    @Override public void fundamentalsDataProtoBuf(com.ib.client.protobuf.FundamentalsDataProto.FundamentalsData fundamentalsData) {}
    @Override public void accountUpdateMultiProtoBuf(com.ib.client.protobuf.AccountUpdateMultiProto.AccountUpdateMulti accountUpdateMulti) {}
    @Override public void symbolSamplesProtoBuf(com.ib.client.protobuf.SymbolSamplesProto.SymbolSamples symbolSamples) {}
    @Override public void historicalScheduleProtoBuf(com.ib.client.protobuf.HistoricalScheduleProto.HistoricalSchedule historicalSchedule) {}
    @Override public void marketRuleProtoBuf(com.ib.client.protobuf.MarketRuleProto.MarketRule marketRule) {}
    @Override public void tickPriceProtoBuf(com.ib.client.protobuf.TickPriceProto.TickPrice tickPrice) {}
    @Override public void nextValidIdProtoBuf(com.ib.client.protobuf.NextValidIdProto.NextValidId nextValidId) {}
    @Override public void positionMultiEndProtoBuf(com.ib.client.protobuf.PositionMultiEndProto.PositionMultiEnd positionMultiEnd) {}
    @Override public void secDefOptParameterEndProtoBuf(com.ib.client.protobuf.SecDefOptParameterEndProto.SecDefOptParameterEnd secDefOptParameterEnd) {}
    @Override public void accountUpdateMultiEndProtoBuf(com.ib.client.protobuf.AccountUpdateMultiEndProto.AccountUpdateMultiEnd accountUpdateMultiEnd) {}
    @Override public void accountSummaryProtoBuf(com.ib.client.protobuf.AccountSummaryProto.AccountSummary accountSummary) {}
    @Override public void userInfoProtoBuf(com.ib.client.protobuf.UserInfoProto.UserInfo userInfo) {}
    @Override public void tickSnapshotEndProtoBuf(com.ib.client.protobuf.TickSnapshotEndProto.TickSnapshotEnd tickSnapshotEnd) {}
    @Override public void realTimeBarTickProtoBuf(com.ib.client.protobuf.RealTimeBarTickProto.RealTimeBarTick realTimeBarTick) {}
    @Override public void updatePortfolioProtoBuf(com.ib.client.protobuf.PortfolioValueProto.PortfolioValue portfolioValue) {}
    @Override public void displayGroupUpdatedProtoBuf(com.ib.client.protobuf.DisplayGroupUpdatedProto.DisplayGroupUpdated displayGroupUpdated) {}
    @Override public void historicalTicksBidAskProtoBuf(com.ib.client.protobuf.HistoricalTicksBidAskProto.HistoricalTicksBidAsk historicalTicksBidAsk) {}
    @Override public void familyCodesProtoBuf(com.ib.client.protobuf.FamilyCodesProto.FamilyCodes familyCodes) {}
    @Override public void currentTimeProtoBuf(com.ib.client.protobuf.CurrentTimeProto.CurrentTime currentTime) {}
    @Override public void contractDataProtoBuf(com.ib.client.protobuf.ContractDataProto.ContractData contractData) {}
    @Override public void newsProvidersProtoBuf(com.ib.client.protobuf.NewsProvidersProto.NewsProviders newsProviders) {}
    @Override public void newsArticleProtoBuf(com.ib.client.protobuf.NewsArticleProto.NewsArticle newsArticle) {}
    @Override public void replaceFAEndProtoBuf(com.ib.client.protobuf.ReplaceFAEndProto.ReplaceFAEnd replaceFAEnd) {}
    @Override public void positionEndProtoBuf(com.ib.client.protobuf.PositionEndProto.PositionEnd positionEnd) {}
    @Override public void marketDataTypeProtoBuf(com.ib.client.protobuf.MarketDataTypeProto.MarketDataType marketDataType) {}
    @Override public void accountDataEndProtoBuf(com.ib.client.protobuf.AccountDataEndProto.AccountDataEnd accountDataEnd) {}
    @Override public void scannerDataProtoBuf(com.ib.client.protobuf.ScannerDataProto.ScannerData scannerData) {}
    @Override public void historicalDataProtoBuf(com.ib.client.protobuf.HistoricalDataProto.HistoricalData historicalData) {}
    @Override public void execDetailsEndProtoBuf(com.ib.client.protobuf.ExecutionDetailsEndProto.ExecutionDetailsEnd executionDetailsEnd) {}
}
