package com.calgary.fili.trader.bot.trader;

import com.ib.client.*;
import com.calgary.fili.trader.bot.storage.TradeLogStore;
import com.calgary.fili.trader.bot.strategy.PingPongStrategy;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Central live-runtime orchestrator for one symbol process.
 *
 * <p>This class is the main integration seam between:</p>
 * <ul>
 *   <li>Spring Boot startup/shutdown lifecycle</li>
 *   <li>live market data (either direct IBKR or Databento)</li>
 *   <li>execution routing (either direct IBKR socket ownership or the shared Python gateway)</li>
 *   <li>the single-threaded {@code PingPongStrategy} actor</li>
 *   <li>cross-bot coordination such as shared capital and shared Databento relay startup</li>
 * </ul>
 *
 * <p>Operationally, one JVM process owns exactly one symbol. The process may still consume a shared
 * Databento relay stream or a shared IBKR execution gateway, but strategy state is symbol-local and must
 * remain isolated from other processes.</p>
 *
 * <p>Important invariants:</p>
 * <ul>
 *   <li>Strategy state changes should be delegated into {@code PingPongStrategy}'s actor queue rather than
 *       mutated directly from callback threads.</li>
 *   <li>Market-data freshness gates order placement when Databento is the source of truth.</li>
 *   <li>Order-type policy is intent-based, not action-only: entries use fast limit, exits/covers use market.</li>
 *   <li>When shared Databento feed mode is enabled, this bot should not open its own Databento session on
 *       relay failure because that would scale poorly across many symbol bots.</li>
 * </ul>
 */
@Component
public class IBKRTrader implements CommandLineRunner, EWrapper {

    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final Logger log = LoggerFactory.getLogger(IBKRTrader.class);
    private static final Set<String> DATABENTO_API_KEY_PLACEHOLDERS = Set.of(
        "replace_me",
        "paste_your_api_key_here",
        "<your-key>",
        "<your_databento_api_key>",
        "changeme"
    );

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
    private DatabentoLiveGateway databentoLiveGateway;
    private SharedIbkrGatewayClient sharedIbkrGatewayClient;
    private final DatabentoFeedHealth databentoFeedHealth = new DatabentoFeedHealth();
    private final ScheduledExecutorService databentoSupervisorScheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile ScheduledFuture<?> databentoFeedMonitorFuture;
    private volatile ScheduledFuture<?> marketScheduleMonitorFuture;
    private final AtomicBoolean databentoRestartPending = new AtomicBoolean(false);
    private final AtomicBoolean databentoSidecarConfigErrorLogged = new AtomicBoolean(false);
    private ModelBundleResolver modelBundleResolver;
    private ModelBundleResolver.ResolvedBundle resolvedModelBundle;

    @Value("${trading.host:127.0.0.1}") private String ibHost;
    @Value("${trading.port:7497}") private int ibPort;
    @Value("${trading.client-id:1}") private int clientId;
    @Value("${trading.market-data.provider:ibkr}") private String marketDataProvider;
    @Value("${trading.symbol:}") private String symbol;
    @Value("${trading.trade-amount:40000}") private int tradeAmount;
    @Value("${trading.max-trades:20}") private int maxTrades;
    @Value("${trading.reversal-percentage:0.0005}") private double reversalPercentage;
    @Value("${trading.stop-loss-percentage:0.004}") private double stopLossPercentage;
    @Value("${trading.max-daily-drawdown:500.0}") private double maxDailyDrawdown;
    @Value("${trading.post-hard-stop-entry-cooldown-ms:300000}") private long postHardStopEntryCooldownMs;
    @Value("${trading.max-hard-stops-per-day:3}") private int maxHardStopsPerDay;
    @Value("${trading.ai.long-entry-threshold:0.68}") private double aiLongEntryThreshold;
    @Value("${trading.ai.short-entry-threshold:0.63}") private double aiShortEntryThreshold;
    @Value("${trading.ai.long-exit-threshold:0.58}") private double aiLongExitThreshold;
    @Value("${trading.ai.short-exit-threshold:0.60}") private double aiShortExitThreshold;
    @Value("${trading.ai.regime-threshold:0.50}") private double aiRegimeThreshold;
    @Value("${trading.ai.entry-threshold-raise-percent:0.0}") private double aiEntryThresholdRaisePercent;
    @Value("${trading.ai.open30.long-entry-threshold:}") private String aiOpen30LongEntryThresholdRaw;
    @Value("${trading.ai.open30.short-entry-threshold:}") private String aiOpen30ShortEntryThresholdRaw;
    @Value("${trading.ai.open30.long-exit-threshold:}") private String aiOpen30LongExitThresholdRaw;
    @Value("${trading.ai.open30.short-exit-threshold:}") private String aiOpen30ShortExitThresholdRaw;
    @Value("${trading.ai.regime.choppy.long-entry-threshold:}") private String aiChoppyLongEntryThresholdRaw;
    @Value("${trading.ai.regime.choppy.short-entry-threshold:}") private String aiChoppyShortEntryThresholdRaw;
    @Value("${trading.ai.regime.choppy.long-exit-threshold:}") private String aiChoppyLongExitThresholdRaw;
    @Value("${trading.ai.regime.choppy.short-exit-threshold:}") private String aiChoppyShortExitThresholdRaw;
    @Value("${trading.ai.regime.trend.long-entry-threshold:}") private String aiTrendLongEntryThresholdRaw;
    @Value("${trading.ai.regime.trend.short-entry-threshold:}") private String aiTrendShortEntryThresholdRaw;
    @Value("${trading.ai.regime.trend.long-exit-threshold:}") private String aiTrendLongExitThresholdRaw;
    @Value("${trading.ai.regime.trend.short-exit-threshold:}") private String aiTrendShortExitThresholdRaw;
    @Value("${trading.ai.regime.volatile.long-entry-threshold:}") private String aiVolatileLongEntryThresholdRaw;
    @Value("${trading.ai.regime.volatile.short-entry-threshold:}") private String aiVolatileShortEntryThresholdRaw;
    @Value("${trading.ai.regime.volatile.long-exit-threshold:}") private String aiVolatileLongExitThresholdRaw;
    @Value("${trading.ai.regime.volatile.short-exit-threshold:}") private String aiVolatileShortExitThresholdRaw;
    @Value("${trading.market-data-request-id:1001}") private int marketDataRequestId;
    @Value("${trading.risk.max-order-notional:500000}") private double maxOrderNotional;
    @Value("${trading.risk.max-daily-orders:40}") private int maxDailyOrders;
    @Value("${trading.risk.max-share-cap:2000}") private int maxShareCap = 2000;
    @Value("${trading.shared-capital.enabled:false}") private boolean sharedCapitalEnabled;
    @Value("${trading.shared-capital.file:runtime/shared-capital.properties}") private String sharedCapitalFile;
    @Value("${trading.shared-capital.total-notional:0}") private double sharedCapitalTotalNotional;
    @Value("${trading.model.dir:}") private String modelDir;
    @Value("${trading.databento.python-bin:python3}") private String databentoPythonBin;
    @Value("${trading.databento.normalizer-script:scripts/databento_live_normalizer.py}") private String databentoNormalizerScript;
    @Value("${trading.databento.env-file:runtime/databento.env}") private String databentoEnvFile;
    @Value("${trading.databento.api.key:}") private String databentoApiKey;
    @Value("${trading.databento.live-gateway:}") private String databentoLiveGatewayOverride;
    @Value("${trading.databento.equity-dataset:DBEQ.BASIC}") private String databentoEquityDataset;
    @Value("${trading.databento.equity-schema:tbbo}") private String databentoEquitySchema;
    @Value("${trading.databento.startup-history-seconds:360}") private double databentoStartupHistorySeconds;
    @Value("${trading.databento.shared-feed.startup-history-seconds:0}") private double databentoSharedFeedStartupHistorySeconds;
    @Value("${trading.databento.startup-history-schema:ohlcv-1s}") private String databentoStartupHistorySchema;
    @Value("${trading.databento.options-dataset:OPRA.PILLAR}") private String databentoOptionsDataset;
    @Value("${trading.databento.options-schema:ohlcv-1s}") private String databentoOptionsSchema;
    @Value("${trading.databento.option-parents:}") private String databentoOptionParents;
    @Value("${trading.databento.heartbeat-seconds:15}") private int databentoHeartbeatSeconds;
    @Value("${trading.databento.startup-delay-seconds:40}") private double databentoStartupDelaySeconds;
    @Value("${trading.databento.auto-restart.enabled:true}") private boolean databentoAutoRestartEnabled;
    @Value("${trading.databento.restart-delay-ms:2000}") private long databentoRestartDelayMs;
    @Value("${trading.databento.max-silence-ms:120000}") private long databentoMaxSilenceMs;
    @Value("${trading.databento.feature-snapshots.enabled:false}") private boolean databentoFeatureSnapshotsEnabled;
    @Value("${trading.databento.feature-snapshots.source:live_normalizer_30s_v1}") private String databentoFeatureSnapshotSource;
    @Value("${trading.databento.feature-snapshots.schema-version:live_normalizer_30s_v1}") private String databentoFeatureSnapshotSchemaVersion;
    @Value("${trading.databento.quote-stale-threshold-ms:5000}") private long databentoQuoteStaleThresholdMs;
    @Value("${trading.databento.sanity.expected-event-schema-version:databento_ndjson_v2}") private String databentoExpectedEventSchemaVersion;
    @Value("${trading.databento.sanity.min-quality-score:0.50}") private double databentoMinQualityScore;
    @Value("${trading.databento.sanity.block-entries-on-invalid:true}") private boolean databentoBlockEntriesOnInvalidSanity;
    @Value("${trading.databento.allow-stale-closing-market-order:true}") private boolean databentoAllowStaleClosingMarketOrder;
    @Value("${trading.databento.model-routing-csv:runtime/databento/model-routing.csv}") private String databentoModelRoutingCsv;
    @Value("${trading.databento.symbol-plan-csv:training_data/databento_30s/symbol_model_plan.csv}") private String databentoSymbolPlanCsv;
    @Value("${trading.databento.shared-feed.enabled:true}") private boolean databentoSharedFeedEnabled;
    @Value("${trading.databento.shared-feed.host:127.0.0.1}") private String databentoSharedFeedHost;
    @Value("${trading.databento.shared-feed.port:9800}") private int databentoSharedFeedPort;
    @Value("${trading.databento.shared-feed.start-if-missing:true}") private boolean databentoSharedFeedStartIfMissing;
    @Value("${trading.databento.shared-feed.fallback-to-private-sidecar:false}") private boolean databentoSharedFeedFallbackToPrivateSidecar;
    @Value("${trading.databento.shared-feed.start-timeout-ms:15000}") private long databentoSharedFeedStartTimeoutMs;
    @Value("${trading.databento.shared-feed.expected-client-count:1}") private int databentoSharedFeedExpectedClientCount;
    @Value("${trading.databento.shared-feed.client-wait-timeout-ms:15000}") private long databentoSharedFeedClientWaitTimeoutMs;
    @Value("${trading.databento.shared-feed.script:scripts/databento_shared_feed_relay.py}") private String databentoSharedFeedScript;
    @Value("${trading.databento.shared-feed.bots-dir:runtime/databento/bots}") private String databentoSharedFeedBotsDir;
    @Value("${trading.databento.shared-feed.lock-file:runtime/databento/shared-feed-relay.lock}") private String databentoSharedFeedLockFile;
    @Value("${trading.databento.shared-feed.pid-file:runtime/databento/shared-feed-relay.pid}") private String databentoSharedFeedPidFile;
    @Value("${trading.databento.shared-feed.log-file:runtime/databento/logs/databento-shared-feed-relay.log}") private String databentoSharedFeedLogFile;
    @Value("${trading.databento.restart-jitter-ms:2500}") private long databentoRestartJitterMs;
    @Value("${trading.ibkr.shared-gateway.enabled:false}") private boolean ibkrSharedGatewayEnabled;
    @Value("${trading.ibkr.shared-gateway.host:127.0.0.1}") private String ibkrSharedGatewayHost;
    @Value("${trading.ibkr.shared-gateway.port:9910}") private int ibkrSharedGatewayPort;
    @Value("${trading.ibkr.shared-gateway.connect-timeout-ms:3000}") private long ibkrSharedGatewayConnectTimeoutMs;
    @Value("${trading.ibkr.shared-gateway.ack-timeout-ms:5000}") private long ibkrSharedGatewayAckTimeoutMs;
    @Value("${trading.ibkr.shared-gateway.skip-direct-connection:false}") private boolean ibkrSharedGatewaySkipDirectConnection;
    @Value("${trading.ibkr.position-sync.timeout-ms:8000}") private long ibkrPositionSyncTimeoutMs;
    @Value("${trading.state.file:trader-state.properties}") private String stateFile;
    @Value("${trading.log.file:trades.csv}") private String tradeLogFile;
    @Value("${trading.log.storage-mode:both}") private String tradeLogStorageMode;
    private String resolvedTradeLogFile;
    private SharedCapitalManager sharedCapitalManager;
    private volatile boolean symbolPositionSeenThisCycle = false;

    private enum PositionSyncMode {
        BLOCK_BAR_FORWARDING,
        PRESERVE_BAR_FORWARDING
    }

    private final AtomicLong positionSyncAttemptSequence = new AtomicLong(0L);
    private volatile long lastPositionSyncAttemptId = 0L;
    private volatile long lastPositionSyncStartedAtMs = 0L;
    private volatile long lastPositionSyncCompletedAtMs = 0L;
    private volatile long lastPositionSyncFailedAtMs = 0L;
    private volatile String lastPositionSyncReason = "";
    private volatile String lastPositionSyncMode = "";
    private volatile String lastPositionSyncTransport = "idle";
    private volatile String lastPositionSyncFailureStage = "";
    private volatile String lastPositionSyncFailureReason = "";
    private volatile String lastPositionSyncAckDetail = "";
    private volatile Integer lastPositionSyncReqId = null;
    private volatile ScheduledFuture<?> positionSyncTimeoutFuture;

    private final MeterRegistry meterRegistry;
    private final TradeLogStore tradeLogStore;

    private record OrderContext(String symbol, String action, int quantity, boolean closingTrade,
                                double reservedNotional, boolean capitalReserved) {}

    @Autowired
    public IBKRTrader(MeterRegistry meterRegistry, TradeLogStore tradeLogStore) {
        this.meterRegistry = meterRegistry;
        this.tradeLogStore = tradeLogStore;
    }

    public IBKRTrader(MeterRegistry meterRegistry) {
        this(meterRegistry, null);
    }

    public double getYesterdayClose() { return this.yesterdayClose; }

    @PostConstruct
    public void postConstruct() {
        sharedCapitalManager = new SharedCapitalManager(
            sharedCapitalEnabled,
            Paths.get(sharedCapitalFile),
            sharedCapitalTotalNotional
        );
        modelBundleResolver = new ModelBundleResolver(modelDir, databentoSymbolPlanCsv, databentoModelRoutingCsv);
        flowInfo("BOOT", "IBKRTrader bean initialized");
        flowData("BOOT", "tradeLog storageMode=" + normalizedTradeLogStorageMode());
        flowData("BOOT", "marketDataProvider=" + normalizedMarketDataProvider() + " routingCsv=" + databentoModelRoutingCsv + " symbolPlanCsv=" + databentoSymbolPlanCsv);
        if (useDatabentoMarketData()) {
            flowData(
                "BOOT",
                "databento sharedFeedEnabled=" + databentoSharedFeedEnabled
                    + " relayHost=" + databentoSharedFeedHost
                    + " relayPort=" + databentoSharedFeedPort
                    + " featureSnapshotsEnabled=" + databentoFeatureSnapshotsEnabled
                    + " liveGateway=" + (databentoLiveGatewayOverride == null || databentoLiveGatewayOverride.isBlank() ? "<default>" : databentoLiveGatewayOverride.trim())
                    + " startIfMissing=" + databentoSharedFeedStartIfMissing
            );
        }
        flowData(
            "BOOT",
            "ibkr sharedGatewayEnabled=" + ibkrSharedGatewayEnabled
                + " host=" + ibkrSharedGatewayHost
                + " port=" + ibkrSharedGatewayPort
                + " skipDirectConnection=" + ibkrSharedGatewaySkipDirectConnection
        );
        if (sharedCapitalManager.isEnabled()) {
            SharedCapitalManager.Snapshot snapshot = sharedCapitalManager.snapshot();
            flowData(
                "BOOT",
                "sharedCapital enabled=true file=" + snapshot.stateFile()
                    + " total=" + snapshot.totalNotional()
                    + " available=" + snapshot.availableNotional()
            );
        } else {
            flowData("BOOT", "sharedCapital enabled=false");
        }
    }

    @Override
    public void run(String... args) throws Exception {
        // Spring hands control here once the application context is fully built.
        // From this point onward the process behaves like a dedicated symbol daemon:
        // connect execution, bootstrap strategy state, subscribe market data, then stay alive.
        flowInfo("BOOT", "SPRING BOOT STARTED: Initializing Trading Bot...");
        if (useSharedIbkrGateway() && ibkrSharedGatewaySkipDirectConnection) {
            // Shared-gateway-only mode means this JVM never opens its own IBKR socket; it boots entirely
            // through the Python execution gateway and then stays alive to receive async events.
            ensureSharedIbkrGatewayConnected(true);
            flowCondition("BOOT", "IBKR_CONNECTED", isSharedIbkrGatewayConnected(), "sharedGateway host=" + ibkrSharedGatewayHost + " port=" + ibkrSharedGatewayPort);
            onConnected();
            Thread.currentThread().join();
            return;
        }
        client = new EClientSocket(this, signal);
        client.eConnect(ibHost, ibPort, clientId);
        flowData("BOOT", "connect host=" + ibHost + " port=" + ibPort + " clientId=" + clientId + " symbol=" + symbol);
        // IBKR delivers callbacks through the reader thread; without it the socket would be connected but mute.
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
        // Treat any successful execution-channel connection as a full symbol bootstrap event.
        // We intentionally re-request positions/open orders and rebuild live subscriptions here so
        // reconnect and cold-start follow the same initialization path.
        positionSyncComplete = false;
        symbolPositionSeenThisCycle = false;
        lastScheduleAllowNewEntries = null;
        boolean symbolPresent = symbol != null && !symbol.isBlank();
        flowCondition("BOOT", "SYMBOL_PRESENT", symbolPresent, "rawSymbol=" + symbol);
        symbol = symbolPresent ? symbol.trim().toUpperCase() : "TSLA";
        // Feed health and the shared gateway both track symbols by normalized uppercase keys.
        databentoFeedHealth.registerSymbol(symbol);
        ensureSharedIbkrGatewayConnected(false);
        registerSymbolWithSharedGateway();
        // The schedule monitor is independent of bar delivery so close logic still fires during feed outages.
        startMarketScheduleMonitor();
        resetOptionVolumeAccumulators("startup");
        // Model resolution happens once per bootstrap/reconnect so strategy inference uses the latest promoted bundle.
        resolvedModelBundle = resolveModelBundle(symbol);
        String effectiveModelDir = resolvedModelBundle != null && resolvedModelBundle.modelDir() != null && !resolvedModelBundle.modelDir().isBlank()
            ? resolvedModelBundle.modelDir()
            : modelDir;
        flowData(
            "BOOT",
            "normalizedSymbol=" + symbol
                + " marketDataProvider=" + normalizedMarketDataProvider()
                + " modelDir=" + (effectiveModelDir == null || effectiveModelDir.isBlank() ? "classpath" : effectiveModelDir)
                + " bundleJob=" + (resolvedModelBundle == null ? "" : resolvedModelBundle.jobName())
                + " bundleVariant=" + (resolvedModelBundle == null ? "" : resolvedModelBundle.variant())
                + " bundleNote=" + (resolvedModelBundle == null ? "" : resolvedModelBundle.note())
        );

        if (shopStrategy == null) {
            shopStrategy = new PingPongStrategy(this, symbol, 0.003, tradeAmount, maxTrades, true, 12, 14, reversalPercentage, stopLossPercentage, maxDailyDrawdown, 1.20, 0.70, effectiveModelDir, postHardStopEntryCooldownMs, maxHardStopsPerDay);
            loadStrategyState();
            meterRegistry.gauge("trading.strategy.stop.queue.depth", shopStrategy, PingPongStrategy::getLastStopQueueDepth);
            meterRegistry.gauge("trading.strategy.stop.ack.latency.last", shopStrategy, PingPongStrategy::getLastStopAckLatencyMs);
            meterRegistry.gauge("trading.strategy.stop.ack.latency.p50", shopStrategy, PingPongStrategy::getStopAckLatencyP50Ms);
            meterRegistry.gauge("trading.strategy.stop.ack.latency.p95", shopStrategy, PingPongStrategy::getStopAckLatencyP95Ms);
            meterRegistry.gauge("trading.strategy.stop.ack.latency.samples", shopStrategy, PingPongStrategy::getStopAckLatencySampleCount);
            meterRegistry.gauge("trading.connection.status", this, IBKRTrader::getConnectionStatus);
            meterRegistry.gauge("trading.reconnection.attempts", this, IBKRTrader::getReconnectionAttempts);
        }

        shopStrategy.setAiThresholds(buildAiThresholdConfig());

        if (this.yesterdayClose > 0) {
            shopStrategy.setYesterdayClose(this.yesterdayClose);
            flowCondition("IBKR.SYNC", "YESTERDAY_CLOSE_AVAILABLE", true, "value=" + this.yesterdayClose);
        } else {
            flowCondition("IBKR.SYNC", "YESTERDAY_CLOSE_AVAILABLE", false, "value=" + this.yesterdayClose);
        }

        // Position sync must happen before the strategy is allowed to act on ticks/bars, otherwise it can trade
        // before it knows whether it already owns inventory from a prior session.
        requestPositions(PositionSyncMode.BLOCK_BAR_FORWARDING, "startup-bootstrap");
        if (useDatabentoMarketData()) {
            startDatabentoFeedMonitor();
            startDatabentoLiveGateway();
        } else {
            stopDatabentoFeedMonitor();
            if (client != null && client.isConnected()) {
                subscribeToMarketData();
            } else {
                flowInfo("IBKR.SUBSCRIBE", "Direct market-data subscription skipped because direct IBKR client is not connected.");
            }
        }

        Contract contract = buildStockContract();
        String queryTime = LocalDate.now(MARKET_ZONE).atStartOfDay(MARKET_ZONE).minusSeconds(1).format(DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss"));
        if (client != null && client.isConnected()) {
            client.reqHistoricalData(marketDataRequestId + 2, contract, queryTime, "1 D", "1 day", "TRADES", 1, 1, false, null);
        } else {
            flowInfo("IBKR.SYNC", "Historical close bootstrap skipped because direct IBKR client is not connected.");
        }
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
        // Any in-memory order tracking may now be stale relative to the broker, so clear local timers/contexts
        // and rebuild truth from sync callbacks after reconnect.
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
        // Reconnect follows the same operational bootstrap pattern as cold start: resume schedule monitoring,
        // reset transient feed-derived state, then resynchronize positions/orders before strategy trading resumes.
        startMarketScheduleMonitor();
        resetOptionVolumeAccumulators("reconnect");
        requestPositions(PositionSyncMode.BLOCK_BAR_FORWARDING, "reconnect-bootstrap");
        requestOpenOrdersSync();
        if (useDatabentoMarketData()) {
            startDatabentoFeedMonitor();
            startDatabentoLiveGateway();
        } else {
            stopDatabentoFeedMonitor();
            subscribeToMarketData();
        }
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
        flowConditionDebug("IBKR.TICK", "EXPECTED_TICKER_ID", expectedTicker, "tickerId=" + tickerId + " expected=" + marketDataRequestId + " field=" + field);
        if (!expectedTicker) return;

        if (field == 9) {
            // IBKR field 9 is prior close; the strategy uses it for daily context and gap-aware logic.
            this.yesterdayClose = price;
            flowConditionDebug("IBKR.TICK", "YESTERDAY_CLOSE_VALID", price > 0.0, "value=" + price);
            flowDataDebug("IBKR.TICK", "field=9 yesterdayClose=" + price);
            if (shopStrategy != null) {
                shopStrategy.setYesterdayClose(price);
            }
            persistStrategyState();
            return;
        }

        if (field == 1) {
            // Bid updates refresh the quote snapshot used for sell-side reference pricing and sizing.
            this.currentBidPrice = price;
            flowDataDebug("IBKR.TICK", "field=1 bid=" + price);
            if (shopStrategy != null) {
                shopStrategy.onQuoteSnapshot(currentBidPrice, currentAskPrice, currentBidSize, currentAskSize, latestShortableShares);
            }
            return;
        }

        if (field == 2) {
            // Ask updates refresh the quote snapshot used for buy-side reference pricing and sizing.
            this.currentAskPrice = price;
            flowDataDebug("IBKR.TICK", "field=2 ask=" + price);
            if (shopStrategy != null) {
                shopStrategy.onQuoteSnapshot(currentBidPrice, currentAskPrice, currentBidSize, currentAskSize, latestShortableShares);
            }
            return;
        }

        if (field == 4) {
            if (Math.abs(price - this.currentLastPrice) > 0.001) {
                this.currentLastPrice = price;
                // Last-trade ticks are only used for exit-style monitoring once position sync has completed.
                // That keeps the strategy from reacting to market motion before inventory state is trusted.
                boolean canForward = shopStrategy != null && positionSyncComplete;
                flowConditionDebug("IBKR->AI.TICK", "FORWARD_TICK_TO_STRATEGY", canForward, "lastPrice=" + price + " positionSyncComplete=" + positionSyncComplete + " strategyReady=" + (shopStrategy != null));
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
            // Normalize bar timestamps into market-local time once so logging, schedule logic, and strategy
            // all reason about the same wall clock.
            barTs = Instant.ofEpochSecond(time).atZone(ZoneOffset.UTC).withZoneSameInstant(MARKET_ZONE);
            flowData("IBKR.BAR", "reqId=" + reqId + " tsEt=" + barTs + " ohlc=" + open + "/" + high + "/" + low + "/" + close + " count=" + count);
            flowCondition("IBKR->AI.BAR", "FORWARD_BAR_TO_STRATEGY", canForward, "positionSyncComplete=" + positionSyncComplete + " strategyReady=" + (shopStrategy != null));
        }

        if (expectedReq && shopStrategy != null && barTs != null) {
            // Schedule enforcement runs even if the strategy later decides to ignore the bar.
            applyMarketSchedule(barTs);
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

            // Strategy ingests the fully normalized 5-second bar only after quote/position gating succeeds.
            shopStrategy.on5SecondBar(time, open, high, low, close, vol, vwapVal);
        }
    }

    public void placeTrade(String symbol, String action, double currentPrice, int quantity, String orderType) {
        // This method is the last synchronous gate before an order leaves the Java process.
        // By the time we reach the actual broker/gateway call, the request should already have been
        // normalized for intent (entry vs exit), quote freshness, notional limits, daily caps, and
        // shared-capital policy. Keeping the policy concentrated here prevents strategy call sites from
        // duplicating routing logic and drifting out of sync.
        rollRiskCountersIfNeeded();
        boolean readyToPlaceOrder = (currentOrderId >= 0 || isSharedIbkrGatewayConnected()) && !isKillSwitchActive();
        if (!readyToPlaceOrder) {
            flowCondition("ORDER.GATE", "READY_TO_PLACE_ORDER", false, "currentOrderId=" + currentOrderId + " sharedGatewayConnected=" + isSharedIbkrGatewayConnected() + " killSwitch=" + isKillSwitchActive());
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }
        flowCondition("ORDER.GATE", "READY_TO_PLACE_ORDER", true, "currentOrderId=" + currentOrderId + " sharedGatewayConnected=" + isSharedIbkrGatewayConnected() + " killSwitch=" + isKillSwitchActive());

        if (orderType == null || orderType.isBlank()) {
            flowCondition("ORDER.GATE", "ORDER_TYPE_PRESENT", false, "orderType=" + orderType);
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }

        boolean isClosingTrade = false;
        if (shopStrategy != null) {
            int currentPos = shopStrategy.getCurrentPosition();
            // Intent matters more than raw action: SELL can be either short-entry or long-exit, and BUY can be
            // either long-entry or short-cover. We infer that here so routing policy stays consistent everywhere.
            if (("SELL".equals(action) && currentPos > 0) || ("BUY".equals(action) && currentPos < 0)) {
                isClosingTrade = true;
                flowInfo("RISK", "Trade identified as EXIT. Bypassing Notional Limits.");
            }
        }
        String enforcedOrderType = resolvePreferredOrderType(action, isClosingTrade);

        if (shouldBlockForStaleDatabentoQuote(enforcedOrderType, isClosingTrade)) {
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }

        // Resolve the best executable reference from the live quote book before notional and share-cap checks.
        double executionReferencePrice = resolveExecutionReferencePrice(action, currentPrice, enforcedOrderType);
        int shareCap = getMaxShareCap();
        int finalQty = Math.min(quantity, shareCap);
        int orderIdToUse = currentOrderId;

        if (quantity <= 0 || executionReferencePrice <= 0.0) {
            flowCondition("ORDER.GATE", "VALID_QTY_PRICE", false, "qty=" + quantity + " price=" + executionReferencePrice + " requestedPrice=" + currentPrice + " lastAsk=" + currentAskPrice);
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }
        flowCondition("ORDER.GATE", "VALID_QTY_PRICE", true, "qty=" + quantity + " price=" + executionReferencePrice + " requestedPrice=" + currentPrice + " lastAsk=" + currentAskPrice);
        if (quantity > shareCap) {
            flowInfo("RISK", "Order quantity clamped by max share cap requested=" + quantity + " cap=" + shareCap + " finalQty=" + finalQty);
        } else {
            flowCondition("ORDER.GATE", "QTY_WITHIN_MAX_SHARE_CAP", true, "requested=" + quantity + " cap=" + shareCap + " finalQty=" + finalQty);
        }

        if (!isClosingTrade && (executionReferencePrice * finalQty) > maxOrderNotional) {
            flowError("RISK", "Order blocked: notional exceeds limit");
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }
        if (dailySubmittedOrders >= maxDailyOrders) {
            flowError("RISK", "Order blocked: max daily order limit reached");
            if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
            return;
        }

        double requestedNotional = Math.max(0.0, executionReferencePrice * finalQty);
        boolean capitalReserved = false;
        if (!isClosingTrade && sharedCapitalManager != null && sharedCapitalManager.isEnabled()) {
            // Opening trades reserve shared notional up front so sibling symbol bots cannot oversubscribe capital
            // between intent creation and eventual fill/cancel callbacks.
            SharedCapitalManager.ReservationDecision decision = sharedCapitalManager.tryReserve(symbol, requestedNotional);
            if (!decision.allowed()) {
                flowError("RISK", "Order blocked: shared capital unavailable " + decision.message());
                if (shopStrategy != null) shopStrategy.onOrderClosed(orderIdToUse, "Cancelled");
                return;
            }
            SharedCapitalManager.Snapshot snapshot = decision.snapshot();
            capitalReserved = true;
            flowData(
                "RISK",
                "Shared capital reserved symbol=" + symbol
                    + " orderId=" + orderIdToUse
                    + " requested=" + requestedNotional
                    + " availableAfter=" + snapshot.availableNotional()
            );
        }

        if (isSharedIbkrGatewayConnected()) {
            try {
                // Shared gateway is the preferred execution path because it centralizes broker connectivity while
                // still sending async order/position events back into this symbol process.
                SharedIbkrGatewayMessage response = sharedIbkrGatewayClient.submitOrder(
                    symbol,
                    action,
                    finalQty,
                    enforcedOrderType,
                    executionReferencePrice,
                    ("LMT".equals(enforcedOrderType) || "FAST_LMT".equals(enforcedOrderType)) ? executionReferencePrice : null,
                    orderType,
                    Map.of("source", "IBKRTrader", "symbol", symbol, "closing_trade", isClosingTrade)
                );
                if (!response.ok) {
                    if (capitalReserved) {
                        releaseSharedCapital(symbol, "shared-gateway-submit-failed");
                    }
                    flowError("IBKR.GATEWAY", "submit_order rejected detail=" + response.detail);
                    if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
                    return;
                }
                Integer sharedGatewayOrderId = response.payloadInt("gatewayOrderId");
                int sharedOrderId = sharedGatewayOrderId == null ? Math.max(1, currentOrderId) : sharedGatewayOrderId;
                String gatewayStatus = response.payloadString("status");
                Integer gatewayRemaining = response.payloadInt("remaining");
                Integer gatewayFilled = response.payloadInt("filled");
                Double gatewayAvgFillPrice = response.payloadDouble("avg_fill_price");
                boolean gatewayEffectiveTerminal = isSharedGatewayEffectivelyTerminal(gatewayStatus, gatewayRemaining, response.payloadBoolean("effective_terminal"));
                orderContextById.put(sharedOrderId, new OrderContext(symbol, action, finalQty, isClosingTrade, requestedNotional, capitalReserved));
                orderSentTimes.put(sharedOrderId, System.currentTimeMillis());
                lastPlacedOrderId = sharedOrderId;
                lastPlacedOrderAction = action;
                lastPlacedOrderQuantity = finalQty;
                if (gatewayEffectiveTerminal) {
                    int filledQty = gatewayFilled == null ? 0 : Math.max(0, gatewayFilled);
                    if (filledQty > 0 && shopStrategy != null) {
                        shopStrategy.onOrderProgress(sharedOrderId, action, filledQty, gatewayRemaining == null ? 0 : Math.max(0, gatewayRemaining), gatewayAvgFillPrice == null ? 0.0 : gatewayAvgFillPrice);
                    }
                    if (capitalReserved && !isClosingTrade && filledQty == 0) {
                        releaseSharedCapital(symbol, "shared-gateway-submit-terminal-no-fill-" + gatewayStatus);
                    } else if (isClosingTrade && filledQty > 0) {
                        releaseSharedCapital(symbol, "shared-gateway-submit-terminal-fill-" + gatewayStatus);
                    }
                    orderContextById.remove(sharedOrderId);
                    orderSentTimes.remove(sharedOrderId);
                    cumulativeFilledByOrderId.remove(sharedOrderId);
                    ScheduledFuture<?> timeoutFuture = staleOrderTimeoutByOrderId.remove(sharedOrderId);
                    if (timeoutFuture != null) {
                        timeoutFuture.cancel(false);
                    }
                    if (shopStrategy != null) {
                        shopStrategy.onOrderClosed(sharedOrderId, gatewayStatus == null || gatewayStatus.isBlank() ? "Inactive" : gatewayStatus);
                    }
                    dailySubmittedOrders++;
                    flowData("ORDER.SEND", "sharedGateway terminal/noop orderId=" + sharedOrderId + " action=" + action + " status=" + gatewayStatus + " filled=" + filledQty + " remaining=" + gatewayRemaining + " type=" + enforcedOrderType);
                    requestPostOrderPositionValidation(symbol, sharedOrderId, "shared-gateway-terminal-submit");
                    return;
                }
                if (shopStrategy != null) shopStrategy.onOrderSubmitted(sharedOrderId, action, finalQty);
                dailySubmittedOrders++;
                flowData("ORDER.SEND", "sharedGateway orderId=" + sharedOrderId + " action=" + action + " type=" + enforcedOrderType + " requestedPrice=" + currentPrice + " executionReferencePrice=" + executionReferencePrice);
                requestPostOrderPositionValidation(symbol, sharedOrderId, "shared-gateway-submit");
                return;
            } catch (IOException e) {
                if (capitalReserved) {
                    releaseSharedCapital(symbol, "shared-gateway-submit-io-failed");
                }
                flowError("IBKR.GATEWAY", "submit_order failed reason=" + e.getMessage());
                if (ibkrSharedGatewaySkipDirectConnection || client == null || !client.isConnected()) {
                    if (shopStrategy != null) shopStrategy.onOrderClosed(currentOrderId, "Cancelled");
                    return;
                }
                flowInfo("IBKR.GATEWAY", "Falling back to direct IBKR order placement because shared gateway submission failed.");
            }
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

        if ("FAST_LMT".equals(enforcedOrderType)) {
            // FAST_LMT crosses slightly through the inside quote to behave like a near-marketable IOC while still
            // preserving an explicit limit bound.
            order.orderType("LMT");
            double aggressiveOffset = 0.05;
            double fastPrice = "BUY".equals(action) ? (executionReferencePrice + aggressiveOffset) : (executionReferencePrice - aggressiveOffset);
            order.lmtPrice(Math.round(fastPrice * 100.0) / 100.0);
            order.tif("IOC");
        } else if ("LMT".equals(enforcedOrderType)) {
            order.orderType("LMT");
            order.lmtPrice(Math.round(executionReferencePrice * 100.0) / 100.0);
            order.tif("IOC");
        } else {
            order.orderType("MKT");
            order.tif("IOC");
        }

        orderContextById.put(orderIdToUse, new OrderContext(symbol, action, finalQty, isClosingTrade, requestedNotional, capitalReserved));
        orderSentTimes.put(orderIdToUse, System.currentTimeMillis());

        try {
            flowData("ORDER.SEND", "orderId=" + orderIdToUse + " action=" + action + " type=" + enforcedOrderType + " requestedPrice=" + currentPrice + " executionReferencePrice=" + executionReferencePrice + " lastAsk=" + currentAskPrice + " lastBid=" + currentBidPrice);
            client.placeOrder(orderIdToUse, contract, order);
        } catch (Exception e) {
            orderContextById.remove(orderIdToUse);
            orderSentTimes.remove(orderIdToUse);
            if (capitalReserved) {
                releaseSharedCapital(symbol, "order-send-failed");
            }
            flowError("ORDER.SEND", "placeOrder failed orderId=" + orderIdToUse + " reason=" + e.getMessage());
            if (shopStrategy != null) shopStrategy.onOrderClosed(orderIdToUse, "Cancelled");
            return;
        }

        lastPlacedOrderId = orderIdToUse;
        lastPlacedOrderAction = action;
        lastPlacedOrderQuantity = finalQty;

        if (shopStrategy != null) shopStrategy.onOrderSubmitted(orderIdToUse, action, finalQty);
        dailySubmittedOrders++;
        currentOrderId++;
        requestPostOrderPositionValidation(symbol, orderIdToUse, "direct-ibkr-submit");
    }

    private void requestPostOrderPositionValidation(String orderSymbol, int orderId, String source) {
        String normalizedSymbol = orderSymbol == null || orderSymbol.isBlank() ? symbol : orderSymbol;
        String normalizedSource = source == null || source.isBlank() ? "order-submit" : source;
        flowInfo(
            "IBKR.SYNC",
            "requesting post-order position validation symbol=" + normalizedSymbol
                + " orderId=" + orderId
                + " source=" + normalizedSource
        );
        requestPositions(PositionSyncMode.PRESERVE_BAR_FORWARDING, "post-order-position-validation-" + normalizedSymbol + "-" + orderId + "-" + normalizedSource);
    }

    protected int getMaxShareCap() {
        return Math.max(1, maxShareCap);
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
            int persistedTradeCount = Integer.parseInt(props.getProperty("strategy.tradeCount", "0"));
            this.yesterdayClose = Double.parseDouble(props.getProperty("strategy.yesterdayClose", "0.0"));
            String restoredDate = props.getProperty("strategy.stateDate", "");
            boolean sameMarketDay = LocalDate.now(MARKET_ZONE).toString().equals(restoredDate);
            int rTrades = sameMarketDay ? persistedTradeCount : 0;
            int restoredHardStops = sameMarketDay ? Integer.parseInt(props.getProperty("strategy.hardStopExitCount", "0")) : 0;
            long restoredLastHardStopTime = sameMarketDay ? Long.parseLong(props.getProperty("strategy.lastHardStopExitTimeMs", "0")) : 0L;
            shopStrategy.restoreState(rPrice, rTrades, true, false, this.yesterdayClose, restoredHardStops, restoredLastHardStopTime);
            if (!sameMarketDay && persistedTradeCount > 0) {
                flowInfo("STATE", "Reset stale tradeCount on startup symbol=" + symbol + " persistedTradeCount=" + persistedTradeCount + " restoredDate=" + restoredDate + " marketDate=" + LocalDate.now(MARKET_ZONE));
                props.setProperty("strategy.tradeCount", "0");
                props.setProperty("strategy.stateDate", LocalDate.now(MARKET_ZONE).toString());
                props.setProperty("strategy.hardStopExitCount", "0");
                props.setProperty("strategy.lastHardStopExitTimeMs", "0");
                try (OutputStream os = Files.newOutputStream(path)) {
                    props.store(os, "Trader State");
                }
            }
            flowData("STATE", "Restored state lastPrice=" + rPrice + " trades=" + rTrades + " persistedTrades=" + persistedTradeCount + " yesterdayClose=" + yesterdayClose + " sameMarketDay=" + sameMarketDay + " hardStopExitCount=" + restoredHardStops + " lastHardStopExitTimeMs=" + restoredLastHardStopTime);
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
        props.setProperty("strategy.stateDate", LocalDate.now(MARKET_ZONE).toString());
        props.setProperty("strategy.hardStopExitCount", String.valueOf(shopStrategy.getHardStopExitCount()));
        props.setProperty("strategy.lastHardStopExitTimeMs", String.valueOf(shopStrategy.getLastHardStopExitTimeMs()));
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
                // EWrapper callbacks are pumped from this loop; every direct IBKR event flows through here.
                signal.waitForSignal();
                try { reader.processMsgs(); } catch (Exception ignored) {}
            }
        });
        readerThread.start();
    }

    @Override public void nextValidId(int orderId) { this.currentOrderId = orderId; flowData("IBKR.SYNC", "nextValidId=" + orderId); }
    @Override public void positionEnd() {
        if (!symbolPositionSeenThisCycle) {
            // A completed sync with no symbol position means we should release any stale reservation left over
            // from a previous process incarnation or interrupted fill lifecycle.
            releaseSharedCapital(symbol, "position-sync-flat");
        }
        positionSyncComplete = true;
        if (shopStrategy != null) {
            shopStrategy.setPositionSynced(true);
        }
        markPositionSyncCompleted("direct-ibkr-position-end", "symbolPositionSeenThisCycle=" + symbolPositionSeenThisCycle);
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
            symbolPositionSeenThisCycle = true;
            flowData(
                "IBKR.SYNC",
                "position update account=" + account + " symbol=" + symbol + " brokerPosition=" + pos.value().intValue() + " avgCost=" + avgCost
            );
            if (shopStrategy != null) shopStrategy.syncPosition(pos.value().intValue(), avgCost);
            // Reconcile reservations against broker-reported inventory so shared capital follows actual exposure,
            // not just submitted orders.
            reconcileSharedCapitalWithPosition(pos.value().intValue(), currentLastPrice > 0.0 ? currentLastPrice : avgCost);
        }
    }

    @Override
    public void orderStatus(int orderId, String status, Decimal filled, Decimal remaining, double avgFillPrice, long permId, int parentId, double lastFillPrice, int clientId, String whyHeld, double mktCapPrice) {
        OrderContext orderContext = orderContextById.get(orderId);
        int filledQty = filled != null && filled.value() != null ? filled.value().intValue() : 0;
        int remainingQty = remaining != null && remaining.value() != null ? remaining.value().intValue() : 0;
        if (avgFillPrice > 0.0) {
            lastOrderAvgFillPrice = avgFillPrice;
        }
        if (orderContext != null && shopStrategy != null) {
            int previousFilledQty = cumulativeFilledByOrderId.getOrDefault(orderId, 0);
            int filledDelta = Math.max(0, filledQty - previousFilledQty);
            if (filledDelta > 0) {
                cumulativeFilledByOrderId.put(orderId, filledQty);
                shopStrategy.onOrderProgress(orderId, orderContext.action(), filledDelta, remainingQty, avgFillPrice);
            }
        }
        boolean terminal = isTerminalOrderStatus(status);
        if (orderContext != null) {
            if (!orderContext.closingTrade() && orderContext.capitalReserved()) {
                // Entry orders either convert their reservation into actual exposure on fill or release it on
                // terminal cancellation/rejection.
                if (terminal) {
                    if (filledQty > 0) {
                        double reservedAmount = avgFillPrice > 0.0 ? avgFillPrice * filledQty : orderContext.reservedNotional();
                        reconcileSharedCapitalWithPosition(filledQty, avgFillPrice > 0.0 ? avgFillPrice : currentLastPrice > 0.0 ? currentLastPrice : 0.0, reservedAmount);
                    } else {
                        releaseSharedCapital(orderContext.symbol(), "entry-order-terminal-" + status);
                    }
                }
            } else if (orderContext.closingTrade() && terminal) {
                if (filledQty > 0 && remainingQty == 0) {
                    releaseSharedCapital(orderContext.symbol(), "closing-order-terminal-" + status);
                }
            }
            if (terminal) {
                // Terminal statuses are the cleanup point for local watchdogs and order bookkeeping.
                orderContextById.remove(orderId);
                orderSentTimes.remove(orderId);
                cumulativeFilledByOrderId.remove(orderId);
                ScheduledFuture<?> timeoutFuture = staleOrderTimeoutByOrderId.remove(orderId);
                if (timeoutFuture != null) {
                    timeoutFuture.cancel(false);
                }
            }
        }
        if (shopStrategy != null) {
            if (terminal) {
                shopStrategy.onOrderClosed(orderId, status);
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        isShuttingDown = true;
        persistStrategyState();
        if (shopStrategy == null || shopStrategy.getCurrentPosition() == 0) {
            releaseSharedCapital(symbol, "shutdown-flat");
        }
        if (databentoLiveGateway != null) {
            databentoLiveGateway.stop();
        }
        if (sharedIbkrGatewayClient != null) {
            sharedIbkrGatewayClient.disconnect();
        }
        // Tear down monitors before disconnecting sockets so background tasks stop generating restart/reconnect work
        // while the process is intentionally exiting.
        stopDatabentoFeedMonitor();
        stopMarketScheduleMonitor();
        if (client != null) client.eDisconnect();
        staleOrderScheduler.shutdown();
        reconnectionScheduler.shutdown();
        databentoSupervisorScheduler.shutdown();
    }

    public void cancelStaleOrder(int orderIdToCancel) {
        if (isSharedIbkrGatewayConnected()) {
            try {
                SharedIbkrGatewayMessage response = sharedIbkrGatewayClient.cancelSymbolOrders(symbol, "strategy-watchdog-stale-order-" + orderIdToCancel);
                if (response.ok) {
                    flowInfo("IBKR.GATEWAY", "cancel_symbol_orders detail=" + response.detail + " orderId=" + orderIdToCancel + " symbol=" + symbol);
                    return;
                }
                flowError("IBKR.GATEWAY", "cancel_symbol_orders rejected detail=" + response.detail + " orderId=" + orderIdToCancel + " symbol=" + symbol);
                if (ibkrSharedGatewaySkipDirectConnection) {
                    return;
                }
            } catch (IOException e) {
                flowError("IBKR.GATEWAY", "cancel_symbol_orders failed orderId=" + orderIdToCancel + " reason=" + e.getMessage());
                if (ibkrSharedGatewaySkipDirectConnection) {
                    return;
                }
            }
        }
        if (client != null && orderIdToCancel >= 0) {
            client.cancelOrder(orderIdToCancel, new OrderCancel());
        }
    }

    public void requestPositions() {
        requestPositions(PositionSyncMode.PRESERVE_BAR_FORWARDING, "strategy-refresh");
    }

    private void requestPositions(PositionSyncMode mode, String reason) {
        String normalizedReason = (reason == null || reason.isBlank()) ? "ibkr-trader-requestPositions" : reason;
        boolean preserveBarForwarding = mode == PositionSyncMode.PRESERVE_BAR_FORWARDING && positionSyncComplete;
        symbolPositionSeenThisCycle = false;
        if (!preserveBarForwarding) {
            positionSyncComplete = false;
        }
        if (shopStrategy != null) {
            // Keep bars/features flowing during opportunistic resyncs, but stop new entries until broker state is
            // re-confirmed. Cold-start/reconnect still uses the stricter full gate above.
            shopStrategy.setPositionSynced(false);
        }
        String initialTransport = isSharedIbkrGatewayConnected() ? "shared-gateway" : ((client != null && client.isConnected()) ? "direct-ibkr" : "unavailable");
        beginPositionSyncAttempt(mode, normalizedReason, preserveBarForwarding, initialTransport);
        if (isSharedIbkrGatewayConnected()) {
            try {
                // Shared gateway sync is preferred because it returns the broker view through the same async event
                // channel used for live order updates.
                SharedIbkrGatewayMessage response = sharedIbkrGatewayClient.requestPositionSync(normalizedReason);
                recordPositionSyncAck("shared-gateway", response);
                flowInfo("IBKR.GATEWAY", "request_position_sync detail=" + response.detail + " reqId=" + response.payloadInt("reqId"));
                return;
            } catch (IOException e) {
                markPositionSyncFailure("shared-gateway-request", e.getMessage());
                flowError("IBKR.GATEWAY", "request_position_sync failed reason=" + e.getMessage());
                if (ibkrSharedGatewaySkipDirectConnection) {
                    return;
                }
                lastPositionSyncTransport = "direct-ibkr-fallback";
                flowInfo("IBKR.SYNC", "position sync fallback to direct-ibkr because shared gateway request failed attemptId=" + lastPositionSyncAttemptId);
            }
        }
        if (client != null && client.isConnected()) {
            lastPositionSyncTransport = "direct-ibkr";
            flowInfo("IBKR.SYNC", "position sync dispatched via direct-ibkr attemptId=" + lastPositionSyncAttemptId + " reason=" + normalizedReason);
            client.reqPositions();
            return;
        }
        markPositionSyncFailure("request-dispatch", "no-available-position-sync-transport");
    }

    private void beginPositionSyncAttempt(PositionSyncMode mode, String reason, boolean preserveBarForwarding, String transport) {
        cancelPositionSyncTimeout();
        long attemptId = positionSyncAttemptSequence.incrementAndGet();
        lastPositionSyncAttemptId = attemptId;
        lastPositionSyncStartedAtMs = System.currentTimeMillis();
        lastPositionSyncReason = reason == null ? "" : reason;
        lastPositionSyncMode = mode == null ? "" : mode.name();
        lastPositionSyncTransport = transport == null || transport.isBlank() ? "unknown" : transport;
        lastPositionSyncFailureStage = "";
        lastPositionSyncFailureReason = "";
        lastPositionSyncFailedAtMs = 0L;
        lastPositionSyncAckDetail = "";
        lastPositionSyncReqId = null;
        int currentPosition = shopStrategy != null ? shopStrategy.getCurrentPosition() : 0;
        flowInfo(
            "IBKR.SYNC",
            "position sync requested attemptId=" + attemptId
                + " reason=" + lastPositionSyncReason
                + " mode=" + lastPositionSyncMode
                + " transport=" + lastPositionSyncTransport
                + " preserveBarForwarding=" + preserveBarForwarding
                + " positionSyncComplete=" + positionSyncComplete
                + " currentPosition=" + currentPosition
                + " openOrders=" + getOpenOrdersCount()
                + " orderInFlight=" + isOrderInFlight()
                + " sharedGatewayConnected=" + isSharedIbkrGatewayConnected()
                + " directIbkrConnected=" + (client != null && client.isConnected())
        );
        schedulePositionSyncTimeout(attemptId);
    }

    private void recordPositionSyncAck(String transport, SharedIbkrGatewayMessage response) {
        lastPositionSyncTransport = transport == null || transport.isBlank() ? lastPositionSyncTransport : transport;
        lastPositionSyncAckDetail = response == null || response.detail == null ? "" : response.detail;
        lastPositionSyncReqId = response == null ? null : response.payloadInt("reqId");
        flowInfo(
            "IBKR.SYNC",
            "position sync acknowledged attemptId=" + lastPositionSyncAttemptId
                + " transport=" + lastPositionSyncTransport
                + " reqId=" + lastPositionSyncReqId
                + " detail=" + lastPositionSyncAckDetail
        );
    }

    private void markPositionSyncFailure(String stage, String detail) {
        lastPositionSyncFailedAtMs = System.currentTimeMillis();
        lastPositionSyncFailureStage = stage == null ? "" : stage;
        lastPositionSyncFailureReason = detail == null ? "" : detail;
        flowError(
            "IBKR.SYNC",
            "position sync unsuccessful attemptId=" + lastPositionSyncAttemptId
                + " stage=" + lastPositionSyncFailureStage
                + " reason=" + lastPositionSyncFailureReason
                + " mode=" + lastPositionSyncMode
                + " transport=" + lastPositionSyncTransport
                + " reqId=" + lastPositionSyncReqId
                + " positionSyncComplete=" + positionSyncComplete
                + " currentPosition=" + (shopStrategy != null ? shopStrategy.getCurrentPosition() : 0)
                + " openOrders=" + getOpenOrdersCount()
                + " orderInFlight=" + isOrderInFlight()
                + " startedAtMs=" + lastPositionSyncStartedAtMs
        );
    }

    private void markPositionSyncCompleted(String source, String detail) {
        lastPositionSyncCompletedAtMs = System.currentTimeMillis();
        lastPositionSyncTransport = source == null || source.isBlank() ? lastPositionSyncTransport : source;
        cancelPositionSyncTimeout();
        flowInfo(
            "IBKR.SYNC",
            "position sync completed attemptId=" + lastPositionSyncAttemptId
                + " source=" + lastPositionSyncTransport
                + " detail=" + (detail == null ? "" : detail)
                + " durationMs=" + Math.max(0L, lastPositionSyncCompletedAtMs - lastPositionSyncStartedAtMs)
                + " symbolPositionSeenThisCycle=" + symbolPositionSeenThisCycle
        );
    }

    private void schedulePositionSyncTimeout(long attemptId) {
        long timeoutMs = Math.max(1000L, ibkrPositionSyncTimeoutMs);
        positionSyncTimeoutFuture = staleOrderScheduler.schedule(() -> {
            if (attemptId != lastPositionSyncAttemptId || positionSyncComplete) {
                return;
            }
            markPositionSyncFailure("timeout", "position sync did not complete within " + timeoutMs + "ms");
        }, timeoutMs, TimeUnit.MILLISECONDS);
    }

    private void cancelPositionSyncTimeout() {
        ScheduledFuture<?> timeoutFuture = positionSyncTimeoutFuture;
        positionSyncTimeoutFuture = null;
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
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

    public Map<String, Object> resetSharedCapitalReservations(boolean force) {
        Map<String, Object> response = new LinkedHashMap<>();
        if (sharedCapitalManager == null || !sharedCapitalManager.isEnabled()) {
            response.put("message", "shared-capital-disabled");
            response.put("status", controlStatus());
            return response;
        }

        int currentPosition = shopStrategy != null ? shopStrategy.getCurrentPosition() : 0;
        boolean unsafeLocalState = currentPosition != 0 || getOpenOrdersCount() > 0 || isOrderInFlight();
        if (unsafeLocalState && !force) {
            response.put(
                "message",
                "shared-capital-reset-blocked-local-state position=" + currentPosition
                    + " openOrders=" + getOpenOrdersCount()
                    + " orderInFlight=" + isOrderInFlight()
            );
            response.put("status", controlStatus());
            return response;
        }

        SharedCapitalManager.ReservationDecision decision = sharedCapitalManager.resetAll();
        response.put("message", decision.message());
        response.put("allowed", decision.allowed());
        response.put("sharedCapital", decision.snapshot());
        response.put("status", controlStatus());
        return response;
    }

    public boolean isConnected() { return (client != null && client.isConnected()) || isSharedIbkrGatewayConnected(); }
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

    public boolean isTradeLogFileEnabled() {
        String storageMode = normalizedTradeLogStorageMode();
        return "file".equals(storageMode) || "both".equals(storageMode);
    }

    public boolean isTradeLogDatabaseEnabled() {
        String storageMode = normalizedTradeLogStorageMode();
        return "database".equals(storageMode) || "both".equals(storageMode);
    }

    public void persistTradeLog(String formattedTimestamp,
                                String tradeSymbol,
                                String exitAction,
                                int quantity,
                                double entryPrice,
                                double exitPrice,
                                double tradePnL,
                                double cumulativePnL,
                                String logFile) {
        if (!isTradeLogDatabaseEnabled()) {
            return;
        }
        if (tradeLogStore == null) {
            flowCondition("TRADE.LOG", "DATABASE_WRITE_OK", false, "symbol=" + tradeSymbol + " action=" + exitAction + " qty=" + quantity + " storageMode=" + normalizedTradeLogStorageMode() + " reason=no-store");
            return;
        }
        boolean persisted = tradeLogStore.saveTrade(
            formattedTimestamp,
            tradeSymbol,
            exitAction,
            quantity,
            entryPrice,
            exitPrice,
            tradePnL,
            cumulativePnL,
            logFile
        );
        flowCondition("TRADE.LOG", "DATABASE_WRITE_OK", persisted, "symbol=" + tradeSymbol + " action=" + exitAction + " qty=" + quantity + " storageMode=" + normalizedTradeLogStorageMode());
    }

    public List<Map<String, Object>> recentTradeLogs(String symbolFilter, int limit) {
        if (tradeLogStore == null) {
            return List.of();
        }
        return tradeLogStore.recentTrades(symbolFilter, limit);
    }

    public int getStrategyStopQueueDepth() { return shopStrategy != null ? shopStrategy.getLastStopQueueDepth() : 0; }
    public double getStrategyStopAckLatencyMs() { return shopStrategy != null ? shopStrategy.getLastStopAckLatencyMs() : 0.0; }
    public double getStrategyStopAckLatencyP50Ms() { return shopStrategy != null ? shopStrategy.getStopAckLatencyP50Ms() : 0.0; }
    public double getStrategyStopAckLatencyP95Ms() { return shopStrategy != null ? shopStrategy.getStopAckLatencyP95Ms() : 0.0; }
    public int getStrategyStopAckLatencySamples() { return shopStrategy != null ? shopStrategy.getStopAckLatencySampleCount() : 0; }
    public double getConnectionStatus() { return isConnected() ? 1.0 : 0.0; }
    public int getReconnectionAttempts() { return reconnectionAttempts; }
    public String getTrackedSymbol() { return symbol == null || symbol.isBlank() ? "UNSET" : symbol.trim().toUpperCase(Locale.US); }
    public boolean isDatabentoMarketDataEnabled() { return useDatabentoMarketData(); }
    public double getDatabentoEnabledMetric() { return useDatabentoMarketData() ? 1.0 : 0.0; }
    public double getDatabentoGatewayRunningMetric() { return useDatabentoMarketData() && currentDatabentoSnapshot().gatewayRunning() ? 1.0 : 0.0; }
    public double getDatabentoRestartCountMetric() { return useDatabentoMarketData() ? currentDatabentoSnapshot().restartCount() : 0.0; }
    public double getDatabentoQuoteAgeMsMetric() {
        if (!useDatabentoMarketData()) return 0.0;
        long age = currentDatabentoSnapshot().primarySymbolHealth().quoteAgeMs();
        return age >= 0L ? age : 0.0;
    }
    public double getDatabentoBarAgeMsMetric() {
        if (!useDatabentoMarketData()) return 0.0;
        long age = currentDatabentoSnapshot().primarySymbolHealth().barAgeMs();
        return age >= 0L ? age : 0.0;
    }
    public double getDatabentoOptionAgeMsMetric() {
        if (!useDatabentoMarketData()) return 0.0;
        long age = currentDatabentoSnapshot().primarySymbolHealth().optionAgeMs();
        return age >= 0L ? age : 0.0;
    }
    public double getDatabentoQuoteFreshMetric() {
        return useDatabentoMarketData() && currentDatabentoSnapshot().primarySymbolHealth().quoteFresh() ? 1.0 : 0.0;
    }
    public boolean isDatabentoFeedHealthy() {
        return !useDatabentoMarketData() || currentDatabentoSnapshot().healthy();
    }
    public DatabentoFeedHealth.Snapshot currentDatabentoSnapshot() {
        return databentoFeedHealth.snapshot(
            getTrackedSymbol(),
            System.currentTimeMillis(),
            databentoQuoteStaleThresholdMs,
            databentoMaxSilenceMs,
            isDatabentoMarketDataExpectedNow()
        );
    }
    public Map<String, Object> databentoFeedHealthStatus() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("enabled", useDatabentoMarketData());
        payload.put("quoteStaleThresholdMs", databentoQuoteStaleThresholdMs);
        payload.put("maxSilenceMs", databentoMaxSilenceMs);
        payload.put("autoRestartEnabled", databentoAutoRestartEnabled);
        payload.put("restartDelayMs", databentoRestartDelayMs);
        payload.putAll(databentoFeedHealth.snapshotAsMap(
            getTrackedSymbol(),
            System.currentTimeMillis(),
            databentoQuoteStaleThresholdMs,
            databentoMaxSilenceMs,
            isDatabentoMarketDataExpectedNow()
        ));
        return payload;
    }

    private String positionSyncStateSummary() {
        if (lastPositionSyncFailedAtMs > 0L && lastPositionSyncFailedAtMs >= lastPositionSyncCompletedAtMs) {
            return "failed:" + compactPositionSyncToken(lastPositionSyncFailureStage, "stage")
                + ":" + compactPositionSyncToken(lastPositionSyncFailureReason, "reason");
        }
        if (positionSyncComplete) {
            return "synced:" + compactPositionSyncToken(lastPositionSyncTransport, "source");
        }
        if (lastPositionSyncStartedAtMs > 0L) {
            return "syncing:" + compactPositionSyncToken(lastPositionSyncMode, "mode")
                + ":" + compactPositionSyncToken(lastPositionSyncTransport, "transport");
        }
        return "unknown";
    }

    private String compactPositionSyncToken(String raw, String fallback) {
        String normalized = raw == null ? "" : raw.trim().toLowerCase(Locale.US);
        if (normalized.isBlank()) {
            return fallback;
        }
        normalized = normalized.replaceAll("[^a-z0-9]+", "-");
        normalized = normalized.replaceAll("^-+|-+$", "");
        if (normalized.isBlank()) {
            return fallback;
        }
        return normalized.length() > 48 ? normalized.substring(0, 48) : normalized;
    }

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
        status.put("strategyTradeCount", shopStrategy != null ? shopStrategy.getTradeCount() : 0);
        status.put("maxTrades", maxTrades);
        status.put("tradeLogStorageMode", normalizedTradeLogStorageMode());
        status.put("tradeLogFileEnabled", isTradeLogFileEnabled());
        status.put("tradeLogDatabaseEnabled", isTradeLogDatabaseEnabled());
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
        status.put("positionSyncComplete", positionSyncComplete);
        status.put("positionSyncAttemptId", lastPositionSyncAttemptId);
        status.put("positionSyncStartedAtMs", lastPositionSyncStartedAtMs);
        status.put("positionSyncCompletedAtMs", lastPositionSyncCompletedAtMs);
        status.put("positionSyncFailedAtMs", lastPositionSyncFailedAtMs);
        status.put("positionSyncReason", lastPositionSyncReason);
        status.put("positionSyncMode", lastPositionSyncMode);
        status.put("positionSyncTransport", lastPositionSyncTransport);
        status.put("positionSyncLastFailureStage", lastPositionSyncFailureStage);
        status.put("positionSyncLastFailureReason", lastPositionSyncFailureReason);
        status.put("positionSyncLastAckDetail", lastPositionSyncAckDetail);
        status.put("positionSyncLastReqId", lastPositionSyncReqId);
        status.put("positionSyncState", positionSyncStateSummary());
        status.put("ibkrSharedGatewayEnabled", useSharedIbkrGateway());
        status.put("ibkrSharedGatewayConnected", isSharedIbkrGatewayConnected());
        status.put("ibkrSharedGatewaySkipDirectConnection", ibkrSharedGatewaySkipDirectConnection);
        status.put("marketDataProvider", normalizedMarketDataProvider());
        status.put("databentoFeedHealthy", isDatabentoFeedHealthy());
        if (useDatabentoMarketData()) {
            status.put("databentoSharedFeedEnabled", useSharedDatabentoFeed());
            status.put("databentoSharedFeedHost", databentoSharedFeedHost);
            status.put("databentoSharedFeedPort", databentoSharedFeedPort);
            status.put("databentoFeed", databentoFeedHealthStatus());
        }
        if (sharedCapitalManager != null && sharedCapitalManager.isEnabled()) {
            SharedCapitalManager.Snapshot snapshot = sharedCapitalManager.snapshot();
            status.put("sharedCapitalEnabled", true);
            status.put("sharedCapitalFile", snapshot.stateFile().toString());
            status.put("sharedCapitalTotalNotional", snapshot.totalNotional());
            status.put("sharedCapitalReservedNotional", snapshot.reservedNotional());
            status.put("sharedCapitalAvailableNotional", snapshot.availableNotional());
            status.put("sharedCapitalReservations", snapshot.reservations());
        } else {
            status.put("sharedCapitalEnabled", false);
        }
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

    public Map<String, Object> resetDailyStrategyState(String reason) {
        Map<String, Object> response = new LinkedHashMap<>();
        if (shopStrategy == null) {
            response.put("message", "strategy-not-ready");
            response.put("reason", reason == null ? "manual" : reason);
            response.put("status", controlStatus());
            return response;
        }
        boolean resetApplied = shopStrategy.resetForNewDayAndWait(1500L);
        persistStrategyState();
        response.put("message", resetApplied ? "daily-strategy-state-reset" : "daily-strategy-state-reset-timeout");
        response.put("reason", reason == null ? "manual" : reason);
        response.put("status", controlStatus());
        return response;
    }

    public void setRuntimeKillSwitch(boolean enabled) {
        this.runtimeKillSwitch = enabled;
        if (enabled && shopStrategy != null) {
            shopStrategy.setEnabled(false);
        }
    }

    public int cancelOpenOrders() {
        if (isSharedIbkrGatewayConnected()) {
            int openCount = orderContextById.size();
            try {
                SharedIbkrGatewayMessage response = sharedIbkrGatewayClient.cancelSymbolOrders(symbol, "cancel-open-orders");
                flowInfo("IBKR.GATEWAY", "cancel_symbol_orders detail=" + response.detail + " cancelledCount=" + response.payloadInt("cancelledCount"));
                Integer cancelledCount = response.payloadInt("cancelledCount");
                return cancelledCount == null ? openCount : cancelledCount;
            } catch (IOException e) {
                flowError("IBKR.GATEWAY", "cancel_symbol_orders failed reason=" + e.getMessage());
                if (ibkrSharedGatewaySkipDirectConnection) {
                    return 0;
                }
            }
        }
        if (!isConnected()) return 0;
        int openCount = orderContextById.size();
        CompletableFuture.runAsync(() -> {
            try {
                client.reqGlobalCancel(new OrderCancel());
                log.info(">>> [FLOW][INFO][IBKR.CANCEL] reqGlobalCancel dispatched openCount={}", openCount);
            } catch (Exception exception) {
                log.error(">>> [ERROR][IBKR.CANCEL] reqGlobalCancel failed openCount={} reason={}", openCount, exception.getMessage(), exception);
            }
        });
        return openCount;
    }

    public String flattenPosition() {
        // Flatten is used by both operational controls and schedule-driven close handling.
        // It intentionally shares the same order-routing policy as ordinary strategy exits:
        // use market for closing intent and compute the best available quote-based reference price first.
        if (shopStrategy == null) return "strategy-not-ready";
        int position = shopStrategy.getCurrentPosition();
        if (isSharedIbkrGatewayConnected()) {
            try {
                String localAction = position > 0 ? "SELL" : position < 0 ? "BUY" : "";
                double flattenReferencePrice = localAction.isBlank()
                    ? Math.max(0.0, currentLastPrice)
                    : resolveExecutionReferencePrice(localAction, currentLastPrice, resolvePreferredOrderType(localAction, true));
                SharedIbkrGatewayMessage response = sharedIbkrGatewayClient.flattenSymbol(
                    symbol,
                    flattenReferencePrice,
                    position == 0 ? null : Math.abs(position),
                    "flatten-position-broker-check"
                );
                if (!response.ok) {
                    return "flatten-rejected-by-gateway";
                }
                Integer gatewayOrderId = response.payloadInt("gatewayOrderId");
                if (gatewayOrderId == null) {
                    return "already-flat";
                }
                String action = response.payloadString("action");
                if (action == null || action.isBlank()) {
                    action = localAction;
                }
                Integer responseQuantity = response.payloadInt("quantity");
                int flattenQuantity = responseQuantity == null ? Math.abs(position) : Math.max(0, responseQuantity);
                if (action == null || action.isBlank() || flattenQuantity <= 0) {
                    return "flatten-requested-untracked";
                }
                int sharedOrderId = gatewayOrderId == null ? Math.max(1, currentOrderId) : gatewayOrderId;
                orderContextById.put(sharedOrderId, new OrderContext(symbol, action, flattenQuantity, true, 0.0, false));
                orderSentTimes.put(sharedOrderId, System.currentTimeMillis());
                lastPlacedOrderId = sharedOrderId;
                lastPlacedOrderAction = action;
                lastPlacedOrderQuantity = flattenQuantity;
                if (shopStrategy != null) shopStrategy.onOrderSubmitted(sharedOrderId, action, flattenQuantity);
                return "flatten-requested";
            } catch (IOException e) {
                flowError("IBKR.GATEWAY", "flatten_symbol failed reason=" + e.getMessage());
                if (ibkrSharedGatewaySkipDirectConnection) {
                    return "flatten-failed-gateway-io";
                }
            }
        }
        if (position == 0) return "already-flat";
        String action = (position > 0) ? "SELL" : "BUY";
        double flattenReferencePrice = resolveExecutionReferencePrice(action, currentLastPrice, resolvePreferredOrderType(action, true));
        if (flattenReferencePrice <= 0.0) return "flatten-failed-no-price";
        placeTrade(symbol, action, flattenReferencePrice, Math.abs(position), "MKT");
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

    private void reconcileSharedCapitalWithPosition(int position, double referencePrice) {
        reconcileSharedCapitalWithPosition(position, referencePrice, tradeAmount);
    }

    private void reconcileSharedCapitalWithPosition(int position, double referencePrice, double fallbackNotional) {
        if (sharedCapitalManager == null || !sharedCapitalManager.isEnabled()) {
            return;
        }
        SharedCapitalManager.ReservationDecision decision = sharedCapitalManager.reconcilePosition(symbol, position, referencePrice, fallbackNotional);
        SharedCapitalManager.Snapshot snapshot = decision.snapshot();
        flowData(
            "RISK",
            "Shared capital reconcile symbol=" + symbol
                + " position=" + position
                + " referencePrice=" + referencePrice
                + " fallbackNotional=" + fallbackNotional
                + " allowed=" + decision.allowed()
                + " message=" + decision.message()
                + " available=" + snapshot.availableNotional()
        );
    }

    private void releaseSharedCapital(String symbol, String reason) {
        if (sharedCapitalManager == null || !sharedCapitalManager.isEnabled()) {
            return;
        }
        SharedCapitalManager.ReservationDecision decision = sharedCapitalManager.release(symbol);
        SharedCapitalManager.Snapshot snapshot = decision.snapshot();
        flowData(
            "RISK",
            "Shared capital release symbol=" + symbol
                + " reason=" + reason
                + " allowed=" + decision.allowed()
                + " message=" + decision.message()
                + " available=" + snapshot.availableNotional()
        );
    }

    private boolean isTerminalOrderStatus(String status) {
        if (status == null || status.isBlank()) {
            return false;
        }
        return "Filled".equalsIgnoreCase(status)
            || "Cancelled".equalsIgnoreCase(status)
            || "ApiCancelled".equalsIgnoreCase(status)
            || "Inactive".equalsIgnoreCase(status);
    }

    private boolean isSharedGatewayEffectivelyTerminal(String status, Integer remaining, Boolean gatewayEffectiveTerminal) {
        if (Boolean.TRUE.equals(gatewayEffectiveTerminal)) {
            return true;
        }
        if (isTerminalOrderStatus(status)) {
            return true;
        }
        return remaining != null && remaining == 0 && isZeroRemainingSharedGatewayLifecycleStatus(status);
    }

    private boolean isZeroRemainingSharedGatewayLifecycleStatus(String status) {
        if (status == null || status.isBlank()) {
            return false;
        }
        // These are cancellation lifecycle statuses where the gateway may report remaining=0 before IBKR emits a
        // final Cancelled/ApiCancelled event. Submission lifecycle statuses (PendingSubmit/ApiPending) are not
        // terminal: IBKR can still fill those IOC/limit orders after the initial submit response, so closing local
        // order state here would hide real broker exposure from the strategy.
        return "PendingCancel".equalsIgnoreCase(status)
            || "CancelSubmitted".equalsIgnoreCase(status);
    }

    private String normalizedTradeLogStorageMode() {
        String rawMode = tradeLogStorageMode == null ? "" : tradeLogStorageMode.trim().toLowerCase(Locale.US);
        return switch (rawMode) {
            case "file", "database", "both" -> rawMode;
            default -> "both";
        };
    }

    private void startDatabentoLiveGateway() {
        // Databento startup has two modes:
        //  1) shared relay mode (preferred for large bot fleets)
        //  2) dedicated sidecar mode (only when shared-feed mode is not in use)
        //
        // In shared-feed mode we deliberately avoid per-bot fallback because a large bot fleet could exceed
        // Databento connection limits if every symbol process tried to recover independently.
        if (!useDatabentoMarketData()) {
            return;
        }
        if (databentoLiveGateway != null && databentoLiveGateway.isRunning()) {
            return;
        }

        Map<String, String> sidecarEnv = resolveDatabentoSidecarEnvironment();
        if (useSharedDatabentoFeed() && tryStartSharedDatabentoRelayGateway(sidecarEnv)) {
            return;
        }
        if (useSharedDatabentoFeed()) {
            if (databentoSharedFeedFallbackToPrivateSidecar) {
                flowInfo("DATABENTO", "Private-sidecar fallback is configured but disabled in shared-feed mode to avoid exhausting Databento connection limits. Waiting for shared relay recovery.");
            }
            return;
        }
        startDedicatedDatabentoLiveGateway(sidecarEnv);
    }

    private boolean tryStartSharedDatabentoRelayGateway(Map<String, String> sidecarEnv) {
        if (!ensureSharedDatabentoRelayReady(sidecarEnv)) {
            return false;
        }
        databentoSidecarConfigErrorLogged.set(false);
        databentoLiveGateway = DatabentoLiveGateway.forRelay(
            databentoSharedFeedHost,
            databentoSharedFeedPort,
            symbol,
            symbol + ":feed-client-" + clientId,
            this::handleDatabentoEvent,
            line -> flowError("DATABENTO.RELAY", line),
            this::handleDatabentoGatewayExit
        );
        try {
            databentoLiveGateway.start();
            databentoFeedHealth.markGatewayStarted(List.of(symbol), System.currentTimeMillis());
            databentoRestartPending.set(false);
            return true;
        } catch (IOException e) {
            flowError("DATABENTO", "Failed to connect to shared relay reason=" + e.getMessage());
            databentoFeedHealth.markGatewayExited(-2, System.currentTimeMillis());
            if (!databentoSharedFeedFallbackToPrivateSidecar) {
                requestDatabentoGatewayRestart("shared-relay-connect-failed:" + e.getClass().getSimpleName());
            }
            return false;
        }
    }

    private void startDedicatedDatabentoLiveGateway(Map<String, String> sidecarEnv) {
        String apiKey = effectiveDatabentoApiKey(sidecarEnv);
        if (!isUsableDatabentoApiKey(apiKey)) {
            logDatabentoSidecarConfigErrorOnce(
                "Databento sidecar not started: missing valid DATABENTO_API_KEY. Set it in the parent environment or in "
                    + Paths.get(databentoEnvFile).toAbsolutePath().normalize()
                    + ", or set trading.databento.api.key"
            );
            return;
        }
        databentoSidecarConfigErrorLogged.set(false);

        List<String> command = new ArrayList<>();
        command.add(databentoPythonBin);
        command.add(Paths.get(databentoNormalizerScript).toAbsolutePath().normalize().toString());
        command.add("--symbols");
        command.add(symbol);
        command.add("--option-parents");
        command.add(databentoOptionParents == null || databentoOptionParents.isBlank() ? symbol : databentoOptionParents.trim());
        command.add("--equity-dataset");
        command.add(databentoEquityDataset);
        command.add("--equity-schema");
        command.add(databentoEquitySchema);
        command.add("--startup-history-seconds");
        command.add(String.format(Locale.US, "%.3f", Math.max(0.0, databentoStartupHistorySeconds)));
        command.add("--startup-history-schema");
        command.add(databentoStartupHistorySchema);
        command.add("--options-dataset");
        command.add(databentoOptionsDataset);
        command.add("--options-schema");
        command.add(databentoOptionsSchema);
        command.add("--heartbeat-seconds");
        command.add(String.valueOf(Math.max(1, databentoHeartbeatSeconds)));
        command.add("--startup-delay-seconds");
        command.add(String.format(Locale.US, "%.3f", Math.max(0.0, databentoStartupDelaySeconds)));
        if (databentoLiveGatewayOverride != null && !databentoLiveGatewayOverride.isBlank()) {
            command.add("--live-gateway");
            command.add(databentoLiveGatewayOverride.trim());
        }
        appendDatabentoFeatureSnapshotArgs(command);

        databentoLiveGateway = new DatabentoLiveGateway(
            command,
            Paths.get("").toAbsolutePath().normalize(),
            sidecarEnv,
            this::handleDatabentoEvent,
            line -> flowError("DATABENTO.STDERR", line),
            this::handleDatabentoGatewayExit
        );
        try {
            databentoLiveGateway.start();
            databentoFeedHealth.markGatewayStarted(List.of(symbol), System.currentTimeMillis());
            databentoRestartPending.set(false);
        } catch (IOException e) {
            flowError("DATABENTO", "Failed to start live gateway reason=" + e.getMessage());
            databentoFeedHealth.markGatewayExited(-1, System.currentTimeMillis());
            requestDatabentoGatewayRestart("start-failed:" + e.getClass().getSimpleName());
        }
    }

    private void handleDatabentoEvent(DatabentoEvent event) {
        if (event == null || event.event == null || event.event.isBlank()) {
            return;
        }
        long nowMs = System.currentTimeMillis();
        if (event.isStatus()) {
            databentoFeedHealth.recordStatus(event.message, nowMs);
            flowInfo("DATABENTO", event.message == null ? "status" : event.message);
            return;
        }
        if (event.isEquityBar()) {
            databentoFeedHealth.recordEquityBar(event, nowMs);
            handleDatabentoEquityBar(event);
            return;
        }
        if (event.isOptionBar()) {
            databentoFeedHealth.recordOptionBar(event, nowMs);
            handleDatabentoOptionBar(event);
        }
    }

    private void handleDatabentoEquityBar(DatabentoEvent event) {
        if (event.symbol == null || !event.symbol.equalsIgnoreCase(symbol)) {
            return;
        }
        if (event.close <= 0.0) {
            return;
        }

        currentLastPrice = event.close;
        if (event.bid > 0.0) currentBidPrice = event.bid;
        if (event.ask > 0.0) currentAskPrice = event.ask;
        if (event.bidSize > 0L) currentBidSize = event.bidSize;
        if (event.askSize > 0L) currentAskSize = event.askSize;

        ZonedDateTime barTs = Instant.ofEpochSecond(event.barEpochSec).atZone(ZoneOffset.UTC).withZoneSameInstant(MARKET_ZONE);
        applyMarketSchedule(barTs);
        boolean entryQualityOk = databentoEventAllowsNewEntries(event);
        if (shopStrategy != null) {
            shopStrategy.setDataQualityAllowsNewEntries(entryQualityOk);
            if (!entryQualityOk) {
                databentoFeedHealth.recordRejectedEquityBar(event, System.currentTimeMillis());
            }
        }
        flowDataDebug("DATABENTO.BAR", "symbol=" + symbol + " tsEt=" + barTs + " ohlc=" + event.open + "/" + event.high + "/" + event.low + "/" + event.close + " vol=" + event.volume + " bid=" + currentBidPrice + " ask=" + currentAskPrice);

        if (shopStrategy != null) {
            shopStrategy.onQuoteSnapshot(currentBidPrice, currentAskPrice, currentBidSize, currentAskSize, latestShortableShares);
            if (event.atBidVol > 0L || event.atAskVol > 0L) {
                shopStrategy.onOrderFlowSnapshot(event.atBidVol, event.atAskVol);
            }
        }

        boolean canForward = shopStrategy != null && positionSyncComplete;
        flowConditionDebug("DATABENTO->AI.BAR", "FORWARD_BAR_TO_STRATEGY", canForward, "positionSyncComplete=" + positionSyncComplete + " strategyReady=" + (shopStrategy != null));
        if (canForward) {
            if (event.hasEnrichedNumericFields()) {
                shopStrategy.onEnrichedFeatureSnapshot(
                    event.effectiveFeatureSnapshotEpochSec(),
                    event.getEnrichedNumericFields(),
                    event.featureSnapshotSchemaVersion,
                    event.featureSnapshotSource
                );
            }
            shopStrategy.onTickForExitsOnly(event.close);
            shopStrategy.onSourceBar(
                event.barEpochSec, event.open, event.high, event.low, event.close, event.volume,
                event.wap > 0.0 ? event.wap : event.close,
                event.tradeCount, event.quoteCount, event.atBidVol, event.atAskVol,
                event.tradeSecondsPresent, event.quoteUpdateSecondsPresent, event.quoteStateSecondsValid, event.syntheticSeconds,
                event.tradeCoverage, event.quoteUpdateCoverage, event.quoteStateCoverage, event.syntheticCoverage,
                event.quoteAgeMsMean, event.quoteAgeMsMax, event.validSpreadCoverage, event.lockedCrossedSeconds, event.qualityScore
            );
        }
    }

    private boolean databentoEventAllowsNewEntries(DatabentoEvent event) {
        if (!databentoBlockEntriesOnInvalidSanity || event == null || !event.isEquityBar()) {
            return true;
        }
        double minQualityScore = Double.isFinite(databentoMinQualityScore)
            ? Math.max(0.0, Math.min(1.0, databentoMinQualityScore))
            : 0.50;
        boolean pass = !event.blocksNewEntries(minQualityScore, databentoExpectedEventSchemaVersion);
        flowConditionDebug(
            "DATABENTO.SANITY",
            "ENTRY_SAFE_EVENT_CONTRACT",
            pass,
            "schema=" + event.eventSchemaVersion
                + " expected=" + databentoExpectedEventSchemaVersion
                + " qualityScore=" + event.effectiveQualityScore()
                + " minQualityScore=" + minQualityScore
                + " flags=" + event.dataQualityFlags
        );
        return pass;
    }

    private void handleDatabentoOptionBar(DatabentoEvent event) {
        if (event.underlying == null || !event.underlying.equalsIgnoreCase(symbol)) {
            return;
        }
        long deltaVolume = Math.max(0L, event.volume);
        if (deltaVolume <= 0L) {
            return;
        }
        if ("P".equalsIgnoreCase(event.right)) {
            latestPutVolume += deltaVolume;
        } else if ("C".equalsIgnoreCase(event.right)) {
            latestCallVolume += deltaVolume;
        } else {
            return;
        }
        if (shopStrategy != null) {
            shopStrategy.onOptionVolumeUpdate(latestPutVolume, latestCallVolume);
        }
    }

    private synchronized void applyMarketSchedule(ZonedDateTime barTs) {
        // This clock gate is intentionally idempotent and may be invoked from either bar-driven flow or the
        // timer-based schedule monitor. The goal is to make open/close transitions reliable even when the
        // market-data stream is degraded near the session boundary.
        if (shopStrategy == null || barTs == null) {
            return;
        }

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
                resetOptionVolumeAccumulators("market-open");
                shopStrategy.resetForNewDay();
                shopStrategy.setEnabled(true);
                flowInfo("SCHEDULE", "9:30 AM ET: Market Open. Strategy awake and indicators reset.");
            }
        }

        if (timeEt.isAfter(LocalTime.of(15, 59, 45)) && timeEt.isBefore(LocalTime.of(16, 0, 0))) {
            boolean closeActionNotDoneToday = !barDateEt.equals(lastCloseFlattenDate);
            if (shopStrategy.isEnabled() && closeActionNotDoneToday) {
                flowInfo("SCHEDULE", "3:59 PM ET: Market closing. Cancelling open orders, syncing broker position, flattening, and going to sleep.");
                performEndOfDayFlatten(barDateEt);
                shopStrategy.setEnabled(false);
                lastCloseFlattenDate = barDateEt;
            }
        }
    }

    protected String performEndOfDayFlatten(LocalDate scheduleDateEt) {
        int cancelled = cancelOpenOrders();
        requestPositionsForEndOfDay("eod-pre-flatten");
        String flattenResult = flattenPosition();
        requestPositionsForEndOfDay("eod-post-flatten-confirm");
        flowInfo(
            "SCHEDULE",
            "EOD flatten workflow completed date=" + scheduleDateEt
                + " cancelledOpenOrders=" + cancelled
                + " flattenResult=" + flattenResult
                + " positionSyncComplete=" + positionSyncComplete
        );
        return flattenResult;
    }

    protected void requestPositionsForEndOfDay(String reason) {
        requestPositions(PositionSyncMode.PRESERVE_BAR_FORWARDING, reason);
    }

    private ModelBundleResolver.ResolvedBundle resolveModelBundle(String tradingSymbol) {
        if (modelBundleResolver == null) {
            return new ModelBundleResolver.ResolvedBundle(tradingSymbol, tradingSymbol, "default", modelDir, "resolver-unavailable");
        }
        return modelBundleResolver.resolveForSymbol(tradingSymbol);
    }

    private PingPongStrategy.AiThresholdConfig buildAiThresholdConfig() {
        return new PingPongStrategy.AiThresholdConfig(
            applyEntryThresholdLift(aiLongEntryThreshold),
            applyEntryThresholdLift(aiShortEntryThreshold),
            aiLongExitThreshold,
            aiShortExitThreshold,
            applyEntryThresholdLift(resolveOptionalThreshold(aiOpen30LongEntryThresholdRaw, aiLongEntryThreshold)),
            applyEntryThresholdLift(resolveOptionalThreshold(aiOpen30ShortEntryThresholdRaw, aiShortEntryThreshold)),
            resolveOptionalThreshold(aiOpen30LongExitThresholdRaw, aiLongExitThreshold),
            resolveOptionalThreshold(aiOpen30ShortExitThresholdRaw, aiShortExitThreshold),
            applyEntryThresholdLift(resolveOptionalThreshold(aiChoppyLongEntryThresholdRaw, aiLongEntryThreshold)),
            applyEntryThresholdLift(resolveOptionalThreshold(aiChoppyShortEntryThresholdRaw, aiShortEntryThreshold)),
            resolveOptionalThreshold(aiChoppyLongExitThresholdRaw, aiLongExitThreshold),
            resolveOptionalThreshold(aiChoppyShortExitThresholdRaw, aiShortExitThreshold),
            applyEntryThresholdLift(resolveOptionalThreshold(aiTrendLongEntryThresholdRaw, aiLongEntryThreshold)),
            applyEntryThresholdLift(resolveOptionalThreshold(aiTrendShortEntryThresholdRaw, aiShortEntryThreshold)),
            resolveOptionalThreshold(aiTrendLongExitThresholdRaw, aiLongExitThreshold),
            resolveOptionalThreshold(aiTrendShortExitThresholdRaw, aiShortExitThreshold),
            applyEntryThresholdLift(resolveOptionalThreshold(aiVolatileLongEntryThresholdRaw, aiLongEntryThreshold)),
            applyEntryThresholdLift(resolveOptionalThreshold(aiVolatileShortEntryThresholdRaw, aiShortEntryThreshold)),
            resolveOptionalThreshold(aiVolatileLongExitThresholdRaw, aiLongExitThreshold),
            resolveOptionalThreshold(aiVolatileShortExitThresholdRaw, aiShortExitThreshold),
            aiRegimeThreshold
        );
    }

    private double applyEntryThresholdLift(double threshold) {
        if (Double.isNaN(threshold) || Double.isInfinite(threshold)) {
            return threshold;
        }
        double raisePercent = Double.isNaN(aiEntryThresholdRaisePercent) || Double.isInfinite(aiEntryThresholdRaisePercent)
            ? 0.0
            : Math.max(0.0, aiEntryThresholdRaisePercent);
        if (raisePercent == 0.0) {
            return threshold;
        }
        double lifted = threshold * (1.0 + (raisePercent / 100.0));
        return Math.max(0.0, Math.min(0.99, lifted));
    }

    private double resolveOptionalThreshold(String rawValue, double fallback) {
        if (rawValue == null || rawValue.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(rawValue.trim());
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private boolean useDatabentoMarketData() {
        return "databento".equals(normalizedMarketDataProvider());
    }

    private boolean useSharedDatabentoFeed() {
        return useDatabentoMarketData() && databentoSharedFeedEnabled;
    }

    private boolean useSharedIbkrGateway() {
        return ibkrSharedGatewayEnabled;
    }

    private boolean isSharedIbkrGatewayConnected() {
        return sharedIbkrGatewayClient != null && sharedIbkrGatewayClient.isConnected();
    }

    private void ensureSharedIbkrGatewayConnected(boolean failFast) {
        if (!useSharedIbkrGateway() || isSharedIbkrGatewayConnected()) {
            return;
        }
        try {
            sharedIbkrGatewayClient = new SharedIbkrGatewayClient(
                ibkrSharedGatewayHost,
                ibkrSharedGatewayPort,
                Duration.ofMillis(Math.max(1000L, ibkrSharedGatewayConnectTimeoutMs)),
                Duration.ofMillis(Math.max(1000L, ibkrSharedGatewayAckTimeoutMs)),
                this::handleSharedIbkrGatewayEvent
            );
            sharedIbkrGatewayClient.connect();
            flowInfo("IBKR.GATEWAY", "Connected shared gateway host=" + ibkrSharedGatewayHost + " port=" + ibkrSharedGatewayPort);
        } catch (IOException e) {
            flowError("IBKR.GATEWAY", "connect failed reason=" + e.getMessage());
            if (failFast) {
                throw new IllegalStateException("Shared IBKR gateway is required but unavailable", e);
            }
        }
    }

    private void registerSymbolWithSharedGateway() {
        if (!isSharedIbkrGatewayConnected()) {
            return;
        }
        try {
            SharedIbkrGatewayMessage response = sharedIbkrGatewayClient.registerSymbol(symbol, symbol + ":client-" + clientId);
            flowInfo("IBKR.GATEWAY", "register_symbol detail=" + response.detail + " symbol=" + symbol);
        } catch (IOException e) {
            flowError("IBKR.GATEWAY", "register_symbol failed reason=" + e.getMessage());
        }
    }

    public void requestOpenOrdersSync() {
        if (isSharedIbkrGatewayConnected()) {
            try {
                SharedIbkrGatewayMessage response = sharedIbkrGatewayClient.requestOpenOrdersSync("ibkr-trader-requestOpenOrders");
                flowInfo("IBKR.GATEWAY", "request_open_orders_sync detail=" + response.detail + " reqId=" + response.payloadInt("reqId"));
                return;
            } catch (IOException e) {
                flowError("IBKR.GATEWAY", "request_open_orders_sync failed reason=" + e.getMessage());
                if (ibkrSharedGatewaySkipDirectConnection) {
                    return;
                }
            }
        }
        if (client != null && client.isConnected()) {
            client.reqOpenOrders();
        }
    }

    private void handleSharedIbkrGatewayEvent(SharedIbkrGatewayMessage message) {
        // Shared-gateway events are asynchronous relative to the command that originally triggered them.
        // This handler is responsible for reconciling Java-side symbol state with the Python gateway's view
        // of positions and order progress so the strategy does not trade on stale ownership assumptions.
        if (message == null) {
            return;
        }
        String eventSymbol = message.symbol == null ? "" : message.symbol.trim().toUpperCase(Locale.US);
        String tracked = symbol == null ? "" : symbol.trim().toUpperCase(Locale.US);
        if (!eventSymbol.isBlank() && !tracked.isBlank() && !eventSymbol.equals(tracked)) {
            return;
        }
        String eventType = message.eventType == null ? "" : message.eventType.trim().toLowerCase(Locale.US);
        switch (eventType) {
            case "position_updated" -> {
                if (message.position != null) {
                    symbolPositionSeenThisCycle = true;
                    positionSyncComplete = true;
                    double avgCost = message.avgCost == null ? (currentLastPrice > 0.0 ? currentLastPrice : 0.0) : message.avgCost;
                    if (shopStrategy != null) {
                        // Gateway position events are treated as authoritative and immediately overwrite local
                        // inventory assumptions after reconnect or async broker changes.
                        shopStrategy.syncPosition(message.position, avgCost);
                        shopStrategy.setPositionSynced(true);
                    }
                    reconcileSharedCapitalWithPosition(message.position, avgCost);
                    markPositionSyncCompleted("shared-gateway-position-updated", "position=" + message.position + " avgCost=" + avgCost);
                }
            }
            case "position_sync_completed" -> {
                if (!symbolPositionSeenThisCycle) {
                    releaseSharedCapital(symbol, "shared-gateway-position-sync-flat");
                }
                // Completion is a distinct signal: it tells the strategy that silence now means flat, not pending.
                positionSyncComplete = true;
                if (shopStrategy != null) {
                    shopStrategy.setPositionSynced(true);
                }
                markPositionSyncCompleted("shared-gateway-position-sync-completed", "symbolPositionSeenThisCycle=" + symbolPositionSeenThisCycle);
            }
            case "order_status", "order_cancelled" -> {
                Integer orderId = message.gatewayOrderId != null ? message.gatewayOrderId : message.ibOrderId;
                if (orderId != null) {
                    OrderContext orderContext = orderContextById.get(orderId);
                    String status = message.payloadString("status");
                    if (status == null || status.isBlank()) {
                        status = message.detail == null || message.detail.isBlank() ? eventType : message.detail;
                    }
                    Integer remaining = message.payloadInt("remaining");
                    Integer filled = message.payloadInt("filled");
                    Double avgFillPrice = message.payloadDouble("avg_fill_price");
                    flowData(
                        "IBKR.GATEWAY.ORDER_STATUS",
                        "eventType=" + eventType
                            + " symbol=" + (eventSymbol.isBlank() ? tracked : eventSymbol)
                            + " gatewayOrderId=" + message.gatewayOrderId
                            + " ibOrderId=" + message.ibOrderId
                            + " permId=" + message.permId
                            + " resolvedOrderId=" + orderId
                            + " status=" + status
                            + " filled=" + filled
                            + " remaining=" + remaining
                            + " avgFillPrice=" + avgFillPrice
                            + " contextFound=" + (orderContext != null)
                            + " detail=" + message.detail
                    );
                    if (avgFillPrice != null && avgFillPrice > 0.0) {
                        lastOrderAvgFillPrice = avgFillPrice;
                    }
                    boolean terminal = isSharedGatewayEffectivelyTerminal(status, remaining, message.payloadBoolean("effective_terminal"));
                    if (orderContext != null && shopStrategy != null && filled != null) {
                        int filledQty = Math.max(0, filled);
                        int previousFilledQty = cumulativeFilledByOrderId.getOrDefault(orderId, 0);
                        int filledDelta = Math.max(0, filledQty - previousFilledQty);
                        if (filledDelta > 0) {
                            cumulativeFilledByOrderId.put(orderId, filledQty);
                            shopStrategy.onOrderProgress(orderId, orderContext.action(), filledDelta, remaining == null ? 0 : Math.max(0, remaining), avgFillPrice == null ? 0.0 : avgFillPrice);
                        }
                    }
                    if (orderContext != null && terminal) {
                        int filledQty = filled == null
                            ? ("filled".equalsIgnoreCase(status) ? orderContext.quantity() : 0)
                            : Math.max(0, filled);
                        if (!orderContext.closingTrade() && orderContext.capitalReserved()) {
                            if (filledQty > 0) {
                                double referencePrice = avgFillPrice != null && avgFillPrice > 0.0
                                    ? avgFillPrice
                                    : (currentLastPrice > 0.0 ? currentLastPrice : orderContext.reservedNotional() / Math.max(1, orderContext.quantity()));
                                double reservedAmount = avgFillPrice != null && avgFillPrice > 0.0
                                    ? avgFillPrice * filledQty
                                    : orderContext.reservedNotional();
                                reconcileSharedCapitalWithPosition(filledQty, referencePrice, reservedAmount);
                            } else {
                                releaseSharedCapital(orderContext.symbol(), "shared-gateway-entry-terminal-" + status);
                            }
                        } else if (orderContext.closingTrade() && filledQty > 0) {
                            releaseSharedCapital(orderContext.symbol(), "shared-gateway-closing-terminal-" + status);
                        }
                    }
                    if (terminal) {
                        // Mirror direct-IBKR cleanup semantics so both execution modes leave the strategy in the
                        // same unlocked state once an order is finished.
                        orderContextById.remove(orderId);
                        orderSentTimes.remove(orderId);
                        cumulativeFilledByOrderId.remove(orderId);
                        ScheduledFuture<?> timeoutFuture = staleOrderTimeoutByOrderId.remove(orderId);
                        if (timeoutFuture != null) {
                            timeoutFuture.cancel(false);
                        }
                        if (shopStrategy != null) {
                            shopStrategy.onOrderClosed(orderId, status);
                        }
                    }
                }
            }
            case "error" -> flowError("IBKR.GATEWAY", message.detail == null ? "event-error" : message.detail);
            default -> flowInfo("IBKR.GATEWAY", "eventType=" + message.eventType + " detail=" + message.detail);
        }
    }

    private boolean ensureSharedDatabentoRelayReady(Map<String, String> sidecarEnv) {
        if (isSharedDatabentoRelayListening()) {
            return true;
        }
        if (!databentoSharedFeedStartIfMissing) {
            logDatabentoSidecarConfigErrorOnce(
                "Databento shared relay is not listening on " + databentoSharedFeedHost + ":" + databentoSharedFeedPort
                    + ". Enable trading.databento.shared-feed.start-if-missing or start the relay manually."
            );
            return false;
        }

        String apiKey = effectiveDatabentoApiKey(sidecarEnv);
        if (!isUsableDatabentoApiKey(apiKey)) {
            logDatabentoSidecarConfigErrorOnce(
                "Databento shared relay not started: missing valid DATABENTO_API_KEY. Set it in the parent environment or in "
                    + Paths.get(databentoEnvFile).toAbsolutePath().normalize()
                    + ", or set trading.databento.api.key"
            );
            return false;
        }

        long timeoutMs = Math.max(3000L, databentoSharedFeedStartTimeoutMs);
        Path lockPath = Paths.get(databentoSharedFeedLockFile).toAbsolutePath().normalize();
        try {
            if (lockPath.getParent() != null) {
                Files.createDirectories(lockPath.getParent());
            }
            try (FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                FileLock lock = null;
                long deadline = System.currentTimeMillis() + timeoutMs;
                while (lock == null && System.currentTimeMillis() < deadline) {
                    try {
                        lock = channel.tryLock();
                    } catch (OverlappingFileLockException ignored) {
                        lock = null;
                    }
                    if (lock == null) {
                        if (isSharedDatabentoRelayListening()) {
                            return true;
                        }
                        sleepQuietly(200L);
                    }
                }
                if (lock != null) {
                    try (FileLock ignored = lock) {
                        if (!isSharedDatabentoRelayListening()) {
                            startSharedDatabentoRelayProcess(sidecarEnv);
                        }
                    }
                }
            }
        } catch (IOException e) {
            flowError("DATABENTO", "Failed to coordinate shared relay startup reason=" + e.getMessage());
        }

        if (waitForSharedDatabentoRelay(timeoutMs)) {
            return true;
        }

        flowError("DATABENTO", "Shared relay did not become ready on " + databentoSharedFeedHost + ":" + databentoSharedFeedPort);
        return false;
    }

    private void startSharedDatabentoRelayProcess(Map<String, String> sidecarEnv) throws IOException {
        Path relayScriptPath = Paths.get(databentoSharedFeedScript).toAbsolutePath().normalize();
        Path normalizerScriptPath = Paths.get(databentoNormalizerScript).toAbsolutePath().normalize();
        Path botsDirPath = Paths.get(databentoSharedFeedBotsDir).toAbsolutePath().normalize();
        Path workingDirPath = Paths.get("").toAbsolutePath().normalize();
        Path pidFilePath = Paths.get(databentoSharedFeedPidFile).toAbsolutePath().normalize();
        Path logFilePath = Paths.get(databentoSharedFeedLogFile).toAbsolutePath().normalize();

        if (!Files.isRegularFile(relayScriptPath)) {
            throw new FileNotFoundException("missing shared relay script: " + relayScriptPath);
        }
        if (!Files.isRegularFile(normalizerScriptPath)) {
            throw new FileNotFoundException("missing normalizer script: " + normalizerScriptPath);
        }
        if (!Files.isDirectory(botsDirPath)) {
            throw new FileNotFoundException("missing bots dir: " + botsDirPath);
        }
        if (pidFilePath.getParent() != null) {
            Files.createDirectories(pidFilePath.getParent());
        }
        if (logFilePath.getParent() != null) {
            Files.createDirectories(logFilePath.getParent());
        }

        List<String> relayCommand = new ArrayList<>();
        relayCommand.add(databentoPythonBin);
        relayCommand.add(relayScriptPath.toString());
        relayCommand.add("--python-bin");
        relayCommand.add(databentoPythonBin);
        relayCommand.add("--normalizer-script");
        relayCommand.add(normalizerScriptPath.toString());
        relayCommand.add("--bots-dir");
        relayCommand.add(botsDirPath.toString());
        relayCommand.add("--working-dir");
        relayCommand.add(workingDirPath.toString());
        relayCommand.add("--listen-host");
        relayCommand.add(databentoSharedFeedHost);
        relayCommand.add("--listen-port");
        relayCommand.add(String.valueOf(databentoSharedFeedPort));
        relayCommand.add("--pid-file");
        relayCommand.add(pidFilePath.toString());
        if (databentoLiveGatewayOverride != null && !databentoLiveGatewayOverride.isBlank()) {
            relayCommand.add("--live-gateway");
            relayCommand.add(databentoLiveGatewayOverride.trim());
        }
        relayCommand.add("--equity-dataset");
        relayCommand.add(databentoEquityDataset);
        relayCommand.add("--equity-schema");
        relayCommand.add(databentoEquitySchema);
        relayCommand.add("--startup-history-seconds");
        relayCommand.add(String.format(Locale.US, "%.3f", effectiveSharedRelayStartupHistorySeconds()));
        relayCommand.add("--startup-history-schema");
        relayCommand.add(databentoStartupHistorySchema);
        relayCommand.add("--options-dataset");
        relayCommand.add(databentoOptionsDataset);
        relayCommand.add("--options-schema");
        relayCommand.add(databentoOptionsSchema);
        relayCommand.add("--heartbeat-seconds");
        relayCommand.add(String.valueOf(Math.max(1, databentoHeartbeatSeconds)));
        relayCommand.add("--startup-delay-seconds");
        relayCommand.add(String.format(Locale.US, "%.3f", Math.max(0.0, databentoStartupDelaySeconds)));
        appendDatabentoFeatureSnapshotArgs(relayCommand);
        relayCommand.add("--expected-client-count");
        relayCommand.add(String.valueOf(Math.max(1, databentoSharedFeedExpectedClientCount)));
        relayCommand.add("--wait-for-clients-timeout-seconds");
        relayCommand.add(String.valueOf(Math.max(1L, databentoSharedFeedClientWaitTimeoutMs) / 1000.0));

        ProcessBuilder builder = new ProcessBuilder(relayCommand);
        builder.directory(workingDirPath.toFile());
        if (sidecarEnv != null && !sidecarEnv.isEmpty()) {
            builder.environment().putAll(sidecarEnv);
        }
        builder.redirectOutput(ProcessBuilder.Redirect.appendTo(logFilePath.toFile()));
        builder.redirectError(ProcessBuilder.Redirect.appendTo(logFilePath.toFile()));
        Process relayProcess = builder.start();
        flowInfo(
            "DATABENTO",
            "Started shared relay pid=" + relayProcess.pid()
                + " host=" + databentoSharedFeedHost
                + " port=" + databentoSharedFeedPort
                + " logFile=" + logFilePath
        );
    }

    private double effectiveSharedRelayStartupHistorySeconds() {
        return Math.max(0.0, databentoSharedFeedStartupHistorySeconds);
    }

    private void appendDatabentoFeatureSnapshotArgs(List<String> command) {
        if (!databentoFeatureSnapshotsEnabled) {
            return;
        }
        command.add("--emit-live-feature-snapshots");
        String source = databentoFeatureSnapshotSource == null ? "" : databentoFeatureSnapshotSource.trim();
        if (!source.isBlank()) {
            command.add("--feature-snapshot-source");
            command.add(source);
        }
        String schemaVersion = databentoFeatureSnapshotSchemaVersion == null ? "" : databentoFeatureSnapshotSchemaVersion.trim();
        if (!schemaVersion.isBlank()) {
            command.add("--feature-snapshot-schema-version");
            command.add(schemaVersion);
        }
    }

    private boolean waitForSharedDatabentoRelay(long timeoutMs) {
        long deadline = System.currentTimeMillis() + Math.max(1000L, timeoutMs);
        while (System.currentTimeMillis() < deadline) {
            if (isSharedDatabentoRelayListening()) {
                return true;
            }
            sleepQuietly(250L);
        }
        return isSharedDatabentoRelayListening();
    }

    private boolean isSharedDatabentoRelayListening() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(databentoSharedFeedHost, databentoSharedFeedPort), 750);
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    private void sleepQuietly(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(Math.max(1L, millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean shouldBlockForStaleDatabentoQuote(String normalizedOrderType, boolean isClosingTrade) {
        if (!useDatabentoMarketData()) {
            return false;
        }
        DatabentoFeedHealth.Snapshot snapshot = currentDatabentoSnapshot();
        DatabentoFeedHealth.SymbolSnapshot primary = snapshot.primarySymbolHealth();
        boolean allowClosingBypass = isClosingTrade
            && databentoAllowStaleClosingMarketOrder
            && "MKT".equalsIgnoreCase(normalizedOrderType);
        boolean quoteFresh = snapshot.gatewayRunning() && primary != null && primary.quoteFresh();
        flowCondition(
            "ORDER.GATE",
            "DATABENTO_QUOTE_FRESH",
            quoteFresh || allowClosingBypass,
            "symbol=" + getTrackedSymbol()
                + " gatewayRunning=" + snapshot.gatewayRunning()
                + " quoteAgeMs=" + (primary == null ? -1L : primary.quoteAgeMs())
                + " quoteFresh=" + (primary != null && primary.quoteFresh())
                + " allowClosingBypass=" + allowClosingBypass
                + " restartRecommended=" + snapshot.restartRecommended()
        );
        if (!quoteFresh && !allowClosingBypass) {
            flowError(
                "ORDER.GATE",
                "Blocked order due to stale Databento quote symbol=" + getTrackedSymbol()
                    + " quoteAgeMs=" + (primary == null ? -1L : primary.quoteAgeMs())
                    + " gatewayRunning=" + snapshot.gatewayRunning()
                    + " restartRecommended=" + snapshot.restartRecommended()
            );
            return true;
        }
        return false;
    }

    private String normalizedMarketDataProvider() {
        return marketDataProvider == null ? "ibkr" : marketDataProvider.trim().toLowerCase(Locale.US);
    }

    private void startDatabentoFeedMonitor() {
        if (!useDatabentoMarketData()) {
            return;
        }
        if (databentoFeedMonitorFuture != null && !databentoFeedMonitorFuture.isCancelled() && !databentoFeedMonitorFuture.isDone()) {
            return;
        }
        long intervalSeconds = Math.max(2L, Math.min(5L, Math.max(1L, databentoHeartbeatSeconds)));
        databentoFeedMonitorFuture = databentoSupervisorScheduler.scheduleAtFixedRate(
            this::monitorDatabentoFeed,
            intervalSeconds,
            intervalSeconds,
            TimeUnit.SECONDS
        );
    }

    private void startMarketScheduleMonitor() {
        if (marketScheduleMonitorFuture != null && !marketScheduleMonitorFuture.isCancelled() && !marketScheduleMonitorFuture.isDone()) {
            return;
        }
        marketScheduleMonitorFuture = databentoSupervisorScheduler.scheduleAtFixedRate(
            this::monitorMarketScheduleClock,
            1L,
            1L,
            TimeUnit.SECONDS
        );
    }

    private void stopDatabentoFeedMonitor() {
        ScheduledFuture<?> future = databentoFeedMonitorFuture;
        if (future != null) {
            future.cancel(false);
        }
        databentoFeedMonitorFuture = null;
    }

    private void stopMarketScheduleMonitor() {
        ScheduledFuture<?> future = marketScheduleMonitorFuture;
        if (future != null) {
            future.cancel(false);
        }
        marketScheduleMonitorFuture = null;
    }

    private void monitorMarketScheduleClock() {
        monitorMarketScheduleClock(ZonedDateTime.now(MARKET_ZONE));
    }

    private void monitorMarketScheduleClock(ZonedDateTime currentTime) {
        if (isShuttingDown || shopStrategy == null) {
            return;
        }
        // The clock monitor is the safety net for close/open transitions when real-time bars stop arriving.
        applyMarketSchedule(currentTime);
    }

    private void monitorDatabentoFeed() {
        if (isShuttingDown || !useDatabentoMarketData()) {
            return;
        }
        if (!hasUsableDatabentoSidecarConfiguration()) {
            return;
        }
        DatabentoFeedHealth.Snapshot snapshot = currentDatabentoSnapshot();
        if (!snapshot.gatewayRunning()) {
            // If the sidecar/relay is gone entirely, restart immediately instead of waiting for quote-age logic.
            requestDatabentoGatewayRestart("gateway-not-running");
            return;
        }
        if (!snapshot.marketDataExpectedNow() || snapshot.withinStartupGrace()) {
            return;
        }
        if (snapshot.restartRecommended()) {
            DatabentoFeedHealth.SymbolSnapshot primary = snapshot.primarySymbolHealth();
            // Restart decisions are based on symbol-specific quote/bar age, which avoids relying on a single
            // callback type to declare the feed healthy.
            requestDatabentoGatewayRestart(
                "feed-silence quoteAgeMs=" + (primary == null ? -1L : primary.quoteAgeMs())
                    + " barAgeMs=" + (primary == null ? -1L : primary.barAgeMs())
                    + " marketDataAgeMs=" + (primary == null ? -1L : primary.marketDataAgeMs())
                    + " lastStatusAgeMs=" + snapshot.lastStatusAgeMs()
                    + " lastStatusMessage=" + (snapshot.lastStatusMessage() == null ? "" : snapshot.lastStatusMessage())
            );
        }
    }

    private void handleDatabentoGatewayExit(int exitCode, boolean unexpected) {
        databentoFeedHealth.markGatewayExited(exitCode, System.currentTimeMillis());
        resetOptionVolumeAccumulators("databento-gateway-exit");
        if (!unexpected || isShuttingDown || !useDatabentoMarketData()) {
            return;
        }
        requestDatabentoGatewayRestart("process-exit-" + exitCode);
    }

    private void requestDatabentoGatewayRestart(String reason) {
        // Restart requests are intentionally de-duplicated and jittered. Without that, a large fleet of symbol
        // bots could all detect the same relay outage and attempt synchronized reconnects/restarts at once.
        if (!useDatabentoMarketData() || isShuttingDown || !databentoAutoRestartEnabled) {
            return;
        }
        if (!hasUsableDatabentoSidecarConfiguration()) {
            return;
        }
        if (!databentoRestartPending.compareAndSet(false, true)) {
            return;
        }
        long nowMs = System.currentTimeMillis();
        resetOptionVolumeAccumulators("databento-restart-requested");
        long stableJitterMs = resolveDatabentoRestartJitterMs();
        long restartDelayMs = Math.max(250L, databentoRestartDelayMs) + stableJitterMs;
        databentoFeedHealth.markRestartRequested(reason, nowMs);
        flowInfo("DATABENTO", "Scheduling sidecar restart reason=" + reason + " delayMs=" + restartDelayMs + " jitterMs=" + stableJitterMs);
        databentoSupervisorScheduler.schedule(() -> {
            try {
                if (databentoLiveGateway != null) {
                    databentoLiveGateway.stop();
                }
                databentoLiveGateway = null;
                if (!isShuttingDown && useDatabentoMarketData()) {
                    startDatabentoLiveGateway();
                }
            } finally {
                databentoRestartPending.set(false);
            }
        }, restartDelayMs, TimeUnit.MILLISECONDS);
    }

    private boolean hasUsableDatabentoSidecarConfiguration() {
        String apiKey = effectiveDatabentoApiKey(resolveDatabentoSidecarEnvironment());
        if (isUsableDatabentoApiKey(apiKey)) {
            databentoSidecarConfigErrorLogged.set(false);
            return true;
        }
        logDatabentoSidecarConfigErrorOnce(
            "Databento sidecar disabled: missing valid DATABENTO_API_KEY. Update the parent environment or "
                + Paths.get(databentoEnvFile).toAbsolutePath().normalize()
                + ", or set trading.databento.api.key"
        );
        return false;
    }

    private void logDatabentoSidecarConfigErrorOnce(String message) {
        if (databentoSidecarConfigErrorLogged.compareAndSet(false, true)) {
            flowError("DATABENTO", message);
        }
    }

    private Map<String, String> resolveDatabentoSidecarEnvironment() {
        Path envPath = Paths.get(databentoEnvFile).toAbsolutePath().normalize();
        if (!Files.isRegularFile(envPath)) {
            return Map.of();
        }

        Map<String, String> env = new LinkedHashMap<>();
        try {
            for (String rawLine : Files.readAllLines(envPath)) {
                String line = rawLine == null ? "" : rawLine.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("export ")) {
                    line = line.substring("export ".length()).trim();
                }
                int equalsIndex = line.indexOf('=');
                if (equalsIndex <= 0) {
                    continue;
                }
                String key = line.substring(0, equalsIndex).trim();
                if (key.isEmpty() || System.getenv(key) != null) {
                    continue;
                }
                String value = stripDatabentoEnvQuotes(line.substring(equalsIndex + 1).trim());
                if (!value.isEmpty()) {
                    env.put(key, value);
                }
            }
        } catch (IOException e) {
            flowError("DATABENTO", "Failed to read sidecar env file path=" + envPath + " reason=" + e.getMessage());
        }

        String configuredApiKey = normalizedDatabentoApiKey(databentoApiKey);
        if (isUsableDatabentoApiKey(configuredApiKey)) {
            env.put("DATABENTO_API_KEY", configuredApiKey);
        }
        return env;
    }

    private String effectiveDatabentoApiKey(Map<String, String> sidecarEnv) {
        String configuredApiKey = normalizedDatabentoApiKey(databentoApiKey);
        if (isUsableDatabentoApiKey(configuredApiKey)) {
            return configuredApiKey;
        }
        String apiKey = System.getenv("DATABENTO_API_KEY");
        if (apiKey != null && !apiKey.isBlank()) {
            return apiKey.trim();
        }
        if (sidecarEnv == null) {
            return "";
        }
        String fallback = sidecarEnv.get("DATABENTO_API_KEY");
        return fallback == null ? "" : fallback.trim();
    }

    private String normalizedDatabentoApiKey(String apiKey) {
        return stripDatabentoEnvQuotes(apiKey == null ? "" : apiKey.trim()).trim();
    }

    private boolean isUsableDatabentoApiKey(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            return false;
        }
        return !DATABENTO_API_KEY_PLACEHOLDERS.contains(apiKey.trim().toLowerCase(Locale.US));
    }

    private String stripDatabentoEnvQuotes(String value) {
        if (value == null || value.length() < 2) {
            return value == null ? "" : value;
        }
        if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private boolean isDatabentoMarketDataExpectedNow() {
        ZonedDateTime now = ZonedDateTime.now(MARKET_ZONE);
        DayOfWeek day = now.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return false;
        }
        LocalTime time = now.toLocalTime();
        return !time.isBefore(LocalTime.of(9, 30)) && time.isBefore(LocalTime.of(16, 0));
    }

    private double resolveExecutionReferencePrice(String action, double requestedPrice, String normalizedOrderType) {
        // We prefer bid/ask over last trade because execution intent is side-sensitive:
        // sellers should anchor to bid, buyers to ask. Last trade remains only a fallback when the quote book
        // is temporarily unavailable.
        double fallback = requestedPrice > 0.0
            ? requestedPrice
            : (currentLastPrice > 0.0 ? currentLastPrice : ("SELL".equalsIgnoreCase(action) ? currentBidPrice : currentAskPrice));
        if ("SELL".equalsIgnoreCase(action) && currentBidPrice > 0.0) {
            return currentBidPrice;
        }
        if (currentAskPrice > 0.0) {
            return currentAskPrice;
        }
        return fallback;
    }

    private String resolvePreferredOrderType(String action, boolean isClosingTrade) {
        // Order policy is intent-based, not action-based.
        // Example: SELL can mean either "open short" (entry => FAST_LMT) or "exit long" (exit => MKT).
        // Likewise BUY can mean either "open long" (entry => FAST_LMT) or "cover short" (exit => MKT).
        return isClosingTrade ? "MKT" : "FAST_LMT";
    }

    private long resolveDatabentoRestartJitterMs() {
        long maxJitterMs = Math.max(0L, databentoRestartJitterMs);
        if (maxJitterMs <= 0L) {
            return 0L;
        }
        long seed = Math.abs(Objects.hash(getTrackedSymbol(), clientId));
        return Math.floorMod(seed, maxJitterMs + 1L);
    }

    private void resetOptionVolumeAccumulators(String reason) {
        latestPutVolume = 0L;
        latestCallVolume = 0L;
        if (shopStrategy != null) {
            shopStrategy.onOptionVolumeUpdate(0L, 0L);
        }
        flowInfo("DATABENTO.OPTIONS", "Reset put/call accumulators reason=" + reason + " symbol=" + getTrackedSymbol());
    }

    // Logger Helpers
    private void flowInfo(String tag, String msg) { log.info(">>> [FLOW][INFO][{}] {}", tag, msg); }
    private boolean isTickerLevelTag(String tag) {
        return "IBKR.TICK".equals(tag)
            || "IBKR->AI.TICK".equals(tag)
            || "IBKR.SUBSCRIBE".equals(tag)
            || "DATABENTO.BAR".equals(tag)
            || "DATABENTO.STDERR".equals(tag)
            || "DATABENTO->AI.BAR".equals(tag)
            || "STRATEGY.TAPE".equals(tag)
            || "STRATEGY.TICK".equals(tag);
    }

    private void flowData(String tag, String msg) {
        if (isTickerLevelTag(tag)) {
            log.debug(">>> [FLOW][DATA][{}] {}", tag, msg);
            return;
        }
        log.info(">>> [FLOW][DATA][{}] {}", tag, msg);
    }

    private void flowCondition(String tag, String check, boolean pass, String context) {
        if (isTickerLevelTag(tag)) {
            log.debug(">>> [FLOW][COND][{}] {}={} | {}", tag, check, pass ? "PASS" : "FAIL", context);
            return;
        }
        log.info(">>> [FLOW][COND][{}] {}={} | {}", tag, check, pass ? "PASS" : "FAIL", context);
    }
    private void flowAnalyze(String tag, String msg) { log.info(">>> [FLOW][ANALYZE][{}] {}", tag, msg); }
    private void flowError(String tag, String msg) { log.error(">>> [ERROR][{}] {}", tag, msg); }
    private void flowDataDebug(String tag, String msg) { log.debug(">>> [FLOW][DATA][{}] {}", tag, msg); }
    private void flowConditionDebug(String tag, String check, boolean pass, String context) { log.debug(">>> [FLOW][COND][{}] {}={} | {}", tag, check, pass ? "PASS" : "FAIL", context); }

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
                flowConditionDebug("IBKR.TICK", "RT_VOLUME_PARSE", false, "value=" + value);
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
