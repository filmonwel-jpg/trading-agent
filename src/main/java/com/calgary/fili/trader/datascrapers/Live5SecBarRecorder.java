package com.calgary.fili.trader.datascrapers;

import com.ib.client.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

public class Live5SecBarRecorder implements EWrapper {

    private static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final DateTimeFormatter FILE_TS_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
    private static final DateTimeFormatter MARKET_TS_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV");
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 7497; // Paper trading port
    private static final int CLIENT_ID = 205; // Unique ID to avoid conflicts
    private static final int MKT_DATA_REQ_ID = 1001;
    private static final int REALTIME_BAR_REQ_ID = 3001;

    private final EJavaSignal signal = new EJavaSignal();
    private final EClientSocket client = new EClientSocket(this, signal);

    private PrintWriter writer;
    private volatile boolean isRunning = true;
    private final String symbol;
    
    // Captured live from the API
    private volatile double yesterdayClose = 0.0;

    public static void main(String[] args) {
        String targetSymbol = (args.length > 0 && !args[0].isBlank()) ? args[0].toUpperCase() : "TSLA";
        new Live5SecBarRecorder(targetSymbol).startRecording();
    }

    public Live5SecBarRecorder(String symbol) {
        this.symbol = symbol;
    }

    public void startRecording() {
        flowInfo("LIVE", "Starting Live 5-Second Recorder for " + symbol + "...");

        try {
            initCsvWriter();

            client.eConnect(HOST, PORT, CLIENT_ID);
            flowData("LIVE.CONNECT", "connect host=" + HOST + " port=" + PORT + " clientId=" + CLIENT_ID);

            final EReader reader = new EReader(client, signal);
            reader.start();
            new Thread(() -> {
                while (client.isConnected() && isRunning) {
                    signal.waitForSignal();
                    try {
                        reader.processMsgs();
                    } catch (Exception e) {
                        System.err.println("Reader exception: " + e.getMessage());
                    }
                }
            }).start();

            Thread.sleep(2000); // Wait for connection

            if (client.isConnected()) {
                Contract contract = new Contract();
                contract.symbol(symbol);
                contract.secType("STK");
                contract.currency("USD");
                contract.exchange("SMART");
                flowData("LIVE.CONTRACT", "symbol=" + symbol + " secType=STK exchange=SMART currency=USD");

                // 1. Fetch Yesterday's Close
                flowInfo("LIVE.YCLOSE", "Fetching Yesterday's Close...");
                client.reqMktData(MKT_DATA_REQ_ID, contract, "", false, false, null);
                Thread.sleep(4000); // Give IBKR 4 seconds to return the tick
                client.cancelMktData(MKT_DATA_REQ_ID);

                if (yesterdayClose == 0.0) {
                    flowCondition("LIVE.YCLOSE", "VALUE>0", false, "value=" + yesterdayClose);
                    flowError("LIVE.YCLOSE", "Could not fetch Yesterday's Close. Defaulting to 0.0.");
                } else {
                    flowCondition("LIVE.YCLOSE", "VALUE>0", true, "value=" + yesterdayClose);
                    flowInfo("LIVE.YCLOSE", "Success. Yesterday's Close: " + yesterdayClose);
                }

                // 2. Start Live 5-Second Bars
                flowInfo("LIVE.BARS", "Subscribing to Live 5-Second Bars reqId=" + REALTIME_BAR_REQ_ID);
                client.reqRealTimeBars(REALTIME_BAR_REQ_ID, contract, 5, "TRADES", false, null);

                // Keep running until 4:05 PM Eastern (Market Close + buffer)
                while (isRunning) {
                    LocalDateTime nowEt = LocalDateTime.now(MARKET_ZONE);
                    if (nowEt.getHour() >= 16 && nowEt.getMinute() >= 5) {
                        flowCondition("LIVE.SESSION", "MARKET_OPEN", false, "nowEt=" + nowEt + " threshold=16:05");
                        flowInfo("LIVE.SESSION", "Market is closed. Shutting down.");
                        isRunning = false;
                    } else {
                        flowCondition("LIVE.SESSION", "MARKET_OPEN", true, "nowEt=" + nowEt);
                    }
                    Thread.sleep(10000); // Check time every 10 seconds
                }

                client.cancelRealTimeBars(REALTIME_BAR_REQ_ID);
                client.eDisconnect();

            } else {
                flowCondition("LIVE.CONNECT", "IBKR_CONNECTED", false, "host=" + HOST + " port=" + PORT + " clientId=" + CLIENT_ID);
                flowError("LIVE.CONNECT", "Failed to connect to IBKR.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                writer.close();
            }
            System.exit(0);
        }
    }

    private void initCsvWriter() throws IOException {
        String dateStr = ZonedDateTime.now(MARKET_ZONE).format(FILE_TS_FORMAT);
        File file = new File(symbol + "_Live_5Sec_Data_" + dateStr + ".csv");
        boolean needsHeader = !file.exists();
        flowData("LIVE.FILE", "path=" + file.getName() + " needsHeader=" + needsHeader);

        writer = new PrintWriter(new FileWriter(file, true));
        if (needsHeader) {
            writer.println("Timestamp,Open,High,Low,Close,Volume,WAP,Count,YesterdayClose");
        }
    }

    @Override
    public void tickPrice(int tickerId, int field, double price, TickAttrib attribs) {
        if (tickerId == MKT_DATA_REQ_ID && field == 9) { // Field 9 = Yesterday's Close
            this.yesterdayClose = price;
            flowData("LIVE.YCLOSE", "tickPrice field=9 value=" + price);
        }
    }

    @Override
    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, Decimal volume, Decimal wap, int count) {
        if (reqId == REALTIME_BAR_REQ_ID && isRunning) {
            ZonedDateTime barTimeEt = Instant.ofEpochSecond(time).atZone(ZoneId.of("UTC")).withZoneSameInstant(MARKET_ZONE);
            String timeStr = barTimeEt.format(MARKET_TS_FORMAT);
            String volStr = (volume != null && volume.value() != null) ? volume.value().toPlainString() : "0";
            String wapStr = (wap != null && wap.value() != null) ? wap.value().toPlainString() : String.valueOf(close);

            boolean validBar = isFinite(open) && isFinite(high) && isFinite(low) && isFinite(close) && high >= low && count >= 0;
            flowCondition("LIVE.BAR", "BAR_VALID", validBar, "ts=" + timeStr + " ohlc=" + open + "/" + high + "/" + low + "/" + close + " count=" + count + " vol=" + volStr);

            String line = String.format("%s,%.4f,%.4f,%.4f,%.4f,%s,%s,%d,%.4f",
                    timeStr, open, high, low, close, volStr, wapStr, count, yesterdayClose);

            writer.println(line);
            writer.flush(); // Ensure it writes to disk immediately

            // Print every minute to the console to show it's alive
            if (barTimeEt.getSecond() == 0 || barTimeEt.getSecond() == 30) {
                flowData("LIVE.BAR", "heartbeat " + line);
            }
        }
    }

    // --- Required EWrapper Boilerplate ---
    @Override public void error(Exception e) { System.err.println("Error: " + e.getMessage()); }
    @Override public void error(String str) { System.err.println("Error: " + str); }
    @Override public void error(int id, long errorCode, int errorVersion, String errorString, String errorExplain) {
        if (errorCode != 2104 && errorCode != 2106) { 
            flowError("LIVE.IBKR", "id=" + id + " code=" + errorCode + " msg=" + errorString);
        }
    }
    @Override public void connectionClosed() { flowInfo("LIVE.CONNECT", "Connection closed."); isRunning = false; }

    private boolean isFinite(double value) {
        return !Double.isNaN(value) && !Double.isInfinite(value);
    }

    private void flowInfo(String stage, String message) {
        System.out.println(">>> [FLOW][INFO][" + stage + "] " + message);
    }

    private void flowData(String stage, String message) {
        System.out.println(">>> [FLOW][DATA][" + stage + "] " + message);
    }

    private void flowError(String stage, String message) {
        System.err.println(">>> [FLOW][ERROR][" + stage + "] " + message);
    }

    private void flowCondition(String stage, String conditionName, boolean passed, String details) {
        String verdict = passed ? "PASS" : "FAIL";
        System.out.println(">>> [FLOW][COND][" + stage + "] " + conditionName + "=" + verdict + " | " + details);
    }
    
    @Override public void currentTimeInMillis(long timeInMillis) {}
    @Override public void tickSize(int tickerId, int field, Decimal size) {}
    @Override public void tickString(int tickerId, int tickType, String value) {}
    @Override public void tickGeneric(int tickerId, int tickType, double value) {}
    @Override public void tickEFP(int tickerId, int tickType, double basisPoints, String formattedBasisPoints, double impliedFuture, int holdDays, String futureLastTradeDate, double dividendImpact, double dividendsToLastTradeDate) {}
    @Override public void orderStatus(int orderId, String status, Decimal filled, Decimal remaining, double avgFillPrice, long permId, int parentId, double lastFillPrice, int clientId, String whyHeld, double mktCapPrice) {}
    @Override public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {}
    @Override public void openOrderEnd() {}
    @Override public void updateAccountValue(String key, String value, String currency, String accountName) {}
    @Override public void updatePortfolio(Contract contract, Decimal position, double marketPrice, double marketValue, double averageCost, double unrealizedPNL, double realizedPNL, String accountName) {}
    @Override public void updateAccountTime(String timeStamp) {}
    @Override public void accountDownloadEnd(String accountName) {}
    @Override public void nextValidId(int orderId) {}
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
    @Override public void historicalData(int reqId, Bar bar) {}
    @Override public void scannerParameters(String xml) {}
    @Override public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance, String benchmark, String projection, String legsStr) {}
    @Override public void scannerDataEnd(int reqId) {}
    @Override public void currentTime(long time) {}
    @Override public void fundamentalData(int reqId, String data) {}
    @Override public void deltaNeutralValidation(int reqId, DeltaNeutralContract deltaNeutralContract) {}
    @Override public void tickSnapshotEnd(int reqId) {}
    @Override public void marketDataType(int reqId, int marketDataType) {}
    @Override public void commissionAndFeesReport(CommissionAndFeesReport commissionAndFeesReport) {}
    @Override public void position(String account, Contract contract, Decimal pos, double avgCost) {}
    @Override public void positionEnd() {}
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
    @Override public void historicalDataUpdate(int reqId, Bar bar) {}
    @Override public void rerouteMktDataReq(int reqId, int conId, String exchange) {}
    @Override public void rerouteMktDepthReq(int reqId, int conId, String exchange) {}
    @Override public void marketRule(int marketRuleId, PriceIncrement[] priceIncrements) {}
    @Override public void pnl(int reqId, double dailyPnL, double unrealizedPnL, double realizedPnL) {}
    @Override public void pnlSingle(int reqId, Decimal pos, double dailyPnL, double unrealizedPnL, double realizedPnL, double value) {}
    @Override public void historicalTicks(int reqId, java.util.List<HistoricalTick> ticks, boolean done) {}
    @Override public void historicalTicksBidAsk(int reqId, java.util.List<HistoricalTickBidAsk> ticks, boolean done) {}
    @Override public void historicalTicksLast(int reqId, java.util.List<HistoricalTickLast> ticks, boolean done) {}
    @Override public void tickByTickAllLast(int reqId, int tickType, long time, double price, Decimal size, TickAttribLast tickAttribLast, String exchange, String specialConditions) {}
    @Override public void tickByTickBidAsk(int reqId, long time, double bidPrice, double askPrice, Decimal bidSize, Decimal askSize, TickAttribBidAsk tickAttribBidAsk) {}
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
    @Override public void historicalDataEnd(int reqId, String startDateStr, String endDateStr) {}
    @Override public void connectAck() {}
    @Override public void tickOptionComputation(int tId, int t, int i, double d, double o, double p, double g, double v, double t2, double u, double b) {}
}