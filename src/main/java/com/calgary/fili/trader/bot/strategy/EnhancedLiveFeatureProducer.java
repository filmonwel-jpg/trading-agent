package com.calgary.fili.trader.bot.strategy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class EnhancedLiveFeatureProducer {
    static final List<String> META_PRODUCER_FEATURE_COLUMNS = List.of(
        "tsm_ret_30s_p50",
        "tsm_ret_120s_p50",
        "tsm_ret_30s_p10",
        "tsm_ret_30s_p90",
        "tsm_up_prob_30s",
        "tsm_vol_forecast_120s",
        "tsm_uncertainty",
        "regime_trend_prob",
        "regime_chop_prob",
        "regime_volatile_prob",
        "regime_transition_prob",
        "news_event_earnings",
        "news_event_analyst",
        "news_event_legal",
        "news_event_earnings_beat_miss",
        "news_event_analyst_upgrade_downgrade",
        "news_event_legal_regulatory",
        "news_event_product_capex",
        "news_event_macro_spillover",
        "news_novelty_score",
        "news_relevance_score",
        "news_embedding_cluster",
        "seq_lstm_up_prob_30s",
        "seq_tcn_up_prob_30s",
        "seq_transformer_up_prob_30s",
        "seq_patchtst_up_prob_30s",
        "seq_model_consensus_up_prob_30s",
        "setup_breakout_prob",
        "setup_pullback_continuation_prob",
        "setup_reversal_prob",
        "setup_trend_exhaustion_prob",
        "setup_failed_breakout_prob"
    );

    record Snapshot(
        double barOpen,
        double barHigh,
        double barLow,
        double barClose,
        double barVolume,
        List<Double> closeHistory,
        List<Double> highHistory,
        List<Double> lowHistory,
        List<Double> volumeHistory
    ) {}

    private EnhancedLiveFeatureProducer() {}

    static Map<String, Float> produce(Snapshot snapshot) {
        double[] close = toArray(snapshot.closeHistory());
        double[] high = toArray(snapshot.highHistory());
        double[] low = toArray(snapshot.lowHistory());
        double[] volume = toArray(snapshot.volumeHistory());

        Map<String, Float> out = new LinkedHashMap<>();
        appendTimesFmProxy(close, out);
        appendRegimeProxy(close, high, low, out);
        appendNewsProxy(out);
        appendSequenceProxy(close, volume, out);
        appendSetupProxy(snapshot, close, high, low, out);
        return out;
    }

    private static void appendTimesFmProxy(double[] close, Map<String, Float> out) {
        double[] ret30s = pctChange(close, 1);
        double[] ret120s = pctChange(close, 4);

        double ret30sP50 = lastFinite(rollingQuantile(ret30s, 40, 8, 0.50), 0.0);
        double ret120sP50 = lastFinite(rollingQuantile(ret120s, 30, 6, 0.50), 0.0);
        double ret30sP10 = lastFinite(rollingQuantile(ret30s, 40, 8, 0.10), 0.0);
        double ret30sP90 = lastFinite(rollingQuantile(ret30s, 40, 8, 0.90), 0.0);
        double upProb = lastFinite(rollingPositiveFraction(ret30s, 30, 6), 0.5);
        double volForecast = lastFinite(rollingStd(ret120s, 30, 6), 0.0);
        double quantileSpread = Math.abs(ret30sP90 - ret30sP10);
        double confidencePenalty = Math.abs(upProb - 0.5) * 2.0;
        double uncertainty = clip((0.60 * quantileSpread + 0.40 * volForecast) * (1.2 - confidencePenalty), 0.0, Double.POSITIVE_INFINITY);

        out.put("tsm_ret_30s_p50", (float) ret30sP50);
        out.put("tsm_ret_120s_p50", (float) ret120sP50);
        out.put("tsm_ret_30s_p10", (float) ret30sP10);
        out.put("tsm_ret_30s_p90", (float) ret30sP90);
        out.put("tsm_up_prob_30s", (float) clip(upProb, 0.0, 1.0));
        out.put("tsm_vol_forecast_120s", (float) clip(volForecast, 0.0, Double.POSITIVE_INFINITY));
        out.put("tsm_uncertainty", (float) uncertainty);
    }

    private static void appendRegimeProxy(double[] close, double[] high, double[] low, Map<String, Float> out) {
        double[] tr = trueRange(high, low, close);
        double[] atr = rollingMean(tr, 12, 4);
        double[] emaFast = ewm(close, 12);
        double[] emaSlow = ewm(close, 26);
        double[] ret = pctChange(close, 1);
        fillNaN(ret, 0.0);
        double[] vol20 = rollingStd(ret, 20, 6);
        double[] netMove = abs(diff(close, 6));
        double[] grossMove = rollingSum(abs(diff(close, 1)), 6, 3);

        double[] trendProb = new double[close.length];
        double[] chopProb = new double[close.length];
        double[] volProb = new double[close.length];
        for (int i = 0; i < close.length; i++) {
            double atrValue = finiteOr(atr[i], 0.0);
            double trendRaw = (emaFast[i] - emaSlow[i]) / (atrValue + 1.0e-9);
            double trendScore = sigmoid(trendRaw);

            double volStd = finiteOr(vol20[i], 0.0);
            double volScore = sigmoid((volStd - 0.0015) / 0.0008);

            double chopRaw = 1.0 - (finiteOr(netMove[i], 0.0) / (finiteOr(grossMove[i], 0.0) + 1.0e-9));
            double chopScore = clip(finiteOr(chopRaw, 0.5), 0.0, 1.0);

            double trendLogit = 2.6 * (trendScore - 0.5);
            double chopLogit = 2.2 * (chopScore - 0.5);
            double volLogit = 2.4 * (volScore - 0.5);
            double[] probs = softmax3(trendLogit, chopLogit, volLogit);
            trendProb[i] = probs[0];
            chopProb[i] = probs[1];
            volProb[i] = probs[2];
        }

        int last = lastIndex(close.length);
        double transition = 0.0;
        if (last > 0) {
            transition = (
                Math.abs(trendProb[last] - trendProb[last - 1])
                    + Math.abs(chopProb[last] - chopProb[last - 1])
                    + Math.abs(volProb[last] - volProb[last - 1])
            ) / 2.0;
        }

        out.put("regime_trend_prob", (float) clip(trendProb[last], 0.0, 1.0));
        out.put("regime_chop_prob", (float) clip(chopProb[last], 0.0, 1.0));
        out.put("regime_volatile_prob", (float) clip(volProb[last], 0.0, 1.0));
        out.put("regime_transition_prob", (float) clip(transition, 0.0, 1.0));
    }

    private static void appendNewsProxy(Map<String, Float> out) {
        double news60 = 0.0;
        double news300 = 0.0;
        double providers = 0.0;
        double sentimentLatest = 0.0;
        double sentimentMean = 0.0;
        double sentimentStd = 0.0;
        double confMean = 0.0;
        double confLatest = 0.0;
        double lagSec = 999999.0;
        double coverage = 0.0;

        double freshness = Math.exp(-clip(lagSec, 0.0, 3600.0) / 300.0);
        double novelty = clip((news60 + 1.0) / (news300 + 2.0), 0.0, 1.0) * freshness;
        double newsNoveltyScore = clip(0.6 * novelty + 0.4 * freshness, 0.0, 1.0);
        double sentimentJump = Math.abs(sentimentLatest - sentimentMean);
        double intensity = clip(news60 / 6.0, 0.0, 1.0);
        double confMix = clip(0.5 * confMean + 0.5 * confLatest, 0.0, 1.0);

        double earningsProb = sigmoid(3.0 * intensity + 2.0 * sentimentJump + 1.2 * confMix - 2.2);
        double analystProb = sigmoid(2.4 * clip(providers / 4.0, 0.0, 1.0) + 1.6 * confMix + 1.2 * intensity - 2.0);
        double legalProb = sigmoid(3.0 * clip(-sentimentLatest, 0.0, 1.0) + 1.8 * sentimentStd + 1.5 * intensity - 2.0);
        double productProb = sigmoid(2.4 * intensity + 1.2 * confMix + 0.8 * clip(Math.abs(sentimentLatest), 0.0, 1.0) - 1.8);
        double macroProb = sigmoid(2.0 * intensity + 1.5 * clip(news300 / 10.0, 0.0, 1.0) + 1.3 * sentimentStd - 2.0);

        double relevance = 0.25 * freshness
            + 0.20 * clip(providers / 6.0, 0.0, 1.0)
            + 0.20 * clip(coverage, 0.0, 1.0)
            + 0.20 * confMix
            + 0.15 * max5(earningsProb, analystProb, legalProb, productProb, macroProb);
        double clusterSeed = Math.floor((sentimentLatest + 1.0) * 2.0) + Math.floor(clip(providers, 0.0, 4.0));

        out.put("news_event_earnings", (float) clip(earningsProb, 0.0, 1.0));
        out.put("news_event_analyst", (float) clip(analystProb, 0.0, 1.0));
        out.put("news_event_legal", (float) clip(legalProb, 0.0, 1.0));
        out.put("news_event_earnings_beat_miss", (float) clip(earningsProb, 0.0, 1.0));
        out.put("news_event_analyst_upgrade_downgrade", (float) clip(analystProb, 0.0, 1.0));
        out.put("news_event_legal_regulatory", (float) clip(legalProb, 0.0, 1.0));
        out.put("news_event_product_capex", (float) clip(productProb, 0.0, 1.0));
        out.put("news_event_macro_spillover", (float) clip(macroProb, 0.0, 1.0));
        out.put("news_novelty_score", (float) newsNoveltyScore);
        out.put("news_relevance_score", (float) clip(relevance, 0.0, 1.0));
        out.put("news_embedding_cluster", (float) (((int) clusterSeed) % 8));
    }

    private static void appendSequenceProxy(double[] close, double[] volume, Map<String, Float> out) {
        double[] ret30s = pctChange(close, 1);
        fillNaN(ret30s, 0.0);
        double[] trend = rollingMean(ret30s, 12, 4);
        double[] vol = rollingStd(ret30s, 16, 6);
        double[] medianVolume = rollingQuantile(volume, 20, 6, 0.50);
        double[] volImpulse = new double[volume.length];
        for (int i = 0; i < volume.length; i++) {
            double median = finiteOr(medianVolume[i], 1.0);
            volImpulse[i] = (volume[i] / (median + 1.0)) - 1.0;
        }
        double[] trendPrev = shift(trend, 1, 0.0);
        double[] ret30sMean4 = rollingMean(ret30s, 4, 2);
        double[] ret30sSum8 = rollingSum(ret30s, 8, 3);

        int last = lastIndex(close.length);
        double volValue = finiteOr(vol[last], 0.0);
        double lstm = sigmoid((finiteOr(trend[last], 0.0) / (volValue + 1.0e-6)) * 0.8 + 0.3 * finiteOr(volImpulse[last], 0.0));
        double tcn = sigmoid(0.9 * finiteOr(trendPrev[last], 0.0) / (volValue + 1.0e-6) + 0.2 * ret30s[last]);
        double transformer = sigmoid(0.7 * finiteOr(trend[last], 0.0) + 0.5 * finiteOr(ret30sMean4[last], 0.0));
        double patchTst = sigmoid(0.5 * finiteOr(ret30sSum8[last], 0.0) / (volValue + 1.0e-6));
        double consensus = (lstm + tcn + transformer + patchTst) / 4.0;

        out.put("seq_lstm_up_prob_30s", (float) clip(lstm, 0.0, 1.0));
        out.put("seq_tcn_up_prob_30s", (float) clip(tcn, 0.0, 1.0));
        out.put("seq_transformer_up_prob_30s", (float) clip(transformer, 0.0, 1.0));
        out.put("seq_patchtst_up_prob_30s", (float) clip(patchTst, 0.0, 1.0));
        out.put("seq_model_consensus_up_prob_30s", (float) clip(consensus, 0.0, 1.0));
    }

    private static void appendSetupProxy(Snapshot snapshot, double[] close, double[] high, double[] low, Map<String, Float> out) {
        double[] prevHigh20 = shift(rollingMax(high, 20, 8), 1, Double.NaN);
        double[] prevLow20 = shift(rollingMin(low, 20, 8), 1, Double.NaN);
        double[] tr = trueRange(high, low, close);
        double[] atr = rollingMean(tr, 14, 5);
        double[] rangeNow = subtract(high, low);
        double[] rangeMean = rollingMean(rangeNow, 12, 4);
        double[] emaFast = ewm(close, 10);
        double[] emaSlow = ewm(close, 24);
        int last = lastIndex(close.length);

        double closeNow = snapshot.barClose();
        double openNow = snapshot.barOpen();
        double highNow = snapshot.barHigh();
        double lowNow = snapshot.barLow();
        double atrValue = finiteOr(atr[last], 0.0);
        double atrNorm = atrValue / (Math.abs(closeNow) + 1.0e-9);
        double body = Math.abs(closeNow - openNow) / (Math.abs(closeNow) + 1.0e-9);
        double upperWick = (highNow - Math.max(openNow, closeNow)) / (Math.abs(closeNow) + 1.0e-9);
        double lowerWick = (Math.min(openNow, closeNow) - lowNow) / (Math.abs(closeNow) + 1.0e-9);
        double expansion = clip((rangeNow[last] / (finiteOr(rangeMean[last], 0.0) + 1.0e-9)) - 1.0, -2.0, 4.0);
        double trendDir = Math.tanh((emaFast[last] - emaSlow[last]) / (atrValue + 1.0e-9));

        double breakoutRaw = finiteOr((closeNow - prevHigh20[last]) / (atrValue + 1.0e-9), 0.0)
            + 0.7 * expansion
            + 0.6 * body
            + 0.4 * clip(trendDir, 0.0, 1.0);
        double breakout = sigmoid(2.2 * breakoutRaw - 0.8);

        double pullbackDist = finiteOr((emaFast[last] - lowNow) / (atrValue + 1.0e-9), 0.0);
        double continuationRaw = 1.1 * clip(trendDir, 0.0, 1.0)
            + 0.8 * clip(pullbackDist, 0.0, 2.0)
            + 0.6 * clip((closeNow - openNow) / (atrValue + 1.0e-9), -2.0, 2.0);
        double continuation = sigmoid(continuationRaw - 0.6);

        double reversalRaw = 0.9 * (upperWick + lowerWick)
            + 0.8 * Math.abs(clip(finiteOr((closeNow - prevLow20[last]) / (atrValue + 1.0e-9), 0.0), -2.0, 2.0))
            - 0.5 * body;
        double reversal = sigmoid(2.0 * reversalRaw - 1.0);

        double extension = Math.abs(closeNow - emaSlow[last]) / (atrValue + 1.0e-9);
        double exhaustionRaw = 0.9 * clip(extension - 1.6, 0.0, 4.0) + 0.8 * (upperWick + lowerWick) + 0.6 * atrNorm;
        double exhaustion = sigmoid(1.7 * exhaustionRaw - 0.7);

        double prevHigh = finiteOr(prevHigh20[last], closeNow);
        double prevLow = finiteOr(prevLow20[last], closeNow);
        double fellBackInside = (closeNow < prevHigh && closeNow > prevLow) ? 1.0 : 0.0;
        double failedRaw = 1.2 * breakout + 0.8 * fellBackInside + 0.6 * reversal - 0.7 * clip(trendDir, 0.0, 1.0);
        double failed = sigmoid(failedRaw - 1.0);

        out.put("setup_breakout_prob", (float) clip(breakout, 0.0, 1.0));
        out.put("setup_pullback_continuation_prob", (float) clip(continuation, 0.0, 1.0));
        out.put("setup_reversal_prob", (float) clip(reversal, 0.0, 1.0));
        out.put("setup_trend_exhaustion_prob", (float) clip(exhaustion, 0.0, 1.0));
        out.put("setup_failed_breakout_prob", (float) clip(failed, 0.0, 1.0));
    }

    private static double[] trueRange(double[] high, double[] low, double[] close) {
        double[] out = new double[close.length];
        for (int i = 0; i < close.length; i++) {
            double prevClose = i > 0 ? close[i - 1] : close[i];
            out[i] = Math.max(high[i] - low[i], Math.max(Math.abs(high[i] - prevClose), Math.abs(low[i] - prevClose)));
        }
        return out;
    }

    private static double[] pctChange(double[] values, int periods) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            if (i < periods || periods <= 0 || values[i - periods] == 0.0) {
                out[i] = Double.NaN;
            } else {
                out[i] = (values[i] - values[i - periods]) / values[i - periods];
            }
        }
        return out;
    }

    private static double[] diff(double[] values, int periods) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = i < periods ? Double.NaN : values[i] - values[i - periods];
        }
        return out;
    }

    private static double[] abs(double[] values) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = Math.abs(values[i]);
        }
        return out;
    }

    private static double[] subtract(double[] left, double[] right) {
        double[] out = new double[left.length];
        for (int i = 0; i < left.length; i++) {
            out[i] = left[i] - right[i];
        }
        return out;
    }

    private static double[] shift(double[] values, int periods, double fill) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = i < periods ? fill : values[i - periods];
        }
        return out;
    }

    private static double[] ewm(double[] values, int span) {
        double[] out = new double[values.length];
        if (values.length == 0) {
            return out;
        }
        double alpha = 2.0 / (span + 1.0);
        out[0] = values[0];
        for (int i = 1; i < values.length; i++) {
            out[i] = (values[i] - out[i - 1]) * alpha + out[i - 1];
        }
        return out;
    }

    private static double[] rollingMean(double[] values, int window, int minPeriods) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            Stats stats = tailStats(values, i, window);
            out[i] = stats.count() >= minPeriods ? stats.mean() : Double.NaN;
        }
        return out;
    }

    private static double[] rollingStd(double[] values, int window, int minPeriods) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            Stats stats = tailStats(values, i, window);
            out[i] = stats.count() >= Math.max(minPeriods, 2) ? stats.sampleStd() : Double.NaN;
        }
        return out;
    }

    private static double[] rollingSum(double[] values, int window, int minPeriods) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            Stats stats = tailStats(values, i, window);
            out[i] = stats.count() >= minPeriods ? stats.sum() : Double.NaN;
        }
        return out;
    }

    private static double[] rollingMax(double[] values, int window, int minPeriods) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            WindowValues stats = tailWindow(values, i, window);
            out[i] = stats.values().size() >= minPeriods ? stats.max() : Double.NaN;
        }
        return out;
    }

    private static double[] rollingMin(double[] values, int window, int minPeriods) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            WindowValues stats = tailWindow(values, i, window);
            out[i] = stats.values().size() >= minPeriods ? stats.min() : Double.NaN;
        }
        return out;
    }

    private static double[] rollingQuantile(double[] values, int window, int minPeriods, double quantile) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            WindowValues stats = tailWindow(values, i, window);
            out[i] = stats.values().size() >= minPeriods ? stats.quantile(quantile) : Double.NaN;
        }
        return out;
    }

    private static double[] rollingPositiveFraction(double[] values, int window, int minPeriods) {
        double[] out = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            WindowValues stats = tailWindow(values, i, window);
            if (stats.values().size() >= minPeriods) {
                int positiveCount = 0;
                for (double value : stats.values()) {
                    if (value > 0.0) {
                        positiveCount++;
                    }
                }
                out[i] = positiveCount / (double) stats.values().size();
            } else {
                out[i] = Double.NaN;
            }
        }
        return out;
    }

    private static Stats tailStats(double[] values, int endInclusive, int window) {
        List<Double> valid = tailWindow(values, endInclusive, window).values();
        double sum = 0.0;
        double sumSq = 0.0;
        for (double value : valid) {
            sum += value;
            sumSq += value * value;
        }
        return new Stats(valid.size(), sum, sumSq);
    }

    private static WindowValues tailWindow(double[] values, int endInclusive, int window) {
        int start = Math.max(0, endInclusive - window + 1);
        List<Double> valid = new ArrayList<>();
        for (int i = start; i <= endInclusive; i++) {
            double value = values[i];
            if (Double.isFinite(value)) {
                valid.add(value);
            }
        }
        return new WindowValues(valid);
    }

    private static void fillNaN(double[] values, double replacement) {
        for (int i = 0; i < values.length; i++) {
            if (!Double.isFinite(values[i])) {
                values[i] = replacement;
            }
        }
    }

    private static double[] softmax3(double a, double b, double c) {
        double max = max3(a, b, c);
        double ea = Math.exp(a - max);
        double eb = Math.exp(b - max);
        double ec = Math.exp(c - max);
        double denom = ea + eb + ec + 1.0e-9;
        return new double[] {ea / denom, eb / denom, ec / denom};
    }

    private static double sigmoid(double x) {
        return 1.0 / (1.0 + Math.exp(-clip(x, -40.0, 40.0)));
    }

    private static double clip(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private static double max3(double a, double b, double c) {
        return Math.max(a, Math.max(b, c));
    }

    private static double max5(double a, double b, double c, double d, double e) {
        return Math.max(max3(a, b, c), Math.max(d, e));
    }

    private static double finiteOr(double value, double fallback) {
        return Double.isFinite(value) ? value : fallback;
    }

    private static int lastIndex(int length) {
        return Math.max(0, length - 1);
    }

    private static double lastFinite(double[] values, double fallback) {
        if (values.length == 0) {
            return fallback;
        }
        return finiteOr(values[values.length - 1], fallback);
    }

    private static double[] toArray(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return new double[] {0.0};
        }
        double[] out = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            out[i] = values.get(i) == null ? 0.0 : values.get(i);
        }
        return out;
    }

    private record Stats(int count, double sum, double sumSq) {
        double mean() {
            return count > 0 ? (sum / count) : Double.NaN;
        }

        double sampleStd() {
            if (count < 2) {
                return Double.NaN;
            }
            double mean = mean();
            double variance = Math.max(0.0, (sumSq - (count * mean * mean)) / (count - 1.0));
            return Math.sqrt(variance);
        }
    }

    private record WindowValues(List<Double> values) {
        double max() {
            return values.stream().mapToDouble(Double::doubleValue).max().orElse(Double.NaN);
        }

        double min() {
            return values.stream().mapToDouble(Double::doubleValue).min().orElse(Double.NaN);
        }

        double quantile(double q) {
            if (values.isEmpty()) {
                return Double.NaN;
            }
            List<Double> sorted = new ArrayList<>(values);
            sorted.sort(Double::compareTo);
            if (sorted.size() == 1) {
                return sorted.getFirst();
            }
            double position = clip(q, 0.0, 1.0) * (sorted.size() - 1);
            int lowerIndex = (int) Math.floor(position);
            int upperIndex = (int) Math.ceil(position);
            double lower = sorted.get(lowerIndex);
            double upper = sorted.get(upperIndex);
            return lower + ((position - lowerIndex) * (upper - lower));
        }
    }
}


