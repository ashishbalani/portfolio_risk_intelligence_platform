package com.portfolio.risk.processingstreams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "streams")
public class StreamsProps {
    private String bootstrapServers = "localhost:9092";
    private String applicationId = "processing-service-streams";
    private String baseCurrency = "USD";
    private boolean exactlyOnce = true;
    private int numStreamThreads = 1;
    private String stateDir = "./build/kstreams";
    private long cacheMaxBytesBuffering = 10_485_760L;
    private int commitIntervalMs = 1000;
    private String defaultKeySerde = "org.apache.kafka.common.serialization.Serdes$StringSerde";
    private String defaultValueSerde = "org.apache.kafka.common.serialization.Serdes$StringSerde";
    private boolean replaceThreadOnException = true;
    private boolean suppressOutputs = true;

    private Topics topics = new Topics();

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getBaseCurrency() {
        return baseCurrency;
    }

    public void setBaseCurrency(String baseCurrency) {
        this.baseCurrency = baseCurrency;
    }

    public boolean isExactlyOnce() {
        return exactlyOnce;
    }

    public void setExactlyOnce(boolean exactlyOnce) {
        this.exactlyOnce = exactlyOnce;
    }

    public int getNumStreamThreads() {
        return numStreamThreads;
    }

    public void setNumStreamThreads(int numStreamThreads) {
        this.numStreamThreads = numStreamThreads;
    }

    public String getStateDir() {
        return stateDir;
    }

    public void setStateDir(String stateDir) {
        this.stateDir = stateDir;
    }

    public long getCacheMaxBytesBuffering() {
        return cacheMaxBytesBuffering;
    }

    public void setCacheMaxBytesBuffering(long cacheMaxBytesBuffering) {
        this.cacheMaxBytesBuffering = cacheMaxBytesBuffering;
    }

    public int getCommitIntervalMs() {
        return commitIntervalMs;
    }

    public void setCommitIntervalMs(int commitIntervalMs) {
        this.commitIntervalMs = commitIntervalMs;
    }

    public String getDefaultKeySerde() {
        return defaultKeySerde;
    }

    public void setDefaultKeySerde(String defaultKeySerde) {
        this.defaultKeySerde = defaultKeySerde;
    }

    public String getDefaultValueSerde() {
        return defaultValueSerde;
    }

    public void setDefaultValueSerde(String defaultValueSerde) {
        this.defaultValueSerde = defaultValueSerde;
    }

    public boolean isReplaceThreadOnException() {
        return replaceThreadOnException;
    }

    public void setReplaceThreadOnException(boolean replaceThreadOnException) {
        this.replaceThreadOnException = replaceThreadOnException;
    }

    public boolean isSuppressOutputs() {
        return suppressOutputs;
    }

    public void setSuppressOutputs(boolean suppressOutputs) {
        this.suppressOutputs = suppressOutputs;
    }

    public Topics getTopics() {
        return topics;
    }

    public void setTopics(Topics topics) {
        this.topics = topics;
    }

    public static class Topics {
        private String trades = "trades.v1";
        private String refdata = "refdata.v1";
        private String fx = "fx.v1";
        private String limits = "limits.v1";
        private String positionsCurrent = "positions-current.v1";
        private String exposuresCurrent = "exposures-current.v1";
        private String riskSignals = "risk-signals.v1";
        private String riskSignalsCurrent = "risk-signals-current.v1";
        private String tradesDlq = "trades.v1.dlq";

        public String getTrades() {
            return trades;
        }

        public void setTrades(String trades) {
            this.trades = trades;
        }

        public String getRefdata() {
            return refdata;
        }

        public void setRefdata(String refdata) {
            this.refdata = refdata;
        }

        public String getFx() {
            return fx;
        }

        public void setFx(String fx) {
            this.fx = fx;
        }

        public String getLimits() {
            return limits;
        }

        public void setLimits(String limits) {
            this.limits = limits;
        }

        public String getPositionsCurrent() {
            return positionsCurrent;
        }

        public void setPositionsCurrent(String positionsCurrent) {
            this.positionsCurrent = positionsCurrent;
        }

        public String getExposuresCurrent() {
            return exposuresCurrent;
        }

        public void setExposuresCurrent(String exposuresCurrent) {
            this.exposuresCurrent = exposuresCurrent;
        }

        public String getRiskSignals() {
            return riskSignals;
        }

        public void setRiskSignals(String riskSignals) {
            this.riskSignals = riskSignals;
        }

        public String getRiskSignalsCurrent() {
            return riskSignalsCurrent;
        }

        public void setRiskSignalsCurrent(String riskSignalsCurrent) {
            this.riskSignalsCurrent = riskSignalsCurrent;
        }

        public String getTradesDlq() {
            return tradesDlq;
        }

        public void setTradesDlq(String tradesDlq) {
            this.tradesDlq = tradesDlq;
        }
    }
}
