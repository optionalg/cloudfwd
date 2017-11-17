package com.splunk.cloudfwd.metrics;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.splunk.cloudfwd.impl.ConnectionImpl;

/**
 * Created by eprokop on 11/16/17.
 */
public class ConnectionDetailMetric extends Metric {
    private long numChannels;
    private long maxTotalChannels_config;
    private long channelsPerDest_config;
    private long batchSizeChars_config;

    public ConnectionDetailMetric(ConnectionImpl c) {
        super(c);
        this.maxTotalChannels_config = c.getSettings().getMaxTotalChannels();
        this.channelsPerDest_config = c.getSettings().getChannelsPerDestination();
        this.batchSizeChars_config = c.getSettings().getEventBatchSize();
        this.numChannels = c.getNumChannels();
    }

    @Override
    protected String setType() {
        return "connection_detail";
    }

    public long getNumChannels() {
        return numChannels;
    }

    public void setNumChannels(long numChannels) {
        this.numChannels = numChannels;
    }

    public long getMaxTotalChannels_config() {
        return maxTotalChannels_config;
    }

    public void setMaxTotalChannels_config(long maxTotalChannels_config) {
        this.maxTotalChannels_config = maxTotalChannels_config;
    }

    public long getChannelsPerDest_config() {
        return channelsPerDest_config;
    }

    public void setChannelsPerDest_config(long channelsPerDest_config) {
        this.channelsPerDest_config = channelsPerDest_config;
    }

    public long getBatchSizeChars_config() {
        return batchSizeChars_config;
    }

    public void setBatchSizeChars_config(long batchSizeChars_config) {
        this.batchSizeChars_config = batchSizeChars_config;
    }

    // Override these so we don't include them when mapping to JSON
    @Override
    @JsonIgnore
    public String getMetricSinkUrl() {
        return "";
    }

    @Override
    @JsonIgnore
    public String getMetricSinkToken() {
        return "";
    }

    @Override
    @JsonIgnore
    public String getRunId() {
        return "";
    }
}
