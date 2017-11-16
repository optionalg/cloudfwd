package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.impl.ConnectionImpl;

/**
 * Created by eprokop on 11/16/17.
 */
public class ConnectionDetailMetric extends Metric {
    private long ackedBatches;
    private long sentBatches;
    private long numChannels;
    private long maxTotalChannels_config;
    private long channelsPerDest_config;

    public ConnectionDetailMetric(ConnectionImpl c) {
        super(c);
        this.maxTotalChannels_config = c.getSettings().getMaxTotalChannels();
        this.channelsPerDest_config = c.getSettings().getChannelsPerDestination();
        this.numChannels = c.getNumChannels();
    }

    @Override
    protected String setType() {
        return "connection_detail";
    }

    public long getSentBatches() {
        return sentBatches;
    }

    public void setSentBatches(long sentBatches) {
        this.sentBatches = sentBatches;
    }

    public long getNumChannels() {
        return numChannels;
    }

    public void setNumChannels(long numChannels) {
        this.numChannels = numChannels;
    }
    
    public long getAckedBatches() {
        return ackedBatches;
    }

    public void setAckedBatches(long ackedBatches) {
        this.ackedBatches = ackedBatches;
    }
}
